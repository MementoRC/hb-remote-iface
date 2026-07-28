"""MQTTCommands: registers the 8 RPC command handlers via NodeContext.

Each command handler calls the corresponding HummingbotAppProtocol method and
returns a typed response dataclass. Errors become MQTTStatusCode.ERROR responses;
timeouts become MQTTStatusCode.ERROR with msg="timeout".

Handlers are invoked from commlib's ThreadPoolExecutor (a non-asyncio thread).
Async app methods are dispatched to the app's event loop via
asyncio.run_coroutine_threadsafe + concurrent.futures.Future.result().
"""

from __future__ import annotations

import asyncio
import concurrent.futures
from typing import TYPE_CHECKING, Any

from remote_iface.hb_compat.logging_compat import get_logger
from remote_iface.protocols.messages import (
    BalanceLimitCommandMessage,
    BalancePaperCommandMessage,
    ConfigCommandMessage,
    HistoryCommandMessage,
    ImportCommandMessage,
    MQTTStatusCode,
    StartCommandMessage,
    StatusCommandMessage,
    StopCommandMessage,
)

if TYPE_CHECKING:
    from remote_iface._commlib.wrappers.rpc_service import RPCService
    from remote_iface.gateway.gateway import MQTTGateway
    from remote_iface.protocols.app import HummingbotAppProtocol

_logger = get_logger("remote_iface.MQTTCommands")

# Topic suffix constants (match upstream TopicSpecs.COMMANDS).
_CMD_START = "/start"
_CMD_STOP = "/stop"
_CMD_CONFIG = "/config"
_CMD_IMPORT = "/import"
_CMD_STATUS = "/status"
_CMD_HISTORY = "/history"
_CMD_BALANCE_LIMIT = "/balance/limit"
_CMD_BALANCE_PAPER = "/balance/paper"


class MQTTCommands:
    """Gateway component that registers 8 RPC handlers for inbound MQTT commands.

    Each handler runs the corresponding async app method inside
    asyncio.wait_for(..., timeout=_timeout). Exceptions map to
    ERROR status codes; timeouts map to ERROR with msg="timeout".
    """

    def __init__(self, app: HummingbotAppProtocol) -> None:
        self._app: HummingbotAppProtocol = app
        self._services: list[RPCService] = []
        self._timeout: float = 30.0  # overridden from gateway config at start()

    # ------------------------------------------------------------------
    # Component protocol
    # ------------------------------------------------------------------

    def start(self, gateway: MQTTGateway) -> None:
        """Create 8 RPC services on gateway._node_context and register their endpoints."""
        self._timeout = gateway._config.command_timeout  # noqa: SLF001
        nc = gateway._node_context  # noqa: SLF001
        prefix = gateway._config.namespace  # noqa: SLF001

        specs: list[tuple[str, type, Any]] = [
            (f"{prefix}{_CMD_START}", StartCommandMessage.Request, self._handle_start),
            (f"{prefix}{_CMD_STOP}", StopCommandMessage.Request, self._handle_stop),
            (f"{prefix}{_CMD_CONFIG}", ConfigCommandMessage.Request, self._handle_config),
            (f"{prefix}{_CMD_IMPORT}", ImportCommandMessage.Request, self._handle_import),
            (f"{prefix}{_CMD_STATUS}", StatusCommandMessage.Request, self._handle_status),
            (f"{prefix}{_CMD_HISTORY}", HistoryCommandMessage.Request, self._handle_history),
            (
                f"{prefix}{_CMD_BALANCE_LIMIT}",
                BalanceLimitCommandMessage.Request,
                self._handle_balance_limit,
            ),
            (
                f"{prefix}{_CMD_BALANCE_PAPER}",
                BalancePaperCommandMessage.Request,
                self._handle_balance_paper,
            ),
        ]

        for rpc_name, msg_type, handler in specs:
            svc = nc.create_rpc(rpc_name=rpc_name, msg_type=msg_type, on_request=handler)
            self._services.append(svc)
            gateway._endpoints.append(svc)  # noqa: SLF001

    def stop(self, gateway: MQTTGateway) -> None:
        """Stop each owned RPC service and remove from gateway._endpoints."""
        for svc in self._services:
            try:
                svc.stop()
            except Exception as exc:  # noqa: BLE001
                _logger.warning(
                    "MQTTCommands: RPC service %r raised during stop: %s",
                    svc.rpc_name,
                    exc,
                )
            if svc in gateway._endpoints:  # noqa: SLF001
                gateway._endpoints.remove(svc)  # noqa: SLF001
        self._services.clear()

    # ------------------------------------------------------------------
    # Internal: run a coroutine on the app event loop from a thread
    # ------------------------------------------------------------------

    def _call_async(self, coro: Any) -> Any:
        """Dispatch *coro* to the app's event loop and block until completion.

        Returns the result on success. Raises the exception on failure.
        TimeoutError is re-raised as-is so callers can map it to TIMEOUT status.
        """
        loop = self._app.ev_loop
        future = asyncio.run_coroutine_threadsafe(
            asyncio.wait_for(coro, timeout=self._timeout), loop
        )
        try:
            return future.result(timeout=self._timeout + 5.0)
        except concurrent.futures.TimeoutError as exc:
            raise TimeoutError() from exc

    # ------------------------------------------------------------------
    # Command handlers
    # ------------------------------------------------------------------

    def _handle_start(self, req: StartCommandMessage.Request) -> StartCommandMessage.Response:
        response = StartCommandMessage.Response()
        try:
            if req.async_backend:
                self._app.start(
                    log_level=req.log_level or "",
                    script=req.script or "",
                    conf=req.conf or "",
                    is_quickstart=req.is_quickstart,
                )
            else:
                result = self._call_async(
                    self._app.start_check(
                        log_level=req.log_level or "",
                        script=req.script or "",
                        conf=req.conf or "",
                        is_quickstart=req.is_quickstart,
                    )
                )
                response.msg = result or ""
            response.status = MQTTStatusCode.SUCCESS
        except TimeoutError:
            response.status = MQTTStatusCode.ERROR
            response.msg = "timeout"
        except Exception as exc:  # noqa: BLE001
            response.status = MQTTStatusCode.ERROR
            response.msg = str(exc)
        return response

    def _handle_stop(self, req: StopCommandMessage.Request) -> StopCommandMessage.Response:
        response = StopCommandMessage.Response()
        try:
            if req.async_backend:
                self._app.stop(skip_order_cancellation=req.skip_order_cancellation)
            else:
                result = self._call_async(self._app.stop_loop())
                response.msg = result or ""
            response.status = MQTTStatusCode.SUCCESS
        except TimeoutError:
            response.status = MQTTStatusCode.ERROR
            response.msg = "timeout"
        except Exception as exc:  # noqa: BLE001
            response.status = MQTTStatusCode.ERROR
            response.msg = str(exc)
        return response

    def _handle_config(self, req: ConfigCommandMessage.Request) -> ConfigCommandMessage.Response:
        response = ConfigCommandMessage.Response()
        try:
            if len(req.params) == 0:
                self._app.config()
            else:
                invalid_params = []
                configurable = set(self._app.configurable_keys())
                for key, value in req.params:
                    if key in configurable:
                        self._app.config(key, value)
                        response.changes.append((key, value))
                    else:
                        invalid_params.append(key)
                if invalid_params:
                    raise ValueError(f"Invalid param key(s): {invalid_params}")
            # Build current config snapshot.
            client_cfg: dict[str, Any] = {}
            strategy_cfg: dict[str, Any] = {}
            cfg_map = self._app.client_config_map
            if hasattr(cfg_map, "dict"):
                client_cfg = cfg_map.dict()  # type: ignore[union-attr]
            elif isinstance(cfg_map, dict):
                client_cfg = cfg_map
            strat_map = self._app.strategy_config_map
            if hasattr(strat_map, "dict"):
                strategy_cfg = strat_map.dict()  # type: ignore[union-attr]
            elif isinstance(strat_map, dict):
                strategy_cfg = strat_map
            response.config = {"client": client_cfg, "strategy": strategy_cfg}
            response.status = MQTTStatusCode.SUCCESS
        except Exception as exc:  # noqa: BLE001
            response.status = MQTTStatusCode.ERROR
            response.msg = str(exc)
        return response

    def _handle_import(self, req: ImportCommandMessage.Request) -> ImportCommandMessage.Response:
        response = ImportCommandMessage.Response()
        try:
            if not req.strategy:
                response.status = MQTTStatusCode.ERROR
                response.msg = "Empty strategy_name given!"
                return response
            result = self._call_async(self._app.import_config_file(f"{req.strategy}.yml"))
            response.msg = result or ""
            response.status = MQTTStatusCode.SUCCESS
        except TimeoutError:
            response.status = MQTTStatusCode.ERROR
            response.msg = "timeout"
        except Exception as exc:  # noqa: BLE001
            response.status = MQTTStatusCode.ERROR
            response.msg = str(exc)
        return response

    def _handle_status(self, req: StatusCommandMessage.Request) -> StatusCommandMessage.Response:
        response = StatusCommandMessage.Response()
        try:
            if not req.async_backend:
                result = self._call_async(self._app.strategy_status())
                response.msg = result or ""
            response.status = MQTTStatusCode.SUCCESS
        except TimeoutError:
            response.status = MQTTStatusCode.ERROR
            response.msg = "timeout"
        except Exception as exc:  # noqa: BLE001
            response.status = MQTTStatusCode.ERROR
            response.msg = str(exc)
        return response

    def _handle_history(self, req: HistoryCommandMessage.Request) -> HistoryCommandMessage.Response:
        response = HistoryCommandMessage.Response()
        try:
            if req.async_backend:
                self._app.history(req.days, req.verbose, req.precision)
            else:
                result = self._call_async(self._app.get_history_trades_json(req.days))
                response.trades = result or []
            response.status = MQTTStatusCode.SUCCESS
        except TimeoutError:
            response.status = MQTTStatusCode.ERROR
            response.msg = "timeout"
        except Exception as exc:  # noqa: BLE001
            response.status = MQTTStatusCode.ERROR
            response.msg = str(exc)
        return response

    def _handle_balance_limit(
        self, req: BalanceLimitCommandMessage.Request
    ) -> BalanceLimitCommandMessage.Response:
        response = BalanceLimitCommandMessage.Response()
        try:
            data = self._app.balance("limit", [req.exchange, req.asset, str(req.amount)])
            response.data = str(data) if data is not None else ""
            response.status = MQTTStatusCode.SUCCESS
        except Exception as exc:  # noqa: BLE001
            response.status = MQTTStatusCode.ERROR
            response.msg = str(exc)
        return response

    def _handle_balance_paper(
        self, req: BalancePaperCommandMessage.Request
    ) -> BalancePaperCommandMessage.Response:
        response = BalancePaperCommandMessage.Response()
        try:
            data = self._app.balance("paper", [req.asset, str(req.amount)])
            response.data = str(data) if data is not None else ""
            response.status = MQTTStatusCode.SUCCESS
        except Exception as exc:  # noqa: BLE001
            response.status = MQTTStatusCode.ERROR
            response.msg = str(exc)
        return response

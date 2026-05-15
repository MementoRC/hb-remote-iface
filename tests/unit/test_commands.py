"""Unit tests for MQTTCommands: 8 handlers × (happy, error, timeout) + lifecycle.

Handlers run synchronously from a worker thread (simulating commlib dispatch).
Tests that need async coroutines spin up a dedicated event loop in a background
thread so asyncio.run_coroutine_threadsafe works correctly.
"""

from __future__ import annotations

import asyncio
import threading
from unittest.mock import AsyncMock, MagicMock

import pytest

from remote_iface.gateway.commands import MQTTCommands
from remote_iface.protocols.app import HummingbotAppProtocol
from remote_iface.protocols.config import GatewayConfig
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

pytestmark = pytest.mark.unit


# ---------------------------------------------------------------------------
# Helpers: provide a live event loop running in a background thread
# ---------------------------------------------------------------------------


class _LoopThread:
    """Runs an event loop in a daemon thread; stopped with close()."""

    def __init__(self) -> None:
        self.loop = asyncio.new_event_loop()
        self._thread = threading.Thread(target=self._run, daemon=True)
        self._ready = threading.Event()
        self._thread.start()
        self._ready.wait(timeout=2.0)

    def _run(self) -> None:
        asyncio.set_event_loop(self.loop)
        self._ready.set()
        self.loop.run_forever()

    def close(self) -> None:
        self.loop.call_soon_threadsafe(self.loop.stop)
        self._thread.join(timeout=3.0)
        self.loop.close()


def _make_app(loop: asyncio.AbstractEventLoop) -> MagicMock:
    app = MagicMock(spec=HummingbotAppProtocol)
    app.ev_loop = loop
    app.instance_id = "test-bot"
    app.configurable_keys.return_value = ["log_level", "paper_trade"]
    app.client_config_map = {"log_level": "INFO"}
    app.strategy_config_map = {}
    return app


def _make_commands_with_gateway(
    app: MagicMock, timeout: float = 5.0
) -> tuple[MQTTCommands, MagicMock]:
    """Return (MQTTCommands, mock gateway) with start() already called."""
    commands = MQTTCommands(app=app)
    gateway = MagicMock()
    gateway._config = GatewayConfig(command_timeout=timeout)
    gateway._node_context = MagicMock()
    gateway._endpoints = []

    def _create_rpc(rpc_name: str, msg_type: type, on_request: object) -> MagicMock:
        svc = MagicMock()
        svc.rpc_name = rpc_name
        svc.stop = MagicMock()
        return svc

    gateway._node_context.create_rpc.side_effect = _create_rpc
    commands.start(gateway)
    return commands, gateway


def _run_in_thread(fn: object, *args: object) -> object:
    """Call fn(*args) in a new thread, return its result (re-raise exceptions)."""
    result: list[object] = []
    exc_holder: list[BaseException] = []

    def _body() -> None:
        try:
            result.append(fn(*args))  # type: ignore[operator]
        except BaseException as e:  # noqa: BLE001
            exc_holder.append(e)

    t = threading.Thread(target=_body)
    t.start()
    t.join(timeout=10.0)
    if exc_holder:
        raise exc_holder[0]
    return result[0] if result else None


# ---------------------------------------------------------------------------
# Lifecycle
# ---------------------------------------------------------------------------


def test_start_registers_eight_endpoints() -> None:
    lt = _LoopThread()
    try:
        app = _make_app(lt.loop)
        _, gateway = _make_commands_with_gateway(app)
        assert len(gateway._endpoints) == 8
    finally:
        lt.close()


def test_stop_removes_endpoints() -> None:
    lt = _LoopThread()
    try:
        app = _make_app(lt.loop)
        commands, gateway = _make_commands_with_gateway(app)
        commands.stop(gateway)
        assert gateway._endpoints == []
    finally:
        lt.close()


def test_stop_calls_stop_on_each_service() -> None:
    lt = _LoopThread()
    try:
        app = _make_app(lt.loop)
        commands, gateway = _make_commands_with_gateway(app)
        services_before = list(commands._services)
        commands.stop(gateway)
        for svc in services_before:
            svc.stop.assert_called_once()
    finally:
        lt.close()


# ---------------------------------------------------------------------------
# _call_async helper
# ---------------------------------------------------------------------------


def test_call_async_returns_result() -> None:
    lt = _LoopThread()
    try:
        app = _make_app(lt.loop)
        commands = MQTTCommands(app=app)
        commands._timeout = 5.0

        async def _coro() -> str:
            return "ok"

        result = _run_in_thread(commands._call_async, _coro())
        assert result == "ok"
    finally:
        lt.close()


def test_call_async_raises_timeout() -> None:
    lt = _LoopThread()
    try:
        app = _make_app(lt.loop)
        commands = MQTTCommands(app=app)
        commands._timeout = 0.05

        async def _slow_coro() -> None:
            await asyncio.sleep(60)

        with pytest.raises(asyncio.TimeoutError):
            _run_in_thread(commands._call_async, _slow_coro())
    finally:
        lt.close()


# ---------------------------------------------------------------------------
# start command
# ---------------------------------------------------------------------------


def test_start_happy_async_backend() -> None:
    lt = _LoopThread()
    try:
        app = _make_app(lt.loop)
        commands = MQTTCommands(app=app)
        commands._timeout = 5.0
        req = StartCommandMessage.Request(async_backend=True)
        resp = _run_in_thread(commands._handle_start, req)
        app.start.assert_called_once()
        assert resp.status == MQTTStatusCode.SUCCESS  # type: ignore[union-attr]
    finally:
        lt.close()


def test_start_happy_sync_backend() -> None:
    lt = _LoopThread()
    try:
        app = _make_app(lt.loop)
        app.start_check = AsyncMock(return_value="started")
        commands = MQTTCommands(app=app)
        commands._timeout = 5.0
        req = StartCommandMessage.Request(async_backend=False)
        resp = _run_in_thread(commands._handle_start, req)
        assert resp.status == MQTTStatusCode.SUCCESS  # type: ignore[union-attr]
        assert resp.msg == "started"  # type: ignore[union-attr]
    finally:
        lt.close()


def test_start_error_response() -> None:
    lt = _LoopThread()
    try:
        app = _make_app(lt.loop)
        app.start.side_effect = RuntimeError("already running")
        commands = MQTTCommands(app=app)
        commands._timeout = 5.0
        req = StartCommandMessage.Request(async_backend=True)
        resp = _run_in_thread(commands._handle_start, req)
        assert resp.status == MQTTStatusCode.ERROR  # type: ignore[union-attr]
        assert "already running" in resp.msg  # type: ignore[union-attr]
    finally:
        lt.close()


def test_start_timeout_response() -> None:
    lt = _LoopThread()
    try:
        app = _make_app(lt.loop)

        async def _hang(*_: object, **__: object) -> str:
            await asyncio.sleep(60)
            return ""

        app.start_check = _hang
        commands = MQTTCommands(app=app)
        commands._timeout = 0.05
        req = StartCommandMessage.Request(async_backend=False)
        resp = _run_in_thread(commands._handle_start, req)
        assert resp.status == MQTTStatusCode.ERROR  # type: ignore[union-attr]
        assert resp.msg == "timeout"  # type: ignore[union-attr]
    finally:
        lt.close()


# ---------------------------------------------------------------------------
# stop command
# ---------------------------------------------------------------------------


def test_stop_happy_async_backend() -> None:
    lt = _LoopThread()
    try:
        app = _make_app(lt.loop)
        commands = MQTTCommands(app=app)
        commands._timeout = 5.0
        req = StopCommandMessage.Request(async_backend=True)
        resp = _run_in_thread(commands._handle_stop, req)
        app.stop.assert_called_once()
        assert resp.status == MQTTStatusCode.SUCCESS  # type: ignore[union-attr]
    finally:
        lt.close()


def test_stop_happy_sync_backend() -> None:
    lt = _LoopThread()
    try:
        app = _make_app(lt.loop)
        app.stop_loop = AsyncMock(return_value="stopped")
        commands = MQTTCommands(app=app)
        commands._timeout = 5.0
        req = StopCommandMessage.Request(async_backend=False)
        resp = _run_in_thread(commands._handle_stop, req)
        assert resp.status == MQTTStatusCode.SUCCESS  # type: ignore[union-attr]
    finally:
        lt.close()


def test_stop_error_response() -> None:
    lt = _LoopThread()
    try:
        app = _make_app(lt.loop)
        app.stop.side_effect = RuntimeError("not running")
        commands = MQTTCommands(app=app)
        commands._timeout = 5.0
        req = StopCommandMessage.Request(async_backend=True)
        resp = _run_in_thread(commands._handle_stop, req)
        assert resp.status == MQTTStatusCode.ERROR  # type: ignore[union-attr]
        assert "not running" in resp.msg  # type: ignore[union-attr]
    finally:
        lt.close()


def test_stop_timeout_response() -> None:
    lt = _LoopThread()
    try:
        app = _make_app(lt.loop)

        async def _hang() -> str:
            await asyncio.sleep(60)
            return ""

        app.stop_loop = _hang
        commands = MQTTCommands(app=app)
        commands._timeout = 0.05
        req = StopCommandMessage.Request(async_backend=False)
        resp = _run_in_thread(commands._handle_stop, req)
        assert resp.status == MQTTStatusCode.ERROR  # type: ignore[union-attr]
        assert resp.msg == "timeout"  # type: ignore[union-attr]
    finally:
        lt.close()


# ---------------------------------------------------------------------------
# config command
# ---------------------------------------------------------------------------


def test_config_happy_no_params() -> None:
    lt = _LoopThread()
    try:
        app = _make_app(lt.loop)
        commands = MQTTCommands(app=app)
        commands._timeout = 5.0
        req = ConfigCommandMessage.Request(params=[])
        resp = _run_in_thread(commands._handle_config, req)
        app.config.assert_called_once_with()
        assert resp.status == MQTTStatusCode.SUCCESS  # type: ignore[union-attr]
    finally:
        lt.close()


def test_config_happy_valid_params() -> None:
    lt = _LoopThread()
    try:
        app = _make_app(lt.loop)
        commands = MQTTCommands(app=app)
        commands._timeout = 5.0
        req = ConfigCommandMessage.Request(params=[("log_level", "DEBUG")])
        resp = _run_in_thread(commands._handle_config, req)
        assert resp.status == MQTTStatusCode.SUCCESS  # type: ignore[union-attr]
        assert ("log_level", "DEBUG") in resp.changes  # type: ignore[union-attr]
    finally:
        lt.close()


def test_config_error_invalid_key() -> None:
    lt = _LoopThread()
    try:
        app = _make_app(lt.loop)
        commands = MQTTCommands(app=app)
        commands._timeout = 5.0
        req = ConfigCommandMessage.Request(params=[("bad_key", "val")])
        resp = _run_in_thread(commands._handle_config, req)
        assert resp.status == MQTTStatusCode.ERROR  # type: ignore[union-attr]
        assert "Invalid param" in resp.msg  # type: ignore[union-attr]
    finally:
        lt.close()


def test_config_error_on_exception() -> None:
    lt = _LoopThread()
    try:
        app = _make_app(lt.loop)
        app.config.side_effect = RuntimeError("config error")
        commands = MQTTCommands(app=app)
        commands._timeout = 5.0
        req = ConfigCommandMessage.Request(params=[])
        resp = _run_in_thread(commands._handle_config, req)
        assert resp.status == MQTTStatusCode.ERROR  # type: ignore[union-attr]
    finally:
        lt.close()


# ---------------------------------------------------------------------------
# import command
# ---------------------------------------------------------------------------


def test_import_happy() -> None:
    lt = _LoopThread()
    try:
        app = _make_app(lt.loop)
        app.import_config_file = AsyncMock(return_value="imported cross_exchange_mining.yml")
        commands = MQTTCommands(app=app)
        commands._timeout = 5.0
        req = ImportCommandMessage.Request(strategy="cross_exchange_mining")
        resp = _run_in_thread(commands._handle_import, req)
        assert resp.status == MQTTStatusCode.SUCCESS  # type: ignore[union-attr]
    finally:
        lt.close()


def test_import_empty_strategy_returns_error() -> None:
    lt = _LoopThread()
    try:
        app = _make_app(lt.loop)
        commands = MQTTCommands(app=app)
        commands._timeout = 5.0
        req = ImportCommandMessage.Request(strategy="")
        resp = _run_in_thread(commands._handle_import, req)
        assert resp.status == MQTTStatusCode.ERROR  # type: ignore[union-attr]
        assert "Empty strategy_name" in resp.msg  # type: ignore[union-attr]
    finally:
        lt.close()


def test_import_error_response() -> None:
    lt = _LoopThread()
    try:
        app = _make_app(lt.loop)
        app.import_config_file = AsyncMock(side_effect=FileNotFoundError("not found"))
        commands = MQTTCommands(app=app)
        commands._timeout = 5.0
        req = ImportCommandMessage.Request(strategy="bad_strat")
        resp = _run_in_thread(commands._handle_import, req)
        assert resp.status == MQTTStatusCode.ERROR  # type: ignore[union-attr]
    finally:
        lt.close()


def test_import_timeout_response() -> None:
    lt = _LoopThread()
    try:
        app = _make_app(lt.loop)

        async def _hang(f: str) -> str:
            await asyncio.sleep(60)
            return ""

        app.import_config_file = _hang
        commands = MQTTCommands(app=app)
        commands._timeout = 0.05
        req = ImportCommandMessage.Request(strategy="my_strat")
        resp = _run_in_thread(commands._handle_import, req)
        assert resp.status == MQTTStatusCode.ERROR  # type: ignore[union-attr]
        assert resp.msg == "timeout"  # type: ignore[union-attr]
    finally:
        lt.close()


# ---------------------------------------------------------------------------
# status command
# ---------------------------------------------------------------------------


def test_status_happy() -> None:
    lt = _LoopThread()
    try:
        app = _make_app(lt.loop)
        app.strategy_status = AsyncMock(return_value="All good")
        commands = MQTTCommands(app=app)
        commands._timeout = 5.0
        req = StatusCommandMessage.Request(async_backend=False)
        resp = _run_in_thread(commands._handle_status, req)
        assert resp.status == MQTTStatusCode.SUCCESS  # type: ignore[union-attr]
        assert resp.msg == "All good"  # type: ignore[union-attr]
    finally:
        lt.close()


def test_status_error_response() -> None:
    lt = _LoopThread()
    try:
        app = _make_app(lt.loop)
        app.strategy_status = AsyncMock(side_effect=RuntimeError("no strategy"))
        commands = MQTTCommands(app=app)
        commands._timeout = 5.0
        req = StatusCommandMessage.Request(async_backend=False)
        resp = _run_in_thread(commands._handle_status, req)
        assert resp.status == MQTTStatusCode.ERROR  # type: ignore[union-attr]
    finally:
        lt.close()


def test_status_timeout_response() -> None:
    lt = _LoopThread()
    try:
        app = _make_app(lt.loop)

        async def _hang() -> str:
            await asyncio.sleep(60)
            return ""

        app.strategy_status = _hang
        commands = MQTTCommands(app=app)
        commands._timeout = 0.05
        req = StatusCommandMessage.Request(async_backend=False)
        resp = _run_in_thread(commands._handle_status, req)
        assert resp.status == MQTTStatusCode.ERROR  # type: ignore[union-attr]
        assert resp.msg == "timeout"  # type: ignore[union-attr]
    finally:
        lt.close()


# ---------------------------------------------------------------------------
# history command
# ---------------------------------------------------------------------------


def test_history_happy_async_backend() -> None:
    lt = _LoopThread()
    try:
        app = _make_app(lt.loop)
        commands = MQTTCommands(app=app)
        commands._timeout = 5.0
        req = HistoryCommandMessage.Request(async_backend=True)
        resp = _run_in_thread(commands._handle_history, req)
        app.history.assert_called_once()
        assert resp.status == MQTTStatusCode.SUCCESS  # type: ignore[union-attr]
    finally:
        lt.close()


def test_history_happy_sync_backend() -> None:
    lt = _LoopThread()
    try:
        app = _make_app(lt.loop)
        app.get_history_trades_json = AsyncMock(return_value=[{"trade": 1}])
        commands = MQTTCommands(app=app)
        commands._timeout = 5.0
        req = HistoryCommandMessage.Request(async_backend=False, days=1.0)
        resp = _run_in_thread(commands._handle_history, req)
        assert resp.status == MQTTStatusCode.SUCCESS  # type: ignore[union-attr]
        assert resp.trades == [{"trade": 1}]  # type: ignore[union-attr]
    finally:
        lt.close()


def test_history_error_response() -> None:
    lt = _LoopThread()
    try:
        app = _make_app(lt.loop)
        app.history.side_effect = RuntimeError("history fail")
        commands = MQTTCommands(app=app)
        commands._timeout = 5.0
        req = HistoryCommandMessage.Request(async_backend=True)
        resp = _run_in_thread(commands._handle_history, req)
        assert resp.status == MQTTStatusCode.ERROR  # type: ignore[union-attr]
    finally:
        lt.close()


def test_history_timeout_response() -> None:
    lt = _LoopThread()
    try:
        app = _make_app(lt.loop)

        async def _hang(days: float) -> list[object]:
            await asyncio.sleep(60)
            return []

        app.get_history_trades_json = _hang
        commands = MQTTCommands(app=app)
        commands._timeout = 0.05
        req = HistoryCommandMessage.Request(async_backend=False)
        resp = _run_in_thread(commands._handle_history, req)
        assert resp.status == MQTTStatusCode.ERROR  # type: ignore[union-attr]
        assert resp.msg == "timeout"  # type: ignore[union-attr]
    finally:
        lt.close()


# ---------------------------------------------------------------------------
# balance_limit command
# ---------------------------------------------------------------------------


def test_balance_limit_happy() -> None:
    lt = _LoopThread()
    try:
        app = _make_app(lt.loop)
        app.balance.return_value = "1000 USDT"
        commands = MQTTCommands(app=app)
        commands._timeout = 5.0
        req = BalanceLimitCommandMessage.Request(exchange="binance", asset="USDT", amount=1000.0)
        resp = _run_in_thread(commands._handle_balance_limit, req)
        assert resp.status == MQTTStatusCode.SUCCESS  # type: ignore[union-attr]
        assert "1000 USDT" in resp.data  # type: ignore[union-attr]
    finally:
        lt.close()


def test_balance_limit_error_response() -> None:
    lt = _LoopThread()
    try:
        app = _make_app(lt.loop)
        app.balance.side_effect = RuntimeError("exchange not found")
        commands = MQTTCommands(app=app)
        commands._timeout = 5.0
        req = BalanceLimitCommandMessage.Request(exchange="bad", asset="USDT", amount=0.0)
        resp = _run_in_thread(commands._handle_balance_limit, req)
        assert resp.status == MQTTStatusCode.ERROR  # type: ignore[union-attr]
        assert "exchange not found" in resp.msg  # type: ignore[union-attr]
    finally:
        lt.close()


def test_balance_limit_null_data() -> None:
    lt = _LoopThread()
    try:
        app = _make_app(lt.loop)
        app.balance.return_value = None
        commands = MQTTCommands(app=app)
        commands._timeout = 5.0
        req = BalanceLimitCommandMessage.Request(exchange="b", asset="ETH", amount=0.5)
        resp = _run_in_thread(commands._handle_balance_limit, req)
        assert resp.status == MQTTStatusCode.SUCCESS  # type: ignore[union-attr]
        assert resp.data == ""  # type: ignore[union-attr]
    finally:
        lt.close()


# ---------------------------------------------------------------------------
# balance_paper command
# ---------------------------------------------------------------------------


def test_balance_paper_happy() -> None:
    lt = _LoopThread()
    try:
        app = _make_app(lt.loop)
        app.balance.return_value = "500 ETH"
        commands = MQTTCommands(app=app)
        commands._timeout = 5.0
        req = BalancePaperCommandMessage.Request(asset="ETH", amount=500.0)
        resp = _run_in_thread(commands._handle_balance_paper, req)
        assert resp.status == MQTTStatusCode.SUCCESS  # type: ignore[union-attr]
        assert "500 ETH" in resp.data  # type: ignore[union-attr]
    finally:
        lt.close()


def test_balance_paper_error_response() -> None:
    lt = _LoopThread()
    try:
        app = _make_app(lt.loop)
        app.balance.side_effect = ValueError("invalid amount")
        commands = MQTTCommands(app=app)
        commands._timeout = 5.0
        req = BalancePaperCommandMessage.Request(asset="ETH", amount=-1.0)
        resp = _run_in_thread(commands._handle_balance_paper, req)
        assert resp.status == MQTTStatusCode.ERROR  # type: ignore[union-attr]
        assert "invalid amount" in resp.msg  # type: ignore[union-attr]
    finally:
        lt.close()


def test_balance_paper_null_data() -> None:
    lt = _LoopThread()
    try:
        app = _make_app(lt.loop)
        app.balance.return_value = "0"
        commands = MQTTCommands(app=app)
        commands._timeout = 5.0
        req = BalancePaperCommandMessage.Request(asset="BTC", amount=0.0)
        resp = _run_in_thread(commands._handle_balance_paper, req)
        assert resp.status == MQTTStatusCode.SUCCESS  # type: ignore[union-attr]
    finally:
        lt.close()

"""Drop-in ``MQTTGateway`` matching the in-tree ``hummingbot.remote_iface.mqtt`` API.

The in-tree class is constructed as ``MQTTGateway(hb_app)`` and exposes a sync
``start()`` / ``stop()``, a sync ``health`` property, ``patch_loggers()`` and
``start_market_events_fw()``. This shim keeps that surface and delegates to the
composition-based core gateway (available as ``.gateway`` for async callers).

No ``hummingbot.*`` symbols are imported here.
"""

from __future__ import annotations

import asyncio
import contextlib
from typing import TYPE_CHECKING, Any, ClassVar

from remote_iface.gateway.log_handler import MQTTLogHandler
from remote_iface.gateway.market_events import MQTTMarketEventForwarder
from remote_iface.hb_compat.factory import create_gateway
from remote_iface.hb_compat.logging_compat import get_logger

if TYPE_CHECKING:
    from collections.abc import Iterable

    from remote_iface.gateway.gateway import MQTTGateway as CoreMQTTGateway
    from remote_iface.protocols.config import BrokerConfig, GatewayConfig

_logger = get_logger(__name__)


class MQTTGateway:
    """Sync, in-tree-shaped facade over the core ``MQTTGateway``."""

    _instance: ClassVar[MQTTGateway | None] = None

    def __init__(
        self,
        hb_app: Any,
        *,
        config: GatewayConfig | None = None,
        broker_config: BrokerConfig | None = None,
    ) -> None:
        self._hb_app = hb_app
        self._gateway: CoreMQTTGateway = create_gateway(hb_app, config, broker_config)
        self._start_task: asyncio.Future[None] | None = None
        self._stop_task: asyncio.Future[None] | None = None
        MQTTGateway._instance = self

    @classmethod
    def main(cls) -> MQTTGateway | None:
        """Return the most recently constructed gateway, or None if none exists.

        Mirrors in-tree ``MQTTGateway.main()``: last-constructed wins and the
        reference is never cleared, not even by ``stop()``.
        """
        return cls._instance

    @property
    def gateway(self) -> CoreMQTTGateway:
        """The underlying core gateway (async API for new callers)."""
        return self._gateway

    @property
    def health(self) -> bool:
        """True while the gateway is running and the broker connection is healthy."""
        return self._gateway.health

    def _loop(self) -> asyncio.AbstractEventLoop:
        """Return the host app's ``ev_loop``, else the currently running loop."""
        loop: asyncio.AbstractEventLoop | None = getattr(self._hb_app, "ev_loop", None)
        if loop is not None:
            return loop
        try:
            return asyncio.get_running_loop()
        except RuntimeError as exc:
            raise RuntimeError(
                "MQTTGateway needs an event loop: hb_app.ev_loop is unset and no loop is running"
            ) from exc

    def start(self) -> None:
        """Start the gateway.

        With a running loop, schedules the start and returns immediately (as in-tree).
        Otherwise blocks via ``run_until_complete``.
        """
        loop = self._loop()
        if loop.is_running():
            self._start_task = loop.create_task(self._gateway.start())
        else:
            loop.run_until_complete(self._gateway.start())

    async def _stop_after(self, pending_start: asyncio.Future[None] | None) -> None:
        """Await a pending start (if any), then stop the gateway.

        The pending start is awaited, not cancelled: cancelling core start() mid
        ``NodeContext.start()`` can leave the transport connected with ``_running``
        False, so core stop() would early-return and leak the connection.
        """
        if pending_start is not None and not pending_start.done():
            with contextlib.suppress(Exception):
                await pending_start
        await self._gateway.stop()

    def stop(self) -> None:
        """Stop the gateway; a still-pending start is awaited first, not cancelled."""
        loop = self._loop()
        pending_start = self._start_task
        self._start_task = None
        if loop.is_running():
            self._stop_task = loop.create_task(self._stop_after(pending_start))
        else:
            loop.run_until_complete(self._stop_after(pending_start))

    def patch_loggers(self, logger_names: Iterable[str] = ()) -> None:
        """Attach the MQTT log handler to the named loggers (idempotent).

        In-tree reads the names from hummingbot's logging config
        (``log_conf["loggers"][name]["mqtt"]``); this package cannot import hummingbot,
        so the host shim passes those names in. No-op if the log handler is disabled.
        """
        handler = self._gateway.get_component(MQTTLogHandler)
        if handler is None:
            _logger.debug("patch_loggers ignored: log handler disabled")
            return
        handler.attach(logger_names)

    def start_market_events_fw(self) -> None:
        """Start forwarding market events; call after the strategy loads and starts.

        No-op if market events are disabled (``enable_events=False``).
        """
        forwarder = self._gateway.get_component(MQTTMarketEventForwarder)
        if forwarder is None:
            _logger.debug("start_market_events_fw ignored: market events disabled")
            return
        forwarder.subscribe_now(self._gateway.app)

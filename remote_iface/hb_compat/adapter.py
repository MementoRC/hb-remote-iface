"""HummingbotAppAdapter: Any-typed wrapper implementing HummingbotAppProtocol.

The adapter wraps any host application object and implements every member of
``HummingbotAppProtocol`` without importing host-application types.  All
``hummingbot.*`` symbols are intentionally excluded — the sub-package must
remain decoupled from the host.

Method breakdown (22 total):
  - 17 pure delegation  : calls self._app.method(...)  unchanged
  -  2 param shims      : start / start_check — coalesce script/conf into v2_conf style
                          (no-op here; signature is already aligned with the protocol)
  -  1 async wrap       : get_history_trades_json — protocol is async; host may be sync
  -  4 registry shims   : subscribe_market_events, register_status_listener,
                          register_strategy_loaded_callback, handle_external_event
                          — maintained internally; host is not assumed to support them
"""

from __future__ import annotations

import asyncio
import logging
from collections.abc import Callable, Iterable
from typing import TYPE_CHECKING, Any

from remote_iface.protocols.app import (
    MarketEventCallback,
    StatusListener,
    UnsubscribeCallable,
)

if TYPE_CHECKING:
    from remote_iface.protocols.notifier import NotifierProtocol

ExternalEventCallback = Callable[[object], None]
"""Callback type for external MQTT events dispatched via handle_external_event."""


class HummingbotAppAdapter:
    """Wraps any host app object and exposes ``HummingbotAppProtocol``.

    The wrapped ``app`` is typed as ``Any`` so this module never imports
    ``HummingbotApplication`` or any other host-side concrete type.

    Registry shims (``subscribe_market_events``, ``register_status_listener``,
    ``register_strategy_loaded_callback``, ``handle_external_event``) are
    maintained inside the adapter.  The host app's own versions of these methods
    (if any) are NOT called — the adapter is the single registry point that the
    gateway sees.
    """

    def __init__(self, app: Any) -> None:
        self._app: Any = app
        # Registry shim state
        self._market_event_callbacks: list[MarketEventCallback] = []
        self._status_listeners: list[StatusListener] = []
        self._strategy_loaded_callbacks: list[Callable[[], None]] = []
        self._external_event_handlers: list[ExternalEventCallback] = []

    # ------------------------------------------------------------------
    # Properties — pure delegation
    # ------------------------------------------------------------------

    @property
    def instance_id(self) -> str:
        return str(self._app.instance_id)  # type: ignore[no-any-return]

    @property
    def ev_loop(self) -> asyncio.AbstractEventLoop:
        return self._app.ev_loop  # type: ignore[no-any-return]

    @property
    def strategy_name(self) -> str | None:
        return self._app.strategy_name  # type: ignore[no-any-return]

    @property
    def strategy(self) -> object | None:
        return self._app.strategy  # type: ignore[no-any-return]

    @property
    def client_config_map(self) -> object:
        return self._app.client_config_map  # type: ignore[no-any-return]

    @property
    def strategy_config_map(self) -> object:
        return self._app.strategy_config_map  # type: ignore[no-any-return]

    # ------------------------------------------------------------------
    # Sync methods — pure delegation
    # ------------------------------------------------------------------

    def logger(self) -> logging.Logger:
        return self._app.logger()  # type: ignore[no-any-return]

    def configurable_keys(self) -> Iterable[str]:
        return self._app.configurable_keys()  # type: ignore[no-any-return]

    def config(self, key: str | None = None, value: object | None = None) -> None:
        self._app.config(key=key, value=value)

    def start(
        self,
        log_level: str,
        script: str,
        conf: str,
        is_quickstart: bool,
    ) -> None:
        """Delegate start() to the host app with the protocol-aligned signature."""
        self._app.start(
            log_level=log_level,
            script=script,
            conf=conf,
            is_quickstart=is_quickstart,
        )

    def stop(self, skip_order_cancellation: bool = False) -> None:
        self._app.stop(skip_order_cancellation=skip_order_cancellation)

    def history(self, days: int, verbose: bool, precision: int) -> None:
        self._app.history(days=days, verbose=verbose, precision=precision)

    def balance(self, mode: str, args: list[str]) -> object:
        return self._app.balance(mode=mode, args=args)  # type: ignore[no-any-return]

    # ------------------------------------------------------------------
    # Async coroutines — delegation / async wrap
    # ------------------------------------------------------------------

    async def start_check(
        self,
        log_level: str,
        script: str,
        conf: str,
        is_quickstart: bool,
    ) -> str:
        """Await start_check on the host app (protocol-aligned; pure delegation)."""
        return await self._app.start_check(  # type: ignore[no-any-return]
            log_level=log_level,
            script=script,
            conf=conf,
            is_quickstart=is_quickstart,
        )

    async def stop_loop(self) -> str:
        return await self._app.stop_loop()  # type: ignore[no-any-return]

    async def import_config_file(self, filename: str) -> str:
        return await self._app.import_config_file(filename=filename)  # type: ignore[no-any-return]

    async def strategy_status(self) -> str:
        return await self._app.strategy_status()  # type: ignore[no-any-return]

    async def get_history_trades_json(self, days: float) -> list[object]:
        """Async wrap: call the host's method whether it is sync or async.

        The protocol contract is ``async def``; the host may expose either a
        coroutine or a plain sync method.  We handle both by checking whether
        the result is awaitable before awaiting it.
        """
        result = self._app.get_history_trades_json(days=days)
        if asyncio.iscoroutine(result):
            return await result  # type: ignore[no-any-return]
        return result  # type: ignore[no-any-return]

    # ------------------------------------------------------------------
    # Notifier — pure delegation (host is assumed to support these)
    # ------------------------------------------------------------------

    def register_notifier(self, notifier: NotifierProtocol) -> None:
        self._app.register_notifier(notifier)

    def unregister_notifier(self, notifier: NotifierProtocol) -> None:
        self._app.unregister_notifier(notifier)

    # ------------------------------------------------------------------
    # Registry shims
    # ------------------------------------------------------------------

    def subscribe_market_events(self, callback: MarketEventCallback) -> UnsubscribeCallable:
        """Register a market-event callback; return an unsubscribe callable."""
        self._market_event_callbacks.append(callback)

        def _unsubscribe() -> None:
            try:
                self._market_event_callbacks.remove(callback)
            except ValueError:
                pass

        return _unsubscribe

    def register_status_listener(self, callback: StatusListener) -> UnsubscribeCallable:
        """Register a status listener; return an unsubscribe callable."""
        self._status_listeners.append(callback)

        def _unsubscribe() -> None:
            try:
                self._status_listeners.remove(callback)
            except ValueError:
                pass

        return _unsubscribe

    def register_strategy_loaded_callback(self, callback: Callable[[], None]) -> None:
        """Register a one-shot callback invoked once a strategy is fully loaded."""
        self._strategy_loaded_callbacks.append(callback)

    def register_external_event_handler(self, callback: ExternalEventCallback) -> None:
        """Register a handler for external MQTT events.

        Not part of ``HummingbotAppProtocol`` — provided as a convenience for
        host-side code that needs to receive dispatched external events without
        the host directly implementing the protocol method.
        """
        self._external_event_handlers.append(callback)

    def handle_external_event(self, event: object) -> None:
        """Dispatch an external MQTT event to all registered external-event handlers.

        The gateway calls this when a message arrives on the ``external/+`` topic.
        The adapter fans it out to any handlers registered via
        ``register_external_event_handler()``, then also forwards to the host app
        if it exposes a ``handle_external_event`` attribute (best-effort, silent on
        AttributeError so the adapter works with minimal host implementations).
        """
        for handler in list(self._external_event_handlers):
            try:
                handler(event)
            except Exception:  # noqa: BLE001
                pass
        # Best-effort forward to host app (host may or may not support this).
        host_method = getattr(self._app, "handle_external_event", None)
        if host_method is not None:
            try:
                host_method(event)
            except Exception:  # noqa: BLE001
                pass

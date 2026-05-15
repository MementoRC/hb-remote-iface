"""HummingbotApp protocol contract and associated type aliases."""

from __future__ import annotations

from collections.abc import Callable, Iterable
from typing import TYPE_CHECKING, Protocol, runtime_checkable

if TYPE_CHECKING:
    import asyncio
    import logging

    from remote_iface.protocols.notifier import NotifierProtocol

# ---------------------------------------------------------------------------
# Type aliases
# ---------------------------------------------------------------------------

MarketEventCallback = Callable[[int, object, object], None]
"""Callable invoked on market events: ``(event_tag, pubsub_obj, event_obj)``."""

StatusListener = Callable[[str, str], None]
"""Callable invoked on status updates: ``(status_text, msg_type)``."""

UnsubscribeCallable = Callable[[], None]
"""Zero-arg callable returned by subscription helpers to cancel a subscription."""


@runtime_checkable
class HummingbotAppProtocol(Protocol):
    """Structural contract for the hummingbot application object.

    The gateway layer depends only on this protocol, never on the concrete
    ``HummingbotApplication`` class. Any object whose public interface matches
    these members satisfies the protocol via ``isinstance`` checks (runtime_checkable).
    """

    # ------------------------------------------------------------------
    # Read-only properties
    # ------------------------------------------------------------------

    @property
    def instance_id(self) -> str:
        """Unique instance identifier used in MQTT topic prefixes."""
        ...

    @property
    def ev_loop(self) -> asyncio.AbstractEventLoop:
        """Running event loop; used for thread-safe coroutine dispatch."""
        ...

    @property
    def strategy_name(self) -> str | None:
        """Name of the currently loaded strategy, or ``None`` if none is loaded."""
        ...

    @property
    def strategy(self) -> object | None:
        """Currently active strategy object; the gateway performs only a ``None``-check."""
        ...

    @property
    def client_config_map(self) -> object:
        """Client configuration map; adapter extracts fields via ``.dict()`` or attribute access."""
        ...

    @property
    def strategy_config_map(self) -> object:
        """Strategy configuration map; exposed under the PUBLIC name (upstream uses private)."""
        ...

    # ------------------------------------------------------------------
    # Sync methods
    # ------------------------------------------------------------------

    def logger(self) -> logging.Logger:
        """Return the application logger (this is a callable, not an attribute)."""
        ...

    def configurable_keys(self) -> Iterable[str]:
        """Return the set of configuration keys that may be updated at runtime."""
        ...

    def config(self, key: str | None = None, value: object | None = None) -> None:
        """Apply a configuration change. ``key=None`` is a display-only request."""
        ...

    def start(
        self,
        log_level: str,
        script: str,
        conf: str,
        is_quickstart: bool,
    ) -> None:
        """Fire-and-forget strategy start. Returns before the strategy is fully initialised."""
        ...

    def stop(self, skip_order_cancellation: bool = False) -> None:
        """Stop the running strategy."""
        ...

    def history(self, days: int, verbose: bool, precision: int) -> None:
        """Print trade history for the given number of past days."""
        ...

    def balance(self, mode: str, args: list[str]) -> object:
        """Query or set balance limits/paper-trade balances."""
        ...

    # ------------------------------------------------------------------
    # Async coroutines
    # ------------------------------------------------------------------

    async def start_check(
        self,
        log_level: str,
        script: str,
        conf: str,
        is_quickstart: bool,
    ) -> str:
        """Await strategy start completion and return a status message."""
        ...

    async def stop_loop(self) -> str:
        """Stop the event loop and return a status message."""
        ...

    async def import_config_file(self, filename: str) -> str:
        """Import a strategy config file by name and return a status message."""
        ...

    async def strategy_status(self) -> str:
        """Return a formatted strategy status string."""
        ...

    async def get_history_trades_json(self, days: float) -> list[object]:
        """Return recent trades as a list of JSON-serialisable objects."""
        ...

    # ------------------------------------------------------------------
    # Event / notifier / status subscription
    # ------------------------------------------------------------------

    def subscribe_market_events(self, callback: MarketEventCallback) -> UnsubscribeCallable:
        """Register a market-event callback.

        Returns an ``UnsubscribeCallable`` that, when called, removes the registration.
        """
        ...

    def register_notifier(self, notifier: NotifierProtocol) -> None:
        """Add a notifier to the application's notification chain."""
        ...

    def unregister_notifier(self, notifier: NotifierProtocol) -> None:
        """Remove a previously registered notifier."""
        ...

    def register_status_listener(self, callback: StatusListener) -> UnsubscribeCallable:
        """Register a push-on-demand status listener.

        Returns an ``UnsubscribeCallable`` that, when called, removes the registration.
        Listeners are invoked by adapter code when a status broadcast is triggered.
        """
        ...

    def register_strategy_loaded_callback(self, callback: Callable[[], None]) -> None:
        """Register a callback to be invoked once a strategy is fully loaded and ready."""
        ...

    def handle_external_event(self, event: object) -> None:
        """Dispatch an external event received from the MQTT bus into the application."""
        ...

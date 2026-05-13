"""Subscriber wrapper over a commlib subscriber primitive."""

from collections.abc import Callable
from typing import Any

from remote_iface._commlib.wrappers.endpoint import Endpoint


class Subscriber(Endpoint):
    """Wraps a commlib subscriber to provide lifecycle management and callback re-wiring.

    Supports two usage patterns: with msg_type (commlib deserializes into a typed instance
    and passes it to the callback) and without msg_type (callback receives a raw dict). The
    actual deserialization is handled by NodeContext when it creates the commlib subscriber;
    this wrapper stores the msg_type only so NodeContext can inspect it on _bind(). Calling
    set_callback() after binding updates the stored callback and, where commlib supports it,
    also updates the live commlib subscriber's callback.
    """

    def __init__(
        self,
        *,
        topic: str,
        on_message: Callable[[object], None],
        msg_type: type | None = None,
    ) -> None:
        super().__init__()
        self.topic: str = topic
        self.msg_type: type | None = msg_type
        self._on_message: Callable[[object], None] = on_message
        self._cp: Any = None

    def _bind(self, commlib_subscriber: Any) -> None:
        """Inject the commlib subscriber created by NodeContext for this topic."""
        self._cp = commlib_subscriber

    def set_callback(self, fn: Callable[[object], None]) -> None:
        """Replace the message callback. Updates the live commlib subscriber where supported."""
        self._on_message = fn
        # Propagate to the commlib layer if it exposes a callback setter; otherwise NodeContext
        # will pick up the new callback on the next start() cycle.
        if self._cp is not None and hasattr(self._cp, "set_callback"):
            self._cp.set_callback(fn)

    def _do_start(self) -> None:
        assert self._cp is not None, (
            f"Subscriber for '{self.topic}' has no bound commlib subscriber — call _bind() first"
        )
        if hasattr(self._cp, "start"):
            self._cp.start()

    def _do_stop(self) -> None:
        if self._cp is not None and hasattr(self._cp, "stop"):
            self._cp.stop()

    @property
    def on_message(self) -> Callable[[object], None]:
        """Return the current message callback (read-only view for NodeContext inspection)."""
        return self._on_message

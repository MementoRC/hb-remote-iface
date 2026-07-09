"""Subscriber wrapper registering into NodeContext's topic-pattern callback table."""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

from remote_iface._commlib.wrappers.endpoint import Endpoint

if TYPE_CHECKING:
    from collections.abc import Callable

    from remote_iface._commlib.node_context import NodeContext


class Subscriber(Endpoint):
    """Wraps a topic pattern + callback, registered into NodeContext._sub_callbacks."""

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
        self._nc: NodeContext | None = None
        self._edge_callback: Callable[[Any], None] | None = None

    def _bind(self, node_context: NodeContext, edge_callback: Callable[[Any], None]) -> None:
        """Inject the owning NodeContext and the edge callback registered for delivery."""
        self._nc = node_context
        self._edge_callback = edge_callback

    def set_callback(self, fn: Callable[[object], None]) -> None:
        """Replace the message callback. Takes effect immediately, even after start(),
        because the edge callback always reads self._on_message live (see NodeContext).
        """
        self._on_message = fn

    def _do_start(self) -> None:
        assert self._nc is not None and self._edge_callback is not None, (
            f"Subscriber for '{self.topic}' has no bound NodeContext — call _bind() first"
        )
        self._nc._sub_callbacks.setdefault(self.topic, []).append(self._edge_callback)  # noqa: SLF001

    def _do_stop(self) -> None:
        if self._nc is not None and self._edge_callback is not None:
            callbacks = self._nc._sub_callbacks.get(self.topic, [])  # noqa: SLF001
            if self._edge_callback in callbacks:
                callbacks.remove(self._edge_callback)

    @property
    def on_message(self) -> Callable[[object], None]:
        """Return the current message callback (read-only view for NodeContext inspection)."""
        return self._on_message

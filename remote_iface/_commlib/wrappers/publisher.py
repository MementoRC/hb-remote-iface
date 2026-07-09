"""Publisher wrapper routing through NodeContext's shared outgoing queue."""

from __future__ import annotations

from typing import TYPE_CHECKING

from remote_iface._commlib.wrappers.endpoint import Endpoint

if TYPE_CHECKING:
    from remote_iface._commlib.node_context import NodeContext


class Publisher(Endpoint):
    """Wraps a topic+msg_type pair, publishing via the owning NodeContext's queue.

    publish() is thread-safe: it may be called from the event loop thread or from a
    worker thread (e.g. an RPC handler's ThreadPoolExecutor), since NodeContext's
    _enqueue_outgoing() uses loop.call_soon_threadsafe().
    """

    def __init__(self, *, topic: str, msg_type: type) -> None:
        super().__init__()
        self.topic: str = topic
        self.msg_type: type = msg_type
        self.publish_failure_count: int = 0
        self._nc: NodeContext | None = None

    def _bind(self, node_context: NodeContext) -> None:
        """Inject the owning NodeContext."""
        self._nc = node_context

    def _do_start(self) -> None:
        assert self._nc is not None, (
            f"Publisher for '{self.topic}' has no bound NodeContext — call _bind() first"
        )

    def _do_stop(self) -> None:
        pass

    def publish(self, message: object) -> None:
        """Publish a message. Drops silently (with counter increment) if not started."""
        if not self._started:
            self._logger.debug(
                "publish() called on stopped Publisher(topic=%r) — dropping", self.topic
            )
            self.publish_failure_count += 1
            return
        try:
            from remote_iface._commlib.serialization import serialize

            payload = serialize(message)
            self._nc._enqueue_outgoing(self.topic, payload, qos=0)  # noqa: SLF001
        except Exception as exc:  # noqa: BLE001
            self.publish_failure_count += 1
            self._logger.warning(
                "Publisher(topic=%r) publish failed: %s: %s",
                self.topic,
                type(exc).__name__,
                exc,
            )

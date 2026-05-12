"""Publisher wrapper over a commlib publisher primitive."""

from typing import Any

from remote_iface._commlib.wrappers.endpoint import Endpoint


class Publisher(Endpoint):
    """Wraps a commlib publisher to provide lifecycle management and failure isolation.

    The underlying commlib publisher object (_cp) is injected after construction via _bind(),
    which NodeContext calls when it instantiates the transport-specific publisher. Until _bind()
    is called, _do_start() will raise AssertionError — NodeContext must bind before starting.
    Publish failures increment a counter and are logged at WARNING; they are never re-raised so
    a single bad message cannot break the send loop.
    """

    def __init__(self, *, topic: str, msg_type: type) -> None:
        super().__init__()
        self.topic: str = topic
        self.msg_type: type = msg_type
        self.publish_failure_count: int = 0
        self._cp: Any = None

    def _bind(self, commlib_publisher: Any) -> None:
        """Inject the commlib publisher created by NodeContext for this topic."""
        self._cp = commlib_publisher

    def _do_start(self) -> None:
        assert self._cp is not None, (
            f"Publisher for '{self.topic}' has no bound commlib publisher — call _bind() first"
        )
        if hasattr(self._cp, "start"):
            self._cp.start()

    def _do_stop(self) -> None:
        if self._cp is not None and hasattr(self._cp, "stop"):
            self._cp.stop()

    def publish(self, message: object) -> None:
        """Publish a message. Drops silently (with counter increment) if not started."""
        if not self._started:
            self._logger.debug(
                "publish() called on stopped Publisher(topic=%r) — dropping", self.topic
            )
            self.publish_failure_count += 1
            return
        try:
            self._cp.publish(message)
        except Exception as exc:  # noqa: BLE001
            self.publish_failure_count += 1
            self._logger.warning(
                "Publisher(topic=%r) publish failed: %s: %s",
                self.topic,
                type(exc).__name__,
                exc,
            )

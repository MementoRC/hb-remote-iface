"""ETopicListener: subscribes to an MQTT topic and calls a user callback on each message."""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from collections.abc import Callable

    from remote_iface._commlib.wrappers.subscriber import Subscriber
    from remote_iface.gateway.gateway import MQTTGateway

_logger = logging.getLogger("remote_iface.external.listeners")


class ETopicListener:
    """Subscribe to a single MQTT topic and dispatch messages to *callback*.

    Takes an explicit MQTTGateway argument (no module-level singleton). Topic
    prefixing is delegated to gateway.topic_for() — when use_bot_prefix=True the
    topic is prefixed with {namespace}/{instance_id}/, matching the upstream
    TopicSpecs.PREFIX convention.

    Call stop() to cancel the subscription.
    """

    def __init__(
        self,
        gateway: MQTTGateway,
        topic: str,
        callback: Callable[[Any, str], None],
        use_bot_prefix: bool = True,
    ) -> None:
        self._topic = gateway.topic_for(topic, bot_prefix=use_bot_prefix)
        self._callback = callback
        self._sub: Subscriber = gateway._node_context.create_subscriber(  # noqa: SLF001
            topic=self._topic,
            on_message=self._dispatch,
        )

    def _dispatch(self, msg: Any) -> None:
        """Forward *msg* to the user callback, passing the resolved topic as the second arg."""
        try:
            self._callback(msg, self._topic)
        except Exception as exc:  # noqa: BLE001
            _logger.warning(
                "ETopicListener(%r): callback raised: %s: %s",
                self._topic,
                type(exc).__name__,
                exc,
            )

    def stop(self) -> None:
        """Stop the underlying commlib subscriber."""
        self._sub.stop()

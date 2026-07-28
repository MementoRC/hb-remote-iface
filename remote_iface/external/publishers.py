"""ETopicPublisher, EMTopicPublisher: user-facing MQTT publishers.

Both classes take an explicit MQTTGateway argument (no module-level singleton).
Topic prefixing is delegated to gateway.topic_for() which implements the same
{namespace}/{instance_id}/{topic} convention as the upstream TopicSpecs.PREFIX.
"""

from __future__ import annotations

import asyncio
import threading
from typing import TYPE_CHECKING, Any

from remote_iface.hb_compat.logging_compat import get_logger

if TYPE_CHECKING:
    from remote_iface._commlib.wrappers.publisher import Publisher
    from remote_iface.gateway.gateway import MQTTGateway

_logger = get_logger("remote_iface.external.publishers")


class ETopicPublisher:
    """Publish dicts to a single fixed MQTT topic.

    Constructs a commlib publisher via gateway._node_context.create_publisher() and
    holds it for the lifetime of this object. Call send() to publish a message dict.
    """

    def __init__(
        self,
        gateway: MQTTGateway,
        topic: str,
        use_bot_prefix: bool = True,
    ) -> None:
        self._topic = gateway.topic_for(topic, bot_prefix=use_bot_prefix)
        self._pub = gateway._node_context.create_publisher(  # noqa: SLF001
            topic=self._topic,
            msg_type=dict,
        )

    def send(self, msg: dict[str, Any]) -> None:
        """Publish *msg* to the fixed topic.

        Thread-safe: calls from off-thread are dispatched back to the main thread
        via call_soon_threadsafe, mirroring the upstream threading guard.
        """
        if threading.current_thread() != threading.main_thread():  # pragma: no cover
            asyncio.get_event_loop().call_soon_threadsafe(self.send, msg)
            return
        self._pub.publish(msg)

    def __call__(self, msg: dict[str, Any]) -> None:
        self.send(msg)


class EMTopicPublisher:
    """Publish dicts to an arbitrary MQTT topic chosen at send time.

    The topic supplied to send() is optionally prefixed with
    {namespace}/{instance_id}/ when use_bot_prefix=True (matching the upstream
    TopicSpecs.PREFIX convention), or used as-is when False.

    Publishers per resolved topic are created lazily and cached so that repeated
    sends to the same topic do not allocate a new commlib publisher each time.
    This is the sub-package equivalent of upstream's create_mpublisher() multi-topic
    publisher (which the sub-package NodeContext does not expose).
    """

    def __init__(
        self,
        gateway: MQTTGateway,
        use_bot_prefix: bool = True,
    ) -> None:
        self._gateway = gateway
        self._use_bot_prefix = use_bot_prefix
        self._publishers: dict[str, Publisher] = {}

    def send(self, topic: str, msg: dict[str, Any]) -> None:
        """Publish *msg* to *topic*, applying bot prefix when use_bot_prefix=True."""
        if threading.current_thread() != threading.main_thread():  # pragma: no cover
            asyncio.get_event_loop().call_soon_threadsafe(self.send, topic, msg)
            return
        resolved = self._gateway.topic_for(topic, bot_prefix=self._use_bot_prefix)
        if resolved not in self._publishers:
            self._publishers[resolved] = self._gateway._node_context.create_publisher(  # noqa: SLF001
                topic=resolved,
                msg_type=dict,
            )
        self._publishers[resolved].publish(msg)

    def __call__(self, topic: str, msg: dict[str, Any]) -> None:
        self.send(topic, msg)

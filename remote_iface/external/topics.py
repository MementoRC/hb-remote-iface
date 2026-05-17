"""ETopicListenerFactory, ETopicQueueFactory: topic-based subscription helpers."""

from __future__ import annotations

import functools
import logging
from collections import deque
from collections.abc import Callable
from typing import TYPE_CHECKING, Any

from remote_iface.external.listeners import ETopicListener

if TYPE_CHECKING:
    from remote_iface.gateway.gateway import MQTTGateway

_logger = logging.getLogger("remote_iface.external.topics")


class ETopicListenerFactory:
    """Create and remove ETopicListener instances."""

    @staticmethod
    def create(
        gateway: MQTTGateway,
        topic: str,
        callback: Callable[[Any, str], None],
        use_bot_prefix: bool = True,
    ) -> ETopicListener:
        """Subscribe *callback* to *topic* and return the live ETopicListener."""
        return ETopicListener(
            gateway=gateway,
            topic=topic,
            callback=callback,
            use_bot_prefix=use_bot_prefix,
        )

    @staticmethod
    def remove(listener: ETopicListener) -> None:
        """Stop *listener* and release its commlib subscription."""
        listener.stop()


class ETopicQueueFactory:
    """Subscribe to a topic and buffer received messages in a deque."""

    @staticmethod
    def create(
        gateway: MQTTGateway,
        topic: str,
        queue_size: int = 1000,
        use_bot_prefix: bool = True,
    ) -> deque[tuple[str, Any]]:
        """Create a bounded deque and subscribe *topic* to fill it.

        Each entry is a ``(resolved_topic, msg)`` tuple matching the upstream convention.
        """
        queue: deque[tuple[str, Any]] = deque(maxlen=queue_size)
        on_msg = functools.partial(ETopicQueueFactory._on_message, queue)
        ETopicListener(
            gateway=gateway,
            topic=topic,
            callback=on_msg,
            use_bot_prefix=use_bot_prefix,
        )
        return queue

    @staticmethod
    def _on_message(queue: deque[tuple[str, Any]], msg: Any, topic: str) -> None:
        """Append *(topic, msg)* to *queue*.  Called by the ETopicListener dispatch loop."""
        queue.append((topic, msg))

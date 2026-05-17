"""ExternalTopicFactory, ExternalEventFactory: public facades for the external API.

These are thin delegators over the internal E* helper classes. They provide the
user-facing entry-point with the same method names as upstream while keeping the
explicit gateway argument convention of the sub-package.

Default values for use_bot_prefix match the upstream defaults:
  - ExternalTopicFactory.create_async / create_queue: use_bot_prefix=False
  - ExternalEventFactory methods: no topic prefix (event routing is name-based)
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

from remote_iface.external.events import EEventListenerFactory, EEventQueueFactory
from remote_iface.external.topics import ETopicListenerFactory, ETopicQueueFactory

if TYPE_CHECKING:
    from collections import deque
    from collections.abc import Callable

    from remote_iface.external.listeners import ETopicListener
    from remote_iface.gateway.gateway import MQTTGateway


class ExternalTopicFactory:
    """Public facade: subscribe to or publish on arbitrary MQTT topics."""

    @staticmethod
    def create_async(
        gateway: MQTTGateway,
        topic: str,
        callback: Callable[[Any, str], None],
        use_bot_prefix: bool = False,
    ) -> ETopicListener:
        """Subscribe *callback* to *topic*; return the ETopicListener for later removal."""
        return ETopicListenerFactory.create(
            gateway=gateway,
            topic=topic,
            callback=callback,
            use_bot_prefix=use_bot_prefix,
        )

    @staticmethod
    def create_queue(
        gateway: MQTTGateway,
        topic: str,
        use_bot_prefix: bool = False,
    ) -> deque[tuple[str, Any]]:
        """Subscribe to *topic* and return a bounded deque that buffers received messages."""
        return ETopicQueueFactory.create(
            gateway=gateway,
            topic=topic,
            use_bot_prefix=use_bot_prefix,
        )

    @staticmethod
    def remove_listener(listener: ETopicListener) -> None:
        """Stop *listener* and release its commlib subscription."""
        ETopicListenerFactory.remove(listener)


class ExternalEventFactory:
    """Public facade: subscribe to named external events."""

    @staticmethod
    def create_queue(
        gateway: MQTTGateway,
        event_name: str,
    ) -> deque[tuple[str, Any]]:
        """Subscribe to *event_name* and return a bounded deque of *(event_name, msg)* tuples."""
        return EEventQueueFactory.create(gateway=gateway, event_name=event_name)

    @staticmethod
    def create_async(
        gateway: MQTTGateway,
        event_name: str,
        callback: Callable[[Any, str], None],
    ) -> None:
        """Subscribe *callback* to *event_name* external events."""
        EEventListenerFactory.create(
            gateway=gateway,
            event_name=event_name,
            callback=callback,
        )

    @staticmethod
    def remove_listener(
        gateway: MQTTGateway,
        event_name: str,
        callback: Callable[[Any, str], None],
    ) -> None:
        """Remove *callback* from *event_name* subscriptions (see EEventListenerFactory.remove)."""
        EEventListenerFactory.remove(
            gateway=gateway,
            event_name=event_name,
            callback=callback,
        )

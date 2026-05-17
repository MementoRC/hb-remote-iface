"""External-facing publish/subscribe API for user controllers, scripts, and strategies.

These classes take an explicit MQTTGateway argument (no module-level singleton).
Topic prefixing follows the upstream use_bot_prefix convention: when True, topics
are prefixed with {namespace}/{instance_id}/ for per-bot scoping.
"""

from remote_iface.external.events import EEventListenerFactory, EEventQueueFactory
from remote_iface.external.factories import ExternalEventFactory, ExternalTopicFactory
from remote_iface.external.listeners import ETopicListener
from remote_iface.external.publishers import EMTopicPublisher, ETopicPublisher
from remote_iface.external.topics import ETopicListenerFactory, ETopicQueueFactory

__all__ = [
    "EEventListenerFactory",
    "EEventQueueFactory",
    "EMTopicPublisher",
    "ETopicListener",
    "ETopicListenerFactory",
    "ETopicPublisher",
    "ETopicQueueFactory",
    "ExternalEventFactory",
    "ExternalTopicFactory",
]

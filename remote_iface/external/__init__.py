"""External-facing publish/subscribe API for user controllers, scripts, and strategies.

These classes take an explicit MQTTGateway argument (no module-level singleton).
Topic prefixing follows the upstream use_bot_prefix convention: when True, topics
are prefixed with {namespace}/{instance_id}/ for per-bot scoping.
"""

from remote_iface.external.listeners import ETopicListener
from remote_iface.external.publishers import EMTopicPublisher, ETopicPublisher

__all__ = [
    "EMTopicPublisher",
    "ETopicListener",
    "ETopicPublisher",
]

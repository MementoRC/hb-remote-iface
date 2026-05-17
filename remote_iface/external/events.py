"""EEventQueueFactory, EEventListenerFactory: external-event subscription helpers.

The sub-package MQTTGateway has no add_external_event_listener() API (unlike the
upstream singleton gateway). Each factory call therefore creates its own commlib
subscriber on the ``external/{event_name}`` topic directly via
``gateway._node_context.create_subscriber()``.  This is intentionally lightweight:
no shared dispatch table, each factory call owns its subscription independently.

Follow-up gap: the sub-package's MQTTExternalEvents component dispatches inbound
external events to ``app.handle_external_event()`` using a wildcard ``external/+``
subscriber.  If the same gateway is used for both component-level and factory-level
subscriptions, two subscribers will be active for the same topic.  This is safe
(commlib supports multiple subscribers per topic) but may deliver messages twice to
the app layer.  A unified dispatch table on MQTTGateway would eliminate the overlap;
that is deferred as a follow-up.
"""

from __future__ import annotations

import functools
import logging
from collections import deque
from collections.abc import Callable
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from remote_iface.gateway.gateway import MQTTGateway

_logger = logging.getLogger("remote_iface.external.events")

_EXTERNAL_TOPIC_PREFIX = "external"


class EEventQueueFactory:
    """Subscribe to an external event topic and buffer received messages in a deque."""

    @staticmethod
    def create(
        gateway: MQTTGateway,
        event_name: str,
        queue_size: int = 1000,
    ) -> deque[tuple[str, Any]]:
        """Create a bounded deque and subscribe to ``external/{event_name}`` to fill it.

        Each entry is a ``(event_name, msg)`` tuple matching the upstream convention.
        """
        queue: deque[tuple[str, Any]] = deque(maxlen=queue_size)
        on_event = functools.partial(EEventQueueFactory._on_event, queue, event_name)
        topic = f"{_EXTERNAL_TOPIC_PREFIX}/{event_name}"
        gateway._node_context.create_subscriber(  # noqa: SLF001
            topic=topic,
            on_message=on_event,
        )
        return queue

    @staticmethod
    def _on_event(
        queue: deque[tuple[str, Any]],
        event_name: str,
        msg: Any,
    ) -> None:
        """Append *(event_name, msg)* to *queue*.  Called by the commlib subscriber."""
        queue.append((event_name, msg))


class EEventListenerFactory:
    """Register and remove callbacks for a specific external event name."""

    @staticmethod
    def create(
        gateway: MQTTGateway,
        event_name: str,
        callback: Callable[[Any, str], None],
    ) -> None:
        """Subscribe *callback* to ``external/{event_name}``.

        The callback receives ``(msg, event_name)`` matching the upstream signature.
        """
        topic = f"{_EXTERNAL_TOPIC_PREFIX}/{event_name}"

        def _dispatch(msg: Any) -> None:
            try:
                callback(msg, event_name)
            except Exception as exc:  # noqa: BLE001
                _logger.warning(
                    "EEventListenerFactory callback for %r raised: %s: %s",
                    event_name,
                    type(exc).__name__,
                    exc,
                )

        gateway._node_context.create_subscriber(  # noqa: SLF001
            topic=topic,
            on_message=_dispatch,
        )

    @staticmethod
    def remove(
        gateway: MQTTGateway,
        event_name: str,
        callback: Callable[[Any, str], None],
    ) -> None:
        """Remove *callback* from the ``external/{event_name}`` subscription.

        Note: the sub-package NodeContext does not expose a per-callback unsubscribe
        method (commlib does not support it at this level). This method is present for
        API parity with upstream; to fully cancel a subscription, stop the gateway and
        restart it without re-registering the callback.  A finer-grained unsubscribe
        API is tracked as a follow-up gap.
        """
        _logger.warning(
            "EEventListenerFactory.remove(%r): per-callback unsubscribe is not supported "
            "by the sub-package NodeContext. The subscription will remain active until "
            "the gateway is stopped.  gateway=%r, callback=%r",
            event_name,
            gateway,
            callback,
        )

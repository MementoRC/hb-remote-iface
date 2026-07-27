"""EEventQueueFactory, EEventListenerFactory: external-event subscription helpers.

The sub-package MQTTGateway has no add_external_event_listener() API (unlike the
upstream singleton gateway).

``EEventQueueFactory`` still creates its own commlib subscriber on the
``external/{event_name}`` topic directly via ``gateway._node_context.create_subscriber()``.
This is intentionally lightweight: no shared dispatch table, each factory call owns its
subscription independently. (Out of scope for issue #13 PR2 — see EEventListenerFactory
below for the migrated path.)

``EEventListenerFactory`` no longer opens its own raw MQTT subscription. Previously it
created an independent commlib subscriber on ``external/{event_name}``, which duplicated
the sub-package's MQTTExternalEvents component — that component already subscribes to a
wildcard ``external/+`` topic and republishes each inbound message onto the in-process
event_bus under the ``external.{event_name}`` key (see gateway/external_events.py). Having
both meant two independent MQTT subscribers active for overlapping topics: safe (commlib
allows multiple subscribers per topic) but a double-subscription/double-delivery risk —
the root cause issue #13 tracks.

``EEventListenerFactory.create()`` now depends ONLY on MQTTExternalEvents' existing
wildcard subscription + event_bus republish: it registers via
``gateway._node_context.subscribe_event("external.{event_name}", handler)`` and creates no
MQTT subscription of its own. ``remove()`` correspondingly unsubscribes the exact same
handler object via ``gateway._node_context.unsubscribe_event()``, using a module-level
registry (see ``_dispatch_registry`` below) to look up the handler created at
``create()``-time from ``(gateway, event_name, callback)`` alone.

IMPORTANT BEHAVIOUR CHANGE: because EEventListenerFactory no longer has its own MQTT
subscription, it now has a HARD DEPENDENCY on MQTTExternalEvents being active (i.e.
``gateway._config.enable_external_events`` truthy) on the same gateway. If
MQTTExternalEvents is not running, EEventListenerFactory subscribers receive NOTHING —
previously they would still receive messages independently via their own subscription.
This trade-off eliminates the double-subscription risk but introduces a new coupling
that callers should be aware of.
"""

from __future__ import annotations

import functools
import logging
from collections import deque
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from collections.abc import Callable

    from remote_iface.gateway.gateway import MQTTGateway

_logger = logging.getLogger("remote_iface.external.events")

_EXTERNAL_TOPIC_PREFIX = "external"

# Registry of live EEventListenerFactory dispatch closures, keyed by
# (id(gateway), event_name, callback) so remove() can look up and unsubscribe the exact
# same handler object registered at create()-time (event_bus unsubscribe is keyed on
# object identity, not just event_type).
_dispatch_registry: dict[tuple[int, str, Callable[[Any, str], None]], Callable[[Any], None]] = {}


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
        """Subscribe *callback* to the ``external.{event_name}`` event_bus key.

        Rides on MQTTExternalEvents' existing wildcard ``external/+`` MQTT subscription
        and event_bus republish — creates no MQTT subscription of its own. The callback
        receives ``(msg, event_name)`` matching the upstream signature.

        Note: this means *callback* only fires if MQTTExternalEvents is active on
        *gateway* (i.e. ``gateway._config.enable_external_events`` is truthy). If it is
        not, no messages will be delivered here at all.
        """
        event_key = f"{_EXTERNAL_TOPIC_PREFIX}.{event_name}"

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

        gateway._node_context.subscribe_event(event_key, _dispatch)  # noqa: SLF001
        _dispatch_registry[(id(gateway), event_name, callback)] = _dispatch

    @staticmethod
    def remove(
        gateway: MQTTGateway,
        event_name: str,
        callback: Callable[[Any, str], None],
    ) -> None:
        """Unsubscribe *callback* from the ``external.{event_name}`` event_bus key.

        Looks up the exact dispatch handler object registered by a prior ``create()``
        call for this ``(gateway, event_name, callback)`` triple and unsubscribes it via
        ``gateway._node_context.unsubscribe_event()``. If no matching registration is
        found (e.g. *callback* was never passed to ``create()``, or was already
        removed), logs a warning and does nothing further.
        """
        key = (id(gateway), event_name, callback)
        dispatch = _dispatch_registry.pop(key, None)
        if dispatch is None:
            _logger.warning(
                "EEventListenerFactory.remove(%r): no registered subscription found for "
                "this callback — it was never created() or was already removed.  "
                "gateway=%r, callback=%r",
                event_name,
                gateway,
                callback,
            )
            return
        event_key = f"{_EXTERNAL_TOPIC_PREFIX}.{event_name}"
        gateway._node_context.unsubscribe_event(event_key, dispatch)  # noqa: SLF001

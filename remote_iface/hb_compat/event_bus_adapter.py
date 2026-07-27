"""EventBusAdapter — bridges remote-iface hb_compat to the ``event_bus`` sub-package.

Mirrors ``strategy_framework.hb_compat.EventBusAdapter``: wraps a real
``event_bus.EventBus`` instance and presents the ``(event_type, handler)``-keyed
publish/subscribe/unsubscribe surface expected by remote-iface call sites, without
this sub-package taking on any ``hummingbot.*`` dependency.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

from event_bus import EventBus, Subscription

if TYPE_CHECKING:
    from collections.abc import Callable


class EventBusAdapter:
    """Thread-unsafe adapter over the ``event_bus`` sub-package's ``EventBus``.

    Delegates publish/subscribe/unsubscribe to an internal :class:`event_bus.EventBus`
    instance while presenting a ``(event_type, handler)``-keyed surface. The underlying
    ``EventBus`` is not thread-safe; callers must synchronise external access if this
    adapter is shared across threads.
    """

    def __init__(self) -> None:
        self._bus = EventBus()
        self._subs: dict[tuple[str, Callable[[dict[str, Any]], None]], Subscription] = {}

    def publish(self, event_type: str, payload: dict[str, Any]) -> None:
        self._bus.publish(event_type, payload)

    def subscribe(self, event_type: str, handler: Callable[[dict[str, Any]], None]) -> None:
        self._subs[(event_type, handler)] = self._bus.subscribe(event_type, handler)

    def unsubscribe(self, event_type: str, handler: Callable[[dict[str, Any]], None]) -> None:
        sub = self._subs.pop((event_type, handler), None)
        if sub is not None:
            self._bus.unsubscribe(sub)

"""Unit tests for EventBusAdapter — wraps event_bus.EventBus for remote-iface hb_compat.

Uses the real ``event_bus.EventBus`` (no mocking) since it is a lightweight,
now-installed dependency; these tests exercise the adapter's publish/subscribe/
unsubscribe bridging semantics against actual bus behaviour.
"""

import pytest

pytestmark = pytest.mark.unit


def test_subscribe_and_publish_delivers_payload_to_handler() -> None:
    from remote_iface.hb_compat.event_bus_adapter import EventBusAdapter

    adapter = EventBusAdapter()
    received: list[dict[str, object]] = []

    def handler(payload: dict[str, object]) -> None:
        received.append(payload)

    adapter.subscribe("order.filled", handler)
    adapter.publish("order.filled", {"order_id": "abc"})

    assert received == [{"order_id": "abc"}]


def test_publish_with_no_subscribers_is_a_noop() -> None:
    from remote_iface.hb_compat.event_bus_adapter import EventBusAdapter

    adapter = EventBusAdapter()

    # Should not raise even though nothing is subscribed to this event type.
    adapter.publish("nobody.listening", {"key": "value"})


def test_unsubscribe_stops_further_delivery() -> None:
    from remote_iface.hb_compat.event_bus_adapter import EventBusAdapter

    adapter = EventBusAdapter()
    received: list[dict[str, object]] = []

    def handler(payload: dict[str, object]) -> None:
        received.append(payload)

    adapter.subscribe("order.filled", handler)
    adapter.publish("order.filled", {"seq": 1})
    adapter.unsubscribe("order.filled", handler)
    adapter.publish("order.filled", {"seq": 2})

    assert received == [{"seq": 1}]


def test_unsubscribe_unknown_pair_is_a_safe_noop() -> None:
    from remote_iface.hb_compat.event_bus_adapter import EventBusAdapter

    adapter = EventBusAdapter()

    def handler(payload: dict[str, object]) -> None:
        pass

    # Never subscribed — must not raise (idempotent / safe no-op).
    adapter.unsubscribe("order.filled", handler)


def test_multiple_independent_handlers_both_receive_payload() -> None:
    from remote_iface.hb_compat.event_bus_adapter import EventBusAdapter

    adapter = EventBusAdapter()
    received_a: list[dict[str, object]] = []
    received_b: list[dict[str, object]] = []

    def handler_a(payload: dict[str, object]) -> None:
        received_a.append(payload)

    def handler_b(payload: dict[str, object]) -> None:
        received_b.append(payload)

    adapter.subscribe("order.filled", handler_a)
    adapter.subscribe("order.filled", handler_b)
    adapter.publish("order.filled", {"order_id": "xyz"})

    assert received_a == [{"order_id": "xyz"}]
    assert received_b == [{"order_id": "xyz"}]

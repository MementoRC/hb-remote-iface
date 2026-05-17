"""Unit tests for ExternalTopicFactory, ExternalEventFactory, and their internal helpers.

Covers:
- ETopicListenerFactory.create / remove
- ETopicQueueFactory.create / _on_message
- EEventListenerFactory.create / remove (no-op + WARNING)
- EEventQueueFactory.create / _on_event
- ExternalTopicFactory.create_async / create_queue / remove_listener
- ExternalEventFactory.create_queue / create_async / remove_listener

Behavioural parity with upstream test_etopic_listener_factory, test_etopic_queue_factory,
test_eevent_queue_factory_class, test_eevent_listener_factory, test_eevent_listener_factory_class.
"""

from __future__ import annotations

from collections import deque
from unittest.mock import MagicMock

import pytest

from remote_iface.external.events import EEventListenerFactory, EEventQueueFactory
from remote_iface.external.factories import ExternalEventFactory, ExternalTopicFactory
from remote_iface.external.listeners import ETopicListener
from remote_iface.external.topics import ETopicListenerFactory, ETopicQueueFactory

pytestmark = pytest.mark.unit

# ---------------------------------------------------------------------------
# Shared helpers
# ---------------------------------------------------------------------------


def _mock_gateway(namespace: str = "hbot", instance_id: str = "bot1") -> MagicMock:
    """Return a minimal MQTTGateway stand-in for factory tests."""
    gw = MagicMock()
    gw._config.namespace = namespace
    gw.app.instance_id = instance_id
    gw.topic_for.side_effect = lambda topic, bot_prefix=True: (
        f"{namespace}/{instance_id}/{topic.lstrip('/')}" if bot_prefix else topic.lstrip("/")
    )
    gw._node_context.create_subscriber = MagicMock(side_effect=lambda **kw: MagicMock())
    gw._node_context.create_publisher = MagicMock(side_effect=lambda **kw: MagicMock())
    return gw


def _noop_clb(msg: object, topic: str) -> None:
    pass


# ---------------------------------------------------------------------------
# ETopicListenerFactory
# ---------------------------------------------------------------------------


class TestETopicListenerFactory:
    def test_create_returns_etopic_listener(self) -> None:
        """create() produces a live ETopicListener."""
        gw = _mock_gateway()
        listener = ETopicListenerFactory.create(gw, "test/a/b", _noop_clb, use_bot_prefix=False)
        assert isinstance(listener, ETopicListener)

    def test_create_subscribes_to_correct_topic_no_prefix(self) -> None:
        """create() with use_bot_prefix=False subscribes to the raw topic."""
        gw = _mock_gateway()
        ETopicListenerFactory.create(gw, "test/a/b", _noop_clb, use_bot_prefix=False)
        call_kwargs = gw._node_context.create_subscriber.call_args[1]
        assert call_kwargs["topic"] == "test/a/b"

    def test_create_subscribes_to_prefixed_topic(self) -> None:
        """create() with use_bot_prefix=True prefixes the topic."""
        gw = _mock_gateway(namespace="hbot", instance_id="bot1")
        ETopicListenerFactory.create(gw, "test/a/b", _noop_clb, use_bot_prefix=True)
        call_kwargs = gw._node_context.create_subscriber.call_args[1]
        assert call_kwargs["topic"] == "hbot/bot1/test/a/b"

    def test_remove_calls_listener_stop(self) -> None:
        """remove() calls stop() on the listener."""
        gw = _mock_gateway()
        listener = ETopicListenerFactory.create(gw, "t", _noop_clb, use_bot_prefix=False)
        subscriber_mock = gw._node_context.create_subscriber.return_value
        ETopicListenerFactory.remove(listener)
        subscriber_mock.stop.assert_called_once()

    def test_create_listener_is_not_none(self) -> None:
        """Mirrors upstream assertion: listener is not None."""
        gw = _mock_gateway()
        listener = ETopicListenerFactory.create(gw, "test/a/b", _noop_clb)
        assert listener is not None


# ---------------------------------------------------------------------------
# ETopicQueueFactory
# ---------------------------------------------------------------------------


class TestETopicQueueFactory:
    def test_create_returns_deque(self) -> None:
        """create() returns a deque instance."""
        gw = _mock_gateway()
        queue = ETopicQueueFactory.create(gw, "test/a/b", use_bot_prefix=False)
        assert isinstance(queue, deque)

    def test_create_subscribes_to_topic(self) -> None:
        """create() registers a subscriber on the gateway node context."""
        gw = _mock_gateway()
        ETopicQueueFactory.create(gw, "test/a/b", use_bot_prefix=False)
        gw._node_context.create_subscriber.assert_called_once()

    def test_create_default_maxlen(self) -> None:
        """Default queue_size=1000 produces a deque with maxlen=1000."""
        gw = _mock_gateway()
        queue = ETopicQueueFactory.create(gw, "t", use_bot_prefix=False)
        assert queue.maxlen == 1000

    def test_create_custom_queue_size(self) -> None:
        """custom queue_size is honoured."""
        gw = _mock_gateway()
        queue = ETopicQueueFactory.create(gw, "t", queue_size=5, use_bot_prefix=False)
        assert queue.maxlen == 5

    def test_on_message_appends_tuple(self) -> None:
        """_on_message appends (topic, msg) to the queue.

        Mirrors upstream: ETopicQueueFactory._on_message(dq, {"a": 1}, "test/external")
        """
        dq: deque[tuple[str, object]] = deque()
        ETopicQueueFactory._on_message(dq, {"a": 1}, "test/external")
        assert len(dq) == 1
        assert dq[0] == ("test/external", {"a": 1})

    def test_on_message_multiple_entries(self) -> None:
        """Multiple _on_message calls all append correctly."""
        dq: deque[tuple[str, object]] = deque()
        ETopicQueueFactory._on_message(dq, {"x": 1}, "t/a")
        ETopicQueueFactory._on_message(dq, {"x": 2}, "t/b")
        assert dq[0] == ("t/a", {"x": 1})
        assert dq[1] == ("t/b", {"x": 2})

    def test_on_message_respects_maxlen(self) -> None:
        """Queue bounded by maxlen drops oldest entries."""
        dq: deque[tuple[str, object]] = deque(maxlen=2)
        ETopicQueueFactory._on_message(dq, {"n": 1}, "t")
        ETopicQueueFactory._on_message(dq, {"n": 2}, "t")
        ETopicQueueFactory._on_message(dq, {"n": 3}, "t")
        assert len(dq) == 2
        assert dq[0][1] == {"n": 2}
        assert dq[1][1] == {"n": 3}


# ---------------------------------------------------------------------------
# EEventQueueFactory
# ---------------------------------------------------------------------------


class TestEEventQueueFactory:
    def test_create_returns_deque(self) -> None:
        """create() returns a deque instance."""
        gw = _mock_gateway()
        queue = EEventQueueFactory.create(gw, "test")
        assert isinstance(queue, deque)

    def test_create_subscribes_on_node_context(self) -> None:
        """create() registers a subscriber via gateway._node_context."""
        gw = _mock_gateway()
        EEventQueueFactory.create(gw, "test")
        gw._node_context.create_subscriber.assert_called_once()

    def test_create_subscribes_to_external_event_topic(self) -> None:
        """Subscriber topic is external/{event_name}."""
        gw = _mock_gateway()
        EEventQueueFactory.create(gw, "myevent")
        call_kwargs = gw._node_context.create_subscriber.call_args[1]
        assert call_kwargs["topic"] == "external/myevent"

    def test_on_event_appends_tuple(self) -> None:
        """_on_event appends (event_name, msg) to the queue.

        Mirrors upstream: EEventQueueFactory._on_event(dq, {"a": 1}, "testevent")
        then assert dq[0] == ("testevent", {"a": 1}).
        """
        dq: deque[tuple[str, object]] = deque()
        EEventQueueFactory._on_event(dq, "testevent", {"a": 1})
        assert len(dq) == 1
        assert dq[0] == ("testevent", {"a": 1})

    def test_on_event_multiple_entries_preserve_order(self) -> None:
        """Multiple _on_event calls append in order."""
        dq: deque[tuple[str, object]] = deque()
        EEventQueueFactory._on_event(dq, "e1", {"n": 1})
        EEventQueueFactory._on_event(dq, "e2", {"n": 2})
        assert dq[0] == ("e1", {"n": 1})
        assert dq[1] == ("e2", {"n": 2})


# ---------------------------------------------------------------------------
# EEventListenerFactory
# ---------------------------------------------------------------------------


class TestEEventListenerFactory:
    def test_create_registers_subscriber(self) -> None:
        """create() registers a commlib subscriber via gateway._node_context."""
        gw = _mock_gateway()
        EEventListenerFactory.create(gw, "myevent", _noop_clb)
        gw._node_context.create_subscriber.assert_called_once()

    def test_create_subscribes_to_external_event_topic(self) -> None:
        """Subscriber topic is external/{event_name}."""
        gw = _mock_gateway()
        EEventListenerFactory.create(gw, "myevent", _noop_clb)
        call_kwargs = gw._node_context.create_subscriber.call_args[1]
        assert call_kwargs["topic"] == "external/myevent"

    def test_create_callback_receives_msg_and_event_name(self) -> None:
        """Dispatched callback receives (msg, event_name) matching upstream signature."""
        received: list[tuple[object, str]] = []

        def clb(msg: object, event_name: str) -> None:
            received.append((msg, event_name))

        gw = _mock_gateway()
        EEventListenerFactory.create(gw, "testevent", clb)
        # Extract the on_message callable registered with create_subscriber
        on_message = gw._node_context.create_subscriber.call_args[1]["on_message"]
        on_message({"data": 42})
        assert received == [({"data": 42}, "testevent")]

    def test_create_callback_exception_does_not_propagate(self) -> None:
        """Dispatch wrapper catches callback exceptions silently."""

        def bad_clb(msg: object, event_name: str) -> None:
            raise ValueError("boom")

        gw = _mock_gateway()
        EEventListenerFactory.create(gw, "ev", bad_clb)
        on_message = gw._node_context.create_subscriber.call_args[1]["on_message"]
        # Must not raise
        on_message({"x": 1})

    def test_remove_is_noop_and_emits_warning(
        self, caplog: pytest.LogCaptureFixture
    ) -> None:
        """EEventListenerFactory.remove() is documented as a no-op due to commlib limitation.

        The method MUST:
        1. Not raise any exception.
        2. Log a WARNING explaining the limitation.

        This limitation is tracked as a follow-up gap; per-callback unsubscribe
        requires NodeContext-level support that commlib-py 0.13.x does not expose.
        """
        gw = _mock_gateway()
        with caplog.at_level("WARNING", logger="remote_iface.external.events"):
            EEventListenerFactory.remove(gw, "myevent", _noop_clb)
        assert len(caplog.records) >= 1
        assert any("unsubscribe" in r.message or "not supported" in r.message for r in caplog.records)

    def test_remove_does_not_raise(self) -> None:
        """remove() must complete without raising regardless of state."""
        gw = _mock_gateway()
        # No exception must escape
        EEventListenerFactory.remove(gw, "ev", _noop_clb)


# ---------------------------------------------------------------------------
# ExternalTopicFactory facade
# ---------------------------------------------------------------------------


class TestExternalTopicFactory:
    def test_create_async_returns_etopic_listener(self) -> None:
        """create_async() returns a non-None ETopicListener."""
        gw = _mock_gateway()
        listener = ExternalTopicFactory.create_async(gw, "test/a/b", _noop_clb)
        assert listener is not None
        assert isinstance(listener, ETopicListener)

    def test_create_async_default_use_bot_prefix_false(self) -> None:
        """Default use_bot_prefix=False — raw topic is used (mirrors upstream default)."""
        gw = _mock_gateway()
        ExternalTopicFactory.create_async(gw, "my/raw/topic", _noop_clb)
        call_kwargs = gw._node_context.create_subscriber.call_args[1]
        assert call_kwargs["topic"] == "my/raw/topic"

    def test_create_async_with_bot_prefix(self) -> None:
        """create_async with use_bot_prefix=True prefixes the topic."""
        gw = _mock_gateway(namespace="hbot", instance_id="b1")
        ExternalTopicFactory.create_async(gw, "t/a", _noop_clb, use_bot_prefix=True)
        call_kwargs = gw._node_context.create_subscriber.call_args[1]
        assert call_kwargs["topic"] == "hbot/b1/t/a"

    def test_create_queue_returns_deque(self) -> None:
        """create_queue() returns a deque."""
        gw = _mock_gateway()
        queue = ExternalTopicFactory.create_queue(gw, "test/a/b")
        assert isinstance(queue, deque)

    def test_create_queue_default_use_bot_prefix_false(self) -> None:
        """create_queue default use_bot_prefix=False — raw topic subscribed."""
        gw = _mock_gateway()
        ExternalTopicFactory.create_queue(gw, "t/raw")
        call_kwargs = gw._node_context.create_subscriber.call_args[1]
        assert call_kwargs["topic"] == "t/raw"

    def test_remove_listener_stops_listener(self) -> None:
        """remove_listener() calls stop() on the listener."""
        gw = _mock_gateway()
        listener = ExternalTopicFactory.create_async(gw, "t", _noop_clb)
        subscriber_mock = gw._node_context.create_subscriber.return_value
        ExternalTopicFactory.remove_listener(listener)
        subscriber_mock.stop.assert_called_once()


# ---------------------------------------------------------------------------
# ExternalEventFactory facade
# ---------------------------------------------------------------------------


class TestExternalEventFactory:
    def test_create_queue_returns_deque(self) -> None:
        """create_queue() returns a deque instance."""
        gw = _mock_gateway()
        queue = ExternalEventFactory.create_queue(gw, "test")
        assert isinstance(queue, deque)

    def test_create_queue_subscribes_to_external_topic(self) -> None:
        """create_queue() subscribes to external/{event_name}."""
        gw = _mock_gateway()
        ExternalEventFactory.create_queue(gw, "myevent")
        call_kwargs = gw._node_context.create_subscriber.call_args[1]
        assert call_kwargs["topic"] == "external/myevent"

    def test_create_async_registers_subscriber(self) -> None:
        """create_async() registers a commlib subscriber."""
        gw = _mock_gateway()
        ExternalEventFactory.create_async(gw, "test.a.b", _noop_clb)
        gw._node_context.create_subscriber.assert_called_once()

    def test_create_async_subscribes_to_external_topic(self) -> None:
        """create_async() topic is external/{event_name}."""
        gw = _mock_gateway()
        ExternalEventFactory.create_async(gw, "myevent", _noop_clb)
        call_kwargs = gw._node_context.create_subscriber.call_args[1]
        assert call_kwargs["topic"] == "external/myevent"

    def test_remove_listener_is_noop_and_logs_warning(
        self, caplog: pytest.LogCaptureFixture
    ) -> None:
        """remove_listener() delegates to EEventListenerFactory.remove() — no-op + WARNING."""
        gw = _mock_gateway()
        with caplog.at_level("WARNING", logger="remote_iface.external.events"):
            ExternalEventFactory.remove_listener(gw, "test.a.b", _noop_clb)
        assert len(caplog.records) >= 1

    def test_remove_listener_does_not_raise(self) -> None:
        """remove_listener() must not raise."""
        gw = _mock_gateway()
        ExternalEventFactory.remove_listener(gw, "ev", _noop_clb)

"""Unit tests for ETopicListener.

Verifies behavioural parity with upstream test_etopic_listener_class.  Gateway
is mocked — no real MQTT broker required.
"""

from __future__ import annotations

from unittest.mock import MagicMock

import pytest

from remote_iface.external.listeners import ETopicListener

pytestmark = pytest.mark.unit

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _mock_gateway(namespace: str = "hbot", instance_id: str = "bot1") -> MagicMock:
    """Return a minimal MQTTGateway stand-in for listener tests."""
    gw = MagicMock()
    gw._config.namespace = namespace
    gw.app.instance_id = instance_id
    gw.topic_for.side_effect = lambda topic, bot_prefix=True: (
        f"{namespace}/{instance_id}/{topic.lstrip('/')}" if bot_prefix else topic.lstrip("/")
    )
    # create_subscriber returns a new Mock so stop() can be asserted on it
    gw._node_context.create_subscriber = MagicMock(side_effect=lambda **kw: MagicMock())
    return gw


# ---------------------------------------------------------------------------
# Construction
# ---------------------------------------------------------------------------


class TestETopicListenerConstruction:
    def test_construction_no_prefix_calls_create_subscriber(self) -> None:
        """ETopicListener subscribes to the raw topic when use_bot_prefix=False."""

        def clb(msg: object, topic: str) -> None:
            pass

        gw = _mock_gateway()
        listener = ETopicListener(gw, "test", clb, use_bot_prefix=False)
        assert listener is not None
        gw._node_context.create_subscriber.assert_called_once()
        # Topic passed must be the raw string
        call_kwargs = gw._node_context.create_subscriber.call_args[1]
        assert call_kwargs["topic"] == "test"

    def test_construction_with_prefix_uses_prefixed_topic(self) -> None:
        """ETopicListener subscribes to {namespace}/{instance_id}/{topic} when use_bot_prefix=True."""

        def clb(msg: object, topic: str) -> None:
            pass

        gw = _mock_gateway(namespace="hbot", instance_id="bot1")
        listener = ETopicListener(gw, "test", clb, use_bot_prefix=True)
        assert listener is not None
        call_kwargs = gw._node_context.create_subscriber.call_args[1]
        assert call_kwargs["topic"] == "hbot/bot1/test"

    def test_construction_default_prefix_is_true(self) -> None:
        """Default use_bot_prefix=True — topic must be prefixed."""

        def clb(msg: object, topic: str) -> None:
            pass

        gw = _mock_gateway(namespace="hbot", instance_id="mybot")
        ETopicListener(gw, "events/foo", clb)
        call_kwargs = gw._node_context.create_subscriber.call_args[1]
        assert call_kwargs["topic"] == "hbot/mybot/events/foo"

    def test_construction_leading_slash_stripped(self) -> None:
        """Leading slash is stripped from topics when use_bot_prefix=True."""

        def clb(msg: object, topic: str) -> None:
            pass

        gw = _mock_gateway(namespace="hbot", instance_id="bot1")
        ETopicListener(gw, "/foo", clb, use_bot_prefix=True)
        call_kwargs = gw._node_context.create_subscriber.call_args[1]
        assert call_kwargs["topic"] == "hbot/bot1/foo"


# ---------------------------------------------------------------------------
# Dispatch
# ---------------------------------------------------------------------------


class TestETopicListenerDispatch:
    def test_dispatch_calls_callback_with_msg_and_topic(self) -> None:
        """_dispatch forwards (msg, resolved_topic) to the user callback."""
        received: list[tuple[object, str]] = []

        def clb(msg: object, topic: str) -> None:
            received.append((msg, topic))

        gw = _mock_gateway(namespace="hbot", instance_id="bot1")
        listener = ETopicListener(gw, "t/a", clb, use_bot_prefix=True)
        listener._dispatch({"key": "val"})

        assert len(received) == 1
        assert received[0] == ({"key": "val"}, "hbot/bot1/t/a")

    def test_dispatch_callback_exception_does_not_propagate(self) -> None:
        """If the callback raises, _dispatch logs a warning but does not re-raise."""

        def bad_clb(msg: object, topic: str) -> None:
            raise ValueError("oops")

        gw = _mock_gateway()
        listener = ETopicListener(gw, "t", bad_clb, use_bot_prefix=False)
        # Must not raise
        listener._dispatch({"data": 1})

    def test_dispatch_exception_logged_as_warning(
        self, caplog: pytest.LogCaptureFixture
    ) -> None:
        """Callback exceptions produce a WARNING log entry."""

        def bad_clb(msg: object, topic: str) -> None:
            raise RuntimeError("dispatch-fail")

        gw = _mock_gateway()
        listener = ETopicListener(gw, "t", bad_clb, use_bot_prefix=False)
        with caplog.at_level("WARNING", logger="remote_iface.external.listeners"):
            listener._dispatch({})
        assert "dispatch-fail" in caplog.text or "RuntimeError" in caplog.text


# ---------------------------------------------------------------------------
# Stop
# ---------------------------------------------------------------------------


class TestETopicListenerStop:
    def test_stop_calls_subscriber_stop(self) -> None:
        """stop() delegates to the underlying commlib Subscriber.stop()."""

        def clb(msg: object, topic: str) -> None:
            pass

        gw = _mock_gateway()
        listener = ETopicListener(gw, "t", clb, use_bot_prefix=False)
        subscriber_mock = gw._node_context.create_subscriber.return_value
        listener.stop()
        subscriber_mock.stop.assert_called_once()

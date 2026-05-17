"""Unit tests for ETopicPublisher and EMTopicPublisher.

Verifies behavioural parity with upstream hummingbot/remote_iface/mqtt.py tests
(test_etopic_publisher, test_etopic_publisher_*).  Gateway is mocked via
_mock_gateway() — no real MQTT broker required.
"""

from __future__ import annotations

from unittest.mock import MagicMock, call

import pytest

from remote_iface.external.publishers import EMTopicPublisher, ETopicPublisher

pytestmark = pytest.mark.unit

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _mock_gateway(namespace: str = "hummingbot", instance_id: str = "test-bot") -> MagicMock:
    """Return a minimal MQTTGateway stand-in sufficient for publisher tests."""
    gw = MagicMock()
    gw._config.namespace = namespace
    gw.app.instance_id = instance_id
    # topic_for() replicates gateway.topic_for semantics inline so tests stay self-contained
    gw.topic_for.side_effect = lambda topic, bot_prefix=True: (
        f"{namespace}/{instance_id}/{topic.lstrip('/')}" if bot_prefix else topic.lstrip("/")
    )
    # create_publisher returns a fresh Mock each call (distinct commlib publisher per topic)
    gw._node_context.create_publisher = MagicMock(side_effect=lambda **kw: MagicMock())
    return gw


# ---------------------------------------------------------------------------
# ETopicPublisher
# ---------------------------------------------------------------------------


class TestETopicPublisher:
    def test_construction_calls_create_publisher(self) -> None:
        """ETopicPublisher.__init__ creates exactly one commlib publisher."""
        gw = _mock_gateway()
        ETopicPublisher(gw, "foo/bar", use_bot_prefix=False)
        gw._node_context.create_publisher.assert_called_once_with(
            topic="foo/bar", msg_type=dict
        )

    def test_send_publishes_to_correct_topic_no_prefix(self) -> None:
        """With use_bot_prefix=False the raw topic is used."""
        gw = _mock_gateway()
        pub = ETopicPublisher(gw, "test/a/b", use_bot_prefix=False)
        msg = {"a": "test", "b": 1}
        pub.send(msg)
        # Underlying commlib publisher must receive the message
        commlib_pub = gw._node_context.create_publisher.return_value
        commlib_pub.publish.assert_called_once_with(msg)
        # Topic passed to create_publisher must be the raw string
        gw._node_context.create_publisher.assert_called_once_with(
            topic="test/a/b", msg_type=dict
        )

    def test_send_publishes_to_prefixed_topic_with_bot_prefix(self) -> None:
        """With use_bot_prefix=True the topic is {namespace}/{instance_id}/{topic}."""
        gw = _mock_gateway(namespace="hbot", instance_id="bot1")
        pub = ETopicPublisher(gw, "foo", use_bot_prefix=True)
        expected_topic = "hbot/bot1/foo"
        gw._node_context.create_publisher.assert_called_once_with(
            topic=expected_topic, msg_type=dict
        )
        msg = {"x": 42}
        pub.send(msg)
        gw._node_context.create_publisher.return_value.publish.assert_called_once_with(msg)

    def test_send_leading_slash_stripped_with_bot_prefix(self) -> None:
        """/foo with bot_prefix=True should produce ns/iid/foo (no double slash)."""
        gw = _mock_gateway(namespace="hbot", instance_id="bot1")
        ETopicPublisher(gw, "/foo", use_bot_prefix=True)
        gw._node_context.create_publisher.assert_called_once_with(
            topic="hbot/bot1/foo", msg_type=dict
        )

    def test_call_dunder_delegates_to_send(self) -> None:
        """__call__ is an alias for send()."""
        gw = _mock_gateway()
        pub = ETopicPublisher(gw, "t", use_bot_prefix=False)
        msg = {"z": 0}
        pub(msg)
        gw._node_context.create_publisher.return_value.publish.assert_called_once_with(msg)


# ---------------------------------------------------------------------------
# EMTopicPublisher
# ---------------------------------------------------------------------------


class TestEMTopicPublisher:
    def test_construction_does_not_create_publisher(self) -> None:
        """EMTopicPublisher is lazy — no commlib publisher allocated at construction."""
        gw = _mock_gateway()
        EMTopicPublisher(gw, use_bot_prefix=False)
        gw._node_context.create_publisher.assert_not_called()

    def test_send_no_prefix_uses_raw_topic(self) -> None:
        """use_bot_prefix=False keeps topic as-is."""
        gw = _mock_gateway()
        pub = EMTopicPublisher(gw, use_bot_prefix=False)
        msg = {"a": "test"}
        pub.send("test/a/b", msg)
        gw._node_context.create_publisher.assert_called_once_with(
            topic="test/a/b", msg_type=dict
        )
        gw._node_context.create_publisher.return_value.publish.assert_called_once_with(msg)

    def test_send_with_prefix_prefixes_topic(self) -> None:
        """use_bot_prefix=True prepends {namespace}/{instance_id}/."""
        gw = _mock_gateway(namespace="hbot", instance_id="bot1")
        pub = EMTopicPublisher(gw, use_bot_prefix=True)
        msg = {"b": 2}
        pub.send("my/topic", msg)
        gw._node_context.create_publisher.assert_called_once_with(
            topic="hbot/bot1/my/topic", msg_type=dict
        )

    def test_publisher_caching_same_topic_reuses_publisher(self) -> None:
        """Sending to the same topic twice must NOT create a second commlib publisher."""
        gw = _mock_gateway()
        pub = EMTopicPublisher(gw, use_bot_prefix=False)
        msg = {"v": 1}
        pub.send("t/a", msg)
        pub.send("t/a", msg)
        # Only one publisher created
        assert gw._node_context.create_publisher.call_count == 1
        # But publish called twice
        assert gw._node_context.create_publisher.return_value.publish.call_count == 2

    def test_publisher_caching_different_topics_create_separate_publishers(self) -> None:
        """Sending to two different topics should allocate two distinct commlib publishers."""
        # Give create_publisher a fresh Mock each call so we can track them independently
        pub_a = MagicMock()
        pub_b = MagicMock()
        gw = _mock_gateway()
        gw._node_context.create_publisher.side_effect = [pub_a, pub_b]

        pub = EMTopicPublisher(gw, use_bot_prefix=False)
        msg = {"m": 0}
        pub.send("t/a", msg)
        pub.send("t/b", msg)

        assert gw._node_context.create_publisher.call_count == 2
        pub_a.publish.assert_called_once_with(msg)
        pub_b.publish.assert_called_once_with(msg)

    def test_send_three_calls_two_topics_one_reuse(self) -> None:
        """Third send to first topic reuses cached publisher (no third allocation)."""
        pub_a = MagicMock()
        pub_b = MagicMock()
        gw = _mock_gateway()
        gw._node_context.create_publisher.side_effect = [pub_a, pub_b]

        pub = EMTopicPublisher(gw, use_bot_prefix=False)
        pub.send("t/a", {"x": 1})
        pub.send("t/b", {"x": 2})
        pub.send("t/a", {"x": 3})  # reuse

        assert gw._node_context.create_publisher.call_count == 2
        assert pub_a.publish.call_count == 2
        assert pub_b.publish.call_count == 1

    def test_call_dunder_delegates_to_send(self) -> None:
        """__call__(topic, msg) is an alias for send(topic, msg)."""
        gw = _mock_gateway()
        pub = EMTopicPublisher(gw, use_bot_prefix=False)
        msg = {"z": 0}
        pub("t/c", msg)
        gw._node_context.create_publisher.assert_called_once()
        gw._node_context.create_publisher.return_value.publish.assert_called_once_with(msg)

    def test_send_multiple_topics_no_prefix(self) -> None:
        """Mirrors upstream test: send two different topics, both publish."""
        gw = _mock_gateway()
        pub = EMTopicPublisher(gw, use_bot_prefix=False)
        test_msg = {"a": "test", "b": 1, "c": False, "d": {}, "e": []}
        pub.send("test/a/b", test_msg)
        pub.send("test/c/d", test_msg)
        assert gw._node_context.create_publisher.call_count == 2

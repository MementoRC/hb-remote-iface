"""Unit tests for MQTTNotifier gateway component."""

from __future__ import annotations

from unittest.mock import MagicMock

import pytest

from remote_iface.gateway.notifier import MQTTNotifier
from remote_iface.protocols.app import HummingbotAppProtocol
from remote_iface.protocols.config import GatewayConfig

pytestmark = pytest.mark.unit


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _make_gateway(enable_notifier: bool = True) -> MagicMock:
    gw = MagicMock()
    gw._config = GatewayConfig(enable_notifier=enable_notifier, namespace="hb")
    gw._app = MagicMock(spec=HummingbotAppProtocol)
    gw._endpoints = []
    mock_pub = MagicMock()
    gw._node_context.create_publisher.return_value = mock_pub
    return gw


# ---------------------------------------------------------------------------
# Gating
# ---------------------------------------------------------------------------


def test_start_is_noop_when_disabled() -> None:
    gw = _make_gateway(enable_notifier=False)
    n = MQTTNotifier()
    n.start(gw)
    gw._node_context.create_publisher.assert_not_called()
    gw._app.register_notifier.assert_not_called()
    assert gw._endpoints == []


# ---------------------------------------------------------------------------
# start()
# ---------------------------------------------------------------------------


def test_start_creates_publisher_on_correct_topic() -> None:
    gw = _make_gateway()
    n = MQTTNotifier()
    n.start(gw)
    gw._node_context.create_publisher.assert_called_once()
    call_kwargs = gw._node_context.create_publisher.call_args.kwargs
    assert call_kwargs["topic"] == "hb/notify"


def test_start_appends_publisher_to_endpoints() -> None:
    gw = _make_gateway()
    n = MQTTNotifier()
    n.start(gw)
    assert len(gw._endpoints) == 1
    assert gw._endpoints[0] is gw._node_context.create_publisher.return_value


def test_start_registers_notifier_with_app() -> None:
    gw = _make_gateway()
    n = MQTTNotifier()
    n.start(gw)
    gw._app.register_notifier.assert_called_once_with(n)


# ---------------------------------------------------------------------------
# stop()
# ---------------------------------------------------------------------------


def test_stop_unregisters_notifier_from_app() -> None:
    gw = _make_gateway()
    n = MQTTNotifier()
    n.start(gw)
    n.stop(gw)
    gw._app.unregister_notifier.assert_called_once_with(n)


def test_stop_stops_publisher() -> None:
    gw = _make_gateway()
    n = MQTTNotifier()
    n.start(gw)
    pub = n._publisher
    n.stop(gw)
    pub.stop.assert_called_once()


def test_stop_removes_publisher_from_endpoints() -> None:
    gw = _make_gateway()
    n = MQTTNotifier()
    n.start(gw)
    n.stop(gw)
    assert gw._endpoints == []


def test_stop_suppresses_unregister_exception() -> None:
    gw = _make_gateway()
    gw._app.unregister_notifier.side_effect = RuntimeError("boom")
    n = MQTTNotifier()
    n.start(gw)
    n.stop(gw)  # must not raise


# ---------------------------------------------------------------------------
# add_msg_to_queue()
# ---------------------------------------------------------------------------


def test_add_msg_to_queue_publishes_via_publisher() -> None:
    gw = _make_gateway()
    n = MQTTNotifier()
    n.start(gw)
    pub = n._publisher
    n.add_msg_to_queue("hello")
    pub.publish.assert_called_once()
    payload = pub.publish.call_args.args[0]
    assert payload.msg == "hello"
    assert payload.seq == 1


def test_add_msg_to_queue_increments_seq() -> None:
    gw = _make_gateway()
    n = MQTTNotifier()
    n.start(gw)
    n.add_msg_to_queue("a")
    n.add_msg_to_queue("b")
    assert n._publisher.publish.call_count == 2
    payloads = [c.args[0] for c in n._publisher.publish.call_args_list]
    assert payloads[0].seq == 1
    assert payloads[1].seq == 2


def test_add_msg_to_queue_noop_when_no_publisher() -> None:
    n = MQTTNotifier()
    n.add_msg_to_queue("no crash")  # publisher is None — must not raise


def test_add_msg_to_queue_suppresses_publish_exception() -> None:
    gw = _make_gateway()
    n = MQTTNotifier()
    n.start(gw)
    n._publisher.publish.side_effect = RuntimeError("broker down")
    n.add_msg_to_queue("message")  # must not raise

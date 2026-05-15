"""Unit tests for MQTTStatusUpdates gateway component."""

from __future__ import annotations

from unittest.mock import MagicMock

import pytest

from remote_iface.gateway.status import MQTTStatusUpdates
from remote_iface.protocols.app import HummingbotAppProtocol
from remote_iface.protocols.config import GatewayConfig

pytestmark = pytest.mark.unit


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _make_gateway(enable_status_updates: bool = True) -> MagicMock:
    gw = MagicMock()
    gw._config = GatewayConfig(enable_status_updates=enable_status_updates, namespace="hb")
    gw._app = MagicMock(spec=HummingbotAppProtocol)
    gw._endpoints = []
    mock_pub = MagicMock()
    gw._node_context.create_publisher.return_value = mock_pub
    # register_status_listener returns an unsubscribe callable
    unsubscribe = MagicMock()
    gw._app.register_status_listener.return_value = unsubscribe
    return gw


# ---------------------------------------------------------------------------
# Gating
# ---------------------------------------------------------------------------


def test_start_is_noop_when_disabled() -> None:
    gw = _make_gateway(enable_status_updates=False)
    s = MQTTStatusUpdates()
    s.start(gw)
    gw._node_context.create_publisher.assert_not_called()
    gw._app.register_status_listener.assert_not_called()


# ---------------------------------------------------------------------------
# start()
# ---------------------------------------------------------------------------


def test_start_creates_publisher() -> None:
    gw = _make_gateway()
    s = MQTTStatusUpdates()
    s.start(gw)
    gw._node_context.create_publisher.assert_called_once()
    call_kwargs = gw._node_context.create_publisher.call_args.kwargs
    assert call_kwargs["topic"] == "hb/status"


def test_start_appends_publisher_to_endpoints() -> None:
    gw = _make_gateway()
    s = MQTTStatusUpdates()
    s.start(gw)
    assert len(gw._endpoints) == 1


def test_start_registers_status_listener() -> None:
    gw = _make_gateway()
    s = MQTTStatusUpdates()
    s.start(gw)
    gw._app.register_status_listener.assert_called_once()
    # The callback passed must be callable
    cb = gw._app.register_status_listener.call_args.args[0]
    assert callable(cb)


def test_no_periodic_timer_created() -> None:
    """Assert that start() does NOT create any asyncio tasks or sleep loops.

    The push-on-demand design means status updates are triggered only by the app
    calling the registered listener callback — never by an internal periodic timer.
    """
    gw = _make_gateway()
    s = MQTTStatusUpdates()
    s.start(gw)
    # Verify no asyncio task attribute exists on the component
    assert not hasattr(s, "_task") or getattr(s, "_task", None) is None
    assert not hasattr(s, "_loop") or getattr(s, "_loop", None) is None


# ---------------------------------------------------------------------------
# stop()
# ---------------------------------------------------------------------------


def test_stop_calls_unsubscribe() -> None:
    gw = _make_gateway()
    unsubscribe = gw._app.register_status_listener.return_value
    s = MQTTStatusUpdates()
    s.start(gw)
    s.stop(gw)
    unsubscribe.assert_called_once()


def test_stop_stops_publisher() -> None:
    gw = _make_gateway()
    s = MQTTStatusUpdates()
    s.start(gw)
    pub = s._publisher
    s.stop(gw)
    pub.stop.assert_called_once()


def test_stop_removes_publisher_from_endpoints() -> None:
    gw = _make_gateway()
    s = MQTTStatusUpdates()
    s.start(gw)
    s.stop(gw)
    assert gw._endpoints == []


def test_stop_suppresses_unsubscribe_exception() -> None:
    gw = _make_gateway()
    gw._app.register_status_listener.return_value = MagicMock(side_effect=RuntimeError("boom"))
    s = MQTTStatusUpdates()
    s.start(gw)
    s.stop(gw)  # must not raise


# ---------------------------------------------------------------------------
# Status callback path
# ---------------------------------------------------------------------------


def test_status_event_callback_publishes_message() -> None:
    gw = _make_gateway()
    s = MQTTStatusUpdates()
    s.start(gw)
    cb = gw._app.register_status_listener.call_args.args[0]
    pub = s._publisher
    cb("Running fine", "INFO")
    pub.publish.assert_called_once()
    payload = pub.publish.call_args.args[0]
    assert payload.msg == "Running fine"
    assert payload.type == "INFO"


def test_status_event_callback_noop_when_no_publisher() -> None:
    s = MQTTStatusUpdates()
    # _on_status_event with no publisher — must not raise
    s._on_status_event("msg", "INFO")

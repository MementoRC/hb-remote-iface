"""Unit tests for MQTTExternalEvents gateway component."""

from __future__ import annotations

from unittest.mock import MagicMock

import pytest

from remote_iface.gateway.external_events import MQTTExternalEvents
from remote_iface.protocols.app import HummingbotAppProtocol
from remote_iface.protocols.config import GatewayConfig

pytestmark = pytest.mark.unit


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _make_gateway(enable_external_events: bool = True) -> MagicMock:
    gw = MagicMock()
    gw._config = GatewayConfig(enable_external_events=enable_external_events, namespace="hb")
    gw._app = MagicMock(spec=HummingbotAppProtocol)
    gw._endpoints = []
    mock_sub = MagicMock()
    gw._node_context.create_subscriber.return_value = mock_sub
    return gw


# ---------------------------------------------------------------------------
# Gating
# ---------------------------------------------------------------------------


def test_start_is_noop_when_disabled() -> None:
    gw = _make_gateway(enable_external_events=False)
    e = MQTTExternalEvents()
    e.start(gw)
    gw._node_context.create_subscriber.assert_not_called()
    assert gw._endpoints == []


# ---------------------------------------------------------------------------
# start()
# ---------------------------------------------------------------------------


def test_start_creates_subscriber_on_wildcard_topic() -> None:
    gw = _make_gateway()
    e = MQTTExternalEvents()
    e.start(gw)
    gw._node_context.create_subscriber.assert_called_once()
    call_kwargs = gw._node_context.create_subscriber.call_args.kwargs
    assert call_kwargs["topic"] == "external/+"


def test_start_appends_subscriber_to_endpoints() -> None:
    gw = _make_gateway()
    e = MQTTExternalEvents()
    e.start(gw)
    assert len(gw._endpoints) == 1
    assert gw._endpoints[0] is gw._node_context.create_subscriber.return_value


# ---------------------------------------------------------------------------
# stop()
# ---------------------------------------------------------------------------


def test_stop_stops_subscriber() -> None:
    gw = _make_gateway()
    e = MQTTExternalEvents()
    e.start(gw)
    sub = e._subscriber
    e.stop(gw)
    sub.stop.assert_called_once()


def test_stop_removes_subscriber_from_endpoints() -> None:
    gw = _make_gateway()
    e = MQTTExternalEvents()
    e.start(gw)
    e.stop(gw)
    assert gw._endpoints == []


def test_stop_suppresses_subscriber_exception() -> None:
    gw = _make_gateway()
    e = MQTTExternalEvents()
    e.start(gw)
    e._subscriber.stop.side_effect = RuntimeError("boom")
    e.stop(gw)  # must not raise


# ---------------------------------------------------------------------------
# Dispatch path
# ---------------------------------------------------------------------------


def test_received_message_dispatches_to_app_handle_external_event() -> None:
    """The on_message callback passed to create_subscriber must call app.handle_external_event."""
    gw = _make_gateway()
    e = MQTTExternalEvents()
    e.start(gw)
    # Retrieve the on_message callback from the create_subscriber call
    cb = gw._node_context.create_subscriber.call_args.kwargs["on_message"]
    fake_msg = MagicMock()
    cb(fake_msg)
    gw._app.handle_external_event.assert_called_once_with(fake_msg)


def test_dispatch_suppresses_app_exception() -> None:
    """handle_external_event raising must not propagate out of the subscriber callback."""
    gw = _make_gateway()
    gw._app.handle_external_event.side_effect = RuntimeError("app broke")
    e = MQTTExternalEvents()
    e.start(gw)
    cb = gw._node_context.create_subscriber.call_args.kwargs["on_message"]
    cb(MagicMock())  # must not raise

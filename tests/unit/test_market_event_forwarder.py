"""Unit tests for MQTTMarketEventForwarder gateway component."""

from __future__ import annotations

from unittest.mock import MagicMock

import pytest

from remote_iface.gateway.market_events import MQTTMarketEventForwarder
from remote_iface.protocols.app import HummingbotAppProtocol
from remote_iface.protocols.config import GatewayConfig

pytestmark = pytest.mark.unit


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _make_gateway(enable_events: bool = True) -> MagicMock:
    gw = MagicMock()
    gw._config = GatewayConfig(enable_events=enable_events, namespace="hb")
    gw._app = MagicMock(spec=HummingbotAppProtocol)
    gw._endpoints = []
    mock_pub = MagicMock()
    gw._node_context.create_publisher.return_value = mock_pub
    unsubscribe = MagicMock()
    gw._app.subscribe_market_events.return_value = unsubscribe
    return gw


# ---------------------------------------------------------------------------
# Gating
# ---------------------------------------------------------------------------


def test_start_is_noop_when_disabled() -> None:
    gw = _make_gateway(enable_events=False)
    f = MQTTMarketEventForwarder()
    f.start(gw)
    gw._node_context.create_publisher.assert_not_called()
    gw._app.register_strategy_loaded_callback.assert_not_called()


# ---------------------------------------------------------------------------
# start()
# ---------------------------------------------------------------------------


def test_start_creates_publisher() -> None:
    gw = _make_gateway()
    f = MQTTMarketEventForwarder()
    f.start(gw)
    gw._node_context.create_publisher.assert_called_once()
    call_kwargs = gw._node_context.create_publisher.call_args.kwargs
    assert call_kwargs["topic"] == "hb/market-events"


def test_start_appends_publisher_to_endpoints() -> None:
    gw = _make_gateway()
    f = MQTTMarketEventForwarder()
    f.start(gw)
    assert len(gw._endpoints) == 1


def test_start_does_not_call_subscribe_market_events() -> None:
    """subscribe_market_events must NOT be called from start() — deferred until strategy load."""
    gw = _make_gateway()
    f = MQTTMarketEventForwarder()
    f.start(gw)
    gw._app.subscribe_market_events.assert_not_called()


def test_start_registers_strategy_loaded_callback() -> None:
    gw = _make_gateway()
    f = MQTTMarketEventForwarder()
    f.start(gw)
    gw._app.register_strategy_loaded_callback.assert_called_once()
    cb = gw._app.register_strategy_loaded_callback.call_args.args[0]
    assert callable(cb)


# ---------------------------------------------------------------------------
# Deferred subscription via strategy-loaded callback
# ---------------------------------------------------------------------------


def test_strategy_loaded_callback_calls_subscribe_market_events() -> None:
    gw = _make_gateway()
    f = MQTTMarketEventForwarder()
    f.start(gw)
    # Simulate strategy load
    cb = gw._app.register_strategy_loaded_callback.call_args.args[0]
    cb()
    gw._app.subscribe_market_events.assert_called_once_with(f._forward_event)


def test_strategy_loaded_callback_stores_unsubscribe_fn() -> None:
    gw = _make_gateway()
    unsubscribe = MagicMock()
    gw._app.subscribe_market_events.return_value = unsubscribe
    f = MQTTMarketEventForwarder()
    f.start(gw)
    cb = gw._app.register_strategy_loaded_callback.call_args.args[0]
    cb()
    assert f._unsubscribe is unsubscribe


# ---------------------------------------------------------------------------
# stop()
# ---------------------------------------------------------------------------


def test_stop_calls_unsubscribe_fn_after_strategy_loaded() -> None:
    gw = _make_gateway()
    unsubscribe = MagicMock()
    gw._app.subscribe_market_events.return_value = unsubscribe
    f = MQTTMarketEventForwarder()
    f.start(gw)
    cb = gw._app.register_strategy_loaded_callback.call_args.args[0]
    cb()
    f.stop(gw)
    unsubscribe.assert_called_once()


def test_stop_stops_publisher() -> None:
    gw = _make_gateway()
    f = MQTTMarketEventForwarder()
    f.start(gw)
    pub = f._publisher
    f.stop(gw)
    pub.stop.assert_called_once()


def test_stop_removes_publisher_from_endpoints() -> None:
    gw = _make_gateway()
    f = MQTTMarketEventForwarder()
    f.start(gw)
    f.stop(gw)
    assert gw._endpoints == []


def test_stop_suppresses_unsubscribe_exception() -> None:
    gw = _make_gateway()
    f = MQTTMarketEventForwarder()
    f.start(gw)
    f._unsubscribe = MagicMock(side_effect=RuntimeError("boom"))
    f.stop(gw)  # must not raise


# ---------------------------------------------------------------------------
# _forward_event()
# ---------------------------------------------------------------------------


def test_forward_event_publishes_message() -> None:
    gw = _make_gateway()
    f = MQTTMarketEventForwarder()
    f.start(gw)
    pub = f._publisher
    f._forward_event(42, "pubsub_obj", "event_obj")
    pub.publish.assert_called_once()
    payload = pub.publish.call_args.args[0]
    assert payload.type == "42"


def test_forward_event_noop_when_no_publisher() -> None:
    f = MQTTMarketEventForwarder()
    f._forward_event(1, "a", "b")  # must not raise

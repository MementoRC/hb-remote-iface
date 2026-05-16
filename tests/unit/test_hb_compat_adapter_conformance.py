"""Conformance and behaviour tests for remote_iface.hb_compat.

Covers:
  - HummingbotAppAdapter satisfies HummingbotAppProtocol (isinstance)
  - create_gateway returns a wired MQTTGateway with all components when all
    enable_* toggles are True
  - Disabling a single toggle excludes exactly that component
  - Registry shim behaviour: subscribe_market_events, register_status_listener,
    register_strategy_loaded_callback, handle_external_event
"""

from __future__ import annotations

from unittest.mock import MagicMock

import pytest

from remote_iface.gateway.gateway import MQTTGateway
from remote_iface.hb_compat import HummingbotAppAdapter, create_gateway
from remote_iface.hb_compat.factory import (
    MQTTCommands,
    MQTTExternalEvents,
    MQTTLogHandler,
    MQTTMarketEventForwarder,
    MQTTNotifier,
    MQTTStatusUpdates,
)
from remote_iface.protocols.app import HummingbotAppProtocol
from remote_iface.protocols.config import BrokerConfig, GatewayConfig

pytestmark = pytest.mark.unit

# ---------------------------------------------------------------------------
# Component count constant — update if a new component is added
# ---------------------------------------------------------------------------

_ALL_COMPONENT_TYPES = (
    MQTTCommands,
    MQTTNotifier,
    MQTTStatusUpdates,
    MQTTMarketEventForwarder,
    MQTTExternalEvents,
    MQTTLogHandler,
)
_TOTAL_COMPONENTS = len(_ALL_COMPONENT_TYPES)  # 6


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _make_mock_app() -> MagicMock:
    """Return a MagicMock spec'd to HummingbotAppProtocol."""
    app = MagicMock(spec=HummingbotAppProtocol)
    app.instance_id = "test-bot"
    app.ev_loop = None
    app.strategy_name = None
    app.strategy = None
    app.client_config_map = {}
    app.strategy_config_map = {}
    return app


def _component_types(gateway: MQTTGateway) -> list[type]:
    return [type(c) for c in gateway._components]


# ---------------------------------------------------------------------------
# Protocol conformance
# ---------------------------------------------------------------------------


def test_adapter_satisfies_protocol() -> None:
    """HummingbotAppAdapter must be recognised as HummingbotAppProtocol."""
    adapter = HummingbotAppAdapter(_make_mock_app())
    assert isinstance(adapter, HummingbotAppProtocol)


def test_adapter_is_runtime_checkable() -> None:
    """HummingbotAppProtocol must support isinstance() checks."""
    try:
        isinstance(object(), HummingbotAppProtocol)
    except TypeError:
        pytest.fail("HummingbotAppProtocol is not runtime_checkable")


# ---------------------------------------------------------------------------
# create_gateway — all toggles ON
# ---------------------------------------------------------------------------


def test_create_gateway_returns_mqtt_gateway() -> None:
    gw = create_gateway(_make_mock_app())
    assert isinstance(gw, MQTTGateway)


def test_create_gateway_all_enabled_has_six_components() -> None:
    config = GatewayConfig()  # all enable_* default to True
    gw = create_gateway(_make_mock_app(), config=config)
    assert len(gw._components) == _TOTAL_COMPONENTS


def test_create_gateway_all_enabled_component_types() -> None:
    config = GatewayConfig()
    gw = create_gateway(_make_mock_app(), config=config)
    types = _component_types(gw)
    for cls in _ALL_COMPONENT_TYPES:
        assert cls in types, f"{cls.__name__} missing from gateway components"


def test_create_gateway_uses_default_config_when_none() -> None:
    gw = create_gateway(_make_mock_app(), config=None)
    assert len(gw._components) == _TOTAL_COMPONENTS


def test_create_gateway_uses_default_broker_when_none() -> None:
    gw = create_gateway(_make_mock_app(), broker_config=None)
    assert gw._broker_config == BrokerConfig()


# ---------------------------------------------------------------------------
# create_gateway — individual enable_* toggles OFF
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    ("toggle", "excluded_type"),
    [
        ("enable_commands", MQTTCommands),
        ("enable_notifier", MQTTNotifier),
        ("enable_status_updates", MQTTStatusUpdates),
        ("enable_events", MQTTMarketEventForwarder),
        ("enable_external_events", MQTTExternalEvents),
        ("enable_log_handler", MQTTLogHandler),
    ],
)
def test_create_gateway_single_toggle_off_excludes_component(
    toggle: str, excluded_type: type
) -> None:
    config = GatewayConfig(**{toggle: False})
    gw = create_gateway(_make_mock_app(), config=config)
    types = _component_types(gw)
    assert excluded_type not in types, f"{excluded_type.__name__} should be absent when {toggle}=False"
    assert len(gw._components) == _TOTAL_COMPONENTS - 1


# ---------------------------------------------------------------------------
# Registry shim: subscribe_market_events
# ---------------------------------------------------------------------------


def test_subscribe_market_events_adds_callback() -> None:
    adapter = HummingbotAppAdapter(_make_mock_app())
    cb = MagicMock()
    adapter.subscribe_market_events(cb)
    assert cb in adapter._market_event_callbacks


def test_subscribe_market_events_returns_callable() -> None:
    adapter = HummingbotAppAdapter(_make_mock_app())
    unsub = adapter.subscribe_market_events(MagicMock())
    assert callable(unsub)


def test_subscribe_market_events_unsubscribe_removes_callback() -> None:
    adapter = HummingbotAppAdapter(_make_mock_app())
    cb = MagicMock()
    unsub = adapter.subscribe_market_events(cb)
    assert cb in adapter._market_event_callbacks
    unsub()
    assert cb not in adapter._market_event_callbacks


def test_subscribe_market_events_unsubscribe_idempotent() -> None:
    """Calling the unsubscribe handle twice must not raise."""
    adapter = HummingbotAppAdapter(_make_mock_app())
    unsub = adapter.subscribe_market_events(MagicMock())
    unsub()
    unsub()  # second call must be silent


def test_subscribe_market_events_multiple_callbacks() -> None:
    adapter = HummingbotAppAdapter(_make_mock_app())
    cb_a, cb_b = MagicMock(), MagicMock()
    unsub_a = adapter.subscribe_market_events(cb_a)
    adapter.subscribe_market_events(cb_b)
    unsub_a()
    assert cb_a not in adapter._market_event_callbacks
    assert cb_b in adapter._market_event_callbacks


# ---------------------------------------------------------------------------
# Registry shim: register_status_listener
# ---------------------------------------------------------------------------


def test_register_status_listener_adds_callback() -> None:
    adapter = HummingbotAppAdapter(_make_mock_app())
    cb = MagicMock()
    adapter.register_status_listener(cb)
    assert cb in adapter._status_listeners


def test_register_status_listener_returns_callable() -> None:
    adapter = HummingbotAppAdapter(_make_mock_app())
    unsub = adapter.register_status_listener(MagicMock())
    assert callable(unsub)


def test_register_status_listener_unsubscribe_removes() -> None:
    adapter = HummingbotAppAdapter(_make_mock_app())
    cb = MagicMock()
    unsub = adapter.register_status_listener(cb)
    unsub()
    assert cb not in adapter._status_listeners


def test_register_status_listener_unsubscribe_idempotent() -> None:
    adapter = HummingbotAppAdapter(_make_mock_app())
    unsub = adapter.register_status_listener(MagicMock())
    unsub()
    unsub()  # must not raise


# ---------------------------------------------------------------------------
# Registry shim: register_strategy_loaded_callback
# ---------------------------------------------------------------------------


def test_register_strategy_loaded_callback_adds() -> None:
    adapter = HummingbotAppAdapter(_make_mock_app())
    cb = MagicMock()
    adapter.register_strategy_loaded_callback(cb)
    assert cb in adapter._strategy_loaded_callbacks


def test_register_strategy_loaded_callback_multiple() -> None:
    adapter = HummingbotAppAdapter(_make_mock_app())
    cb_a, cb_b = MagicMock(), MagicMock()
    adapter.register_strategy_loaded_callback(cb_a)
    adapter.register_strategy_loaded_callback(cb_b)
    assert adapter._strategy_loaded_callbacks == [cb_a, cb_b]


# ---------------------------------------------------------------------------
# Registry shim: handle_external_event
# ---------------------------------------------------------------------------


def test_handle_external_event_invokes_registered_handlers() -> None:
    """handle_external_event fans out to all registered external-event handlers."""
    adapter = HummingbotAppAdapter(_make_mock_app())
    handler_a, handler_b = MagicMock(), MagicMock()
    adapter.register_external_event_handler(handler_a)
    adapter.register_external_event_handler(handler_b)
    event = {"key": "value"}
    adapter.handle_external_event(event)
    handler_a.assert_called_once_with(event)
    handler_b.assert_called_once_with(event)


def test_handle_external_event_no_handlers_is_noop() -> None:
    """handle_external_event with no registered handlers must not raise."""
    # Use a mock app without handle_external_event attr to avoid best-effort forward
    bare_app = MagicMock(spec=[])  # spec with no attributes
    adapter = HummingbotAppAdapter(bare_app)
    adapter.handle_external_event({"key": "value"})  # must not raise


def test_handle_external_event_failing_handler_does_not_propagate() -> None:
    """A handler that raises must not prevent subsequent handlers from running."""
    bare_app = MagicMock(spec=[])
    adapter = HummingbotAppAdapter(bare_app)
    bad_handler = MagicMock(side_effect=RuntimeError("boom"))
    good_handler = MagicMock()
    adapter.register_external_event_handler(bad_handler)
    adapter.register_external_event_handler(good_handler)
    adapter.handle_external_event(object())  # must not raise
    good_handler.assert_called_once()


def test_handle_external_event_forwards_to_host_app() -> None:
    """handle_external_event also best-effort forwards to host app if supported."""
    mock_app = _make_mock_app()  # MagicMock spec'd to protocol — has handle_external_event
    adapter = HummingbotAppAdapter(mock_app)
    event = object()
    adapter.handle_external_event(event)
    mock_app.handle_external_event.assert_called_once_with(event)


# ---------------------------------------------------------------------------
# Delegation smoke tests
# ---------------------------------------------------------------------------


def test_adapter_delegates_stop() -> None:
    mock_app = _make_mock_app()
    adapter = HummingbotAppAdapter(mock_app)
    adapter.stop(skip_order_cancellation=True)
    mock_app.stop.assert_called_once_with(skip_order_cancellation=True)


def test_adapter_delegates_config() -> None:
    mock_app = _make_mock_app()
    adapter = HummingbotAppAdapter(mock_app)
    adapter.config(key="log_level", value="DEBUG")
    mock_app.config.assert_called_once_with(key="log_level", value="DEBUG")


async def test_adapter_delegates_stop_loop() -> None:
    mock_app = _make_mock_app()

    async def _stop_loop() -> str:
        return "stopped"

    mock_app.stop_loop = _stop_loop
    adapter = HummingbotAppAdapter(mock_app)
    result = await adapter.stop_loop()
    assert result == "stopped"


async def test_get_history_trades_json_async_wrap_coroutine() -> None:
    """If host returns a coroutine, it must be awaited."""
    mock_app = _make_mock_app()

    async def _history(days: float) -> list[object]:
        return [{"trade": 1}]

    mock_app.get_history_trades_json = _history
    adapter = HummingbotAppAdapter(mock_app)
    result = await adapter.get_history_trades_json(days=1.0)
    assert result == [{"trade": 1}]


async def test_get_history_trades_json_async_wrap_sync() -> None:
    """If host returns a plain list (sync), it must be returned directly."""
    mock_app = _make_mock_app()
    mock_app.get_history_trades_json.return_value = [{"trade": 2}]
    adapter = HummingbotAppAdapter(mock_app)
    result = await adapter.get_history_trades_json(days=1.0)
    assert result == [{"trade": 2}]


# ---------------------------------------------------------------------------
# Public surface — top-level re-exports
# ---------------------------------------------------------------------------


def test_top_level_exports_adapter() -> None:
    import remote_iface

    assert remote_iface.HummingbotAppAdapter is HummingbotAppAdapter


def test_top_level_exports_create_gateway() -> None:
    import remote_iface

    assert remote_iface.create_gateway is create_gateway

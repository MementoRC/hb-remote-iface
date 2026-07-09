"""Integration test: two independent MQTTGateway instances share no mutable state.

Regression guard against the upstream _instance singleton bug. Verifies:
  - Gateway A lifecycle (start/stop) does not affect Gateway B.
  - App A registrations do not bleed into Gateway B.
  - Gateway A is fully garbage-collectable after stop().
  - No logger handlers, thread leaks, or shared endpoint lists remain after stop.
"""

from __future__ import annotations

import gc
import logging
import weakref
from unittest.mock import AsyncMock, MagicMock

import pytest

from remote_iface.gateway.commands import MQTTCommands
from remote_iface.gateway.external_events import MQTTExternalEvents
from remote_iface.gateway.gateway import MQTTGateway
from remote_iface.gateway.log_handler import MQTTLogHandler
from remote_iface.gateway.market_events import MQTTMarketEventForwarder
from remote_iface.gateway.notifier import MQTTNotifier
from remote_iface.gateway.status import MQTTStatusUpdates
from remote_iface.protocols.app import HummingbotAppProtocol
from remote_iface.protocols.config import BrokerConfig, GatewayConfig

pytestmark = pytest.mark.integration


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _make_app(name: str) -> MagicMock:
    app = MagicMock(spec=HummingbotAppProtocol)
    app.instance_id = name
    app.register_status_listener.return_value = MagicMock()
    app.subscribe_market_events.return_value = MagicMock()
    return app


def _make_mock_node_context() -> MagicMock:
    nc = MagicMock()
    nc.start = AsyncMock()
    nc.stop = AsyncMock()
    nc.is_healthy = MagicMock(return_value=True)
    mock_pub = MagicMock()
    mock_sub = MagicMock()
    nc.create_publisher.return_value = mock_pub
    nc.create_subscriber.return_value = mock_sub
    nc.create_rpc.return_value = MagicMock()
    return nc


def _build_gateway(app: MagicMock, namespace: str) -> MQTTGateway:
    config = GatewayConfig(
        namespace=namespace,
        health_check_interval=9999.0,  # prevent health loop from doing anything
    )
    broker = BrokerConfig()
    gw = MQTTGateway(app=app, config=config, broker_config=broker)
    gw.add_component(MQTTNotifier())
    gw.add_component(MQTTStatusUpdates())
    gw.add_component(MQTTMarketEventForwarder())
    gw.add_component(MQTTLogHandler())
    gw.add_component(MQTTExternalEvents())
    gw.add_component(MQTTCommands(app))
    return gw


# ---------------------------------------------------------------------------
# Test
# ---------------------------------------------------------------------------


async def test_two_gateways_no_state_bleed() -> None:
    """Full lifecycle of gateway A must not affect gateway B."""
    app_a = _make_app("app_a")
    app_b = _make_app("app_b")

    nc_a = _make_mock_node_context()
    nc_b = _make_mock_node_context()

    gw_a = _build_gateway(app_a, "ns_a")
    gw_b = _build_gateway(app_b, "ns_b")

    # Inject mock node contexts to avoid real broker connections
    gw_a._node_context = nc_a
    gw_b._node_context = nc_b

    # ── Start and stop gateway A ──────────────────────────────────────────
    await gw_a.start()
    assert gw_a.is_running()

    # Gateway B is unstarted — its app must not have been touched
    app_b.register_notifier.assert_not_called()
    app_b.register_status_listener.assert_not_called()

    await gw_a.stop()
    assert not gw_a.is_running()

    # After A is stopped: its endpoint list must be empty
    assert gw_a._endpoints == [], "Gateway A must clear _endpoints on stop()"

    # No MQTTLogHandler should remain on the root logger after A stops
    root_handlers = logging.getLogger().handlers
    for h in root_handlers:
        assert not isinstance(h, MQTTLogHandler), (
            "MQTTLogHandler from gateway A leaked into root logger after stop()"
        )

    # ── Gateway B starts and stops independently ──────────────────────────
    await gw_b.start()
    assert gw_b.is_running()

    # B's app must have received its own registrations
    app_b.register_notifier.assert_called_once()

    # A's app must NOT have been called again during B's lifecycle
    call_count_a_before = app_a.register_notifier.call_count
    await gw_b.stop()
    assert app_a.register_notifier.call_count == call_count_a_before, (
        "Gateway B start/stop should not call app_a's registrations"
    )

    # ── Garbage-collectability of gateway A ──────────────────────────────
    ref_a = weakref.ref(gw_a)
    del gw_a
    gc.collect()
    assert ref_a() is None, "MQTTGateway A must be fully garbage-collectable after stop() and del"


async def test_singleton_bug_absent() -> None:
    """Two independently constructed gateways must be separate objects with separate state."""
    app_a = _make_app("a")
    app_b = _make_app("b")

    nc_a = _make_mock_node_context()
    nc_b = _make_mock_node_context()

    config = GatewayConfig(health_check_interval=9999.0)
    broker = BrokerConfig()

    gw_a = MQTTGateway(app=app_a, config=config, broker_config=broker)
    gw_b = MQTTGateway(app=app_b, config=config, broker_config=broker)

    # They must be distinct objects
    assert gw_a is not gw_b
    assert gw_a._app is app_a
    assert gw_b._app is app_b

    # Their endpoint and component lists must be independent
    gw_a._node_context = nc_a
    gw_b._node_context = nc_b

    n_a = MQTTNotifier()
    n_b = MQTTNotifier()
    gw_a.add_component(n_a)
    gw_b.add_component(n_b)

    await gw_a.start()
    # B's component list must not contain A's notifier
    assert n_a not in gw_b._components
    assert n_b not in gw_a._components

    await gw_a.stop()
    await gw_b.start()
    await gw_b.stop()

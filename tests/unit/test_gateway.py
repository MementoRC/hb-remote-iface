"""Unit tests for MQTTGateway."""

from __future__ import annotations

import asyncio
import logging
from unittest.mock import MagicMock, patch

import pytest

from remote_iface.gateway.gateway import Component, MQTTGateway, _make_node_context
from remote_iface.protocols.app import HummingbotAppProtocol
from remote_iface.protocols.config import BrokerConfig, GatewayConfig

pytestmark = pytest.mark.unit


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


def _make_app() -> MagicMock:
    """Return a MagicMock that satisfies HummingbotAppProtocol isinstance check."""
    app = MagicMock(spec=HummingbotAppProtocol)
    app.instance_id = "test-bot"
    # ev_loop is set per-test; default to None (overridden in tests that need it)
    app.ev_loop = None
    return app


def _make_gateway(
    health_check_interval: float = 0.05,
    consecutive_failure_threshold: int = 3,
) -> tuple[MQTTGateway, MagicMock]:
    app = _make_app()
    config = GatewayConfig(
        health_check_interval=health_check_interval,
        consecutive_failure_threshold=consecutive_failure_threshold,
    )
    broker = BrokerConfig()
    gw = MQTTGateway(app=app, config=config, broker_config=broker)
    return gw, app


def _mock_node_context(healthy: bool = True) -> MagicMock:
    nc = MagicMock()
    nc.start = MagicMock()
    nc.stop = MagicMock()
    nc.is_healthy = MagicMock(return_value=healthy)
    return nc


# ---------------------------------------------------------------------------
# Construction tests
# ---------------------------------------------------------------------------


def test_construction_rejects_non_protocol_app() -> None:
    config = GatewayConfig()
    broker = BrokerConfig()
    with pytest.raises(TypeError, match="HummingbotAppProtocol"):
        MQTTGateway(app=object(), config=config, broker_config=broker)  # type: ignore[arg-type]


def test_construction_rejects_plain_dict_app() -> None:
    config = GatewayConfig()
    broker = BrokerConfig()
    with pytest.raises(TypeError):
        MQTTGateway(app={}, config=config, broker_config=broker)  # type: ignore[arg-type]


def test_construction_accepts_protocol_conforming_app() -> None:
    gw, _ = _make_gateway()
    assert gw is not None


def test_initial_state_not_running() -> None:
    gw, _ = _make_gateway()
    assert gw.is_running() is False


def test_initial_endpoints_empty() -> None:
    gw, _ = _make_gateway()
    assert gw._endpoints == []


def test_initial_components_empty() -> None:
    gw, _ = _make_gateway()
    assert gw._components == []


# ---------------------------------------------------------------------------
# add_component tests
# ---------------------------------------------------------------------------


def test_add_component_before_start() -> None:
    gw, _ = _make_gateway()
    comp = MagicMock(spec=Component)
    gw.add_component(comp)
    assert comp in gw._components


def test_add_component_after_start_raises() -> None:
    gw, _ = _make_gateway()
    gw._running = True  # directly simulate running state
    comp = MagicMock(spec=Component)
    with pytest.raises(RuntimeError, match="add_component"):
        gw.add_component(comp)


# ---------------------------------------------------------------------------
# start() / stop() idempotence tests
# ---------------------------------------------------------------------------


async def test_start_idempotent(caplog: pytest.LogCaptureFixture) -> None:
    gw, _ = _make_gateway()
    gw._node_context = _mock_node_context()
    with caplog.at_level(logging.WARNING, logger="remote_iface.MQTTGateway"):
        await gw.start()
        assert gw.is_running() is True
        await gw.start()  # second call — should be no-op with WARN
    assert "already running" in caplog.text
    await gw.stop()


async def test_stop_before_start_is_noop(caplog: pytest.LogCaptureFixture) -> None:
    gw, _ = _make_gateway()
    with caplog.at_level(logging.DEBUG, logger="remote_iface.MQTTGateway"):
        await gw.stop()  # must not raise
    assert "not running" in caplog.text


async def test_start_sets_running_flag() -> None:
    gw, _ = _make_gateway()
    gw._node_context = _mock_node_context()
    await gw.start()
    assert gw.is_running() is True
    await gw.stop()


async def test_stop_clears_running_flag() -> None:
    gw, _ = _make_gateway()
    gw._node_context = _mock_node_context()
    await gw.start()
    await gw.stop()
    assert gw.is_running() is False


# ---------------------------------------------------------------------------
# Component lifecycle tests
# ---------------------------------------------------------------------------


async def test_component_start_called_in_order() -> None:
    gw, _ = _make_gateway()
    gw._node_context = _mock_node_context()
    call_order: list[str] = []
    comp_a = MagicMock(spec=Component)
    comp_a.start.side_effect = lambda gw_: call_order.append("a")
    comp_b = MagicMock(spec=Component)
    comp_b.start.side_effect = lambda gw_: call_order.append("b")
    gw.add_component(comp_a)
    gw.add_component(comp_b)
    await gw.start()
    assert call_order == ["a", "b"]
    await gw.stop()


async def test_component_stop_called_in_reverse_order() -> None:
    gw, _ = _make_gateway()
    gw._node_context = _mock_node_context()
    call_order: list[str] = []
    comp_a = MagicMock(spec=Component)
    comp_a.start.side_effect = lambda gw_: None
    comp_a.stop.side_effect = lambda gw_: call_order.append("a")
    comp_b = MagicMock(spec=Component)
    comp_b.start.side_effect = lambda gw_: None
    comp_b.stop.side_effect = lambda gw_: call_order.append("b")
    gw.add_component(comp_a)
    gw.add_component(comp_b)
    await gw.start()
    await gw.stop()
    assert call_order == ["b", "a"]


async def test_component_start_failure_logged_but_continues() -> None:
    gw, _ = _make_gateway()
    gw._node_context = _mock_node_context()
    failing_comp = MagicMock(spec=Component)
    failing_comp.start.side_effect = RuntimeError("boom")
    good_comp = MagicMock(spec=Component)
    gw.add_component(failing_comp)
    gw.add_component(good_comp)
    await gw.start()  # must not raise
    good_comp.start.assert_called_once()
    await gw.stop()


async def test_component_stop_failure_logged_but_continues() -> None:
    gw, _ = _make_gateway()
    gw._node_context = _mock_node_context()
    comp_a = MagicMock(spec=Component)
    comp_a.start.side_effect = lambda gw_: None
    comp_a.stop.side_effect = RuntimeError("stop-boom")
    comp_b = MagicMock(spec=Component)
    comp_b.start.side_effect = lambda gw_: None
    gw.add_component(comp_a)
    gw.add_component(comp_b)
    await gw.start()
    await gw.stop()  # must not raise
    comp_b.stop.assert_called_once()


# ---------------------------------------------------------------------------
# restart() test
# ---------------------------------------------------------------------------


async def test_restart_creates_new_node_context() -> None:
    gw, _ = _make_gateway()
    gw._node_context = _mock_node_context()
    old_nc = gw._node_context
    await gw.start()
    # Patch _make_node_context to return a fresh mock
    new_nc = _mock_node_context()
    with patch("remote_iface.gateway.gateway._make_node_context", return_value=new_nc):
        await gw.restart()
    assert gw._node_context is new_nc
    assert gw._node_context is not old_nc
    await gw.stop()


# ---------------------------------------------------------------------------
# Health loop tests
# ---------------------------------------------------------------------------


async def test_health_loop_cancelled_error_exits_cleanly() -> None:
    gw, _ = _make_gateway(health_check_interval=0.01)
    gw._node_context = _mock_node_context()
    await gw.start()
    # Give the health loop a moment to start running, then stop.
    await asyncio.sleep(0.02)
    await gw.stop()
    assert not gw.is_running()


async def test_health_loop_resets_failure_counter_on_success() -> None:
    gw, _ = _make_gateway(health_check_interval=0.02, consecutive_failure_threshold=3)
    gw._node_context = _mock_node_context(healthy=True)
    gw._consecutive_failures = 2  # pre-set
    await gw.start()
    await asyncio.sleep(0.06)  # allow 2-3 health checks
    assert gw._consecutive_failures == 0
    await gw.stop()


async def test_health_loop_triggers_restart_at_threshold() -> None:
    gw, _ = _make_gateway(health_check_interval=0.02, consecutive_failure_threshold=2)
    gw._node_context = _mock_node_context(healthy=False)

    restart_called = False

    async def _fake_restart() -> None:
        nonlocal restart_called
        restart_called = True
        # Make node healthy so loop doesn't keep restarting.
        gw._node_context.is_healthy.return_value = True
        gw._running = True  # keep alive after "restart"
        gw._consecutive_failures = 0

    gw.restart = _fake_restart  # type: ignore[method-assign]
    await gw.start()
    await asyncio.sleep(0.12)  # enough time for threshold to be reached
    await gw.stop()
    assert restart_called, "restart() should have been called"


async def test_health_loop_increments_failure_counter() -> None:
    gw, _ = _make_gateway(health_check_interval=0.02, consecutive_failure_threshold=100)
    gw._node_context = _mock_node_context(healthy=False)

    await gw.start()
    await asyncio.sleep(0.07)  # ~3 ticks
    count = gw._consecutive_failures
    await gw.stop()
    assert count >= 2


# ---------------------------------------------------------------------------
# is_running / is_healthy tests
# ---------------------------------------------------------------------------


async def test_is_running_reflects_state() -> None:
    gw, _ = _make_gateway()
    gw._node_context = _mock_node_context()
    assert gw.is_running() is False
    await gw.start()
    assert gw.is_running() is True
    await gw.stop()
    assert gw.is_running() is False


async def test_is_healthy_delegates_to_node_context() -> None:
    gw, _ = _make_gateway()
    nc = _mock_node_context()
    nc.is_healthy = MagicMock(return_value=True)
    gw._node_context = nc
    assert await gw.is_healthy() is True
    nc.is_healthy.return_value = False
    assert await gw.is_healthy() is False
    assert nc.is_healthy.call_count == 2


# ---------------------------------------------------------------------------
# _make_node_context helper
# ---------------------------------------------------------------------------


def test_make_node_context_produces_node_context() -> None:
    from remote_iface._commlib.node_context import NodeContext

    nc = _make_node_context(BrokerConfig(), "test-ns")
    assert isinstance(nc, NodeContext)
    assert nc._node_name == "test-ns"

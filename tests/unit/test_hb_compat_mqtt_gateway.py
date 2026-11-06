"""Unit tests for the drop-in hb_compat MQTTGateway (in-tree API shape)."""

from __future__ import annotations

import asyncio
import logging
from unittest.mock import AsyncMock, MagicMock

import pytest

from remote_iface.gateway.gateway import MQTTGateway as CoreMQTTGateway
from remote_iface.gateway.log_handler import MQTTLogHandler
from remote_iface.gateway.market_events import MQTTMarketEventForwarder
from remote_iface.hb_compat import MQTTGateway
from remote_iface.protocols.app import HummingbotAppProtocol
from remote_iface.protocols.config import GatewayConfig

pytestmark = pytest.mark.unit

_LOGGER_NAME = "remote_iface.test.compat_gateway"


def _make_app(loop: asyncio.AbstractEventLoop | None = None) -> MagicMock:
    app = MagicMock(spec=HummingbotAppProtocol)
    app.instance_id = "test-bot"
    app.ev_loop = loop
    return app


def _mock_node_context(healthy: bool = True) -> MagicMock:
    nc = MagicMock()
    nc.start = AsyncMock()
    nc.stop = AsyncMock()
    nc.is_healthy = MagicMock(return_value=healthy)
    return nc


def _make_compat(
    loop: asyncio.AbstractEventLoop | None = None,
    config: GatewayConfig | None = None,
    healthy: bool = True,
) -> tuple[MQTTGateway, MagicMock]:
    app = _make_app(loop)
    gw = MQTTGateway(app, config=config)
    gw.gateway._node_context = _mock_node_context(healthy)
    return gw, app


def test_construct_with_app_only() -> None:
    gw = MQTTGateway(_make_app())
    assert isinstance(gw.gateway, CoreMQTTGateway)
    assert gw.gateway.is_running() is False


def test_compat_class_distinct_from_core() -> None:
    assert MQTTGateway is not CoreMQTTGateway


# ---------------------------------------------------------------------------
# health
# ---------------------------------------------------------------------------


async def test_health_lifecycle() -> None:
    gw, _ = _make_compat(asyncio.get_running_loop())
    assert gw.health is False
    await gw.gateway.start()
    assert gw.health is True
    await gw.gateway.stop()
    assert gw.health is False


async def test_health_false_when_transport_unhealthy() -> None:
    gw, _ = _make_compat(asyncio.get_running_loop(), healthy=False)
    await gw.gateway.start()
    try:
        assert gw.health is False
    finally:
        await gw.gateway.stop()


# ---------------------------------------------------------------------------
# start / stop
# ---------------------------------------------------------------------------


async def test_start_in_running_loop_returns_immediately() -> None:
    gw, _ = _make_compat(asyncio.get_running_loop())
    assert gw.start() is None
    assert gw.gateway.is_running() is False
    await asyncio.sleep(0.01)
    assert gw.gateway.is_running() is True
    await gw.gateway.stop()


async def test_stop_in_running_loop() -> None:
    gw, _ = _make_compat(asyncio.get_running_loop())
    gw.start()
    await asyncio.sleep(0.01)
    assert gw.stop() is None
    await asyncio.sleep(0.01)
    assert gw.gateway.is_running() is False


async def test_stop_awaits_pending_start_then_stops() -> None:
    gw, _ = _make_compat(asyncio.get_running_loop())
    gw.start()
    gw.stop()
    assert gw._stop_task is not None
    await gw._stop_task
    assert gw.gateway.is_running() is False
    gw.gateway._node_context.start.assert_awaited_once()
    gw.gateway._node_context.stop.assert_awaited_once()


async def test_loop_falls_back_to_running_loop() -> None:
    gw, _ = _make_compat(None)
    gw.start()
    await asyncio.sleep(0.01)
    assert gw.gateway.is_running() is True
    await gw.gateway.stop()


def test_loop_raises_without_any_loop() -> None:
    gw, _ = _make_compat(None)
    with pytest.raises(RuntimeError, match="event loop"):
        gw.start()


def test_start_and_stop_with_non_running_loop() -> None:
    loop = asyncio.new_event_loop()
    try:
        gw, _ = _make_compat(loop)
        gw.start()
        assert gw.gateway.is_running() is True
        gw.stop()
        assert gw.gateway.is_running() is False
    finally:
        loop.close()


# ---------------------------------------------------------------------------
# patch_loggers
# ---------------------------------------------------------------------------


async def test_patch_loggers_idempotent_and_removed_on_stop() -> None:
    gw, _ = _make_compat(asyncio.get_running_loop())
    await gw.gateway.start()
    target = logging.getLogger(_LOGGER_NAME)
    try:
        gw.patch_loggers([_LOGGER_NAME])
        gw.patch_loggers([_LOGGER_NAME])
        handlers = [h for h in target.handlers if isinstance(h, MQTTLogHandler)]
        assert len(handlers) == 1
    finally:
        await gw.gateway.stop()
    assert not any(isinstance(h, MQTTLogHandler) for h in target.handlers)


async def test_patch_loggers_noop_when_log_handler_disabled() -> None:
    gw, _ = _make_compat(asyncio.get_running_loop(), config=GatewayConfig(enable_log_handler=False))
    await gw.gateway.start()
    try:
        gw.patch_loggers([_LOGGER_NAME])
        target = logging.getLogger(_LOGGER_NAME)
        assert not any(isinstance(h, MQTTLogHandler) for h in target.handlers)
    finally:
        await gw.gateway.stop()


# ---------------------------------------------------------------------------
# start_market_events_fw
# ---------------------------------------------------------------------------


async def test_start_market_events_fw_subscribes_once() -> None:
    gw, _ = _make_compat(asyncio.get_running_loop())
    adapter = gw.gateway.app
    await gw.gateway.start()
    try:
        assert adapter._market_event_callbacks == []
        gw.start_market_events_fw()
        gw.start_market_events_fw()
        for callback in list(adapter._strategy_loaded_callbacks):
            callback()
        assert len(adapter._market_event_callbacks) == 1
    finally:
        await gw.gateway.stop()


async def test_start_market_events_fw_noop_when_events_disabled() -> None:
    gw, _ = _make_compat(asyncio.get_running_loop(), config=GatewayConfig(enable_events=False))
    await gw.gateway.start()
    try:
        gw.start_market_events_fw()
        assert gw.gateway.app._market_event_callbacks == []
    finally:
        await gw.gateway.stop()


async def test_get_component_lookup() -> None:
    gw, _ = _make_compat(asyncio.get_running_loop())
    assert isinstance(gw.gateway.get_component(MQTTMarketEventForwarder), MQTTMarketEventForwarder)
    assert gw.gateway.get_component(int) is None

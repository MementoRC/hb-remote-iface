"""Unit tests for the drop-in hb_compat ETopicPublisher (in-tree API shape)."""

from __future__ import annotations

from typing import Any
from unittest.mock import AsyncMock, MagicMock

import pytest

from remote_iface.hb_compat import ETopicPublisher, MQTTGateway
from remote_iface.protocols.app import HummingbotAppProtocol
from remote_iface.protocols.config import GatewayConfig

pytestmark = pytest.mark.unit


def _make_app() -> MagicMock:
    app = MagicMock(spec=HummingbotAppProtocol)
    app.instance_id = "test-bot"
    app.ev_loop = None
    return app


def _mock_node_context() -> MagicMock:
    nc = MagicMock()
    nc.start = AsyncMock()
    nc.stop = AsyncMock()
    nc.is_healthy = MagicMock(return_value=True)
    return nc


def _make_compat() -> MQTTGateway:
    gw = MQTTGateway(_make_app(), config=GatewayConfig(namespace="hb"))
    gw.gateway._node_context = _mock_node_context()
    return gw


def _user_publisher_calls(nc: MagicMock) -> list[Any]:
    """create_publisher calls made by ETopicPublisher (dict payloads), not gateway components."""
    return [c for c in nc.create_publisher.call_args_list if c.kwargs["msg_type"] is dict]


def _published_topics(nc: MagicMock) -> list[str]:
    return [c.kwargs["topic"] for c in _user_publisher_calls(nc)]


def test_raises_when_no_gateway() -> None:
    with pytest.raises(Exception, match="^MQTT Gateway not yet initialized$"):
        ETopicPublisher("performance")


async def test_strategy_v2_base_pattern() -> None:
    gw = _make_compat()
    await gw.gateway.start()
    pub = ETopicPublisher("performance", use_bot_prefix=True)
    pub({"c1": {}})
    nc = gw.gateway._node_context
    assert _published_topics(nc) == ["hb/test-bot/performance"]
    nc.create_publisher.return_value.publish.assert_called_once_with({"c1": {}})
    await gw.gateway.stop()


async def test_default_use_bot_prefix_is_false() -> None:
    gw = _make_compat()
    await gw.gateway.start()
    ETopicPublisher("performance").send({"a": 1})
    assert _published_topics(gw.gateway._node_context) == ["performance"]
    await gw.gateway.stop()


async def test_explicit_compat_gateway() -> None:
    gw = _make_compat()
    await gw.gateway.start()
    ETopicPublisher("t", gateway=gw).send({"a": 1})
    assert _published_topics(gw.gateway._node_context) == ["t"]
    await gw.gateway.stop()


async def test_explicit_core_gateway() -> None:
    gw = _make_compat()
    await gw.gateway.start()
    ETopicPublisher("t", True, gateway=gw.gateway).send({"a": 1})
    assert _published_topics(gw.gateway._node_context) == ["hb/test-bot/t"]
    await gw.gateway.stop()


async def test_send_after_stop_does_not_raise() -> None:
    gw = _make_compat()
    await gw.gateway.start()
    pub = ETopicPublisher("performance", use_bot_prefix=True)
    await gw.gateway.stop()
    pub({"c1": {}})
    gw.gateway._node_context.create_publisher.return_value.publish.assert_not_called()


def test_send_before_start_does_not_raise() -> None:
    gw = _make_compat()
    ETopicPublisher("t", gateway=gw)({"a": 1})
    gw.gateway._node_context.create_publisher.assert_not_called()


async def test_send_swallows_publisher_errors() -> None:
    gw = _make_compat()
    await gw.gateway.start()
    gw.gateway._node_context.create_publisher.side_effect = RuntimeError("boom")
    ETopicPublisher("t", gateway=gw)({"a": 1})  # must not raise
    await gw.gateway.stop()


async def test_publish_after_restart_uses_new_node_context(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    gw = _make_compat()
    await gw.gateway.start()
    pub = ETopicPublisher("t", gateway=gw)
    pub({"n": 1})
    old_nc = gw.gateway._node_context
    new_nc = _mock_node_context()
    monkeypatch.setattr("remote_iface.gateway.gateway._make_node_context", lambda *_a: new_nc)
    await gw.gateway.restart()
    assert gw.gateway._node_context is new_nc
    pub({"n": 2})
    assert len(_user_publisher_calls(new_nc)) == 1
    new_nc.create_publisher.return_value.publish.assert_called_once_with({"n": 2})
    old_nc.create_publisher.return_value.publish.assert_called_once_with({"n": 1})
    await gw.gateway.stop()


async def test_publisher_reused_between_sends() -> None:
    gw = _make_compat()
    await gw.gateway.start()
    pub = ETopicPublisher("t", gateway=gw)
    pub({"n": 1})
    pub({"n": 2})
    assert len(_user_publisher_calls(gw.gateway._node_context)) == 1
    await gw.gateway.stop()

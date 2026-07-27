"""Integration regression test for issue #13's concrete payoff.

Simulates two overlapping EEventListenerFactory subscriptions (for DIFFERENT event
names) active on the SAME gateway at the same time — the original bug's exact
precondition, where a specific per-event listener and the generic MQTTExternalEvents
wildcard listener could each independently subscribe to overlapping raw MQTT topics
and both fire on a single inbound message.

This test exercises the REAL, FULL pipeline end-to-end: a real NodeContext (only the
underlying aiomqtt.Client is faked, via the same minimal fake used in
tests/unit/test_node_context.py), a real MQTTGateway with a real MQTTExternalEvents
component (enable_external_events=True — the SOLE raw ``external/+`` MQTT subscriber),
and EEventListenerFactory.create() registering two independent listeners on that one
gateway. No event_bus/dispatch internals are mocked.

Placed under tests/integration/ (rather than tests/unit/test_external_factories.py or
tests/unit/test_node_context.py) because it wires together multiple real collaborators
(MQTTGateway + MQTTExternalEvents + NodeContext + EEventListenerFactory) end-to-end,
matching this directory's existing convention (see test_two_gateways_no_state_bleed.py)
of composing real gateway components rather than exercising a single unit in isolation.
"""

from __future__ import annotations

import asyncio
import json
from unittest.mock import MagicMock

import pytest

from remote_iface._commlib.node_context import NodeContext
from remote_iface.external.events import EEventListenerFactory
from remote_iface.gateway.external_events import MQTTExternalEvents
from remote_iface.gateway.gateway import MQTTGateway
from remote_iface.protocols.app import HummingbotAppProtocol
from remote_iface.protocols.config import BrokerConfig, GatewayConfig

pytestmark = pytest.mark.integration


# ---------------------------------------------------------------------------
# Fakes (aiomqtt.Client is the only thing faked — everything else is real)
# ---------------------------------------------------------------------------


class _FakeMessage:
    def __init__(self, topic: str, payload: bytes) -> None:
        self.topic = topic
        self.payload = payload


class _FakeAiomqttClient:
    """Minimal async-context-manager fake matching aiomqtt.Client's used surface
    (mirrors tests/unit/test_node_context.py's ``_FakeAiomqttClient``). Yields queued
    messages with a small delay between each so the test can assert intermediate
    delivery state between the two inbound messages."""

    def __init__(self, *, incoming: list[_FakeMessage] | None = None) -> None:
        self.published: list[tuple[str, bytes, int]] = []
        self.subscribed: list[str] = []
        self._incoming = incoming or []

    async def __aenter__(self) -> _FakeAiomqttClient:
        return self

    async def __aexit__(self, *exc_info: object) -> bool:
        return False

    async def subscribe(self, topic: str, qos: int = 0) -> None:
        self.subscribed.append(topic)

    async def publish(self, topic: str, payload: bytes, qos: int = 0) -> None:
        self.published.append((topic, payload, qos))

    @property
    def messages(self):
        async def _gen():
            for m in self._incoming:
                yield m
                await asyncio.sleep(0.15)
            # Idle forever afterward so _dispatch_incoming doesn't exit and race stop().
            await asyncio.Event().wait()

        return _gen()


def _make_app() -> MagicMock:
    app = MagicMock(spec=HummingbotAppProtocol)
    app.instance_id = "test-bot"
    return app


# ---------------------------------------------------------------------------
# Test
# ---------------------------------------------------------------------------


async def test_overlapping_gateway_subscriptions_do_not_duplicate_delivery() -> None:
    """Two EEventListenerFactory listeners for DIFFERENT event names, both active on
    the SAME gateway alongside MQTTExternalEvents' single wildcard subscription, must
    each fire exactly once per matching message and never for the other's event —
    confirming the structural fix (EEventListenerFactory rides entirely on
    MQTTExternalEvents' subscription + event_bus republish instead of opening its own
    competing raw MQTT subscription) actually eliminates duplicate delivery
    end-to-end."""
    order_filled_payload = json.dumps({"data": {"order_id": "abc"}}).encode("utf-8")
    other_event_payload = json.dumps({"data": {"x": 1}}).encode("utf-8")
    messages = [
        _FakeMessage("external/order_filled", order_filled_payload),
        _FakeMessage("external/other_event", other_event_payload),
    ]
    fake_client = _FakeAiomqttClient(incoming=messages)

    app = _make_app()
    config = GatewayConfig(namespace="hbot", health_check_interval=9999.0)
    broker = BrokerConfig()
    gw = MQTTGateway(app=app, config=config, broker_config=broker)

    # Swap in a real NodeContext backed by the fake aiomqtt transport (real
    # NodeContext/EventBusAdapter — only the underlying aiomqtt.Client is faked).
    gw._node_context = NodeContext(node_name="hbot", transport_factory=lambda: fake_client)
    gw.add_component(MQTTExternalEvents())

    order_filled_calls: list[tuple[object, str]] = []
    other_event_calls: list[tuple[object, str]] = []

    # Two overlapping listeners registered on the SAME gateway simultaneously — the
    # original bug's exact precondition (a specific listener + the wildcard listener
    # both active for overlapping topics on one gateway).
    EEventListenerFactory.create(
        gw, "order_filled", lambda msg, event_name: order_filled_calls.append((msg, event_name))
    )
    EEventListenerFactory.create(
        gw, "other_event", lambda msg, event_name: other_event_calls.append((msg, event_name))
    )

    await gw.start()

    # ── After the FIRST message only ──────────────────────────────────────
    await asyncio.sleep(0.05)
    assert app.handle_external_event.call_count == 1, (
        "app.handle_external_event must be called exactly once for the first message"
    )
    assert len(order_filled_calls) == 1, "order_filled listener must fire exactly once"
    assert order_filled_calls[0][1] == "order_filled"
    assert order_filled_calls[0][0].data == {"order_id": "abc"}
    assert other_event_calls == [], (
        "other_event listener fired for order_filled's message — cross-event leakage "
        "(the duplicate-delivery failure mode issue #13 guards against)"
    )

    # ── After the SECOND message ──────────────────────────────────────────
    await asyncio.sleep(0.2)
    assert app.handle_external_event.call_count == 2, (
        "app.handle_external_event must be called exactly once per message, not doubled"
    )
    assert len(other_event_calls) == 1, "other_event listener must fire exactly once"
    assert other_event_calls[0][1] == "other_event"
    assert other_event_calls[0][0].data == {"x": 1}
    assert len(order_filled_calls) == 1, (
        "order_filled listener must NOT fire again for the other_event message"
    )

    await gw.stop()

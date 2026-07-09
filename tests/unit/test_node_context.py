"""Unit tests for NodeContext's aiomqtt.Client-based connection loop.

NodeContext.start()/stop() are async now — they run a single aiomqtt.Client connection
as an asyncio.Task on the caller's event loop, replacing the commlib-Node-on-a-thread
model. These tests use a minimal fake aiomqtt client (matching the async-context-manager
+ subscribe/publish/messages surface) instead of a real broker.
"""

import asyncio

import pytest

pytestmark = pytest.mark.unit


class _FakeMessage:
    def __init__(self, topic: str, payload: bytes) -> None:
        self.topic = topic
        self.payload = payload


class _FakeAiomqttClient:
    """Minimal async-context-manager fake matching aiomqtt.Client's used surface."""

    def __init__(self, *, incoming: list[_FakeMessage] | None = None) -> None:
        self.published: list[tuple[str, bytes, int]] = []
        self.subscribed: list[str] = []
        self._incoming = incoming or []

    async def __aenter__(self) -> "_FakeAiomqttClient":
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
            # Idle forever afterward so _dispatch_incoming doesn't exit and race stop().
            await asyncio.Event().wait()

        return _gen()


# ---------------------------------------------------------------------------
# Lifecycle / health
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_node_context_start_connects_and_sets_healthy() -> None:
    from remote_iface._commlib.node_context import NodeContext

    fake_client = _FakeAiomqttClient()
    nc = NodeContext(node_name="n1", transport_factory=lambda: fake_client)

    await nc.start()
    await asyncio.sleep(0.05)
    assert nc.is_healthy() is True

    await nc.stop()
    assert nc.is_healthy() is False


@pytest.mark.asyncio
async def test_idempotent_start_and_stop() -> None:
    from remote_iface._commlib.node_context import NodeContext

    fake_client = _FakeAiomqttClient()
    nc = NodeContext(node_name="n1", transport_factory=lambda: fake_client)

    await nc.start()
    await nc.start()  # second start must be no-op
    await asyncio.sleep(0.05)

    await nc.stop()
    await nc.stop()  # second stop must be no-op


# ---------------------------------------------------------------------------
# Publisher
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_publisher_publish_enqueues_and_client_publishes() -> None:
    from remote_iface._commlib.node_context import NodeContext

    fake_client = _FakeAiomqttClient()
    nc = NodeContext(node_name="n1", transport_factory=lambda: fake_client)
    pub = nc.create_publisher(topic="t/1", msg_type=dict)

    await nc.start()
    await asyncio.sleep(0.05)

    pub.publish({"x": 1})
    await asyncio.sleep(0.05)

    assert fake_client.published
    topic, _payload, _qos = fake_client.published[0]
    assert topic == "t/1"

    await nc.stop()


@pytest.mark.asyncio
async def test_publisher_publish_before_start_drops_and_counts_failure() -> None:
    from remote_iface._commlib.node_context import NodeContext

    fake_client = _FakeAiomqttClient()
    nc = NodeContext(node_name="n1", transport_factory=lambda: fake_client)
    pub = nc.create_publisher(topic="t/x", msg_type=dict)

    pub._started = False  # simulate not-yet-started wrapper
    pub.publish({"x": 1})

    assert pub.publish_failure_count == 1


# ---------------------------------------------------------------------------
# Subscriber
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_subscriber_receives_matching_message() -> None:
    from remote_iface._commlib.node_context import NodeContext
    from remote_iface._commlib.serialization import serialize

    msg = _FakeMessage("t/2", serialize({"y": 2}))
    fake_client = _FakeAiomqttClient(incoming=[msg])
    nc = NodeContext(node_name="n1", transport_factory=lambda: fake_client)

    received = []
    nc.create_subscriber(topic="t/2", on_message=received.append, msg_type=None)

    await nc.start()
    await asyncio.sleep(0.05)
    await nc.stop()

    assert received and received[0] == {"y": 2}


@pytest.mark.asyncio
async def test_subscriber_wildcard_topic_matches() -> None:
    from remote_iface._commlib.node_context import NodeContext
    from remote_iface._commlib.serialization import serialize

    msg = _FakeMessage("t/abc/data", serialize({"w": 1}))
    fake_client = _FakeAiomqttClient(incoming=[msg])
    nc = NodeContext(node_name="n1", transport_factory=lambda: fake_client)

    received = []
    nc.create_subscriber(topic="t/+/data", on_message=received.append, msg_type=None)

    await nc.start()
    await asyncio.sleep(0.05)
    await nc.stop()

    assert received and received[0] == {"w": 1}


@pytest.mark.asyncio
async def test_subscriber_set_callback_after_start_takes_effect() -> None:
    """Regression test for the wrapper.on_message closure fix — set_callback() must work live."""
    from remote_iface._commlib.node_context import NodeContext
    from remote_iface._commlib.serialization import serialize

    msg = _FakeMessage("t/3", serialize({"z": 3}))
    fake_client = _FakeAiomqttClient(incoming=[msg])
    nc = NodeContext(node_name="n1", transport_factory=lambda: fake_client)

    received_old, received_new = [], []
    sub = nc.create_subscriber(topic="t/3", on_message=received_old.append, msg_type=None)
    sub.set_callback(received_new.append)

    await nc.start()
    await asyncio.sleep(0.05)
    await nc.stop()

    assert not received_old
    assert received_new and received_new[0] == {"z": 3}


# ---------------------------------------------------------------------------
# RPC
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_rpc_service_dispatches_and_replies() -> None:
    from remote_iface._commlib.node_context import NodeContext
    from remote_iface._commlib.serialization import serialize

    def handler(_req: object) -> dict:
        return {"status": 200}

    envelope = {"header": {"reply_to": "reply/topic"}, "data": {}}
    msg = _FakeMessage("cmd/1", serialize(envelope))
    fake_client = _FakeAiomqttClient(incoming=[msg])
    nc = NodeContext(node_name="n1", transport_factory=lambda: fake_client)
    nc.create_rpc(rpc_name="cmd/1", msg_type=dict, on_request=handler)

    await nc.start()
    await asyncio.sleep(0.2)
    await nc.stop()

    assert any(topic == "reply/topic" for topic, _payload, _qos in fake_client.published)


@pytest.mark.asyncio
async def test_rpc_dispatch_does_not_block_event_loop_under_concurrent_load() -> None:
    """Regression for the double-hop executor bug: multiple concurrent RPC requests must
    not tie up event-loop-thread dispatch (_dispatch_rpc runs inline and non-blocking —
    the actual handler call is offloaded to RPCService's own executor via a Future).

    Uses N slow handlers on a single-topic RPCService (max_workers=1 default pool
    sizing doesn't matter here — what matters is that dispatch itself never blocks the
    loop waiting on .result()). We assert the event loop stayed responsive (a concurrent
    asyncio.sleep-based heartbeat kept ticking) while requests were in flight, and that
    all replies eventually arrive.
    """
    import time

    from remote_iface._commlib.node_context import NodeContext
    from remote_iface._commlib.serialization import serialize

    def slow_handler(_req: object) -> dict:
        time.sleep(0.15)
        return {"ok": True}

    messages = [
        _FakeMessage("cmd/slow", serialize({"header": {"reply_to": f"reply/{i}"}, "data": {}}))
        for i in range(4)
    ]
    fake_client = _FakeAiomqttClient(incoming=messages)
    nc = NodeContext(node_name="n1", transport_factory=lambda: fake_client)
    nc.create_rpc(rpc_name="cmd/slow", msg_type=dict, on_request=slow_handler)

    heartbeat_ticks = 0

    async def _heartbeat() -> None:
        nonlocal heartbeat_ticks
        for _ in range(20):
            await asyncio.sleep(0.02)
            heartbeat_ticks += 1

    await nc.start()
    await asyncio.gather(_heartbeat(), asyncio.sleep(0.6))
    await nc.stop()

    # The event loop must have kept ticking the heartbeat throughout — if _dispatch_rpc
    # were blocking (old double-hop design), the loop thread would stall and ticks would
    # be starved/delayed well below the expected count.
    assert heartbeat_ticks >= 15, (
        f"only {heartbeat_ticks}/20 heartbeat ticks fired — event loop appears blocked"
    )
    replies = {topic for topic, _payload, _qos in fake_client.published}
    assert replies == {f"reply/{i}" for i in range(4)}


@pytest.mark.asyncio
async def test_rpc_reply_envelope_uses_millisecond_timestamp() -> None:
    """Regression: reply timestamp must be milliseconds (int(time.time()*1000)), not
    nanoseconds — the ns/ms wire-format bug from commlib's gen_timestamp() (decision-d9106708)
    must not resurface now that the aiomqtt path builds its own envelope."""
    import json
    import time

    from remote_iface._commlib.node_context import NodeContext
    from remote_iface._commlib.serialization import serialize

    def handler(_req: object) -> dict:
        return {"ok": True}

    envelope = {"header": {"reply_to": "reply/ts"}, "data": {}}
    msg = _FakeMessage("cmd/ts", serialize(envelope))
    fake_client = _FakeAiomqttClient(incoming=[msg])
    nc = NodeContext(node_name="n1", transport_factory=lambda: fake_client)
    nc.create_rpc(rpc_name="cmd/ts", msg_type=dict, on_request=handler)

    before_ms = int(time.time() * 1000)
    await nc.start()
    await asyncio.sleep(0.2)
    await nc.stop()
    after_ms = int(time.time() * 1000)

    reply = next(p for t, p, _q in fake_client.published if t == "reply/ts")
    body = json.loads(reply)
    ts = body["header"]["timestamp"]
    assert before_ms - 1000 <= ts <= after_ms + 1000


# ---------------------------------------------------------------------------
# Reconnect behavior
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_run_reconnects_after_mqtt_error() -> None:
    import aiomqtt

    from remote_iface._commlib.node_context import NodeContext

    attempts: list[_FakeAiomqttClient] = []

    class _FailOnceClient(_FakeAiomqttClient):
        async def __aenter__(self) -> "_FailOnceClient":
            attempts.append(self)
            if len(attempts) == 1:
                raise aiomqtt.MqttError("simulated disconnect")
            return self

    def _factory() -> _FailOnceClient:
        return _FailOnceClient()

    nc = NodeContext(node_name="n1", transport_factory=_factory)
    nc._reconnect_interval_s = 0  # type: ignore[attr-defined]  # speed up test if attr exists

    import remote_iface._commlib.node_context as nc_module

    original_interval = nc_module._RECONNECT_INTERVAL_S
    nc_module._RECONNECT_INTERVAL_S = 0.01
    try:
        await nc.start()
        await asyncio.sleep(0.2)
        assert len(attempts) >= 2
        assert nc.is_healthy() is True
        await nc.stop()
    finally:
        nc_module._RECONNECT_INTERVAL_S = original_interval

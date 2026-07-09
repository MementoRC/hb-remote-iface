"""NodeContext: owns a single aiomqtt.Client connection loop.

This is the ONLY file (along with transport.py) permitted to import aiomqtt.
All wrappers receive registration hooks via _bind() and must not import aiomqtt directly.
"""

from __future__ import annotations

import asyncio
import contextlib
import json
import logging
import time
import typing
from typing import TYPE_CHECKING, Any

from remote_iface._commlib.serialization import serialize
from remote_iface._commlib.wrappers.publisher import Publisher
from remote_iface._commlib.wrappers.rpc_service import RPCService
from remote_iface._commlib.wrappers.subscriber import Subscriber

if TYPE_CHECKING:
    from collections.abc import Callable

    import aiomqtt

    from remote_iface._commlib.transport import TransportFactory
    from remote_iface._commlib.wrappers.endpoint import Endpoint

_logger = logging.getLogger("remote_iface.NodeContext")

_RECONNECT_INTERVAL_S = 5.0


class NodeContext:
    """Owns a single aiomqtt connection, creates wrappers, manages shared lifecycle.

    Replaces the commlib-Node-on-a-thread model with a single aiomqtt.Client run as an
    asyncio.Task on the caller's event loop (design mirrors upstream's validated
    MQTTGateway._run(), hummingbot/remote_iface/mqtt.py:540-580).
    """

    def __init__(self, *, node_name: str, transport_factory: TransportFactory) -> None:
        self._node_name: str = node_name
        self._transport_factory: TransportFactory = transport_factory
        self._wrappers: list[Endpoint] = []
        self._started: bool = False

        self._loop: asyncio.AbstractEventLoop | None = None
        self._run_task: asyncio.Task[None] | None = None
        self._stopped: asyncio.Event = asyncio.Event()
        self._connected: bool = False
        self._outgoing: asyncio.Queue[tuple[str, bytes, int]] = asyncio.Queue()
        # Pub/Sub callbacks keyed by topic pattern (supports +/# wildcards).
        self._sub_callbacks: dict[str, list[Callable[[Any], None]]] = {}
        # RPC handlers keyed by exact command topic.
        self._command_table: dict[str, tuple[type, Callable[[Any], Any]]] = {}

    # ------------------------------------------------------------------
    # Lifecycle
    # ------------------------------------------------------------------

    async def start(self) -> None:
        """Bind pending wrappers, launch the connection loop as a background task."""
        if self._started:
            return
        self._loop = asyncio.get_running_loop()
        self._stopped.clear()
        self._run_task = self._loop.create_task(
            self._run(), name=f"mqtt-nodecontext-{self._node_name}"
        )
        await asyncio.sleep(0)
        self._started = True

    async def stop(self) -> None:
        """Cancel the connection loop and wait for it to exit."""
        if not self._started:
            return
        self._stopped.set()
        if self._run_task is not None:
            self._run_task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await self._run_task
            self._run_task = None
        self._connected = False
        self._started = False

    async def _run(self) -> None:
        """Connect, subscribe, run drain/dispatch tasks; reconnect on MqttError."""
        import aiomqtt  # aiomqtt import boundary

        while not self._stopped.is_set():
            tasks: list[asyncio.Task[None]] = []
            try:
                assert self._loop is not None, "_run() invoked before start() set self._loop"
                client = typing.cast("aiomqtt.Client", self._transport_factory())
                async with client:
                    self._connected = True
                    for topic in self._desired_subscriptions():
                        await client.subscribe(topic, qos=0)
                    tasks = [
                        self._loop.create_task(self._drain_outgoing(client)),
                        self._loop.create_task(self._dispatch_incoming(client)),
                    ]
                    done, _pending = await asyncio.wait(
                        tasks, return_when=asyncio.FIRST_EXCEPTION
                    )
                    for t in done:
                        exc = t.exception()
                        if exc is not None:
                            raise exc
            except asyncio.CancelledError:
                raise
            except aiomqtt.MqttError as exc:
                _logger.warning(
                    "NodeContext(%r): MQTT disconnected: %s — reconnecting in %.1fs",
                    self._node_name, exc, _RECONNECT_INTERVAL_S,
                )
            except Exception as exc:  # noqa: BLE001
                _logger.error(
                    "NodeContext(%r): connection error: %s: %s — reconnecting in %.1fs",
                    self._node_name, type(exc).__name__, exc, _RECONNECT_INTERVAL_S,
                )
            finally:
                self._connected = False
                for t in tasks:
                    t.cancel()
                if tasks:
                    await asyncio.gather(*tasks, return_exceptions=True)

            if self._stopped.is_set():
                break
            await asyncio.sleep(_RECONNECT_INTERVAL_S)

    def _desired_subscriptions(self) -> list[str]:
        return list(self._sub_callbacks.keys()) + list(self._command_table.keys())

    async def _drain_outgoing(self, client: Any) -> None:
        while True:
            topic, payload, qos = await self._outgoing.get()
            await client.publish(topic, payload=payload, qos=qos)

    async def _dispatch_incoming(self, client: Any) -> None:
        async for message in client.messages:
            topic = str(message.topic)
            try:
                data = json.loads(message.payload)
            except (ValueError, TypeError):
                _logger.debug(
                    "NodeContext(%r): dropping malformed JSON payload on %r",
                    self._node_name, topic, exc_info=True,
                )
                continue

            if topic in self._command_table:
                # Dispatch inline on the event loop thread — cheap (dict lookups + type
                # coercion). The actual handler call is submitted onto RPCService's OWN
                # bounded executor by handler() below, so this never blocks the loop and
                # never routes through the process-wide default executor (avoids the
                # double-hop where a default-executor thread sits blocked on .result()
                # while the real work runs on a second, unrelated pool).
                self._dispatch_rpc(topic, data)
                continue

            for pattern, callbacks in list(self._sub_callbacks.items()):
                if self._topic_matches(pattern, topic):
                    for cb in list(callbacks):
                        try:
                            cb(data)
                        except Exception:  # noqa: BLE001
                            _logger.error(
                                "NodeContext(%r): subscriber callback raised on %r",
                                self._node_name, topic, exc_info=True,
                            )

    def _dispatch_rpc(self, topic: str, payload: dict[str, Any]) -> None:
        """Runs inline on the event loop thread — request coercion is cheap, and the
        handler call itself is offloaded to RPCService's own bounded executor via
        `handler(request)`, which returns a concurrent.futures.Future immediately rather
        than blocking (see RPCService._dispatch). Reply-publishing happens in
        _on_rpc_future_done, invoked from the executor thread once the handler completes.
        """
        msg_type, handler = self._command_table[topic]
        header = payload.get("header", {}) if isinstance(payload, dict) else {}
        data = payload.get("data", {}) if isinstance(payload, dict) else {}
        reply_to = header.get("reply_to") if isinstance(header, dict) else None
        try:
            request = (
                msg_type.model_validate(data or {})
                if hasattr(msg_type, "model_validate")
                else msg_type(**(data or {}))
            )
            future = handler(request)
        except Exception:  # noqa: BLE001
            _logger.error(
                "NodeContext(%r): RPC handler raised on %r", self._node_name, topic, exc_info=True
            )
            return
        if reply_to:
            future.add_done_callback(
                lambda fut, _topic=topic, _reply_to=reply_to: self._on_rpc_future_done(
                    fut, _topic, _reply_to
                )
            )

    def _on_rpc_future_done(self, future: Any, topic: str, reply_to: str) -> None:
        """Completion callback for an RPC handler Future — runs on the RPCService's OWN
        executor thread (not the event loop thread), so _enqueue_outgoing's thread-safe
        call_soon_threadsafe path is required here.
        """
        try:
            response = future.result()
        except Exception:  # noqa: BLE001
            _logger.error(
                "NodeContext(%r): RPC handler raised on %r", self._node_name, topic, exc_info=True
            )
            return
        self._enqueue_outgoing(reply_to, self._wrap_response(response), qos=1)

    def _wrap_response(self, response: Any) -> bytes:
        """Build the reply envelope. Milliseconds timestamp matches upstream exactly
        (hummingbot/remote_iface/mqtt.py:647-658) — see decision-d9106708.

        Reuses serialization.serialize()'s pydantic/dataclass/dict extraction logic
        (via a plain dict re-serialize) instead of duplicating slots=True handling here.
        """
        # serialize() already knows how to coerce pydantic models, dataclasses (incl.
        # slots=True), and plain dicts into JSON bytes — round-trip through it once to
        # get a plain dict for the "data" field without re-implementing that logic.
        data = json.loads(serialize(response))
        body = {
            "header": {
                "reply_to": "",
                "timestamp": int(time.time() * 1000),
                "content_type": "json",
                "encoding": "utf8",
                "agent": "aiomqtt",
            },
            "data": data,
        }
        return json.dumps(body).encode("utf-8")

    @staticmethod
    def _topic_matches(pattern: str, topic: str) -> bool:
        if "+" not in pattern and "#" not in pattern:
            return pattern == topic
        p_parts = pattern.split("/")
        t_parts = topic.split("/")
        for i, p in enumerate(p_parts):
            if p == "#":
                return True
            if i >= len(t_parts):
                return False
            if p != "+" and p != t_parts[i]:
                return False
        return len(p_parts) == len(t_parts)

    def _enqueue_outgoing(self, topic: str, payload: bytes, qos: int) -> None:
        """Thread-safe enqueue — callable from the event loop thread or a worker thread
        (RPC handlers run in RPCService's owned ThreadPoolExecutor, a different thread)."""
        assert self._loop is not None
        self._loop.call_soon_threadsafe(self._outgoing.put_nowait, (topic, payload, qos))

    # ------------------------------------------------------------------
    # Factory methods (API unchanged from the commlib-based version)
    # ------------------------------------------------------------------

    def create_publisher(self, *, topic: str, msg_type: type) -> Publisher:
        wrapper = Publisher(topic=topic, msg_type=msg_type)
        wrapper._bind(self)  # noqa: SLF001
        self._wrappers.append(wrapper)
        wrapper.start()
        return wrapper

    def create_subscriber(
        self,
        *,
        topic: str,
        on_message: Callable[[Any], None],
        msg_type: type | None = None,
    ) -> Subscriber:
        wrapper = Subscriber(topic=topic, on_message=on_message, msg_type=msg_type)

        def _edge_callback(raw: Any) -> None:
            if isinstance(raw, dict) and msg_type is not None:
                msg = (
                    msg_type.model_validate(raw)
                    if hasattr(msg_type, "model_validate")
                    else msg_type(**raw)
                )
            else:
                msg = raw
            # Read the wrapper's LIVE callback (not the closed-over on_message param) so
            # Subscriber.set_callback() calls after start() actually take effect.
            wrapper.on_message(msg)

        wrapper._bind(self, _edge_callback)  # noqa: SLF001
        self._wrappers.append(wrapper)
        wrapper.start()
        return wrapper

    def create_rpc(
        self,
        *,
        rpc_name: str,
        msg_type: type,
        on_request: Callable[[Any], Any],
    ) -> RPCService:
        wrapper = RPCService(rpc_name=rpc_name, msg_type=msg_type, on_request=on_request)
        wrapper._bind(self)  # noqa: SLF001
        self._wrappers.append(wrapper)
        wrapper.start()
        return wrapper

    def is_healthy(self) -> bool:
        return self._connected

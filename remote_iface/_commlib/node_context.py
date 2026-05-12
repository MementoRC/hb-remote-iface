"""NodeContext: owns commlib.node.Node, exposes factory methods, isolates the commlib import.

This is the ONLY file (along with transport.py and serialization.py) permitted to import commlib.*.
All wrappers receive commlib objects via _bind() and must not import commlib directly.
"""

from __future__ import annotations

import logging
import threading
from typing import TYPE_CHECKING, Any

from remote_iface._commlib.wrappers.publisher import Publisher
from remote_iface._commlib.wrappers.rpc_service import RPCService
from remote_iface._commlib.wrappers.subscriber import Subscriber

if TYPE_CHECKING:
    from collections.abc import Callable

    from remote_iface._commlib.transport import TransportFactory
    from remote_iface._commlib.wrappers.endpoint import Endpoint

_logger = logging.getLogger("remote_iface.NodeContext")


class NodeContext:
    """Owns a commlib Node, creates wrappers, and manages the shared lifecycle.

    Construction is always safe (no commlib imports). start() creates the Node and
    binds all deferred wrappers; stop() tears down in reverse registration order.

    Factory methods (create_publisher / create_subscriber / create_rpc) are the intended
    production path. Wrappers' __init__ remains public for unit tests.
    """

    def __init__(self, *, node_name: str, transport_factory: TransportFactory) -> None:
        self._node_name: str = node_name
        self._transport_factory: TransportFactory = transport_factory
        self._node: Any = None
        self._wrappers: list[Endpoint] = []
        # Wrappers registered before start() that need binding once the Node exists.
        self._pending_publishers: list[Publisher] = []
        # Subscribers paired with their edge callbacks for deferred binding.
        self._pending_subscribers: list[tuple[Subscriber, Callable[[Any], None]]] = []
        self._pending_rpc_services: list[RPCService] = []
        self._started: bool = False
        # Thread that runs Node.run() — kept as daemon so it cannot prevent process exit.
        self._node_thread: threading.Thread | None = None

    # ------------------------------------------------------------------
    # Lifecycle
    # ------------------------------------------------------------------

    def start(self) -> None:
        """Create the commlib Node, bind deferred wrappers, start everything.

        Node.run() in commlib 0.13.2 is synchronous (no wait= parameter — node.py:302).
        Running it directly would block forever; we push it onto a daemon thread so the
        caller is not blocked. This replaces the wait=False pattern that existed in the
        now-deleted 0.11.x-era branch (commit e996eb6).
        """
        if self._started:
            return

        from commlib.node import Node  # commlib import boundary — only entry point

        conn_params = self._transport_factory()
        self._node = Node(
            node_name=self._node_name,
            connection_params=conn_params,
            heartbeats=False,  # Heartbeat thread is an optional luxury; disable by default
            # to avoid spawning an extra thread per NodeContext in tests.
        )

        # Bind all wrappers that were created before start() was called.
        self._bind_pending()

        # node.run() blocks until node.stop() — run in a daemon thread (design:node_context).
        self._node_thread = threading.Thread(
            target=self._node.run,
            name=f"commlib-node-{self._node_name}",
            daemon=True,
        )
        self._node_thread.start()

        # Start our wrappers after the node thread is launched so the commlib objects are live.
        for wrapper in self._wrappers:
            wrapper.start()

        self._started = True

    def stop(self) -> None:
        """Stop all wrappers (reverse order) then stop the commlib Node.

        Reverse-order teardown (last-registered stopped first) mirrors typical resource
        ownership: later-created objects may depend on earlier-created ones, so we unwind
        in reverse (design:268). Each wrapper's stop() is already exception-safe via
        Endpoint.stop(), but we add an outer guard for defense in depth.
        """
        if not self._started:
            return

        # Stop wrappers in reverse registration order.
        for wrapper in reversed(self._wrappers):
            try:
                wrapper.stop()
            except Exception as exc:  # noqa: BLE001
                _logger.warning(
                    "NodeContext(%r): wrapper %s raised during stop: %s: %s",
                    self._node_name,
                    type(wrapper).__name__,
                    type(exc).__name__,
                    exc,
                )

        # Stop the commlib Node — this causes node.run() (in the daemon thread) to return.
        if self._node is not None:
            try:
                self._node.stop()
            except Exception as exc:  # noqa: BLE001
                _logger.warning(
                    "NodeContext(%r): commlib Node.stop() raised: %s: %s",
                    self._node_name,
                    type(exc).__name__,
                    exc,
                )

        # Join the node thread with a short timeout; log if it lingers.
        if self._node_thread is not None and self._node_thread.is_alive():
            self._node_thread.join(timeout=5.0)
            if self._node_thread.is_alive():
                _logger.warning(
                    "NodeContext(%r): node thread did not exit within 5s",
                    self._node_name,
                )

        self._node = None
        self._node_thread = None
        self._started = False
        # Do NOT clear _wrappers — a subsequent start() should reuse them.

    # ------------------------------------------------------------------
    # Factory methods
    # ------------------------------------------------------------------

    def create_publisher(self, *, topic: str, msg_type: type) -> Publisher:
        """Create a Publisher wrapper for `topic`, deferring commlib binding if not started."""
        wrapper = Publisher(topic=topic, msg_type=msg_type)
        if self._node is not None:
            cp = self._node.create_publisher(topic=topic, msg_type=msg_type)
            wrapper._bind(cp)  # noqa: SLF001
            wrapper.start()
        else:
            self._pending_publishers.append(wrapper)
        self._wrappers.append(wrapper)
        return wrapper

    def create_subscriber(
        self,
        *,
        topic: str,
        on_message: Callable[[Any], None],
        msg_type: type | None = None,
    ) -> Subscriber:
        """Create a Subscriber wrapper, deserializing payloads at the edge before delivery.

        Deserialize-at-edge (design:132): the callback delivered to commlib wraps
        `on_message` so the user callback always receives a typed object or dict, never raw bytes.
        Wildcard topics (containing '+' or '#') use create_psubscriber so MQTT pattern matching
        works correctly — Node.create_subscriber uses exact-match subscription internally.
        """
        from remote_iface._commlib.serialization import deserialize  # avoid circular at top level

        wrapper = Subscriber(topic=topic, on_message=on_message, msg_type=msg_type)

        def _edge_callback(raw: Any) -> None:
            # raw may be bytes (from MQTT payload) or already a dict (commlib may pre-parse).
            if isinstance(raw, (bytes, bytearray)):
                msg = deserialize(bytes(raw), msg_type)
            elif isinstance(raw, dict) and msg_type is not None:
                if hasattr(msg_type, "model_validate"):
                    msg = msg_type.model_validate(raw)
                else:
                    msg = msg_type(**raw)
            else:
                msg = raw
            on_message(msg)

        if self._node is not None:
            self._bind_subscriber(wrapper, _edge_callback, topic, msg_type)
            wrapper.start()
        else:
            # Pair wrapper with its edge callback so _bind_pending can bind both together.
            self._pending_subscribers.append((wrapper, _edge_callback))
        self._wrappers.append(wrapper)
        return wrapper

    def create_rpc(
        self,
        *,
        rpc_name: str,
        msg_type: type,
        on_request: Callable[[Any], Any],
    ) -> RPCService:
        """Create an RPCService wrapper, deferring commlib binding if not started."""
        wrapper = RPCService(rpc_name=rpc_name, msg_type=msg_type, on_request=on_request)
        if self._node is not None:
            cr = self._node.create_rpc(rpc_name=rpc_name, msg_type=msg_type, on_request=on_request)
            wrapper._bind(cr)  # noqa: SLF001
            wrapper.start()
        else:
            self._pending_rpc_services.append(wrapper)
        self._wrappers.append(wrapper)
        return wrapper

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    def _bind_pending(self) -> None:
        """Bind all wrappers deferred from pre-start factory calls to the now-live Node."""
        assert self._node is not None  # called only from start() after Node is constructed

        for wrapper in self._pending_publishers:
            cp = self._node.create_publisher(topic=wrapper.topic, msg_type=wrapper.msg_type)
            wrapper._bind(cp)  # noqa: SLF001

        for wrapper, edge_cb in self._pending_subscribers:
            self._bind_subscriber(wrapper, edge_cb, wrapper.topic, wrapper.msg_type)

        for wrapper in self._pending_rpc_services:
            cr = self._node.create_rpc(
                rpc_name=wrapper.rpc_name,
                msg_type=wrapper.msg_type,
                on_request=wrapper.on_request,
            )
            wrapper._bind(cr)  # noqa: SLF001

        self._pending_publishers.clear()
        self._pending_subscribers.clear()
        self._pending_rpc_services.clear()

    def _bind_subscriber(
        self,
        wrapper: Subscriber,
        edge_callback: Callable[[Any], None],
        topic: str,
        msg_type: type | None,
    ) -> None:
        """Bind a Subscriber wrapper to the correct commlib endpoint type.

        Topics with MQTT wildcards ('+' or '#') require create_psubscriber for pattern
        matching; exact topics use create_subscriber (node.py:419-458).
        """
        is_wildcard = "+" in topic or "#" in topic
        if is_wildcard:
            cs = self._node.create_psubscriber(  # type: ignore[union-attr]
                topic=topic, on_message=edge_callback, msg_type=msg_type
            )
        else:
            cs = self._node.create_subscriber(  # type: ignore[union-attr]
                topic=topic, on_message=edge_callback, msg_type=msg_type
            )
        wrapper._bind(cs)  # noqa: SLF001

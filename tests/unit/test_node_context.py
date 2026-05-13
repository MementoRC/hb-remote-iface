"""Unit tests for NodeContext orchestration (wrapper lifecycle, binding, reverse-order stop).

NodeContext imports commlib.node.Node lazily inside start() via:
    from commlib.node import Node

The correct patch target for that local import is "commlib.node.Node" because Python's import
system resolves it from the commlib.node module namespace.  Patching "remote_iface._commlib.
node_context.Node" would only work if Node were bound at module level, which it is not.
"""

import threading
import time
from unittest.mock import MagicMock, patch

import pytest

from remote_iface._commlib.node_context import NodeContext
from remote_iface._commlib.wrappers.publisher import Publisher
from remote_iface._commlib.wrappers.rpc_service import RPCService
from remote_iface._commlib.wrappers.subscriber import Subscriber

pytestmark = pytest.mark.unit

_SHORT_TIMEOUT = 1.0  # seconds for thread-related assertions

# Patch target: the Node class inside the commlib.node module (the source of the lazy import).
_NODE_PATCH = "commlib.node.Node"


def _make_mock_node() -> MagicMock:
    """Return a MagicMock that satisfies the commlib Node interface used by NodeContext."""
    node = MagicMock()
    node.run = MagicMock(return_value=None)
    return node


def _make_context() -> NodeContext:
    """Construct a NodeContext with a dummy transport factory (no real MQTT needed)."""

    def _transport_factory() -> object:
        return MagicMock()

    return NodeContext(node_name="test-node", transport_factory=_transport_factory)


def _start_with_mock(mock_node: MagicMock, ctx: NodeContext) -> None:
    """Start ctx with commlib.node.Node patched to return mock_node."""
    with patch(_NODE_PATCH, return_value=mock_node):
        ctx.start()


# ---------------------------------------------------------------------------
# Factory / binding tests
# ---------------------------------------------------------------------------


def test_create_publisher_before_start_returns_unbound_wrapper() -> None:
    mock_node = _make_mock_node()
    ctx = _make_context()
    pub = ctx.create_publisher(topic="t/1", msg_type=dict)
    assert isinstance(pub, Publisher)
    assert pub._cp is None  # not yet bound
    assert pub in ctx._wrappers
    _start_with_mock(mock_node, ctx)
    ctx.stop()


def test_start_binds_pending_wrappers() -> None:
    mock_node = _make_mock_node()
    ctx = _make_context()
    pub = ctx.create_publisher(topic="t/1", msg_type=dict)
    assert pub._cp is None
    _start_with_mock(mock_node, ctx)
    try:
        assert pub._cp is not None  # bound during start()
        assert pub.is_started is True
    finally:
        ctx.stop()


def test_create_publisher_after_start_immediately_binds() -> None:
    mock_node = _make_mock_node()
    ctx = _make_context()
    _start_with_mock(mock_node, ctx)
    try:
        pub = ctx.create_publisher(topic="t/2", msg_type=dict)
        assert pub._cp is not None
        assert pub.is_started is True
    finally:
        ctx.stop()


def test_create_subscriber_after_start_immediately_binds_and_starts() -> None:
    """Lines 184-191: create_subscriber when already started — binds and starts immediately."""
    mock_node = _make_mock_node()
    ctx = _make_context()
    _start_with_mock(mock_node, ctx)
    try:
        on_msg = MagicMock()
        sub = ctx.create_subscriber(topic="t/sub", on_message=on_msg, msg_type=dict)
        assert isinstance(sub, Subscriber)
        assert sub._cp is not None
        assert sub.is_started is True
        assert sub in ctx._wrappers
    finally:
        ctx.stop()


def test_create_rpc_after_start_immediately_binds_and_starts() -> None:
    """Lines 202-210: create_rpc when already started — binds and starts immediately."""
    mock_node = _make_mock_node()
    ctx = _make_context()
    _start_with_mock(mock_node, ctx)
    try:
        on_request = MagicMock(return_value={})
        rpc = ctx.create_rpc(rpc_name="my.rpc", msg_type=dict, on_request=on_request)
        assert isinstance(rpc, RPCService)
        assert rpc._cp is not None
        assert rpc.is_started is True
        assert rpc in ctx._wrappers
    finally:
        ctx.stop()


def test_create_subscriber_wildcard_topic_uses_psubscriber() -> None:
    """Lines 252-261: wildcard topic (+/#) routes to create_psubscriber, not create_subscriber."""
    mock_node = _make_mock_node()
    ctx = _make_context()
    _start_with_mock(mock_node, ctx)
    try:
        on_msg = MagicMock()
        ctx.create_subscriber(topic="t/+/data", on_message=on_msg, msg_type=None)
        mock_node.create_psubscriber.assert_called_once()
        mock_node.create_subscriber.assert_not_called()
    finally:
        ctx.stop()


def test_create_subscriber_exact_topic_uses_subscriber() -> None:
    """Lines 257-261: exact topic routes to create_subscriber, not create_psubscriber."""
    mock_node = _make_mock_node()
    ctx = _make_context()
    _start_with_mock(mock_node, ctx)
    try:
        on_msg = MagicMock()
        ctx.create_subscriber(topic="t/exact", on_message=on_msg, msg_type=None)
        mock_node.create_subscriber.assert_called_once()
        mock_node.create_psubscriber.assert_not_called()
    finally:
        ctx.stop()


def test_stop_logs_warning_when_node_stop_raises() -> None:
    """Lines 112-120: stop() swallows Node.stop() exceptions and logs warning."""
    mock_node = _make_mock_node()
    mock_node.stop = MagicMock(side_effect=RuntimeError("node boom"))
    ctx = _make_context()
    _start_with_mock(mock_node, ctx)
    ctx.stop()  # must not raise
    assert not ctx._started


# ---------------------------------------------------------------------------
# Reverse-order stop
# ---------------------------------------------------------------------------


def test_stop_invokes_each_wrapper_stop_in_reverse_order() -> None:
    mock_node = _make_mock_node()
    ctx = _make_context()
    _start_with_mock(mock_node, ctx)

    stop_order: list[str] = []

    # Create three publishers post-start so they're immediately bound.
    p1 = ctx.create_publisher(topic="t/p1", msg_type=dict)
    p2 = ctx.create_publisher(topic="t/p2", msg_type=dict)
    p3 = ctx.create_publisher(topic="t/p3", msg_type=dict)

    # Wrap each wrapper's stop() to record the call order.
    for label, wrapper in [("P1", p1), ("P2", p2), ("P3", p3)]:
        _orig = wrapper.stop

        def _recording_stop(_lbl: str = label, _fn=_orig) -> None:
            stop_order.append(_lbl)
            _fn()

        wrapper.stop = _recording_stop  # type: ignore[method-assign]

    ctx.stop()
    assert stop_order == ["P3", "P2", "P1"]


def test_stop_continues_after_one_wrapper_raises() -> None:
    mock_node = _make_mock_node()
    ctx = _make_context()
    _start_with_mock(mock_node, ctx)

    stopped: list[str] = []
    p1 = ctx.create_publisher(topic="t/q1", msg_type=dict)
    p2 = ctx.create_publisher(topic="t/q2", msg_type=dict)

    # p2 is registered second → stopped first in reverse order; it raises.
    p2.stop = MagicMock(side_effect=RuntimeError("boom"))  # type: ignore[method-assign]

    def _recording_p1_stop() -> None:
        stopped.append("P1")

    p1.stop = _recording_p1_stop  # type: ignore[method-assign]

    ctx.stop()  # must not raise; P1 must still be called after P2 raises
    assert "P1" in stopped


# ---------------------------------------------------------------------------
# Idempotency
# ---------------------------------------------------------------------------


def test_idempotent_start_and_stop() -> None:
    mock_node = _make_mock_node()
    ctx = _make_context()
    with patch(_NODE_PATCH, return_value=mock_node):
        ctx.start()
        ctx.start()  # second start must be no-op
    ctx.stop()
    ctx.stop()  # second stop must be no-op


# ---------------------------------------------------------------------------
# Node daemon thread
# ---------------------------------------------------------------------------


def test_node_run_is_called_in_background_thread() -> None:
    mock_node = _make_mock_node()
    run_started = threading.Event()
    stop_called = threading.Event()

    def _blocking_run() -> None:
        run_started.set()
        stop_called.wait(timeout=_SHORT_TIMEOUT)

    mock_node.run = _blocking_run

    ctx = _make_context()

    with patch(_NODE_PATCH, return_value=mock_node):
        t0 = time.monotonic()
        ctx.start()
        elapsed = time.monotonic() - t0

    # start() must return promptly (< 0.5s) even though node.run() blocks.
    assert elapsed < 0.5, f"start() blocked for {elapsed:.2f}s — daemon thread not working"
    assert run_started.wait(timeout=_SHORT_TIMEOUT), "node.run() never called in daemon thread"

    stop_called.set()
    ctx.stop()

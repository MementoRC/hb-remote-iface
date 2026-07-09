"""Unit tests for Subscriber wrapper (lifecycle, binding, callback replacement).

Subscriber no longer owns a commlib subscribe primitive — it registers an edge callback
into the owning NodeContext's _sub_callbacks table, keyed by topic pattern.
"""

from unittest.mock import MagicMock

import pytest

from remote_iface._commlib.wrappers.subscriber import Subscriber

pytestmark = pytest.mark.unit


def _noop(msg: object) -> None:
    pass


def test_construct_with_msg_type() -> None:
    sub = Subscriber(topic="test/topic", on_message=_noop, msg_type=dict)
    assert sub.topic == "test/topic"
    assert sub.msg_type is dict
    assert sub.on_message is _noop


def test_construct_without_msg_type() -> None:
    sub = Subscriber(topic="test/topic", on_message=_noop)
    assert sub.msg_type is None
    assert sub.on_message is _noop


def test_bind_sets_node_context_and_edge_callback() -> None:
    sub = Subscriber(topic="test/topic", on_message=_noop)
    mock_nc = MagicMock()
    edge_cb = MagicMock()
    sub._bind(mock_nc, edge_cb)
    assert sub._nc is mock_nc
    assert sub._edge_callback is edge_cb


def test_set_callback_replaces_stored_callback() -> None:
    sub = Subscriber(topic="test/topic", on_message=_noop)
    new_callback = MagicMock()
    sub.set_callback(new_callback)
    assert sub.on_message is new_callback


def test_do_start_asserts_when_unbound() -> None:
    sub = Subscriber(topic="test/topic", on_message=_noop)
    with pytest.raises(AssertionError):
        sub.start()


def test_start_registers_edge_callback_in_node_context_sub_callbacks() -> None:
    sub = Subscriber(topic="test/topic", on_message=_noop)
    mock_nc = MagicMock()
    mock_nc._sub_callbacks = {}
    edge_cb = MagicMock()
    sub._bind(mock_nc, edge_cb)
    sub.start()
    assert sub.is_started is True
    assert mock_nc._sub_callbacks["test/topic"] == [edge_cb]


def test_stop_removes_edge_callback_from_node_context_sub_callbacks() -> None:
    sub = Subscriber(topic="test/topic", on_message=_noop)
    mock_nc = MagicMock()
    mock_nc._sub_callbacks = {}
    edge_cb = MagicMock()
    sub._bind(mock_nc, edge_cb)
    sub.start()
    sub.stop()
    assert sub.is_started is False
    assert edge_cb not in mock_nc._sub_callbacks.get("test/topic", [])

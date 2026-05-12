"""Unit tests for Subscriber wrapper (lifecycle, binding, callback replacement)."""

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


def test_bind_sets_commlib_object() -> None:
    sub = Subscriber(topic="test/topic", on_message=_noop)
    mock_cs = MagicMock()
    sub._bind(mock_cs)
    assert sub._cp is mock_cs


def test_set_callback_replaces_stored_callback() -> None:
    sub = Subscriber(topic="test/topic", on_message=_noop)
    new_callback = MagicMock()
    sub.set_callback(new_callback)
    assert sub.on_message is new_callback


def test_set_callback_propagates_to_commlib_if_supported() -> None:
    sub = Subscriber(topic="test/topic", on_message=_noop)
    mock_cs = MagicMock()
    sub._bind(mock_cs)
    new_callback = MagicMock()
    sub.set_callback(new_callback)
    mock_cs.set_callback.assert_called_once_with(new_callback)


def test_start_then_stop_lifecycle() -> None:
    sub = Subscriber(topic="test/topic", on_message=_noop)
    mock_cs = MagicMock()
    sub._bind(mock_cs)
    sub.start()
    assert sub.is_started is True
    mock_cs.start.assert_called_once()
    sub.stop()
    assert sub.is_started is False
    mock_cs.stop.assert_called_once()

"""Unit tests for Publisher wrapper (lifecycle, binding, publish failure isolation)."""

from unittest.mock import MagicMock

import pytest

from remote_iface._commlib.wrappers.publisher import Publisher

pytestmark = pytest.mark.unit


def _make_publisher() -> Publisher:
    return Publisher(topic="test/topic", msg_type=dict)


def test_construct_keyword_only_args() -> None:
    with pytest.raises(TypeError):
        Publisher("test/topic", dict)  # type: ignore[call-arg]


def test_bind_sets_commlib_object() -> None:
    pub = _make_publisher()
    mock_cp = MagicMock()
    pub._bind(mock_cp)
    assert pub._cp is mock_cp
    assert pub.is_started is False


def test_publish_before_start_increments_failure_count() -> None:
    pub = _make_publisher()
    assert pub.publish_failure_count == 0
    pub.publish({"key": "value"})
    assert pub.publish_failure_count == 1


def test_publish_after_start_calls_commlib_publish() -> None:
    pub = _make_publisher()
    mock_cp = MagicMock()
    pub._bind(mock_cp)
    pub.start()
    message = {"key": "value"}
    pub.publish(message)
    mock_cp.publish.assert_called_once_with(message)


def test_publish_swallows_commlib_exception_and_counts_it() -> None:
    pub = _make_publisher()
    mock_cp = MagicMock()
    mock_cp.publish.side_effect = RuntimeError("transport gone")
    pub._bind(mock_cp)
    pub.start()
    pub.publish({"x": 1})  # must NOT raise
    assert pub.publish_failure_count == 1


def test_stop_calls_commlib_stop() -> None:
    pub = _make_publisher()
    mock_cp = MagicMock()
    pub._bind(mock_cp)
    pub.start()
    pub.stop()
    mock_cp.stop.assert_called_once()

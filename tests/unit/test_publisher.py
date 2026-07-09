"""Unit tests for Publisher wrapper (lifecycle, binding, publish failure isolation).

Publisher no longer owns a commlib publish primitive — it routes through the owning
NodeContext's thread-safe outgoing queue via _enqueue_outgoing().
"""

from unittest.mock import MagicMock

import pytest

from remote_iface._commlib.wrappers.publisher import Publisher

pytestmark = pytest.mark.unit


def _make_publisher() -> Publisher:
    return Publisher(topic="test/topic", msg_type=dict)


def _make_bound_publisher() -> tuple[Publisher, MagicMock]:
    pub = _make_publisher()
    mock_nc = MagicMock()
    pub._bind(mock_nc)
    return pub, mock_nc


def test_construct_keyword_only_args() -> None:
    with pytest.raises(TypeError):
        Publisher("test/topic", dict)  # type: ignore[call-arg]


def test_bind_sets_node_context() -> None:
    pub, mock_nc = _make_bound_publisher()
    assert pub._nc is mock_nc
    assert pub.is_started is False


def test_publish_before_start_increments_failure_count() -> None:
    pub = _make_publisher()
    assert pub.publish_failure_count == 0
    pub.publish({"key": "value"})
    assert pub.publish_failure_count == 1


def test_publish_after_start_enqueues_via_node_context() -> None:
    pub, mock_nc = _make_bound_publisher()
    pub.start()
    message = {"key": "value"}
    pub.publish(message)
    mock_nc._enqueue_outgoing.assert_called_once()
    call = mock_nc._enqueue_outgoing.call_args
    topic, payload = call.args
    assert topic == "test/topic"
    assert payload == b'{"key": "value"}'
    assert call.kwargs == {"qos": 0}


def test_publish_swallows_enqueue_exception_and_counts_it() -> None:
    pub, mock_nc = _make_bound_publisher()
    mock_nc._enqueue_outgoing.side_effect = RuntimeError("transport gone")
    pub.start()
    pub.publish({"x": 1})  # must NOT raise
    assert pub.publish_failure_count == 1


def test_do_start_asserts_when_unbound() -> None:
    pub = _make_publisher()
    with pytest.raises(AssertionError):
        pub.start()


def test_stop_is_a_noop_and_does_not_raise() -> None:
    pub, _mock_nc = _make_bound_publisher()
    pub.start()
    pub.stop()  # must not raise
    assert pub.is_started is False

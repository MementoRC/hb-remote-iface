"""Unit tests for RPCService wrapper (thread/executor lifecycle, leak-fix invariants)."""

from unittest.mock import MagicMock

import pytest

from remote_iface._commlib.wrappers.rpc_service import RPCService

pytestmark = pytest.mark.unit

_SHORT_TIMEOUT = 0.5  # seconds — keeps tests fast while still allowing threads to exit


def _on_request(msg: object) -> object:
    return {}


def _make_rpc(*, stop_timeout: float = _SHORT_TIMEOUT) -> RPCService:
    return RPCService(
        rpc_name="test.rpc", msg_type=dict, on_request=_on_request, stop_timeout=stop_timeout
    )


def _make_bound_rpc(*, stop_timeout: float = _SHORT_TIMEOUT) -> tuple[RPCService, MagicMock]:
    """Return a bound RPCService and its mock commlib object."""
    svc = _make_rpc(stop_timeout=stop_timeout)
    mock_cp = MagicMock()
    svc._bind(mock_cp)
    return svc, mock_cp


def test_construct_with_keyword_args() -> None:
    svc = RPCService(rpc_name="my.rpc", msg_type=dict, on_request=_on_request)
    assert svc.rpc_name == "my.rpc"
    assert svc.msg_type is dict
    assert svc.on_request is _on_request


def test_start_creates_executor_and_main_thread() -> None:
    svc, _ = _make_bound_rpc()
    svc.start()
    try:
        assert svc._executor is not None
        assert svc._main_thread is not None
        assert svc._main_thread.is_alive()
    finally:
        svc.stop()


def test_stop_drains_executor_and_joins_thread() -> None:
    svc, _ = _make_bound_rpc()
    svc.start()
    svc.stop()
    assert svc._executor is None
    assert svc._main_thread is None


def test_stop_with_no_executor_does_not_raise() -> None:
    svc = _make_rpc()
    # Never bound or started — _executor and _main_thread remain None.
    svc.stop()  # must not raise


def test_main_thread_exits_on_stop_event() -> None:
    svc, _ = _make_bound_rpc()
    svc.start()
    # Capture thread reference before stop() clears it.
    thread = svc._main_thread
    assert thread is not None
    svc.stop()
    # Give it a moment beyond the join inside stop() to confirm it's dead.
    thread.join(timeout=_SHORT_TIMEOUT)
    assert not thread.is_alive()


def test_double_stop_idempotent_and_no_thread_zombie() -> None:
    svc, _ = _make_bound_rpc()
    svc.start()
    svc.stop()
    svc.stop()  # must not raise, no zombie thread
    assert svc._executor is None
    assert svc._main_thread is None


def test_restart_after_stop_creates_fresh_executor() -> None:
    svc, _ = _make_bound_rpc()
    svc.start()
    first_executor = svc._executor
    first_thread = svc._main_thread
    svc.stop()
    svc.start()
    try:
        assert svc._executor is not None
        assert svc._main_thread is not None
        # Fresh instances — not the same objects as the first start.
        assert svc._executor is not first_executor
        assert svc._main_thread is not first_thread
    finally:
        svc.stop()


def test_stop_when_commlib_stop_raises_still_drains_local_resources() -> None:
    svc, mock_cp = _make_bound_rpc()
    mock_cp.stop.side_effect = RuntimeError("commlib exploded")
    svc.start()
    svc.stop()  # must not raise
    assert svc._executor is None
    assert svc._main_thread is None


def test_start_injects_executor_via_executor_attribute() -> None:
    """Lines 68-69: commlib object with 'executor' attribute gets our executor injected."""
    svc = _make_rpc()
    mock_cp = MagicMock(spec=["executor", "run", "stop"])
    mock_cp.executor = None  # ensure attribute exists (spec alone declares it)
    svc._bind(mock_cp)
    svc.start()
    try:
        assert mock_cp.executor is svc._executor
    finally:
        svc.stop()


def test_start_injects_executor_via_underscore_executor_attribute() -> None:
    """Lines 70-71: commlib object with '_executor' (no 'executor') gets our executor injected."""
    svc = _make_rpc()
    mock_cp = MagicMock(spec=["_executor", "run", "stop"])
    mock_cp._executor = None
    svc._bind(mock_cp)
    svc.start()
    try:
        assert mock_cp._executor is svc._executor
    finally:
        svc.stop()


def test_start_logs_debug_when_no_executor_attribute() -> None:
    """Lines 72-77: commlib object with neither executor attribute — debug-logged, no crash."""
    svc = _make_rpc()
    # spec limits attributes to only 'run' and 'stop' — no executor attribute of any kind.
    mock_cp = MagicMock(spec=["run", "stop"])
    svc._bind(mock_cp)
    svc.start()  # must not raise
    try:
        assert svc._executor is not None  # wrapper still owns its executor
    finally:
        svc.stop()


def test_start_calls_start_when_no_run_attribute() -> None:
    """Lines 88-89: commlib object without 'run' but with 'start' uses start() API."""
    svc = _make_rpc()
    mock_cp = MagicMock(spec=["start", "stop"])
    svc._bind(mock_cp)
    svc.start()
    try:
        mock_cp.start.assert_called_once()
        assert not hasattr(mock_cp, "run")  # confirm spec excluded 'run'
    finally:
        svc.stop()

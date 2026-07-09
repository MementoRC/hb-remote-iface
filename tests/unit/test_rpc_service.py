"""Unit tests for RPCService wrapper (executor lifecycle, NodeContext command-table binding).

RPCService no longer owns a commlib RPC primitive or a main-loop thread — it registers
(msg_type, self._dispatch) into the owning NodeContext._command_table and owns a bounded
ThreadPoolExecutor so handler dispatch is isolated from other services on the same
NodeContext.
"""

from unittest.mock import MagicMock

import pytest

from remote_iface._commlib.wrappers.rpc_service import RPCService

pytestmark = pytest.mark.unit

_SHORT_TIMEOUT = 0.5  # seconds — keeps tests fast while still allowing the executor to settle


def _on_request(msg: object) -> object:
    return {}


def _make_rpc(*, stop_timeout: float = _SHORT_TIMEOUT) -> RPCService:
    return RPCService(
        rpc_name="test.rpc", msg_type=dict, on_request=_on_request, stop_timeout=stop_timeout
    )


def _make_bound_rpc(*, stop_timeout: float = _SHORT_TIMEOUT) -> tuple[RPCService, MagicMock]:
    """Return a bound RPCService and its mock NodeContext."""
    svc = _make_rpc(stop_timeout=stop_timeout)
    mock_nc = MagicMock()
    mock_nc._command_table = {}
    svc._bind(mock_nc)
    return svc, mock_nc


def test_construct_with_keyword_args() -> None:
    svc = RPCService(rpc_name="my.rpc", msg_type=dict, on_request=_on_request)
    assert svc.rpc_name == "my.rpc"
    assert svc.msg_type is dict
    assert svc.on_request is _on_request


def test_do_start_asserts_when_unbound() -> None:
    svc = _make_rpc()
    with pytest.raises(AssertionError):
        svc.start()


def test_start_creates_executor_and_registers_command_table_entry() -> None:
    svc, mock_nc = _make_bound_rpc()
    svc.start()
    try:
        assert svc._executor is not None
        assert mock_nc._command_table["test.rpc"] == (dict, svc._dispatch)
    finally:
        svc.stop()


def test_stop_drains_executor_and_removes_command_table_entry() -> None:
    svc, mock_nc = _make_bound_rpc()
    svc.start()
    svc.stop()
    assert svc._executor is None
    assert "test.rpc" not in mock_nc._command_table


def test_stop_with_no_executor_does_not_raise() -> None:
    svc = _make_rpc()
    # Never bound or started — _executor remains None.
    svc.stop()  # must not raise


def test_double_stop_idempotent_and_no_executor_leak() -> None:
    svc, mock_nc = _make_bound_rpc()
    svc.start()
    svc.stop()
    svc.stop()  # must not raise
    assert svc._executor is None
    assert "test.rpc" not in mock_nc._command_table


def test_restart_after_stop_creates_fresh_executor() -> None:
    svc, _mock_nc = _make_bound_rpc()
    svc.start()
    first_executor = svc._executor
    svc.stop()
    svc.start()
    try:
        assert svc._executor is not None
        assert svc._executor is not first_executor
    finally:
        svc.stop()


def test_dispatch_submits_to_owned_executor_and_returns_result() -> None:
    svc, _mock_nc = _make_bound_rpc()
    svc.start()
    try:
        result = svc._dispatch({"req": True})
        assert result == {}
    finally:
        svc.stop()

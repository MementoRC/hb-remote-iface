"""Unit tests for Endpoint lifecycle contract (idempotent start/stop, never raises)."""

import pytest

from remote_iface._commlib.wrappers.endpoint import Endpoint

pytestmark = pytest.mark.unit


class _TrackedEndpoint(Endpoint):
    """Concrete subclass that records how many times _do_start/_do_stop are invoked."""

    def __init__(self, *, raise_on_stop: bool = False) -> None:
        super().__init__()
        self.start_count: int = 0
        self.stop_count: int = 0
        self._raise_on_stop = raise_on_stop

    def _do_start(self) -> None:
        self.start_count += 1

    def _do_stop(self) -> None:
        self.stop_count += 1
        if self._raise_on_stop:
            raise RuntimeError("intentional stop failure")


def test_start_then_stop_sets_is_started_flag() -> None:
    ep = _TrackedEndpoint()
    ep.start()
    assert ep.is_started is True
    ep.stop()
    assert ep.is_started is False


def test_double_start_is_idempotent() -> None:
    ep = _TrackedEndpoint()
    ep.start()
    ep.start()
    assert ep.start_count == 1
    assert ep.is_started is True


def test_double_stop_is_idempotent() -> None:
    ep = _TrackedEndpoint()
    ep.start()
    ep.stop()
    ep.stop()
    assert ep.stop_count == 1
    assert ep.is_started is False


def test_stop_before_start_is_no_op() -> None:
    ep = _TrackedEndpoint()
    ep.stop()  # must not raise
    assert ep.stop_count == 0
    assert ep.is_started is False


def test_stop_never_raises_even_if_do_stop_raises() -> None:
    ep = _TrackedEndpoint(raise_on_stop=True)
    ep.start()
    ep.stop()  # must NOT raise despite _do_stop() raising RuntimeError


def test_stop_clears_started_flag_even_on_failure() -> None:
    ep = _TrackedEndpoint(raise_on_stop=True)
    ep.start()
    ep.stop()
    assert ep.is_started is False


def test_start_then_stop_then_start_again() -> None:
    ep = _TrackedEndpoint()
    ep.start()
    ep.stop()
    ep.start()
    assert ep.is_started is True
    assert ep.start_count == 2

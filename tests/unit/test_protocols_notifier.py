"""Unit tests for NotifierProtocol structural contract."""

from __future__ import annotations

import pytest

from remote_iface.protocols.notifier import NotifierProtocol

pytestmark = pytest.mark.unit


class _GoodNotifier:
    """Minimal implementation that satisfies NotifierProtocol."""

    def add_msg_to_queue(self, msg: str) -> None:
        pass


class _BadNotifier:
    """Missing add_msg_to_queue — does NOT satisfy NotifierProtocol."""

    def send(self, msg: str) -> None:
        pass


def test_good_notifier_isinstance() -> None:
    assert isinstance(_GoodNotifier(), NotifierProtocol)


def test_bad_notifier_not_isinstance() -> None:
    assert not isinstance(_BadNotifier(), NotifierProtocol)


def test_notifier_protocol_exported_from_package() -> None:
    from remote_iface.protocols import NotifierProtocol as Exported

    assert Exported is NotifierProtocol


def test_notifier_protocol_is_runtime_checkable() -> None:
    """Protocol must carry @runtime_checkable so isinstance works."""
    # If not runtime_checkable, isinstance raises TypeError.
    try:
        isinstance(object(), NotifierProtocol)
    except TypeError:
        pytest.fail("NotifierProtocol is not runtime_checkable")

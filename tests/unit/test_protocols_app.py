"""Unit tests for HummingbotAppProtocol structural contract and type aliases."""

from __future__ import annotations

import asyncio
import logging
from typing import TYPE_CHECKING

import pytest

from remote_iface.protocols.app import (
    HummingbotAppProtocol,
    MarketEventCallback,
    StatusListener,
    UnsubscribeCallable,
)

if TYPE_CHECKING:
    from collections.abc import Iterable
    from typing import Any

    from remote_iface.protocols.notifier import NotifierProtocol

pytestmark = pytest.mark.unit


def _noop_unsubscribe() -> None:
    pass


class _StubApp:
    """Minimal stub implementing every HummingbotAppProtocol member."""

    @property
    def instance_id(self) -> str:
        return "stub-instance"

    @property
    def ev_loop(self) -> asyncio.AbstractEventLoop:
        return asyncio.get_event_loop()

    @property
    def strategy_name(self) -> str | None:
        return None

    @property
    def strategy(self) -> object | None:
        return None

    @property
    def client_config_map(self) -> object:
        return {}

    @property
    def strategy_config_map(self) -> object:
        return {}

    def logger(self) -> logging.Logger:
        return logging.getLogger("stub")

    def configurable_keys(self) -> Iterable[str]:
        return []

    def config(self, key: str | None = None, value: object | None = None) -> None:
        pass

    def start(
        self,
        log_level: str,
        script: str,
        conf: str,
        is_quickstart: bool,
    ) -> None:
        pass

    def stop(self, skip_order_cancellation: bool = False) -> None:
        pass

    def history(self, days: int, verbose: bool, precision: int) -> None:
        pass

    def balance(self, mode: str, args: list[str]) -> object:
        return {}

    async def start_check(
        self,
        log_level: str,
        script: str,
        conf: str,
        is_quickstart: bool,
    ) -> str:
        return ""

    async def stop_loop(self) -> str:
        return ""

    async def import_config_file(self, filename: str) -> str:
        return ""

    async def strategy_status(self) -> str:
        return ""

    async def get_history_trades_json(self, days: float) -> list[object]:
        return []

    def subscribe_market_events(self, callback: MarketEventCallback) -> UnsubscribeCallable:
        return _noop_unsubscribe

    def register_notifier(self, notifier: NotifierProtocol) -> None:
        pass

    def unregister_notifier(self, notifier: NotifierProtocol) -> None:
        pass

    def register_status_listener(self, callback: StatusListener) -> UnsubscribeCallable:
        return _noop_unsubscribe


class _BadApp:
    """Missing several required methods — does NOT satisfy HummingbotAppProtocol."""

    @property
    def instance_id(self) -> str:
        return "bad"


# ---------------------------------------------------------------------------
# isinstance checks
# ---------------------------------------------------------------------------


def test_stub_app_satisfies_protocol() -> None:
    assert isinstance(_StubApp(), HummingbotAppProtocol)


def test_bad_app_does_not_satisfy_protocol() -> None:
    assert not isinstance(_BadApp(), HummingbotAppProtocol)


def test_protocol_is_runtime_checkable() -> None:
    try:
        isinstance(object(), HummingbotAppProtocol)
    except TypeError:
        pytest.fail("HummingbotAppProtocol is not runtime_checkable")


# ---------------------------------------------------------------------------
# Type alias exports
# ---------------------------------------------------------------------------


def test_market_event_callback_exported() -> None:
    from remote_iface.protocols import MarketEventCallback as Exported

    assert Exported is MarketEventCallback


def test_status_listener_exported() -> None:
    from remote_iface.protocols import StatusListener as Exported

    assert Exported is StatusListener


def test_unsubscribe_callable_exported() -> None:
    from remote_iface.protocols import UnsubscribeCallable as Exported

    assert Exported is UnsubscribeCallable


def test_hummingbot_app_protocol_exported_from_package() -> None:
    from remote_iface.protocols import HummingbotAppProtocol as Exported

    assert Exported is HummingbotAppProtocol


# ---------------------------------------------------------------------------
# Verify no forbidden imports in app.py
# ---------------------------------------------------------------------------


def test_app_module_has_no_commlib_import() -> None:
    import inspect

    import remote_iface.protocols.app as app_mod

    src = inspect.getsource(app_mod)
    assert "commlib" not in src, "protocols/app.py must not import commlib"
    assert "_commlib" not in src, "protocols/app.py must not import _commlib"


def test_type_aliases_are_callable_types() -> None:
    """Type aliases must be Callable-based (checked via __class_getitem__ support)."""
    # All three are parameterised generics of Callable — checking they are not None suffices.
    assert MarketEventCallback is not None
    assert StatusListener is not None
    assert UnsubscribeCallable is not None
    # They should come from typing / collections.abc namespace
    origin = getattr(MarketEventCallback, "__origin__", None)
    assert origin is not None, "MarketEventCallback should be a parameterised Callable"


def _make_market_event_callback() -> Any:
    """Helper: construct a value matching MarketEventCallback signature."""

    def cb(_tag: int, _pubsub: object, _event: object) -> None:
        pass

    return cb


def _make_status_listener() -> Any:
    """Helper: construct a value matching StatusListener signature."""

    def cb(_text: str, _msg_type: str) -> None:
        pass

    return cb


def test_subscribe_market_events_returns_callable() -> None:
    app = _StubApp()
    cb = _make_market_event_callback()
    unsub = app.subscribe_market_events(cb)
    assert callable(unsub)


def test_register_status_listener_returns_callable() -> None:
    app = _StubApp()
    cb = _make_status_listener()
    unsub = app.register_status_listener(cb)
    assert callable(unsub)

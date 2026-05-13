"""Unit tests for plain-dataclass message types and MQTTStatusCode."""

from __future__ import annotations

import inspect
import re

import pytest

from remote_iface.protocols.messages import (
    BalanceLimitCommandMessage,
    BalancePaperCommandMessage,
    ConfigCommandMessage,
    ExternalEventMessage,
    HistoryCommandMessage,
    ImportCommandMessage,
    InternalEventMessage,
    LogMessage,
    MQTTStatusCode,
    NotifyMessage,
    StartCommandMessage,
    StatusCommandMessage,
    StatusUpdateMessage,
    StopCommandMessage,
)

pytestmark = pytest.mark.unit


# ---------------------------------------------------------------------------
# No forbidden imports
# ---------------------------------------------------------------------------


def test_messages_module_has_no_commlib_import() -> None:
    import remote_iface.protocols.messages as mod

    src = inspect.getsource(mod)
    bad = re.findall(r"^\s*(?:from|import)\s+(?:_?commlib)(?:\.|\s|$)", src, re.MULTILINE)
    assert not bad, f"protocols/messages.py must not import commlib; found: {bad}"


def test_messages_module_has_no_hummingbot_import() -> None:
    import remote_iface.protocols.messages as mod

    src = inspect.getsource(mod)
    bad = re.findall(r"^\s*(?:from|import)\s+hummingbot(?:\.|\s|$)", src, re.MULTILINE)
    assert not bad, f"protocols/messages.py must not import hummingbot; found: {bad}"


# ---------------------------------------------------------------------------
# Pub/Sub message defaults
# ---------------------------------------------------------------------------


def test_notify_message_defaults() -> None:
    m = NotifyMessage()
    assert m.seq is None
    assert m.timestamp is None
    assert m.msg is None


def test_notify_message_explicit() -> None:
    m = NotifyMessage(seq=1, timestamp=1000, msg="hello")
    assert m.seq == 1
    assert m.timestamp == 1000
    assert m.msg == "hello"


def test_status_update_message_defaults() -> None:
    m = StatusUpdateMessage()
    assert m.timestamp is None
    assert m.type is None
    assert m.msg is None


def test_status_update_message_explicit() -> None:
    m = StatusUpdateMessage(timestamp=2000, type="INFO", msg="running")
    assert m.type == "INFO"
    assert m.msg == "running"


def test_internal_event_message_defaults() -> None:
    m = InternalEventMessage()
    assert m.timestamp is None
    assert m.type is None
    assert m.data is None


def test_internal_event_message_explicit() -> None:
    m = InternalEventMessage(timestamp=3000, type="order", data={"id": "42"})
    assert m.data == {"id": "42"}


def test_log_message_defaults() -> None:
    m = LogMessage()
    assert m.timestamp == 0.0
    assert m.msg == ""
    assert m.level_no == 0
    assert m.level_name == ""
    assert m.logger_name == ""


def test_log_message_explicit() -> None:
    m = LogMessage(
        timestamp=1.5,
        msg="test log",
        level_no=20,
        level_name="INFO",
        logger_name="mylogger",
    )
    assert m.level_no == 20
    assert m.logger_name == "mylogger"


def test_external_event_message_defaults() -> None:
    m = ExternalEventMessage()
    assert m.timestamp is None
    assert m.sequence is None
    assert m.type is None
    assert m.data is None


def test_external_event_message_explicit() -> None:
    m = ExternalEventMessage(timestamp=4000, sequence=7, type="signal", data={"val": 1})
    assert m.sequence == 7
    assert m.data == {"val": 1}


# ---------------------------------------------------------------------------
# RPC request/response pairs
# ---------------------------------------------------------------------------


def test_start_command_request_defaults() -> None:
    r = StartCommandMessage.Request()
    assert r.log_level is None
    assert r.script is None
    assert r.conf is None
    assert r.is_quickstart is False
    assert r.async_backend is False


def test_start_command_response_defaults() -> None:
    r = StartCommandMessage.Response()
    assert r.status == 0
    assert r.msg == ""


def test_start_command_request_explicit() -> None:
    r = StartCommandMessage.Request(
        log_level="DEBUG", script="cross_exchange.py", is_quickstart=True
    )
    assert r.log_level == "DEBUG"
    assert r.is_quickstart is True


def test_stop_command_request_defaults() -> None:
    r = StopCommandMessage.Request()
    assert r.skip_order_cancellation is False
    assert r.async_backend is False


def test_stop_command_response_defaults() -> None:
    r = StopCommandMessage.Response()
    assert r.status == 0
    assert r.msg == ""


def test_config_command_request_defaults() -> None:
    r = ConfigCommandMessage.Request()
    assert r.params == []


def test_config_command_request_mutable_default_is_independent() -> None:
    r1 = ConfigCommandMessage.Request()
    r2 = ConfigCommandMessage.Request()
    r1.params.append(("key", "val"))
    assert r2.params == [], "mutable default must not be shared between instances"


def test_config_command_response_defaults() -> None:
    r = ConfigCommandMessage.Response()
    assert r.changes == []
    assert r.config == {}
    assert r.status == 0
    assert r.msg == ""


def test_config_command_response_mutable_default_is_independent() -> None:
    r1 = ConfigCommandMessage.Response()
    r2 = ConfigCommandMessage.Response()
    r1.changes.append("x")
    assert r2.changes == []


def test_import_command_request_defaults() -> None:
    r = ImportCommandMessage.Request()
    assert r.strategy == ""


def test_import_command_response_defaults() -> None:
    r = ImportCommandMessage.Response()
    assert r.status == 0
    assert r.msg == ""


def test_status_command_request_defaults() -> None:
    r = StatusCommandMessage.Request()
    assert r.async_backend is False


def test_status_command_response_defaults() -> None:
    r = StatusCommandMessage.Response()
    assert r.status == 0
    assert r.msg == ""
    assert r.data is None


def test_history_command_request_defaults() -> None:
    r = HistoryCommandMessage.Request()
    assert r.days == 0.0
    assert r.verbose is False
    assert r.precision == 8
    assert r.async_backend is False


def test_history_command_response_defaults() -> None:
    r = HistoryCommandMessage.Response()
    assert r.status == 0
    assert r.msg == ""
    assert r.trades == []


def test_history_command_response_trades_independent() -> None:
    r1 = HistoryCommandMessage.Response()
    r2 = HistoryCommandMessage.Response()
    r1.trades.append({"id": 1})
    assert r2.trades == []


def test_balance_limit_request_defaults() -> None:
    r = BalanceLimitCommandMessage.Request()
    assert r.exchange == ""
    assert r.asset == ""
    assert r.amount == 0.0


def test_balance_limit_response_defaults() -> None:
    r = BalanceLimitCommandMessage.Response()
    assert r.status == 0
    assert r.msg == ""
    assert r.data == ""


def test_balance_paper_request_defaults() -> None:
    r = BalancePaperCommandMessage.Request()
    assert r.asset == ""
    assert r.amount == 0.0


def test_balance_paper_response_defaults() -> None:
    r = BalancePaperCommandMessage.Response()
    assert r.status == 0
    assert r.msg == ""
    assert r.data == ""


# ---------------------------------------------------------------------------
# MQTTStatusCode
# ---------------------------------------------------------------------------


def test_mqtt_status_code_success() -> None:
    assert MQTTStatusCode.SUCCESS == 200


def test_mqtt_status_code_error() -> None:
    assert MQTTStatusCode.ERROR == 400


def test_mqtt_status_codes_are_ints() -> None:
    assert isinstance(MQTTStatusCode.SUCCESS, int)
    assert isinstance(MQTTStatusCode.ERROR, int)


# ---------------------------------------------------------------------------
# Package-level exports
# ---------------------------------------------------------------------------


def test_all_message_types_exported_from_package() -> None:
    import remote_iface.protocols as pkg

    assert pkg.BalanceLimitCommandMessage is BalanceLimitCommandMessage
    assert pkg.BalancePaperCommandMessage is BalancePaperCommandMessage
    assert pkg.ConfigCommandMessage is ConfigCommandMessage
    assert pkg.ExternalEventMessage is ExternalEventMessage
    assert pkg.HistoryCommandMessage is HistoryCommandMessage
    assert pkg.ImportCommandMessage is ImportCommandMessage
    assert pkg.InternalEventMessage is InternalEventMessage
    assert pkg.LogMessage is LogMessage
    assert pkg.MQTTStatusCode is MQTTStatusCode
    assert pkg.NotifyMessage is NotifyMessage
    assert pkg.StartCommandMessage is StartCommandMessage
    assert pkg.StatusCommandMessage is StatusCommandMessage
    assert pkg.StatusUpdateMessage is StatusUpdateMessage
    assert pkg.StopCommandMessage is StopCommandMessage

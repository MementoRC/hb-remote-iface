"""Unit tests for MQTTLogHandler gateway component."""

from __future__ import annotations

import logging
from unittest.mock import MagicMock

import pytest

from remote_iface.gateway.log_handler import MQTTLogHandler
from remote_iface.protocols.app import HummingbotAppProtocol
from remote_iface.protocols.config import GatewayConfig

pytestmark = pytest.mark.unit


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _make_gateway(enable_log_handler: bool = True) -> MagicMock:
    gw = MagicMock()
    gw._config = GatewayConfig(enable_log_handler=enable_log_handler, namespace="hb")
    gw._app = MagicMock(spec=HummingbotAppProtocol)
    gw._endpoints = []
    mock_pub = MagicMock()
    gw._node_context.create_publisher.return_value = mock_pub
    return gw


def _make_record(msg: str = "test message", level: int = logging.INFO) -> logging.LogRecord:
    return logging.LogRecord(
        name="test.logger",
        level=level,
        pathname="test.py",
        lineno=1,
        msg=msg,
        args=(),
        exc_info=None,
    )


# ---------------------------------------------------------------------------
# Gating
# ---------------------------------------------------------------------------


def test_start_is_noop_when_disabled() -> None:
    gw = _make_gateway(enable_log_handler=False)
    h = MQTTLogHandler()
    h.start(gw)
    gw._node_context.create_publisher.assert_not_called()
    assert h not in logging.getLogger().handlers


# ---------------------------------------------------------------------------
# start()
# ---------------------------------------------------------------------------


def test_start_creates_publisher() -> None:
    gw = _make_gateway()
    h = MQTTLogHandler()
    try:
        h.start(gw)
        gw._node_context.create_publisher.assert_called_once()
        call_kwargs = gw._node_context.create_publisher.call_args.kwargs
        assert call_kwargs["topic"] == "hb/log"
    finally:
        h.stop(gw)


def test_start_appends_publisher_to_endpoints() -> None:
    gw = _make_gateway()
    h = MQTTLogHandler()
    try:
        h.start(gw)
        assert len(gw._endpoints) == 1
    finally:
        h.stop(gw)


def test_start_attaches_to_root_logger() -> None:
    gw = _make_gateway()
    h = MQTTLogHandler()
    try:
        h.start(gw)
        assert h in logging.getLogger().handlers
    finally:
        h.stop(gw)


# ---------------------------------------------------------------------------
# stop()
# ---------------------------------------------------------------------------


def test_stop_removes_from_root_logger() -> None:
    gw = _make_gateway()
    h = MQTTLogHandler()
    h.start(gw)
    h.stop(gw)
    assert h not in logging.getLogger().handlers


def test_stop_stops_publisher() -> None:
    gw = _make_gateway()
    h = MQTTLogHandler()
    h.start(gw)
    pub = h._publisher
    h.stop(gw)
    pub.stop.assert_called_once()


def test_stop_removes_publisher_from_endpoints() -> None:
    gw = _make_gateway()
    h = MQTTLogHandler()
    h.start(gw)
    h.stop(gw)
    assert gw._endpoints == []


# ---------------------------------------------------------------------------
# emit()
# ---------------------------------------------------------------------------


def test_emit_publishes_log_message() -> None:
    gw = _make_gateway()
    h = MQTTLogHandler()
    try:
        h.start(gw)
        record = _make_record("hello world", logging.WARNING)
        h.emit(record)
        h._publisher.publish.assert_called_once()
        payload = h._publisher.publish.call_args.args[0]
        assert payload.level_no == logging.WARNING
        assert payload.level_name == "WARNING"
        assert payload.logger_name == "test.logger"
        assert "hello world" in payload.msg
    finally:
        h.stop(gw)


def test_emit_suppresses_exceptions() -> None:
    gw = _make_gateway()
    h = MQTTLogHandler()
    try:
        h.start(gw)
        h._publisher.publish.side_effect = RuntimeError("broker gone")
        record = _make_record()
        h.emit(record)  # must not raise
    finally:
        h.stop(gw)


def test_emit_noop_when_no_publisher() -> None:
    h = MQTTLogHandler()
    record = _make_record()
    h.emit(record)  # publisher is None — must not raise

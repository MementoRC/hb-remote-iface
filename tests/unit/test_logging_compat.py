"""Unit tests for the hb_compat logging shim wrapping hb-logger (issue #16).

Exercises ``get_logger()`` directly against the real ``logger`` (hb-logger)
dependency — no mocking, mirroring ``test_event_bus_adapter.py``'s approach
for the other hb_compat adapter.
"""

import logging

import pytest

pytestmark = pytest.mark.unit


def test_get_logger_returns_hummingbot_logger_instance() -> None:
    from logger import HummingbotLogger

    from remote_iface.hb_compat.logging_compat import get_logger

    result = get_logger("remote_iface.tests.logging_compat")

    assert isinstance(result, HummingbotLogger)


def test_get_logger_same_name_returns_same_instance() -> None:
    from remote_iface.hb_compat.logging_compat import get_logger

    first = get_logger("remote_iface.tests.logging_compat.same")
    second = get_logger("remote_iface.tests.logging_compat.same")

    assert first is second


def test_get_logger_matches_stdlib_getlogger_for_same_name() -> None:
    from remote_iface.hb_compat.logging_compat import get_logger

    name = "remote_iface.tests.logging_compat.stdlib_equivalence"
    result = get_logger(name)

    assert result is logging.getLogger(name)


def test_network_level_reexported() -> None:
    from logging import DEBUG

    from remote_iface.hb_compat.logging_compat import NETWORK

    assert NETWORK > DEBUG

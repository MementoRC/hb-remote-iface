"""Unit tests for GatewayConfig and BrokerConfig Pydantic models."""

from __future__ import annotations

import pytest
from pydantic import ValidationError

from remote_iface.protocols.config import BrokerConfig, GatewayConfig

pytestmark = pytest.mark.unit


# ---------------------------------------------------------------------------
# Default construction
# ---------------------------------------------------------------------------


def test_gateway_config_default_construction() -> None:
    cfg = GatewayConfig()
    assert cfg.namespace == "hummingbot"
    assert isinstance(cfg.broker, BrokerConfig)
    assert cfg.enable_commands is True
    assert cfg.enable_notifier is True
    assert cfg.enable_events is True
    assert cfg.enable_external_events is True
    assert cfg.enable_log_handler is True
    assert cfg.health_check_interval == 1.0
    assert cfg.restart_short_delay == 5.0
    assert cfg.restart_long_delay == 10.0
    assert cfg.external_event_queue_size == 1000
    assert cfg.command_timeout == 30.0


def test_broker_config_default_construction() -> None:
    bc = BrokerConfig()
    assert bc.host == "localhost"
    assert bc.port == 1883
    assert bc.username == ""
    assert bc.password == ""
    assert bc.ssl is False


def test_gateway_config_contains_default_broker() -> None:
    cfg = GatewayConfig()
    assert cfg.broker.host == "localhost"
    assert cfg.broker.port == 1883


# ---------------------------------------------------------------------------
# Custom values round-trip
# ---------------------------------------------------------------------------


def test_gateway_config_custom_values() -> None:
    cfg = GatewayConfig(
        namespace="mybot",
        broker=BrokerConfig(host="mqtt.example.com", port=8883, ssl=True),
        enable_commands=False,
        command_timeout=60.0,
        external_event_queue_size=500,
    )
    assert cfg.namespace == "mybot"
    assert cfg.broker.host == "mqtt.example.com"
    assert cfg.broker.port == 8883
    assert cfg.broker.ssl is True
    assert cfg.enable_commands is False
    assert cfg.command_timeout == 60.0
    assert cfg.external_event_queue_size == 500


# ---------------------------------------------------------------------------
# Port validator
# ---------------------------------------------------------------------------


def test_broker_port_zero_rejected() -> None:
    with pytest.raises(ValidationError, match="port"):
        BrokerConfig(port=0)


def test_broker_port_negative_rejected() -> None:
    with pytest.raises(ValidationError, match="port"):
        BrokerConfig(port=-1)


def test_broker_port_too_large_rejected() -> None:
    with pytest.raises(ValidationError, match="port"):
        BrokerConfig(port=70000)


def test_broker_port_boundary_values_accepted() -> None:
    assert BrokerConfig(port=1).port == 1
    assert BrokerConfig(port=65535).port == 65535


# ---------------------------------------------------------------------------
# Timing validators
# ---------------------------------------------------------------------------


def test_negative_health_check_interval_rejected() -> None:
    with pytest.raises(ValidationError):
        GatewayConfig(health_check_interval=-0.1)


def test_negative_restart_short_delay_rejected() -> None:
    with pytest.raises(ValidationError):
        GatewayConfig(restart_short_delay=-1.0)


def test_negative_restart_long_delay_rejected() -> None:
    with pytest.raises(ValidationError):
        GatewayConfig(restart_long_delay=-1.0)


def test_negative_command_timeout_rejected() -> None:
    with pytest.raises(ValidationError):
        GatewayConfig(command_timeout=-0.001)


def test_zero_timing_values_accepted() -> None:
    cfg = GatewayConfig(
        health_check_interval=0.0,
        restart_short_delay=0.0,
        restart_long_delay=0.0,
        command_timeout=0.0,
    )
    assert cfg.health_check_interval == 0.0
    assert cfg.command_timeout == 0.0


# ---------------------------------------------------------------------------
# Queue size validator
# ---------------------------------------------------------------------------


def test_queue_size_zero_rejected() -> None:
    with pytest.raises(ValidationError):
        GatewayConfig(external_event_queue_size=0)


def test_queue_size_negative_rejected() -> None:
    with pytest.raises(ValidationError):
        GatewayConfig(external_event_queue_size=-10)


def test_queue_size_one_accepted() -> None:
    cfg = GatewayConfig(external_event_queue_size=1)
    assert cfg.external_event_queue_size == 1


# ---------------------------------------------------------------------------
# extra="forbid"
# ---------------------------------------------------------------------------


def test_gateway_config_rejects_unknown_fields() -> None:
    with pytest.raises(ValidationError, match="extra"):
        GatewayConfig(unknown_field="oops")  # type: ignore[call-arg]


# ---------------------------------------------------------------------------
# Exports from package __init__
# ---------------------------------------------------------------------------


def test_broker_config_exported_from_package() -> None:
    from remote_iface.protocols import BrokerConfig as Exported

    assert Exported is BrokerConfig


def test_gateway_config_exported_from_package() -> None:
    from remote_iface.protocols import GatewayConfig as Exported

    assert Exported is GatewayConfig

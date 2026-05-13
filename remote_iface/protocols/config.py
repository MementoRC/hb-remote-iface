"""Gateway and broker configuration models."""

from __future__ import annotations

from pydantic import BaseModel, ConfigDict, Field, field_validator


class BrokerConfig(BaseModel):
    """MQTT broker connection parameters."""

    host: str = "localhost"
    port: int = 1883
    username: str = ""
    password: str = ""
    ssl: bool = False

    @field_validator("port", mode="before")
    @classmethod
    def _validate_port(cls, v: object) -> object:
        """Reject ports outside the valid TCP range 1–65535."""
        if isinstance(v, int) and not (1 <= v <= 65535):
            raise ValueError(f"port must be between 1 and 65535, got {v}")
        return v


class GatewayConfig(BaseModel):
    """Top-level gateway configuration.

    All fields have sensible defaults that match upstream ``MQTTGateway`` behaviour.
    Unknown extra fields are forbidden to surface configuration typos early.
    """

    model_config = ConfigDict(extra="forbid")

    namespace: str = "hummingbot"

    broker: BrokerConfig = Field(default_factory=BrokerConfig)

    # Feature toggles — default ON, matching upstream behaviour
    enable_commands: bool = True
    enable_notifier: bool = True
    enable_events: bool = True
    enable_external_events: bool = True
    enable_log_handler: bool = True

    # Timing / connection management
    health_check_interval: float = 1.0
    restart_short_delay: float = 5.0
    restart_long_delay: float = 10.0

    # Capacities and timeouts
    external_event_queue_size: int = 1000
    command_timeout: float = 30.0

    @field_validator(
        "health_check_interval",
        "restart_short_delay",
        "restart_long_delay",
        "command_timeout",
        mode="before",
    )
    @classmethod
    def _validate_non_negative(cls, v: object) -> object:
        """Reject negative timing values."""
        if isinstance(v, (int, float)) and v < 0:
            raise ValueError(f"timing value must be >= 0, got {v}")
        return v

    @field_validator("external_event_queue_size", mode="before")
    @classmethod
    def _validate_queue_size(cls, v: object) -> object:
        """Reject non-positive queue sizes."""
        if isinstance(v, int) and v <= 0:
            raise ValueError(f"external_event_queue_size must be > 0, got {v}")
        return v

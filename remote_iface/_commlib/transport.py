"""MQTT transport configuration and factory for the commlib private layer."""

from collections.abc import Callable
from dataclasses import dataclass


@dataclass(frozen=True)
class TransportConfig:
    """Immutable MQTT connection configuration.

    Maps 1-to-1 onto commlib.transports.mqtt.ConnectionParameters fields
    (mqtt.py:66-73, connection.py:13-30). Frozen so configs cannot be mutated after
    construction — the factory closure captures a snapshot of intent, not a live reference.
    """

    host: str = "localhost"
    port: int = 1883
    username: str | None = None
    password: str | None = None
    # MQTT protocol keepalive in seconds (commlib default: 60).
    keepalive: int = 60
    # SSL fields forwarded from BaseConnectionParameters (connection.py:27-28).
    ssl: bool = False
    ssl_insecure: bool = False


# A TransportFactory is a zero-argument callable that produces a commlib
# ConnectionParameters-shaped object each time it is called.
type TransportFactory = Callable[[], object]


def default_mqtt_transport_factory(config: TransportConfig) -> TransportFactory:
    """Return a callable that produces a commlib MQTT ConnectionParameters per `config`.

    Lazy import keeps commlib out of the module-load critical path and allows the factory to
    be constructed before the pixi env is fully initialised (e.g. during config validation).
    """

    def _factory() -> object:
        from commlib.transports.mqtt import ConnectionParameters  # commlib import boundary

        return ConnectionParameters(
            host=config.host,
            port=config.port,
            username=config.username or "",
            password=config.password or "",
            keepalive=config.keepalive,
            ssl=config.ssl,
            ssl_insecure=config.ssl_insecure,
        )

    return _factory

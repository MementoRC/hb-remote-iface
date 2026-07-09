"""MQTT transport configuration and factory for the aiomqtt-based transport layer."""

from collections.abc import Callable
from dataclasses import dataclass


@dataclass(frozen=True)
class TransportConfig:
    """Immutable MQTT connection configuration.

    Maps 1-to-1 onto aiomqtt.Client's connect-time kwargs. Frozen so configs cannot be
    mutated after construction — the factory closure captures a snapshot of intent.
    """

    host: str = "localhost"
    port: int = 1883
    username: str | None = None
    password: str | None = None
    keepalive: int = 60
    ssl: bool = False
    # Not yet mapped onto aiomqtt.TLSParameters — matches upstream's own gap
    # (hummingbot/remote_iface/mqtt.py:527 also ignores an "insecure" toggle).
    ssl_insecure: bool = False


type TransportFactory = Callable[[], object]


def default_mqtt_transport_factory(config: TransportConfig) -> TransportFactory:
    """Return a callable that produces a fresh aiomqtt.Client per `config` on each call.

    A fresh client per call matches upstream's _create_client() pattern
    (hummingbot/remote_iface/mqtt.py:524-535): NodeContext's reconnect loop calls this
    factory once per connection attempt, never reusing a client across reconnects.
    Lazy import keeps aiomqtt out of the module-load critical path.

    Note: aiomqtt.Client.__init__ calls asyncio.get_running_loop() internally, so this
    factory (and thus the returned callable) must be invoked from within a running event
    loop — matching the "build now, connect later via `async with`" pattern the caller
    (NodeContext's reconnect loop) already runs inside.
    """

    def _factory() -> object:
        import aiomqtt  # aiomqtt import boundary

        tls_params = aiomqtt.TLSParameters() if config.ssl else None
        return aiomqtt.Client(
            hostname=config.host,
            port=config.port,
            username=config.username or None,
            password=config.password or None,
            keepalive=config.keepalive,
            tls_params=tls_params,
        )

    return _factory

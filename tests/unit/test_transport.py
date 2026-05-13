"""Unit tests for TransportConfig and default_mqtt_transport_factory."""

import pytest

from remote_iface._commlib.transport import TransportConfig, default_mqtt_transport_factory

pytestmark = pytest.mark.unit


def test_transport_config_defaults() -> None:
    cfg = TransportConfig()
    assert cfg.host == "localhost"
    assert cfg.port == 1883


def test_default_mqtt_transport_factory_returns_callable() -> None:
    cfg = TransportConfig()
    factory = default_mqtt_transport_factory(cfg)
    assert callable(factory)


def test_factory_call_produces_connection_params_with_correct_host_port(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    recorded: dict[str, object] = {}

    class _FakeConnectionParameters:
        def __init__(self, **kwargs: object) -> None:
            recorded.update(kwargs)
            for k, v in kwargs.items():
                setattr(self, k, v)

    # Patch commlib's MQTT ConnectionParameters so the test does not depend on paho.
    import sys

    fake_module = type(sys)("commlib.transports.mqtt")
    fake_module.ConnectionParameters = _FakeConnectionParameters  # type: ignore[attr-defined]
    monkeypatch.setitem(sys.modules, "commlib.transports.mqtt", fake_module)

    cfg = TransportConfig(host="broker.example.com", port=8883, username="u", password="p")
    factory = default_mqtt_transport_factory(cfg)
    params = factory()

    assert params.host == "broker.example.com"  # type: ignore[attr-defined]
    assert params.port == 8883  # type: ignore[attr-defined]
    assert params.username == "u"  # type: ignore[attr-defined]
    assert params.password == "p"  # type: ignore[attr-defined]


def test_transport_config_is_frozen() -> None:
    cfg = TransportConfig()
    with pytest.raises((AttributeError, TypeError)):
        cfg.host = "other"  # type: ignore[misc]

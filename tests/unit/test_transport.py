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


async def test_factory_call_produces_aiomqtt_client_with_correct_host_port() -> None:
    import aiomqtt

    cfg = TransportConfig(host="broker.example.com", port=8883, username="u", password="p")
    factory = default_mqtt_transport_factory(cfg)
    client = factory()

    assert isinstance(client, aiomqtt.Client)
    assert client._hostname == "broker.example.com"  # noqa: SLF001 - no public accessor exists
    assert client._port == 8883  # noqa: SLF001 - no public accessor exists


def test_transport_config_is_frozen() -> None:
    cfg = TransportConfig()
    with pytest.raises((AttributeError, TypeError)):
        cfg.host = "other"  # type: ignore[misc]


async def test_default_mqtt_transport_factory_builds_aiomqtt_client() -> None:
    import aiomqtt

    config = TransportConfig(
        host="broker.local", port=1884, username="u", password="p", keepalive=30
    )
    factory = default_mqtt_transport_factory(config)
    client = factory()

    assert isinstance(client, aiomqtt.Client)
    # aiomqtt.Client.identifier is a public property; paho leaves the id as an empty
    # string until connect-time auto-generation, so we only assert it resolves cleanly.
    assert isinstance(client.identifier, str)


async def test_default_mqtt_transport_factory_ssl_uses_tls_parameters(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    import aiomqtt

    captured: dict[str, object] = {}
    real_client = aiomqtt.Client

    def _spy_client(*args: object, **kwargs: object) -> aiomqtt.Client:
        captured.update(kwargs)
        return real_client(*args, **kwargs)  # type: ignore[arg-type]

    monkeypatch.setattr(aiomqtt, "Client", _spy_client)

    config = TransportConfig(host="broker.local", ssl=True)
    factory = default_mqtt_transport_factory(config)
    factory()

    assert isinstance(captured["tls_params"], aiomqtt.TLSParameters)


async def test_default_mqtt_transport_factory_no_ssl_leaves_tls_params_unset(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    import aiomqtt

    captured: dict[str, object] = {}
    real_client = aiomqtt.Client

    def _spy_client(*args: object, **kwargs: object) -> aiomqtt.Client:
        captured.update(kwargs)
        return real_client(*args, **kwargs)  # type: ignore[arg-type]

    monkeypatch.setattr(aiomqtt, "Client", _spy_client)

    config = TransportConfig(host="broker.local", ssl=False)
    factory = default_mqtt_transport_factory(config)
    factory()

    assert captured["tls_params"] is None

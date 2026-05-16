"""create_gateway factory: wire any host app into the MQTT gateway.

This factory is the primary integration point.  It:
  1. Wraps the host ``app`` in ``HummingbotAppAdapter`` (decoupling the gateway
     from any host-specific type).
  2. Builds ``MQTTGateway`` with the supplied (or default) config.
  3. Conditionally registers each component via ``add_component()`` according to
     the ``enable_*`` toggles in ``GatewayConfig``.

No ``hummingbot.*`` symbols are imported here.
"""

from __future__ import annotations

from typing import Any

from remote_iface.gateway.commands import MQTTCommands
from remote_iface.gateway.external_events import MQTTExternalEvents
from remote_iface.gateway.gateway import MQTTGateway
from remote_iface.gateway.log_handler import MQTTLogHandler
from remote_iface.gateway.market_events import MQTTMarketEventForwarder
from remote_iface.gateway.notifier import MQTTNotifier
from remote_iface.gateway.status import MQTTStatusUpdates
from remote_iface.hb_compat.adapter import HummingbotAppAdapter
from remote_iface.protocols.config import BrokerConfig, GatewayConfig


def create_gateway(
    app: Any,
    config: GatewayConfig | None = None,
    broker_config: BrokerConfig | None = None,
) -> MQTTGateway:
    """Build a fully wired ``MQTTGateway`` for the given host application.

    Args:
        app: Any object satisfying ``HummingbotAppProtocol`` (host-side concrete
            class is intentionally not named — the sub-package stays decoupled).
        config: Gateway feature and timing configuration.  Defaults to
            ``GatewayConfig()`` (all components enabled, localhost broker).
        broker_config: MQTT broker connection parameters.  Defaults to
            ``BrokerConfig()`` (localhost:1883, no auth).

    Returns:
        A ``MQTTGateway`` with components registered per the ``enable_*`` toggles.
        The gateway is NOT started; call ``await gateway.start()`` to connect.
    """
    resolved_config = config or GatewayConfig()
    resolved_broker = broker_config or BrokerConfig()

    adapter = HummingbotAppAdapter(app)
    gateway = MQTTGateway(
        app=adapter,
        config=resolved_config,
        broker_config=resolved_broker,
    )

    if resolved_config.enable_commands:
        gateway.add_component(MQTTCommands())
    if resolved_config.enable_notifier:
        gateway.add_component(MQTTNotifier())
    if resolved_config.enable_status_updates:
        gateway.add_component(MQTTStatusUpdates())
    if resolved_config.enable_events:
        gateway.add_component(MQTTMarketEventForwarder())
    if resolved_config.enable_external_events:
        gateway.add_component(MQTTExternalEvents())
    if resolved_config.enable_log_handler:
        gateway.add_component(MQTTLogHandler())

    return gateway

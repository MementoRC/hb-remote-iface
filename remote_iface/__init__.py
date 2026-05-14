"""hb-remote-iface: MQTT/RPC Remote Interface Layer.

Provides a drop-in replacement for hummingbot.remote_iface.*, implementing
the MQTT and RPC interface layer for hummingbot as a standalone sub-package.
"""

from remote_iface.__about__ import __version__
from remote_iface.gateway import MQTTCommands, MQTTGateway
from remote_iface.protocols import (
    BrokerConfig,
    GatewayConfig,
    HummingbotAppProtocol,
    MQTTStatusCode,
)

__all__ = [
    "__version__",
    "BrokerConfig",
    "GatewayConfig",
    "HummingbotAppProtocol",
    "MQTTCommands",
    "MQTTGateway",
    "MQTTStatusCode",
]

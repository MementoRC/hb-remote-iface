"""Private commlib wrapper layer.

The ONLY place in remote_iface that imports commlib.*. External code must depend on this
layer's public API, never on commlib directly.
"""

from remote_iface._commlib.node_context import NodeContext
from remote_iface._commlib.serialization import deserialize, serialize
from remote_iface._commlib.transport import (
    TransportConfig,
    TransportFactory,
    default_mqtt_transport_factory,
)
from remote_iface._commlib.wrappers.endpoint import Endpoint
from remote_iface._commlib.wrappers.publisher import Publisher
from remote_iface._commlib.wrappers.rpc_service import RPCService
from remote_iface._commlib.wrappers.subscriber import Subscriber

__all__ = [
    "Endpoint",
    "NodeContext",
    "Publisher",
    "RPCService",
    "Subscriber",
    "TransportConfig",
    "TransportFactory",
    "default_mqtt_transport_factory",
    "deserialize",
    "serialize",
]

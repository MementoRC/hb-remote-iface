"""Host-side compatibility layer for hb-remote-iface.

This module provides the adapter and factory that wire any object satisfying
``HummingbotAppProtocol`` into the gateway without the sub-package taking a
hard dependency on the host application (no ``hummingbot.*`` imports here).

Pattern mirrors ``strategy_framework.hb_compat.OrchestratorAdapter`` from the
hb-strategy-framework sub-package.

Typical usage::

    from remote_iface.hb_compat import create_gateway

    gw = create_gateway(app=hb_app_instance)
    await gw.start()
"""

from remote_iface.hb_compat.adapter import HummingbotAppAdapter
from remote_iface.hb_compat.event_bus_adapter import EventBusAdapter
from remote_iface.hb_compat.factory import create_gateway
from remote_iface.hb_compat.logging_compat import NETWORK, HummingbotLogger, get_logger

__all__ = [
    "NETWORK",
    "EventBusAdapter",
    "HummingbotAppAdapter",
    "HummingbotLogger",
    "create_gateway",
    "get_logger",
]

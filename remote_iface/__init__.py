"""hb-remote-iface: MQTT/RPC Remote Interface Layer.

Provides a drop-in replacement for hummingbot.remote_iface.*, implementing
the MQTT and RPC interface layer for hummingbot as a standalone sub-package.

No public API symbols are exported yet — this is a scaffold release.
Public API will be added as hummingbot.remote_iface.* is migrated here.
"""

from remote_iface.__about__ import __version__

__all__ = [
    "__version__",
]

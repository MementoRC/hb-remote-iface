"""MQTTStatusUpdates: push-on-demand status publisher."""

from __future__ import annotations

import time
from typing import TYPE_CHECKING

from remote_iface.hb_compat.logging_compat import get_logger
from remote_iface.protocols.messages import StatusUpdateMessage

if TYPE_CHECKING:
    from collections.abc import Callable

    from remote_iface._commlib.wrappers.publisher import Publisher
    from remote_iface.gateway.gateway import MQTTGateway

_logger = get_logger("remote_iface.MQTTStatusUpdates")

_TOPIC_STATUS = "/status"


class MQTTStatusUpdates:
    """Gateway component that publishes status updates on demand.

    Registers a listener with the app via register_status_listener(). When the app
    triggers a status broadcast, the listener builds a StatusUpdateMessage and publishes
    it. There is no periodic timer — pushes happen only when the app requests them.
    """

    def __init__(self) -> None:
        self._publisher: Publisher | None = None
        self._unsubscribe: Callable[[], None] | None = None

    # ------------------------------------------------------------------
    # Component protocol
    # ------------------------------------------------------------------

    def start(self, gateway: MQTTGateway) -> None:
        """Create Publisher and register status listener if enable_status_updates is set."""
        if not gateway._config.enable_status_updates:  # noqa: SLF001
            return
        nc = gateway._node_context  # noqa: SLF001
        prefix = gateway._config.namespace  # noqa: SLF001
        self._publisher = nc.create_publisher(
            topic=f"{prefix}{_TOPIC_STATUS}", msg_type=StatusUpdateMessage
        )
        gateway._endpoints.append(self._publisher)  # noqa: SLF001
        self._unsubscribe = gateway._app.register_status_listener(self._on_status_event)  # noqa: SLF001

    def stop(self, gateway: MQTTGateway) -> None:
        """Invoke the unsubscribe callable and stop Publisher."""
        if self._unsubscribe is not None:
            try:
                self._unsubscribe()
            except Exception as exc:  # noqa: BLE001
                _logger.debug("MQTTStatusUpdates: unsubscribe raised (ignored): %s", exc)
            self._unsubscribe = None
        if self._publisher is not None:
            try:
                self._publisher.stop()
            except Exception as exc:  # noqa: BLE001
                _logger.debug("MQTTStatusUpdates: publisher stop raised (ignored): %s", exc)
            if self._publisher in gateway._endpoints:  # noqa: SLF001
                gateway._endpoints.remove(self._publisher)  # noqa: SLF001
            self._publisher = None

    # ------------------------------------------------------------------
    # Status listener callback
    # ------------------------------------------------------------------

    def _on_status_event(self, status_text: str, msg_type: str) -> None:
        """Receive a status push from the app and publish it over MQTT."""
        if self._publisher is None:
            return
        payload = StatusUpdateMessage(
            timestamp=int(time.time() * 1000),
            type=msg_type,
            msg=status_text,
        )
        try:
            self._publisher.publish(payload)
        except Exception as exc:  # noqa: BLE001
            _logger.warning("MQTTStatusUpdates: publish failed: %s", exc)

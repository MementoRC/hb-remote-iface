"""MQTTNotifier: publishes notification messages to the notify MQTT topic."""

from __future__ import annotations

import time
from typing import TYPE_CHECKING

from remote_iface.hb_compat.logging_compat import get_logger
from remote_iface.protocols.messages import NotifyMessage

if TYPE_CHECKING:
    from remote_iface._commlib.wrappers.publisher import Publisher
    from remote_iface.gateway.gateway import MQTTGateway

_logger = get_logger("remote_iface.MQTTNotifier")

_TOPIC_NOTIFY = "/notify"


class MQTTNotifier:
    """Gateway component that implements NotifierProtocol.

    On start(), registers itself with the app and creates a Publisher to the notify topic.
    On add_msg_to_queue(), publishes a NotifyMessage dataclass via the Publisher.
    """

    def __init__(self) -> None:
        self._publisher: Publisher | None = None
        self._seq: int = 0

    # ------------------------------------------------------------------
    # Component protocol
    # ------------------------------------------------------------------

    def start(self, gateway: MQTTGateway) -> None:
        """Create Publisher and register as notifier if enable_notifier is set."""
        if not gateway._config.enable_notifier:  # noqa: SLF001
            return
        nc = gateway._node_context  # noqa: SLF001
        prefix = gateway._config.namespace  # noqa: SLF001
        self._publisher = nc.create_publisher(
            topic=f"{prefix}{_TOPIC_NOTIFY}", msg_type=NotifyMessage
        )
        gateway._endpoints.append(self._publisher)  # noqa: SLF001
        gateway._app.register_notifier(self)  # noqa: SLF001

    def stop(self, gateway: MQTTGateway) -> None:
        """Unregister from app and stop Publisher."""
        try:
            gateway._app.unregister_notifier(self)  # noqa: SLF001
        except Exception as exc:  # noqa: BLE001
            _logger.debug("MQTTNotifier: unregister_notifier raised (ignored): %s", exc)
        if self._publisher is not None:
            try:
                self._publisher.stop()
            except Exception as exc:  # noqa: BLE001
                _logger.debug("MQTTNotifier: publisher stop raised (ignored): %s", exc)
            if self._publisher in gateway._endpoints:  # noqa: SLF001
                gateway._endpoints.remove(self._publisher)  # noqa: SLF001
            self._publisher = None

    # ------------------------------------------------------------------
    # NotifierProtocol
    # ------------------------------------------------------------------

    def add_msg_to_queue(self, msg: str) -> None:
        """Publish a notification message to the notify topic."""
        if self._publisher is None:
            return
        self._seq += 1
        payload = NotifyMessage(
            seq=self._seq,
            timestamp=int(time.time() * 1000),
            msg=msg,
        )
        try:
            self._publisher.publish(payload)
        except Exception as exc:  # noqa: BLE001
            _logger.warning("MQTTNotifier: publish failed: %s", exc)

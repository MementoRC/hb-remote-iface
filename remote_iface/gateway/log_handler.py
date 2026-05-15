"""MQTTLogHandler: logging.Handler that publishes log records to the log MQTT topic."""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING

from remote_iface.protocols.messages import LogMessage

if TYPE_CHECKING:
    from remote_iface._commlib.wrappers.publisher import Publisher
    from remote_iface.gateway.gateway import MQTTGateway

_TOPIC_LOG = "/log"


class MQTTLogHandler(logging.Handler):
    """Gateway component and logging.Handler that forwards log records over MQTT.

    On start(), attaches itself to the root logger. On emit(), builds a LogMessage
    dataclass and publishes it. Exceptions in emit() are suppressed — a broken broker
    must never affect logging infrastructure.
    """

    def __init__(self) -> None:
        super().__init__()
        self._publisher: Publisher | None = None

    # ------------------------------------------------------------------
    # Component protocol
    # ------------------------------------------------------------------

    def start(self, gateway: MQTTGateway) -> None:
        """Create Publisher and attach to root logger if enable_log_handler is set."""
        if not gateway._config.enable_log_handler:  # noqa: SLF001
            return
        nc = gateway._node_context  # noqa: SLF001
        prefix = gateway._config.namespace  # noqa: SLF001
        self._publisher = nc.create_publisher(topic=f"{prefix}{_TOPIC_LOG}", msg_type=LogMessage)
        gateway._endpoints.append(self._publisher)  # noqa: SLF001
        logging.getLogger().addHandler(self)

    def stop(self, gateway: MQTTGateway) -> None:
        """Remove from root logger and stop Publisher."""
        logging.getLogger().removeHandler(self)
        if self._publisher is not None:
            try:
                self._publisher.stop()
            except Exception as exc:  # noqa: BLE001
                logging.getLogger(__name__).debug(
                    "MQTTLogHandler: publisher stop raised (ignored): %s", exc
                )
            if self._publisher in gateway._endpoints:  # noqa: SLF001
                gateway._endpoints.remove(self._publisher)  # noqa: SLF001
            self._publisher = None

    # ------------------------------------------------------------------
    # logging.Handler
    # ------------------------------------------------------------------

    def emit(self, record: logging.LogRecord) -> None:
        """Publish a log record over MQTT. All exceptions are suppressed."""
        try:
            if self._publisher is None:
                return
            payload = LogMessage(
                timestamp=record.created,
                msg=self.format(record),
                level_no=record.levelno,
                level_name=record.levelname,
                logger_name=record.name,
            )
            self._publisher.publish(payload)
        except Exception:  # noqa: BLE001
            pass

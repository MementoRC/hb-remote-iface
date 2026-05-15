"""MQTTExternalEvents: subscribes to external/+ MQTT topic and dispatches to app."""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING

from remote_iface.protocols.messages import ExternalEventMessage

if TYPE_CHECKING:
    from remote_iface._commlib.wrappers.subscriber import Subscriber
    from remote_iface.gateway.gateway import MQTTGateway

_logger = logging.getLogger("remote_iface.MQTTExternalEvents")

_TOPIC_EXTERNAL = "external/+"


class MQTTExternalEvents:
    """Gateway component that subscribes to inbound external events.

    Subscribes to the wildcard topic ``external/+`` so any sub-topic under
    ``external/`` is delivered. Each received payload is dispatched directly to
    ``app.handle_external_event()`` — no queue buffering (the app is responsible
    for its own backpressure).
    """

    def __init__(self) -> None:
        self._subscriber: Subscriber | None = None

    # ------------------------------------------------------------------
    # Component protocol
    # ------------------------------------------------------------------

    def start(self, gateway: MQTTGateway) -> None:
        """Create Subscriber and bind external event dispatch if enable_external_events is set."""
        if not gateway._config.enable_external_events:  # noqa: SLF001
            return
        nc = gateway._node_context  # noqa: SLF001
        app = gateway._app  # noqa: SLF001

        def _dispatch(msg: object) -> None:
            try:
                app.handle_external_event(msg)
            except Exception as exc:  # noqa: BLE001
                _logger.warning("MQTTExternalEvents: handle_external_event raised: %s", exc)

        self._subscriber = nc.create_subscriber(
            topic=_TOPIC_EXTERNAL,
            on_message=_dispatch,
            msg_type=ExternalEventMessage,
        )
        gateway._endpoints.append(self._subscriber)  # noqa: SLF001

    def stop(self, gateway: MQTTGateway) -> None:
        """Stop Subscriber."""
        if self._subscriber is not None:
            try:
                self._subscriber.stop()
            except Exception as exc:  # noqa: BLE001
                _logger.debug("MQTTExternalEvents: subscriber stop raised (ignored): %s", exc)
            if self._subscriber in gateway._endpoints:  # noqa: SLF001
                gateway._endpoints.remove(self._subscriber)  # noqa: SLF001
            self._subscriber = None

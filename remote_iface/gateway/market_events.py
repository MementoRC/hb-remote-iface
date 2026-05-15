"""MQTTMarketEventForwarder: deferred-start market event publisher."""

from __future__ import annotations

import logging
import time
from typing import TYPE_CHECKING

from remote_iface.protocols.messages import InternalEventMessage

if TYPE_CHECKING:
    from collections.abc import Callable

    from remote_iface._commlib.wrappers.publisher import Publisher
    from remote_iface.gateway.gateway import MQTTGateway
    from remote_iface.protocols.app import HummingbotAppProtocol

_logger = logging.getLogger("remote_iface.MQTTMarketEventForwarder")

_TOPIC_EVENTS = "/market-events"


class MQTTMarketEventForwarder:
    """Gateway component that forwards market events to MQTT after strategy load.

    Two-stage wiring:
      1. start() registers a strategy-loaded callback with the app. No subscription yet.
      2. The callback calls app.subscribe_market_events() once the strategy is ready.

    This preserves the upstream contract: market events are only forwarded once markets
    are fully initialised (i.e., after the strategy is loaded).
    """

    def __init__(self) -> None:
        self._publisher: Publisher | None = None
        self._unsubscribe: Callable[[], None] | None = None

    # ------------------------------------------------------------------
    # Component protocol
    # ------------------------------------------------------------------

    def start(self, gateway: MQTTGateway) -> None:
        """Create Publisher and register strategy-loaded callback if enable_events is set.

        Does NOT call subscribe_market_events here — that happens in the callback
        registered with app.register_strategy_loaded_callback().
        """
        if not gateway._config.enable_events:  # noqa: SLF001
            return
        nc = gateway._node_context  # noqa: SLF001
        prefix = gateway._config.namespace  # noqa: SLF001
        self._publisher = nc.create_publisher(
            topic=f"{prefix}{_TOPIC_EVENTS}", msg_type=InternalEventMessage
        )
        gateway._endpoints.append(self._publisher)  # noqa: SLF001
        app: HummingbotAppProtocol = gateway._app  # noqa: SLF001
        gateway._app.register_strategy_loaded_callback(  # noqa: SLF001
            self._make_loaded_callback(app)
        )

    def stop(self, gateway: MQTTGateway) -> None:
        """Cancel market event subscription and stop Publisher."""
        if self._unsubscribe is not None:
            try:
                self._unsubscribe()
            except Exception as exc:  # noqa: BLE001
                _logger.debug("MQTTMarketEventForwarder: unsubscribe raised (ignored): %s", exc)
            self._unsubscribe = None
        if self._publisher is not None:
            try:
                self._publisher.stop()
            except Exception as exc:  # noqa: BLE001
                _logger.debug("MQTTMarketEventForwarder: publisher stop raised (ignored): %s", exc)
            if self._publisher in gateway._endpoints:  # noqa: SLF001
                gateway._endpoints.remove(self._publisher)  # noqa: SLF001
            self._publisher = None

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    def _make_loaded_callback(self, app: HummingbotAppProtocol) -> Callable[[], None]:
        """Return a closure that subscribes to market events when the strategy loads."""

        def _on_strategy_loaded() -> None:
            if self._publisher is None:
                return
            try:
                self._unsubscribe = app.subscribe_market_events(self._forward_event)
            except Exception as exc:  # noqa: BLE001
                _logger.warning("MQTTMarketEventForwarder: subscribe_market_events raised: %s", exc)

        return _on_strategy_loaded

    def _forward_event(self, event_tag: int, pubsub_obj: object, event_obj: object) -> None:
        """Receive a market event tuple and publish it over MQTT."""
        if self._publisher is None:
            return
        payload = InternalEventMessage(
            timestamp=int(time.time() * 1000),
            type=str(event_tag),
            data={"pubsub": str(pubsub_obj), "event": str(event_obj)},
        )
        try:
            self._publisher.publish(payload)
        except Exception as exc:  # noqa: BLE001
            _logger.warning("MQTTMarketEventForwarder: publish failed: %s", exc)

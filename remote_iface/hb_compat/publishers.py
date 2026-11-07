"""Drop-in ``ETopicPublisher`` matching the in-tree ``hummingbot.remote_iface.mqtt`` API.

In-tree signature: ``ETopicPublisher(topic, use_bot_prefix=False)``; the gateway is
resolved once via ``MQTTGateway.main()`` and ``send()`` / ``__call__`` never raise.

Lifecycle findings that shape this shim (core ``NodeContext`` / ``Publisher``):
  * Before ``NodeContext.start()`` a publish hits an assertion (no loop yet) inside
    ``Publisher.publish``, which is caught and counted as a failure: nothing is sent.
  * After ``stop()`` the old context keeps accepting ``_enqueue_outgoing`` calls into
    a queue nothing drains (or raises on a closed loop): messages are lost silently.
  * ``restart()`` rebuilds ``_node_context``; a cached core publisher stays bound to
    the dead context, so every later publish is lost.

Design: the core publisher is created lazily on first send and re-created whenever
``core._node_context`` is no longer the context it was created for. While the core
gateway is not running, sends are dropped with a debug log. In-tree instead queues
silently in that state; dropping is the deliberate difference (nothing is replayed
on reconnect).

No ``hummingbot.*`` symbols are imported here.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

from remote_iface.external.publishers import ETopicPublisher as _CoreETopicPublisher
from remote_iface.hb_compat.logging_compat import get_logger
from remote_iface.hb_compat.mqtt_gateway import MQTTGateway

if TYPE_CHECKING:
    from remote_iface._commlib.node_context import NodeContext
    from remote_iface.gateway.gateway import MQTTGateway as CoreMQTTGateway

_logger = get_logger(__name__)


class ETopicPublisher:
    """Publish dicts to one fixed MQTT topic; never raises from ``send``."""

    def __init__(
        self,
        topic: str,
        use_bot_prefix: bool | None = False,
        *,
        gateway: MQTTGateway | CoreMQTTGateway | None = None,
    ) -> None:
        resolved = gateway if gateway is not None else MQTTGateway.main()
        if resolved is None:
            raise Exception("MQTT Gateway not yet initialized")  # noqa: TRY002
        self._core: CoreMQTTGateway = (
            resolved.gateway if isinstance(resolved, MQTTGateway) else resolved
        )
        self._topic = topic
        self._use_bot_prefix = bool(use_bot_prefix)
        self._pub: _CoreETopicPublisher | None = None
        self._pub_context: NodeContext | None = None

    def _publisher(self) -> _CoreETopicPublisher:
        """Return a core publisher bound to the gateway's current NodeContext."""
        context = self._core._node_context  # noqa: SLF001
        if self._pub is None or self._pub_context is not context:
            self._pub = _CoreETopicPublisher(
                self._core, self._topic, use_bot_prefix=self._use_bot_prefix
            )
            self._pub_context = context
        return self._pub

    def send(self, msg: dict[str, Any]) -> None:
        """Publish *msg*; dropped (debug log) if the gateway is not running."""
        try:
            if not self._core.is_running():
                _logger.debug("ETopicPublisher(%r): gateway not running — dropping", self._topic)
                return
            self._publisher().send(msg)
        except Exception as exc:  # noqa: BLE001
            _logger.warning(
                "ETopicPublisher(%r): send failed: %s: %s", self._topic, type(exc).__name__, exc
            )

    def __call__(self, msg: dict[str, Any]) -> None:
        self.send(msg)

"""MQTTGateway: lifecycle orchestrator for gateway components.

Owns the NodeContext, manages a list of components, and runs a health-monitoring
asyncio task that triggers restart when the broker becomes unreachable.
"""

from __future__ import annotations

import asyncio
import contextlib
import logging
from typing import TYPE_CHECKING, Protocol, runtime_checkable

from remote_iface._commlib.node_context import NodeContext
from remote_iface._commlib.transport import TransportConfig, default_mqtt_transport_factory
from remote_iface.protocols.app import HummingbotAppProtocol

if TYPE_CHECKING:
    from remote_iface._commlib.wrappers.endpoint import Endpoint
    from remote_iface.protocols.config import BrokerConfig, GatewayConfig

_logger = logging.getLogger("remote_iface.MQTTGateway")


@runtime_checkable
class Component(Protocol):
    """Lifecycle protocol for gateway components.

    Components are created before the gateway starts and hold a reference to the
    gateway. On start() they create their endpoints via gateway._node_context and
    append them to gateway._endpoints. On stop() they drain and remove their endpoints.
    """

    def start(self, gateway: MQTTGateway) -> None:
        """Start component: create endpoints and append to gateway._endpoints."""
        ...

    def stop(self, gateway: MQTTGateway) -> None:
        """Stop component: drain owned endpoints and remove from gateway._endpoints."""
        ...


def _make_node_context(broker_config: BrokerConfig, namespace: str) -> NodeContext:
    """Build a NodeContext from a BrokerConfig and namespace string."""
    transport_cfg = TransportConfig(
        host=broker_config.host,
        port=broker_config.port,
        username=broker_config.username,
        password=broker_config.password,
        ssl=broker_config.ssl,
    )
    factory = default_mqtt_transport_factory(transport_cfg)
    return NodeContext(node_name=namespace, transport_factory=factory)


class MQTTGateway:
    """Composition-based MQTT gateway.

    Holds components (started/stopped in order) and a NodeContext (transport layer).
    No singleton. No inheritance from commlib.Node.
    """

    def __init__(
        self,
        app: HummingbotAppProtocol,
        config: GatewayConfig,
        broker_config: BrokerConfig,
    ) -> None:
        if not isinstance(app, HummingbotAppProtocol):
            raise TypeError(f"app must implement HummingbotAppProtocol, got {type(app).__name__!r}")
        self._app: HummingbotAppProtocol = app
        self._config: GatewayConfig = config
        self._broker_config: BrokerConfig = broker_config
        self._node_context: NodeContext = _make_node_context(broker_config, config.namespace)
        self._components: list[Component] = []
        self._endpoints: list[Endpoint] = []
        self._running: bool = False
        self._health_task: asyncio.Task[None] | None = None
        self._consecutive_failures: int = 0

    # ------------------------------------------------------------------
    # Component registration
    # ------------------------------------------------------------------

    def add_component(self, component: Component) -> None:
        """Register a component. Raises RuntimeError if the gateway is already running."""
        if self._running:
            raise RuntimeError(
                "Cannot add_component after gateway.start() — components must be registered "
                "before the gateway is started."
            )
        self._components.append(component)

    # ------------------------------------------------------------------
    # Lifecycle
    # ------------------------------------------------------------------

    async def start(self) -> None:
        """Start the gateway: start NodeContext, start components, launch health loop.

        Idempotent: if already running, logs WARN and returns immediately.
        NodeContext start failure is re-raised (no transport = cannot proceed).
        Individual component start failures are logged and skipped.
        """
        if self._running:
            _logger.warning("MQTTGateway.start() called while already running — ignoring")
            return

        # Transport MUST succeed; failure here is fatal for this start() attempt.
        self._node_context.start()

        for component in self._components:
            try:
                component.start(self)
            except Exception as exc:  # noqa: BLE001
                _logger.error(
                    "Component %s raised during start: %s: %s — continuing",
                    type(component).__name__,
                    type(exc).__name__,
                    exc,
                )

        loop = asyncio.get_running_loop()
        self._health_task = loop.create_task(self._health_loop(), name="mqtt-gateway-health")
        self._running = True

    async def stop(self) -> None:
        """Stop the gateway: cancel health loop, stop components in reverse, stop NodeContext.

        Idempotent: if not running, logs DEBUG and returns immediately. Never raises.
        """
        if not self._running:
            _logger.debug("MQTTGateway.stop() called while not running — ignoring")
            return

        # Cancel the health monitoring task.
        if self._health_task is not None and not self._health_task.done():
            self._health_task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await self._health_task
        self._health_task = None

        # Stop components in reverse order.
        for component in reversed(self._components):
            try:
                component.stop(self)
            except Exception as exc:  # noqa: BLE001
                _logger.error(
                    "Component %s raised during stop: %s: %s — continuing",
                    type(component).__name__,
                    type(exc).__name__,
                    exc,
                )

        self._endpoints.clear()

        try:
            self._node_context.stop()
        except Exception as exc:  # noqa: BLE001
            _logger.error("NodeContext.stop() raised: %s: %s", type(exc).__name__, exc)

        self._running = False

    async def restart(self) -> None:
        """Full restart: stop → rebuild NodeContext → start.

        Used by the health loop on repeated failures and available for admin use.
        """
        await self.stop()
        self._endpoints.clear()
        self._node_context = _make_node_context(self._broker_config, self._config.namespace)
        await self.start()

    # ------------------------------------------------------------------
    # Health monitoring
    # ------------------------------------------------------------------

    async def _health_loop(self) -> None:
        """Poll NodeContext health every config.health_check_interval seconds.

        Increments _consecutive_failures on each failed check; resets on success.
        When failures reach config.consecutive_failure_threshold, triggers restart().
        Exits cleanly on CancelledError.
        """
        try:
            while True:
                await asyncio.sleep(self._config.health_check_interval)
                try:
                    healthy = await self.is_healthy()
                except Exception:  # noqa: BLE001
                    healthy = False

                if healthy:
                    self._consecutive_failures = 0
                else:
                    self._consecutive_failures += 1
                    _logger.warning(
                        "MQTTGateway: consecutive health failures: %d/%d",
                        self._consecutive_failures,
                        self._config.consecutive_failure_threshold,
                    )
                    if self._consecutive_failures >= self._config.consecutive_failure_threshold:
                        _logger.error("MQTTGateway: health failure threshold reached — restarting")
                        self._consecutive_failures = 0
                        await self.restart()
        except asyncio.CancelledError:
            pass

    # ------------------------------------------------------------------
    # State accessors
    # ------------------------------------------------------------------

    def is_running(self) -> bool:
        """Return True if the gateway has been started and not yet stopped."""
        return self._running

    async def is_healthy(self) -> bool:
        """Delegate health check to the underlying NodeContext."""
        return self._node_context.is_healthy()

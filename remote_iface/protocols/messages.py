"""Plain dataclass message types for MQTT pub/sub and RPC command pairs.

No commlib imports. No hummingbot imports. Importable in isolation.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from typing import Any


# ---------------------------------------------------------------------------
# Pub/Sub messages
# ---------------------------------------------------------------------------

# Pub/sub message types use slots=True (no mutable defaults).
# RPC command wrapper classes use plain `class` as namespaces; only
# the inner Request/Response dataclasses carry data.


@dataclass(slots=True)
class NotifyMessage:
    """Outbound notification pushed to the notifier topic."""

    seq: int | None = None
    timestamp: int | None = None
    msg: str | None = None


@dataclass(slots=True)
class StatusUpdateMessage:
    """Outbound status update pushed to the status topic."""

    timestamp: int | None = None
    type: str | None = None  # shadows builtin; matches upstream field name
    msg: str | None = None


@dataclass(slots=True)
class InternalEventMessage:
    """Internal hummingbot event forwarded over MQTT."""

    timestamp: int | None = None
    type: str | None = None  # shadows builtin; matches upstream field name
    data: dict[str, Any] | None = None


@dataclass(slots=True)
class LogMessage:
    """Structured log record forwarded over MQTT."""

    timestamp: float = 0.0
    msg: str = ""
    level_no: int = 0
    level_name: str = ""
    logger_name: str = ""


@dataclass(slots=True)
class ExternalEventMessage:
    """Inbound external event received from a remote publisher."""

    timestamp: int | None = None
    sequence: int | None = None
    type: str | None = None  # shadows builtin; matches upstream field name
    data: dict[str, Any] | None = None


# ---------------------------------------------------------------------------
# RPC command message pairs
# ---------------------------------------------------------------------------


class StartCommandMessage:
    """RPC message pair for the ``start`` command."""

    @dataclass(slots=True)
    class Request:
        """Start command request payload."""

        log_level: str | None = None
        script: str | None = None
        conf: str | None = None
        is_quickstart: bool = False
        async_backend: bool = False

    @dataclass(slots=True)
    class Response:
        """Start command response payload."""

        status: int = 0
        msg: str = ""


class StopCommandMessage:
    """RPC message pair for the ``stop`` command."""

    @dataclass(slots=True)
    class Request:
        """Stop command request payload."""

        skip_order_cancellation: bool = False
        async_backend: bool = False

    @dataclass(slots=True)
    class Response:
        """Stop command response payload."""

        status: int = 0
        msg: str = ""


class ConfigCommandMessage:
    """RPC message pair for the ``config`` command."""

    @dataclass(slots=True)
    class Request:
        """Config command request payload."""

        params: list[tuple[str, Any]] = field(default_factory=list)

    @dataclass(slots=True)
    class Response:
        """Config command response payload."""

        changes: list[Any] = field(default_factory=list)
        config: dict[str, Any] = field(default_factory=dict)
        status: int = 0
        msg: str = ""


class ImportCommandMessage:
    """RPC message pair for the ``import`` command."""

    @dataclass(slots=True)
    class Request:
        """Import command request payload."""

        strategy: str = ""

    @dataclass(slots=True)
    class Response:
        """Import command response payload."""

        status: int = 0
        msg: str = ""


class StatusCommandMessage:
    """RPC message pair for the ``status`` command."""

    @dataclass(slots=True)
    class Request:
        """Status command request payload."""

        async_backend: bool = False

    @dataclass(slots=True)
    class Response:
        """Status command response payload."""

        status: int = 0
        msg: str = ""
        data: Any = None


class HistoryCommandMessage:
    """RPC message pair for the ``history`` command."""

    @dataclass(slots=True)
    class Request:
        """History command request payload."""

        days: float = 0.0
        verbose: bool = False
        precision: int = 8
        async_backend: bool = False

    @dataclass(slots=True)
    class Response:
        """History command response payload."""

        status: int = 0
        msg: str = ""
        trades: list[Any] = field(default_factory=list)


class BalanceLimitCommandMessage:
    """RPC message pair for the ``balance limit`` command."""

    @dataclass(slots=True)
    class Request:
        """Balance limit request payload."""

        exchange: str = ""
        asset: str = ""
        amount: float = 0.0

    @dataclass(slots=True)
    class Response:
        """Balance limit response payload."""

        status: int = 0
        msg: str = ""
        data: str = ""


class BalancePaperCommandMessage:
    """RPC message pair for the ``balance paper`` command."""

    @dataclass(slots=True)
    class Request:
        """Balance paper request payload."""

        asset: str = ""
        amount: float = 0.0

    @dataclass(slots=True)
    class Response:
        """Balance paper response payload."""

        status: int = 0
        msg: str = ""
        data: str = ""


# ---------------------------------------------------------------------------
# Status codes
# ---------------------------------------------------------------------------


class MQTTStatusCode:
    """Integer status codes used in RPC response payloads.

    Semantics match HTTP-style conventions: 2xx = success, 4xx = client error.
    """

    SUCCESS: int = 200
    ERROR: int = 400

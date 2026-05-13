"""Public API for the protocols contracts layer.

This package is importable without commlib, hummingbot, or any _commlib internals.
It provides structural Protocol definitions, Pydantic config models, and plain dataclass
message types that form the stable contract boundary for the remote interface gateway.
"""

from __future__ import annotations

from remote_iface.protocols.app import (
    HummingbotAppProtocol,
    MarketEventCallback,
    StatusListener,
    UnsubscribeCallable,
)
from remote_iface.protocols.config import BrokerConfig, GatewayConfig
from remote_iface.protocols.messages import (
    BalanceLimitCommandMessage,
    BalancePaperCommandMessage,
    ConfigCommandMessage,
    ExternalEventMessage,
    HistoryCommandMessage,
    ImportCommandMessage,
    InternalEventMessage,
    LogMessage,
    MQTTStatusCode,
    NotifyMessage,
    StartCommandMessage,
    StatusCommandMessage,
    StatusUpdateMessage,
    StopCommandMessage,
)
from remote_iface.protocols.notifier import NotifierProtocol

__all__ = [
    "BalanceLimitCommandMessage",
    "BalancePaperCommandMessage",
    "BrokerConfig",
    "ConfigCommandMessage",
    "ExternalEventMessage",
    "GatewayConfig",
    "HistoryCommandMessage",
    "HummingbotAppProtocol",
    "ImportCommandMessage",
    "InternalEventMessage",
    "LogMessage",
    "MQTTStatusCode",
    "MarketEventCallback",
    "NotifierProtocol",
    "NotifyMessage",
    "StartCommandMessage",
    "StatusCommandMessage",
    "StatusListener",
    "StatusUpdateMessage",
    "StopCommandMessage",
    "UnsubscribeCallable",
]

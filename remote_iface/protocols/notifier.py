"""Notifier protocol contract."""

from __future__ import annotations

from typing import Protocol, runtime_checkable


@runtime_checkable
class NotifierProtocol(Protocol):
    """Contract for objects that can receive notification messages.

    Any object implementing ``add_msg_to_queue`` satisfies this protocol.
    Upstream reference: ``hummingbot.notifier.notifier_base.NotifierBase``.
    """

    def add_msg_to_queue(self, msg: str) -> None:
        """Enqueue a notification message for delivery."""
        ...

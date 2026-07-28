"""Abstract base class for commlib endpoint wrappers."""

from abc import ABC, abstractmethod

from remote_iface.hb_compat.logging_compat import HummingbotLogger, get_logger


class Endpoint(ABC):
    """Lifecycle base for commlib endpoint wrappers (publishers, subscribers).

    Enforces idempotent start/stop semantics: calling start() twice is safe, as is calling
    stop() on an already-stopped endpoint. Subclass exceptions during _do_stop() are caught
    and logged at WARNING so that stop() never propagates — critical for teardown chains where
    one failing endpoint must not block others from stopping.
    """

    def __init__(self) -> None:
        self._started: bool = False
        # Named after the concrete subclass so log lines identify the actual wrapper type.
        self._logger: HummingbotLogger = get_logger(f"remote_iface.{type(self).__name__}")

    def start(self) -> None:
        """Start the endpoint. No-op if already started."""
        if self._started:
            self._logger.debug("%s already started — skipping", type(self).__name__)
            return
        self._do_start()
        self._started = True

    def stop(self) -> None:
        """Stop the endpoint. No-op if not started; never raises on subclass failure."""
        if not self._started:
            self._logger.debug("%s already stopped — skipping", type(self).__name__)
            return
        try:
            self._do_stop()
        except Exception as exc:  # noqa: BLE001
            # Log but do not re-raise — failures in one wrapper must not prevent others stopping.
            self._logger.warning(
                "%s raised during stop: %s: %s",
                type(self).__name__,
                type(exc).__name__,
                exc,
            )
        # Always clear the flag, even on failure, so the endpoint is not considered started.
        self._started = False

    @abstractmethod
    def _do_start(self) -> None:
        """Perform the actual start operation. Called only when not already started."""

    @abstractmethod
    def _do_stop(self) -> None:
        """Perform the actual stop operation. Called only when currently started."""

    @property
    def is_started(self) -> bool:
        """Return True if the endpoint has been started and not yet stopped."""
        return self._started

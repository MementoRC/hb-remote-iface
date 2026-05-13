"""RPCService wrapper over a commlib RPC service primitive.

This is the primary fix for the per-suite ~1600-thread leak: the wrapper owns the
ThreadPoolExecutor and is responsible for draining it on stop(), regardless of whether the
underlying commlib layer exposes its own executor lifecycle.
"""

import threading
from collections.abc import Callable
from concurrent.futures import ThreadPoolExecutor
from typing import Any

from remote_iface._commlib.wrappers.endpoint import Endpoint


class RPCService(Endpoint):
    """Wraps a commlib RPC service to provide lifecycle management and bounded thread ownership.

    The underlying commlib RPC service object (_cp) is injected after construction via _bind(),
    which NodeContext calls when it instantiates the transport-specific RPC service. Until
    _bind() is called, _do_start() will raise AssertionError. The wrapper owns the
    ThreadPoolExecutor (design:16-17) so that _do_stop() can drain it deterministically —
    the commlib layer cannot be trusted to release its worker threads, which is the source of
    the ~1600-thread leak observed per test suite (design:132-133).
    """

    def __init__(
        self,
        *,
        rpc_name: str,
        msg_type: type,
        on_request: Callable[[object], object],
        max_workers: int = 5,
        stop_timeout: float = 5.0,
    ) -> None:
        super().__init__()
        self._rpc_name: str = rpc_name
        self._msg_type: type = msg_type
        self._on_request: Callable[[object], object] = on_request
        self._max_workers: int = max_workers
        self._stop_timeout: float = stop_timeout
        self._cp: Any = None
        # Owned by this wrapper, not by commlib — leak-fix invariant (design:16-17, 132-133).
        self._executor: ThreadPoolExecutor | None = None
        self._main_thread: threading.Thread | None = None
        # Separate stop signal so _run_main_loop can exit during _do_stop(), which runs before
        # Endpoint.stop() clears _started. Cannot use _started as the exit signal from here.
        self._stop_event: threading.Event = threading.Event()

    def _bind(self, commlib_rpc_service: Any) -> None:
        """Inject the commlib RPC service created by NodeContext for this rpc_name."""
        self._cp = commlib_rpc_service

    def _do_start(self) -> None:
        assert self._cp is not None, (
            f"RPCService for '{self._rpc_name}' has no bound commlib RPC service"
            " — call _bind() first"
        )
        # Wrapper-owned executor: this is the resource the commlib layer fails to release
        # (design:16-17, 132-133). We own it so we can drain it unconditionally in _do_stop().
        self._executor = ThreadPoolExecutor(
            max_workers=self._max_workers,
            thread_name_prefix=f"rpc-{self._rpc_name}",
        )
        # Inject our executor into commlib if it exposes an executor attribute so that commlib
        # dispatches through our owned pool rather than creating its own internal threads.
        if hasattr(self._cp, "executor"):
            self._cp.executor = self._executor
        elif hasattr(self._cp, "_executor"):
            self._cp._executor = self._executor  # noqa: SLF001
        else:
            self._logger.debug(
                "RPCService(%r): commlib object exposes no executor attribute"
                " — wrapper executor will be drained on stop but commlib may create its own",
                self._rpc_name,
            )

        self._main_thread = threading.Thread(
            target=self._run_main_loop,
            name=f"rpc-main-{self._rpc_name}",
            daemon=True,
        )

        # Tolerate both commlib API shapes: run(wait=False) and start().
        if hasattr(self._cp, "run"):
            self._cp.run(wait=False)
        elif hasattr(self._cp, "start"):
            self._cp.start()

        self._main_thread.start()

    def _run_main_loop(self) -> None:
        # Placeholder for richer dispatch logic that gateway/ may add in a later slice.
        # Exits when _stop_event is set — which _do_stop() signals before joining this thread.
        while not self._stop_event.is_set():
            self._stop_event.wait(timeout=0.1)

    def _do_stop(self) -> None:
        # Step 1: signal the main loop to exit before we touch the commlib layer.
        self._stop_event.set()

        # Step 2: stop the commlib RPC service; failure is non-fatal.
        if self._cp is not None:
            try:
                self._cp.stop()
            except Exception as exc:  # noqa: BLE001
                self._logger.warning(
                    "RPCService(%r): commlib stop() raised %s: %s",
                    self._rpc_name,
                    type(exc).__name__,
                    exc,
                )

        # Step 3: shut down the owned executor — this is the actual leak fix.
        if self._executor is not None:
            self._executor.shutdown(wait=False, cancel_futures=True)
            self._executor = None

        # Step 4: join the main loop thread within the configured timeout.
        if self._main_thread is not None and self._main_thread.is_alive():
            self._main_thread.join(timeout=self._stop_timeout)
            if self._main_thread.is_alive():
                self._logger.warning(
                    "RPCService(%r): main thread did not exit within %.1fs",
                    self._rpc_name,
                    self._stop_timeout,
                )
        self._main_thread = None

        # Step 5: reset the stop event so a subsequent start()/stop() cycle works correctly.
        self._stop_event = threading.Event()

    @property
    def rpc_name(self) -> str:
        """Return the RPC service name (read-only view for NodeContext inspection)."""
        return self._rpc_name

    @property
    def msg_type(self) -> type:
        """Return the expected request message type."""
        return self._msg_type

    @property
    def on_request(self) -> Callable[[object], object]:
        """Return the current request handler callback."""
        return self._on_request

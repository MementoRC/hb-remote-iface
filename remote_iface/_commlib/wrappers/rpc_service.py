"""RPCService wrapper: owns a bounded ThreadPoolExecutor for handler dispatch,
registers into NodeContext._command_table for incoming request routing.
"""

from __future__ import annotations

from concurrent.futures import Future, ThreadPoolExecutor
from typing import TYPE_CHECKING

from remote_iface._commlib.wrappers.endpoint import Endpoint

if TYPE_CHECKING:
    from collections.abc import Callable

    from remote_iface._commlib.node_context import NodeContext


class RPCService(Endpoint):
    """Wraps an RPC topic + handler, registered into NodeContext._command_table.

    Owns a bounded ThreadPoolExecutor(max_workers=5) so one slow handler cannot starve
    other RPC services sharing the same NodeContext — preserved from the commlib-based
    design as a deliberate concurrency-isolation property (not upstream's shared executor).
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
        self._nc: NodeContext | None = None
        self._executor: ThreadPoolExecutor | None = None

    def _bind(self, node_context: NodeContext) -> None:
        """Inject the owning NodeContext."""
        self._nc = node_context

    def _do_start(self) -> None:
        assert self._nc is not None, (
            f"RPCService for '{self._rpc_name}' has no bound NodeContext — call _bind() first"
        )
        self._executor = ThreadPoolExecutor(
            max_workers=self._max_workers,
            thread_name_prefix=f"rpc-{self._rpc_name}",
        )
        self._nc._command_table[self._rpc_name] = (self._msg_type, self._dispatch)  # noqa: SLF001

    def _dispatch(self, request: object) -> Future:
        """Submit the handler call onto this service's OWN bounded executor and return
        the Future immediately (non-blocking) — called by NodeContext._dispatch_rpc.

        Deliberately does NOT block-and-wait here: NodeContext._dispatch_rpc runs inline
        on the event loop thread (no run_in_executor hop), so blocking in this method
        would stall the event loop. The caller attaches a completion callback instead.
        """
        return self._executor.submit(self._on_request, request)

    def _do_stop(self) -> None:
        if self._nc is not None:
            self._nc._command_table.pop(self._rpc_name, None)  # noqa: SLF001
        if self._executor is not None:
            self._executor.shutdown(wait=False, cancel_futures=True)
            self._executor = None

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

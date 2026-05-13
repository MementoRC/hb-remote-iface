"""In-process transport factory for tests using commlib's mock transport.

commlib 0.13.2's mock transport (commlib.transports.mock) provides ConnectionParameters and
a MockTransport that sets _connected=True on start() — but no shared in-process pub/sub
routing exists in the library. Each endpoint gets its own MockTransport instance and operates
independently, which is sufficient for unit tests that verify lifecycle contract without
needing end-to-end message delivery.

Decision: use commlib.transports.mock.ConnectionParameters directly via
make_mock_transport_factory(). No custom FakeMQTTBroker class is needed for Slice 4 because
all unit tests under test_*.py monkeypatch the commlib layer rather than exercising pub/sub.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

from commlib.transports.mock import ConnectionParameters

if TYPE_CHECKING:
    from collections.abc import Callable


def make_mock_transport_factory() -> Callable[[], ConnectionParameters]:
    """Return a zero-argument factory that produces a mock ConnectionParameters each call.

    Use this as the transport_factory argument to NodeContext in tests that need a live
    NodeContext without a real MQTT broker.
    """

    def _factory() -> ConnectionParameters:
        # host and port are required by BaseConnectionParameters (pydantic BaseModel).
        return ConnectionParameters(host="localhost", port=1883)

    return _factory

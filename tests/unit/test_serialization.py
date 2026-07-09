"""Unit tests for serialize/deserialize roundtrip via stdlib json."""

from dataclasses import dataclass

import pytest
from pydantic import BaseModel

from remote_iface._commlib.serialization import deserialize, serialize
from remote_iface.protocols.messages import NotifyMessage

pytestmark = pytest.mark.unit


def test_serialize_dict_returns_bytes_and_roundtrips() -> None:
    data = {"x": 1, "y": "hello"}
    payload = serialize(data)
    assert isinstance(payload, bytes)
    result = deserialize(payload)
    assert result == data


def test_deserialize_with_pydantic_model() -> None:
    class Msg(BaseModel):
        count: int
        label: str

    data = {"count": 42, "label": "ok"}
    payload = serialize(data)
    msg = deserialize(payload, msg_type=Msg)
    assert isinstance(msg, Msg)
    assert msg.count == 42
    assert msg.label == "ok"


def test_deserialize_with_dataclass_msg_type() -> None:
    @dataclass
    class Msg:
        count: int
        label: str

    data = {"count": 7, "label": "test"}
    payload = serialize(data)
    msg = deserialize(payload, msg_type=Msg)
    assert isinstance(msg, Msg)
    assert msg.count == 7
    assert msg.label == "test"


def test_deserialize_without_msg_type_returns_raw_dict() -> None:
    data = {"a": 1, "b": 2}
    payload = serialize(data)
    result = deserialize(payload)
    assert isinstance(result, dict)
    assert result == data


def test_serialize_pydantic_model_uses_model_dump() -> None:
    """Line 22 branch: hasattr(message, 'model_dump') — Pydantic model path."""

    class Msg(BaseModel):
        x: int
        y: str

    msg = Msg(x=3, y="hi")
    payload = serialize(msg)
    assert isinstance(payload, bytes)
    result = deserialize(payload)
    assert result == {"x": 3, "y": "hi"}


def test_serialize_plain_object_uses_dict() -> None:
    """Line 24 branch: hasattr(message, '__dict__') — plain object (not Pydantic, not dict)."""

    class Plain:
        def __init__(self, a: int, b: str) -> None:
            self.a = a
            self.b = b

    obj = Plain(a=99, b="world")
    payload = serialize(obj)
    assert isinstance(payload, bytes)
    result = deserialize(payload)
    assert result["a"] == 99
    assert result["b"] == "world"


def test_serialize_dataclass_roundtrip() -> None:
    """Test non-slotted dataclass (as per task plan)."""

    @dataclass
    class _Sample:
        a: int
        b: str

    raw = serialize(_Sample(a=1, b="x"))
    assert isinstance(raw, bytes)
    restored = deserialize(raw, _Sample)
    assert restored == _Sample(a=1, b="x")


def test_serialize_slotted_dataclass_roundtrip() -> None:
    """Test slotted dataclass (real message types in this repo all use slots=True).

    This is the critical test: NotifyMessage and other real message types from
    remote_iface.protocols.messages use @dataclass(slots=True), which do NOT have
    __dict__. The serialize() function must handle slotted dataclasses correctly
    using dataclasses.asdict() instead of relying on __dict__.
    """
    msg = NotifyMessage(seq=42, timestamp=1000, msg="test")
    payload = serialize(msg)
    assert isinstance(payload, bytes)

    # Deserialize back to the same type
    restored = deserialize(payload, msg_type=NotifyMessage)
    assert isinstance(restored, NotifyMessage)
    assert restored.seq == 42
    assert restored.timestamp == 1000
    assert restored.msg == "test"
    assert restored == msg


def test_serialize_slotted_dataclass_with_none_fields() -> None:
    """Test slotted dataclass with optional fields (all None)."""
    msg = NotifyMessage()  # All fields default to None
    payload = serialize(msg)
    assert isinstance(payload, bytes)

    restored = deserialize(payload, msg_type=NotifyMessage)
    assert restored == msg
    assert restored.seq is None
    assert restored.timestamp is None
    assert restored.msg is None

"""Unit tests for serialize/deserialize roundtrip via JSONSerializer facade."""

from dataclasses import dataclass

import pytest
from pydantic import BaseModel

from remote_iface._commlib.serialization import deserialize, serialize

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

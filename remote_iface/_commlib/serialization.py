"""Thin serialize/deserialize facade over commlib's JSONSerializer.

JSONSerializer.serialize() returns str (not bytes) — serializer.py:80.
JSONSerializer.deserialize() accepts str and returns Dict[str, Any] — serializer.py:89.
We encode/decode UTF-8 at this boundary so callers deal only with bytes.
"""

from typing import Any


def serialize(message: Any) -> bytes:
    """Encode a message to UTF-8 bytes via commlib's JSONSerializer.

    Accepts a commlib msg instance, a pydantic model, or a plain dict.
    Pydantic models are converted via model_dump() before serialization so
    nested types (Decimal, enum, etc.) are reduced to JSON primitives by
    JSONSerializer.make_primitives() (serializer.py:125-138).
    """
    from commlib.serializer import JSONSerializer  # commlib import boundary

    if hasattr(message, "model_dump"):
        data: Any = message.model_dump()
    elif hasattr(message, "__dict__"):
        data = message.__dict__
    else:
        data = message
    return JSONSerializer.serialize(data).encode("utf-8")


def deserialize(payload: bytes, msg_type: type | None = None) -> Any:
    """Decode UTF-8 bytes and optionally hydrate into msg_type.

    Deserialize-at-edge pattern (design:132): callers receive typed objects,
    not raw dicts, so the transport boundary is the single point of type coercion.
    """
    from commlib.serializer import JSONSerializer  # commlib import boundary

    data: dict[str, Any] = JSONSerializer.deserialize(payload.decode("utf-8"))
    if msg_type is None:
        return data
    # Pydantic v2 models: use model_validate for strict coercion.
    if hasattr(msg_type, "model_validate"):
        return msg_type.model_validate(data)
    # commlib msg types and plain dataclasses: keyword-unpack.
    return msg_type(**data)

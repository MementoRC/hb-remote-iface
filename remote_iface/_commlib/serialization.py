"""Thin serialize/deserialize facade over stdlib json.

Replaces the commlib JSONSerializer facade (Phase 0/1 finding: commlib's own JSON
encoding added no behavior beyond str->bytes framing that stdlib json doesn't already do).
"""

import dataclasses
import json
from typing import Any


def serialize(message: Any) -> bytes:
    """Encode a message to UTF-8 JSON bytes.

    Accepts a pydantic model, a dataclass (with or without slots=True), or a plain dict.
    """
    if hasattr(message, "model_dump"):
        # Pydantic v2 model
        data: Any = message.model_dump()
    elif dataclasses.is_dataclass(message) and not isinstance(message, type):
        # Dataclass instance (handles both slots=True and slots=False). asdict() only
        # accepts instances, not dataclass types themselves (is_dataclass() is True for
        # both), so the isinstance(message, type) check excludes the class case.
        data = dataclasses.asdict(message)
    elif hasattr(message, "__dict__"):
        # Plain object with __dict__
        data = message.__dict__
    else:
        # Assume it's already a dict or JSON-serializable primitive
        data = message
    return json.dumps(data).encode("utf-8")


def deserialize(payload: bytes, msg_type: type | None = None) -> Any:
    """Decode UTF-8 JSON bytes and optionally hydrate into msg_type.

    Deserialize-at-edge pattern (design:132): callers receive typed objects,
    not raw dicts, so the transport boundary is the single point of type coercion.
    """
    data: dict[str, Any] = json.loads(payload.decode("utf-8"))
    if msg_type is None:
        return data
    # Pydantic v2 models: use model_validate for strict coercion.
    if hasattr(msg_type, "model_validate"):
        return msg_type.model_validate(data)
    # Dataclasses and plain types: keyword-unpack.
    return msg_type(**data)

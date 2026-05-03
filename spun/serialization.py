from typing import Any

import cloudpickle

from .errors import SerializationError


def dumps(value: Any) -> bytes:
    try:
        return cloudpickle.dumps(value)
    except Exception as exc:
        raise SerializationError(str(exc)) from exc


def loads(value: bytes) -> Any:
    try:
        return cloudpickle.loads(value)
    except Exception as exc:
        raise SerializationError(str(exc)) from exc

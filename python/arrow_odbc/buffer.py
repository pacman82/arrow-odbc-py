from typing import Optional, Tuple
from cffi import FFI


def to_bytes_and_len(value: Optional[str]) -> Tuple[bytes, int]:
    if value is None:
        value_bytes = FFI.NULL
        value_len = 0
    else:
        value_bytes = value.encode("utf-8")
        value_len = len(value_bytes)

    return (value_bytes, value_len)

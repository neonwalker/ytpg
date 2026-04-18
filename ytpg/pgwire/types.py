from __future__ import annotations

import struct
from typing import Any

OID_BOOL = 16
OID_INT4 = 23
OID_INT8 = 20
OID_FLOAT8 = 701
OID_TEXT = 25
OID_TIMESTAMP = 1114
OID_NUMERIC = 1700
OID_BYTEA = 17
OID_UNKNOWN = 0


def python_to_oid(value: Any) -> int:
    if value is None:
        return OID_TEXT
    if isinstance(value, bool):
        return OID_BOOL
    if isinstance(value, int):
        return OID_INT8 if abs(value) > 2**31 else OID_INT4
    if isinstance(value, float):
        return OID_FLOAT8
    return OID_TEXT


def encode_value(value: Any) -> bytes | None:
    if value is None:
        return None
    if isinstance(value, bool):
        return b"t" if value else b"f"
    if isinstance(value, (int, float)):
        return str(value).encode()
    if isinstance(value, bytes):
        return (r"\x" + value.hex()).encode()
    return str(value).encode()

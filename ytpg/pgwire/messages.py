from __future__ import annotations

import struct
from typing import Any

from ytpg.pgwire.types import OID_TEXT, encode_value, python_to_oid


def _pack_msg(type_byte: bytes, body: bytes) -> bytes:
    return type_byte + struct.pack("!I", len(body) + 4) + body


def authentication_ok() -> bytes:
    return _pack_msg(b"R", struct.pack("!I", 0))


def parameter_status(name: str, value: str) -> bytes:
    body = name.encode() + b"\x00" + value.encode() + b"\x00"
    return _pack_msg(b"S", body)


def backend_key_data(pid: int = 1, secret: int = 0) -> bytes:
    return _pack_msg(b"K", struct.pack("!II", pid, secret))


def ready_for_query(status: str = "I") -> bytes:
    return _pack_msg(b"Z", status.encode())


def row_description(columns: list[str], type_oids: list[int] | None = None) -> bytes:
    if type_oids is None:
        type_oids = [OID_TEXT] * len(columns)

    field_count = len(columns)
    body = struct.pack("!H", field_count)
    for name, oid in zip(columns, type_oids):
        body += (
            name.encode() + b"\x00"
            + struct.pack("!I", 0)
            + struct.pack("!H", 0)
            + struct.pack("!I", oid)
            + struct.pack("!h", -1)
            + struct.pack("!I", 0)
            + struct.pack("!H", 0)
        )
    return _pack_msg(b"T", body)


def data_row(values: list[Any]) -> bytes:
    field_count = len(values)
    body = struct.pack("!H", field_count)
    for v in values:
        encoded = encode_value(v)
        if encoded is None:
            body += struct.pack("!i", -1)
        else:
            body += struct.pack("!I", len(encoded)) + encoded
    return _pack_msg(b"D", body)


def command_complete(tag: str) -> bytes:
    return _pack_msg(b"C", tag.encode() + b"\x00")


def error_response(
    message: str,
    code: str = "XX000",
    severity: str = "ERROR",
    hint: str | None = None,
    detail: str | None = None,
) -> bytes:
    body = b""
    body += b"S" + severity.encode() + b"\x00"
    body += b"C" + code.encode() + b"\x00"
    body += b"M" + message.encode() + b"\x00"
    if detail:
        body += b"D" + detail.encode() + b"\x00"
    if hint:
        body += b"H" + hint.encode() + b"\x00"
    body += b"\x00"
    return _pack_msg(b"E", body)


def notice_response(message: str) -> bytes:
    body = b"SNOTICE\x00" + b"M" + message.encode() + b"\x00" + b"\x00"
    return _pack_msg(b"N", body)

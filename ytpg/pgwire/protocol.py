from __future__ import annotations

import logging
import struct
from typing import AsyncIterator

from ytpg.pgwire import messages as msg

logger = logging.getLogger(__name__)

_PG_VERSION = 196608

_SSL_REQUEST = 80877103


class PGWireProtocol:
    def __init__(self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
        import asyncio
        self._reader = reader
        self._writer = writer
        self.database: str = ""
        self.user: str = ""
        self.parameters: dict[str, str] = {}

    async def startup_handshake(self) -> None:
        length_bytes = await self._reader.readexactly(4)
        total_len = struct.unpack("!I", length_bytes)[0]
        body = await self._reader.readexactly(total_len - 4)

        version = struct.unpack("!I", body[:4])[0]

        if version == _SSL_REQUEST:
            self._writer.write(b"N")
            await self._writer.drain()
            length_bytes = await self._reader.readexactly(4)
            total_len = struct.unpack("!I", length_bytes)[0]
            body = await self._reader.readexactly(total_len - 4)
            version = struct.unpack("!I", body[:4])[0]

        if version != _PG_VERSION:
            raise ValueError(f"Unsupported protocol version: {version}")

        params_raw = body[4:]
        pairs = params_raw.split(b"\x00")
        i = 0
        while i + 1 < len(pairs):
            key = pairs[i].decode("utf-8", errors="replace")
            val = pairs[i + 1].decode("utf-8", errors="replace")
            if key:
                self.parameters[key] = val
            i += 2

        self.user = self.parameters.get("user", "ytpg")
        self.database = self.parameters.get("database", "")

        self._writer.write(msg.authentication_ok())
        for k, v in [
            ("server_version", "14.0"),
            ("client_encoding", "UTF8"),
            ("server_encoding", "UTF8"),
            ("DateStyle", "ISO, MDY"),
            ("TimeZone", "UTC"),
            ("integer_datetimes", "on"),
        ]:
            self._writer.write(msg.parameter_status(k, v))
        self._writer.write(msg.backend_key_data())
        self._writer.write(msg.ready_for_query("I"))
        await self._writer.drain()
        logger.debug("Startup complete: user=%s database=%s", self.user, self.database)

    async def queries(self) -> AsyncIterator[str]:
        while True:
            type_byte = await self._reader.read(1)
            if not type_byte:
                return

            msg_type = type_byte[0:1]
            length_bytes = await self._reader.readexactly(4)
            msg_len = struct.unpack("!I", length_bytes)[0] - 4

            body = await self._reader.readexactly(msg_len) if msg_len > 0 else b""

            if msg_type == b"Q":
                sql = body.rstrip(b"\x00").decode("utf-8", errors="replace").strip()
                if sql:
                    yield sql
            elif msg_type == b"X":
                logger.debug("Client sent Terminate")
                return
            elif msg_type == b"p":
                pass
            else:
                logger.debug("Ignoring message type %r", msg_type)

    async def send_result(self, result) -> None:
        from ytpg.engine import QueryResult
        from ytpg.errors import PGError

        if isinstance(result, PGError):
            self._writer.write(msg.error_response(
                message=result.message,
                code=result.code,
                severity=result.severity,
                hint=result.hint,
            ))
            self._writer.write(msg.ready_for_query("I"))
            await self._writer.drain()
            return

        if result.columns:
            oids = None
            if result.rows:
                from ytpg.pgwire.types import python_to_oid
                oids = [python_to_oid(v) for v in result.rows[0]]
            self._writer.write(msg.row_description(result.columns, oids))
            for row in result.rows:
                self._writer.write(msg.data_row(row))

        self._writer.write(msg.command_complete(result.command))
        self._writer.write(msg.ready_for_query("I"))
        await self._writer.drain()

    async def send_error(self, exc: Exception) -> None:
        from ytpg.errors import PGError
        if isinstance(exc, PGError):
            await self.send_result(exc)
        else:
            self._writer.write(msg.error_response(
                message=str(exc),
                code="XX000",
                severity="ERROR",
            ))
            self._writer.write(msg.ready_for_query("I"))
            await self._writer.drain()

    def close(self) -> None:
        try:
            self._writer.close()
        except Exception:
            pass

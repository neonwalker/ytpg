from __future__ import annotations

import asyncio
import logging

logger = logging.getLogger(__name__)


class YTPGProxy:
    def __init__(
        self,
        channel_id: str,
        youtube,
        backend,
        host: str = "localhost",
        port: int = 5432,
    ) -> None:
        self._channel_id = channel_id
        self._youtube = youtube
        self._backend = backend
        self._host = host
        self._port = port
        self._write_pool = None
        self._server = None

    async def start(self) -> None:
        from ytpg.write_queue import WriteQueuePool

        self._write_pool = WriteQueuePool(self._backend, self._channel_id)
        self._server = await asyncio.start_server(
            self._handle_connection,
            self._host,
            self._port,
        )
        addr = self._server.sockets[0].getsockname()
        logger.info("ytpg proxy listening on %s:%d (channel=%s)", addr[0], addr[1], self._channel_id)

    async def serve_forever(self) -> None:
        if self._server is None:
            await self.start()
        async with self._server:
            await self._server.serve_forever()

    async def stop(self) -> None:
        if self._server:
            self._server.close()
            await self._server.wait_closed()
        if self._write_pool:
            await self._write_pool.stop_all()

    async def _handle_connection(
        self,
        reader: asyncio.StreamReader,
        writer: asyncio.StreamWriter,
    ) -> None:
        from ytpg.engine import Engine
        from ytpg.pgwire.protocol import PGWireProtocol

        peer = writer.get_extra_info("peername")
        logger.debug("New connection from %s", peer)

        protocol = PGWireProtocol(reader, writer)
        try:
            await protocol.startup_handshake()
        except Exception as exc:
            logger.warning("Startup handshake failed from %s: %s", peer, exc)
            protocol.close()
            return

        channel_id = protocol.database or self._channel_id

        engine = Engine(
            backend=self._backend,
            channel_id=channel_id,
            write_queue=self._write_pool,
        )

        try:
            async for sql in protocol.queries():
                logger.debug("Query [%s]: %s", peer, sql[:200])
                try:
                    result = await engine.execute(sql)
                    await protocol.send_result(result)
                except Exception as exc:
                    logger.exception("Error executing query: %s", sql)
                    await protocol.send_error(exc)
        except asyncio.IncompleteReadError:
            pass
        except Exception as exc:
            logger.exception("Connection error from %s: %s", peer, exc)
        finally:
            protocol.close()
            logger.debug("Connection from %s closed", peer)

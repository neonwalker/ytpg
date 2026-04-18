from __future__ import annotations

import asyncio
import logging
import time
from typing import Any

from ytpg.vtt import CueClock, append_cue_to_vtt, last_end_us_from_vtt, parse_vtt

logger = logging.getLogger(__name__)

ROTATION_THRESHOLD = 750_000

CHECKPOINT_WRITES = 16
CHECKPOINT_INTERVAL = 60.0


class WriteQueue:
    def __init__(self, backend, video_id: str, channel_id: str) -> None:
        self._backend = backend
        self._video_id = video_id
        self._channel_id = channel_id

        self._queue: asyncio.Queue = asyncio.Queue()
        self._worker_task: asyncio.Task | None = None

        self._clock: CueClock | None = None
        self._next_lsn: int = 1
        self._prev_comment_id: str | None = None
        self._prev_root: str = ""
        self._writes_since_checkpoint: int = 0
        self._last_checkpoint_ts: float = 0.0
        self._cues_since_checkpoint: list[dict] = []

    def start(self) -> None:
        self._worker_task = asyncio.create_task(self._process(), name=f"wq-{self._video_id}")

    async def stop(self) -> None:
        if self._worker_task:
            self._worker_task.cancel()
            try:
                await self._worker_task
            except asyncio.CancelledError:
                pass

    async def submit(self, payload: dict[str, Any]) -> None:
        await self.submit_many([payload])

    async def submit_many(
        self,
        payloads: list[dict[str, Any]],
        prefetched_vtt: str | None = None,
    ) -> None:
        future: asyncio.Future = asyncio.get_event_loop().create_future()
        await self._queue.put((payloads, future, prefetched_vtt))
        await future

    async def _process(self) -> None:
        await self._rehydrate()

        while True:
            payloads, future, prefetched_vtt = await self._queue.get()
            try:
                await self._execute_write(payloads, prefetched_vtt)
                future.set_result(None)
            except Exception as exc:
                logger.error("Write failed for video %s: %s", self._video_id, exc)
                future.set_exception(exc)
            finally:
                self._queue.task_done()

    async def _rehydrate(self) -> None:
        loop = asyncio.get_event_loop()

        def _load():
            pinned = self._backend.get_pinned_comment(self._video_id)
            pinned_lsn = pinned.get("lsn", 0) if pinned else 0
            pinned_comment_id = pinned.get("_comment_id") if pinned else None
            pinned_root = pinned.get("root", "") if pinned else ""

            raw = self._backend.fetch_raw_vtt(self._video_id)
            log = parse_vtt(raw)
            max_cue_lsn = max((c.get("_lsn", 0) for c in log), default=0)
            last_end = last_end_us_from_vtt(raw)

            return pinned_lsn, max_cue_lsn, last_end, pinned_comment_id, pinned_root, log

        try:
            pinned_lsn, max_cue_lsn, last_end, prev_comment_id, prev_root, log = \
                await loop.run_in_executor(None, _load)

            self._next_lsn = max(pinned_lsn, max_cue_lsn) + 1
            self._clock = CueClock(last_end)
            self._prev_comment_id = prev_comment_id
            self._prev_root = prev_root
            self._cues_since_checkpoint = list(log)
            logger.info(
                "WriteQueue rehydrated: video=%s next_lsn=%d",
                self._video_id, self._next_lsn,
            )
        except Exception as exc:
            logger.warning("WriteQueue rehydrate failed (starting fresh): %s", exc)
            self._clock = CueClock(0)

    async def _execute_write(
        self,
        payloads: list[dict[str, Any]],
        prefetched_vtt: str | None = None,
    ) -> None:
        loop = asyncio.get_event_loop()

        await self._check_concurrent_writer()

        for payload in payloads:
            payload["_lsn"] = self._next_lsn
            self._next_lsn += 1

        def _write():
            raw = prefetched_vtt
            if raw is None:
                raw = self._backend.fetch_raw_vtt(self._video_id)
            if len(raw) > ROTATION_THRESHOLD:
                logger.info(
                    "VTT rotation triggered for %s (size=%d)",
                    self._video_id, len(raw),
                )
                self._rotate()
                raw = "WEBVTT\n\n"
            for p in payloads:
                raw, _, _ = append_cue_to_vtt(raw, p, self._clock)
            self._backend.upload_vtt(self._video_id, raw)

        await loop.run_in_executor(None, _write)
        self._cues_since_checkpoint.extend(payloads)
        self._writes_since_checkpoint += len(payloads)

        now = time.monotonic()
        if (
            self._writes_since_checkpoint >= CHECKPOINT_WRITES
            or (now - self._last_checkpoint_ts) >= CHECKPOINT_INTERVAL
        ):
            await loop.run_in_executor(None, self._write_checkpoint)

    async def _check_concurrent_writer(self) -> None:
        from ytpg.errors import PGError
        loop = asyncio.get_event_loop()

        def _read():
            return self._backend.get_pinned_comment(self._video_id)

        pinned = await loop.run_in_executor(None, _read)
        if pinned and pinned.get("lsn", 0) >= self._next_lsn:
            from ytpg.errors import PGError
            raise PGError(
                code="40001",
                message="concurrent writer detected — resync from YouTube",
            )

    def _write_checkpoint(self) -> None:
        from ytpg.merkle import build_checkpoint

        checkpoint = build_checkpoint(
            lsn=self._next_lsn - 1,
            cues=self._cues_since_checkpoint,
            prev_root=self._prev_root,
            prev_comment_id=self._prev_comment_id,
        )
        new_comment_id = self._backend.post_checkpoint(self._video_id, checkpoint)

        old_id = self._prev_comment_id
        self._prev_comment_id = new_comment_id
        self._prev_root = checkpoint["root"]
        self._writes_since_checkpoint = 0
        self._last_checkpoint_ts = time.monotonic()

        if old_id:
            self._backend.delete_comment(old_id)

    def _rotate(self) -> None:
        from ytpg.manifest import get_manifest, update_manifest
        import json

        new_video_id = self._backend.create_video(
            title=f"ytpg::segment::{self._video_id}",
            description=json.dumps({
                "ytpg_version": "0.1",
                "segment_of": self._video_id,
                "created_at": int(time.time()),
            }),
        )

        manifest = get_manifest(self._backend._yt, self._channel_id)
        tables = manifest.get("tables", {})

        table_name = None
        for name, entry in tables.items():
            head = entry["head"] if isinstance(entry, dict) else entry
            if head == self._video_id:
                table_name = name
                break

        if table_name:
            old_entry = tables[table_name]
            if isinstance(old_entry, dict):
                segments = old_entry.get("segments", [old_entry["head"]])
            else:
                segments = [old_entry]

            segments.append(new_video_id)
            tables[table_name] = {"head": new_video_id, "segments": segments}
            update_manifest(self._backend._yt, self._channel_id, manifest)

        self._video_id = new_video_id
        self._clock = CueClock(0)
        logger.info("Rotated to new segment video: %s", new_video_id)


class WriteQueuePool:
    def __init__(self, backend, channel_id: str) -> None:
        self._backend = backend
        self._channel_id = channel_id
        self._queues: dict[str, WriteQueue] = {}

    def get_or_create(self, table_name: str, video_id: str) -> WriteQueue:
        if table_name not in self._queues:
            wq = WriteQueue(self._backend, video_id, self._channel_id)
            wq.start()
            self._queues[table_name] = wq
        return self._queues[table_name]

    async def submit(
        self,
        table_name: str,
        video_id: str,
        payload: dict,
    ) -> None:
        wq = self.get_or_create(table_name, video_id)
        await wq.submit(payload)

    async def submit_many(
        self,
        table_name: str,
        video_id: str,
        payloads: list[dict],
        prefetched_vtt: str | None = None,
    ) -> None:
        wq = self.get_or_create(table_name, video_id)
        await wq.submit_many(payloads, prefetched_vtt=prefetched_vtt)

    async def stop_all(self) -> None:
        for wq in self._queues.values():
            await wq.stop()
        self._queues.clear()

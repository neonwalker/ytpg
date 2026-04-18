from __future__ import annotations

import functools
import logging
import time
from typing import Any, Callable

from ytpg.errors import PGError
from ytpg.planner import (
    AppendCueOp,
    CreateTableOp,
    DropTableOp,
    ExplainOp,
    FetchAndMaterializeOp,
    Planner,
)

logger = logging.getLogger(__name__)


class QueryResult:
    __slots__ = ("columns", "rows", "command", "tag", "elapsed_ms")

    def __init__(
        self,
        columns: list[str],
        rows: list[list[Any]],
        command: str,
        tag: str,
        elapsed_ms: float = 0.0,
    ) -> None:
        self.columns = columns
        self.rows = rows
        self.command = command
        self.tag = tag
        self.elapsed_ms = elapsed_ms


def _apply_where(
    rows: list[dict],
    where: Callable[[dict], bool] | dict[str, Any] | None,
) -> list[dict]:
    if where is None:
        return rows
    if callable(where):
        return [r for r in rows if where(r)]
    return [r for r in rows if all(r.get(k) == v for k, v in where.items())]


def _display_name(key: str) -> str:
    return key.rsplit(".", 1)[-1] if "." in key else key


def _project(rows: list[dict], columns: list[str] | None) -> tuple[list[str], list[list[Any]]]:
    if not rows:
        col_names = columns or []
        return [_display_name(c) for c in col_names], []
    all_keys: list[str] = list(dict.fromkeys(k for row in rows for k in row.keys()))
    selected = columns if columns else all_keys
    return [_display_name(k) for k in selected], [[row.get(c) for c in selected] for row in rows]


def _prefix_keys(rows: list[dict], alias: str) -> list[dict]:
    if not alias:
        return rows
    return [{f"{alias}.{k}": v for k, v in r.items()} for r in rows]


def _nested_loop_join(
    left_rows: list[dict],
    right_rows: list[dict],
    kind: str,
    on_pred: Callable[[dict], bool] | None,
) -> list[dict]:
    out: list[dict] = []
    right_matched = [False] * len(right_rows)
    for lrow in left_rows:
        lmatched = False
        for j, rrow in enumerate(right_rows):
            combined = {**lrow, **rrow}
            if on_pred is None or on_pred(combined):
                out.append(combined)
                lmatched = True
                right_matched[j] = True
        if not lmatched and kind in ("LEFT", "FULL"):
            out.append(dict(lrow))
    if kind in ("RIGHT", "FULL"):
        for j, rrow in enumerate(right_rows):
            if not right_matched[j]:
                out.append(dict(rrow))
    return out


def _compare_rows(
    a: dict,
    b: dict,
    orderings: list[tuple[str, bool, bool]],
) -> int:
    for col, desc, nulls_first in orderings:
        av = a.get(col)
        bv = b.get(col)
        if av is None and bv is None:
            continue
        if av is None or bv is None:
            if av is None:
                return -1 if nulls_first else 1
            return 1 if nulls_first else -1
        try:
            if av < bv:
                return 1 if desc else -1
            if av > bv:
                return -1 if desc else 1
        except TypeError:
            sa, sb = str(av), str(bv)
            if sa < sb:
                return 1 if desc else -1
            if sa > sb:
                return -1 if desc else 1
    return 0


class Engine:
    def __init__(self, backend, channel_id: str, write_queue=None) -> None:
        self._backend = backend
        self._channel_id = channel_id
        self._planner = Planner(backend._yt, channel_id, backend=backend)
        self._write_queue = write_queue
        self._next_lsn_by_video: dict[str, int] = {}
        self._oid_to_table: dict[int, str] = {}

    async def _ensure_lsn(self, video_id: str) -> None:
        if video_id in self._next_lsn_by_video:
            return
        import asyncio
        loop = asyncio.get_event_loop()

        def _rehydrate() -> int:
            from ytpg.vtt import parse_vtt
            pinned = self._backend.get_pinned_comment(video_id)
            pinned_lsn = pinned.get("lsn", 0) if pinned else 0
            raw = self._backend.fetch_raw_vtt(video_id)
            log = parse_vtt(raw)
            max_cue_lsn = max((c.get("_lsn", 0) for c in log), default=0)
            return max(pinned_lsn, max_cue_lsn) + 1

        self._next_lsn_by_video[video_id] = await loop.run_in_executor(None, _rehydrate)

    def _alloc_lsn(self, video_id: str) -> int:
        lsn = self._next_lsn_by_video[video_id]
        self._next_lsn_by_video[video_id] = lsn + 1
        return lsn

    async def execute(self, sql: str) -> QueryResult:
        t0 = time.monotonic()
        result = await self._handle_catalog_query(sql)
        if result is None:
            ops = self._planner.plan(sql)
            result = await self._execute_ops(ops, sql)
        result.elapsed_ms = (time.monotonic() - t0) * 1000
        return result

    async def _handle_catalog_query(self, sql: str) -> QueryResult | None:
        import asyncio
        import re

        upper = sql.upper()
        if "PG_CATALOG" not in upper and "INFORMATION_SCHEMA" not in upper:
            return None

        loop = asyncio.get_event_loop()

        if "PG_CATALOG.PG_DATABASE" in upper or "PG_DATABASE" in upper and "DATNAME" in upper:
            rows = [[
                self._channel_id,
                "ytpg",
                "UTF8",
                "en_US.UTF-8",
                "en_US.UTF-8",
                "",
            ]]
            return QueryResult(
                columns=["Name", "Owner", "Encoding", "Collate", "Ctype", "Access privileges"],
                rows=rows,
                command=f"SELECT {len(rows)}",
                tag="SELECT",
            )

        if "PG_CATALOG.PG_NAMESPACE" in upper and "NSPNAME" in upper and "PG_CATALOG.PG_CLASS" not in upper:
            rows = [["public", "ytpg"]]
            return QueryResult(
                columns=["Name", "Owner"],
                rows=rows,
                command=f"SELECT {len(rows)}",
                tag="SELECT",
            )

        if "PG_CATALOG.PG_ATTRIBUTE" in upper and "ATTRELID" in upper:
            m = re.search(r"attrelid\s*=\s*'?(\d+)'?", sql, re.IGNORECASE)
            if m:
                oid = int(m.group(1))
                table_name = self._oid_to_table.get(oid)
                if table_name:
                    def _get_schema():
                        from ytpg.schema import get_table_schema
                        return get_table_schema(self._backend, self._channel_id, table_name)

                    schema_cols = await loop.run_in_executor(None, _get_schema)
                    rows = [
                        [
                            c["name"],
                            c.get("type", "text").lower(),
                            None,
                            not c.get("nullable", True),
                            None,
                            "",
                            "",
                        ]
                        for c in schema_cols
                    ]
                    return QueryResult(
                        columns=[
                            "Column", "Type", "Default", "Not Null",
                            "Collation", "Identity", "Generated",
                        ],
                        rows=rows,
                        command=f"SELECT {len(rows)}",
                        tag="SELECT",
                    )
            return QueryResult(
                columns=[
                    "Column", "Type", "Default", "Not Null",
                    "Collation", "Identity", "Generated",
                ],
                rows=[],
                command="SELECT 0",
                tag="SELECT",
            )

        if "PG_CATALOG.PG_CLASS" in upper:
            oid_detail = re.search(
                r"\bc\.oid\s*=\s*'?(\d+)'?", sql, re.IGNORECASE,
            )
            if oid_detail and "RELCHECKS" in upper:
                rows = [[
                    "0",
                    "r",
                    "f",
                    "f",
                    "f",
                    "f",
                    "f",
                    "f",
                    "f",
                    "",
                    "0",
                    "",
                    "p",
                    "d",
                    "heap",
                ]]
                return QueryResult(
                    columns=[
                        "relchecks", "relkind", "relhasindex", "relhasrules",
                        "relhastriggers", "relrowsecurity", "relforcerowsecurity",
                        "relhasoids", "relispartition", "reloptions",
                        "reltablespace", "reloftype", "relpersistence",
                        "relreplident", "amname",
                    ],
                    rows=rows,
                    command=f"SELECT {len(rows)}",
                    tag="SELECT",
                )

            relname_match = re.search(
                r"relname\s+OPERATOR\(pg_catalog\.~\)\s+'\^\(([^)]+)\)\$'",
                sql, re.IGNORECASE,
            )
            if relname_match:
                target = relname_match.group(1)

                def _list():
                    from ytpg.manifest import list_tables
                    return list_tables(self._backend._yt, self._channel_id)

                names = await loop.run_in_executor(None, _list)
                if target in names:
                    oid = 16384 + (abs(hash(target)) % 1_000_000)
                    self._oid_to_table[oid] = target
                    rows = [[str(oid), "public", target]]
                else:
                    rows = []
                return QueryResult(
                    columns=["oid", "nspname", "relname"],
                    rows=rows,
                    command=f"SELECT {len(rows)}",
                    tag="SELECT",
                )

            if "RELKIND IN" in upper or "RELKIND" in upper and "RELNAMESPACE" in upper:
                def _list():
                    from ytpg.manifest import list_tables
                    return list_tables(self._backend._yt, self._channel_id)

                names = await loop.run_in_executor(None, _list)
                rows = [["public", name, "table", "ytpg"] for name in sorted(names)]
                return QueryResult(
                    columns=["Schema", "Name", "Type", "Owner"],
                    rows=rows,
                    command=f"SELECT {len(rows)}",
                    tag="SELECT",
                )

        return QueryResult(columns=[], rows=[], command="SELECT 0", tag="SELECT")

    async def _execute_ops(self, ops: list, original_sql: str) -> QueryResult:
        if not ops:
            return QueryResult([], [], "OK", "OK")

        first = ops[0]

        if isinstance(first, ExplainOp):
            return await self._execute_explain(first)

        if isinstance(first, FetchAndMaterializeOp):
            return await self._execute_select(first)

        if isinstance(first, AppendCueOp):
            affected = 0
            for op in ops:
                affected += await self._execute_write(op)
            tag = ops[0].payload.get("_op", "INSERT")
            if tag == "INSERT":
                cmd = f"INSERT 0 {affected}"
            else:
                cmd = f"{tag} {affected}"
            return QueryResult([], [], cmd, tag)

        if isinstance(first, CreateTableOp):
            return await self._execute_create(first)

        if isinstance(first, DropTableOp):
            return await self._execute_drop(first)

        raise PGError(code="XX000", message=f"Unknown op type: {type(first).__name__}")

    async def _execute_select(self, op: FetchAndMaterializeOp) -> QueryResult:
        import asyncio

        loop = asyncio.get_event_loop()

        def _materialize(segments: list[str]) -> list[dict]:
            from ytpg.vtt import parse_vtt, materialize
            combined_log: list[dict] = []
            for seg in segments:
                raw = self._backend.fetch_raw_vtt(seg)
                combined_log.extend(parse_vtt(raw))
            return materialize(combined_log)

        def _fetch():
            base_segments = op.segments or [op.video_id]
            if op.joins:
                left = _prefix_keys(_materialize(base_segments), op.alias or op.table_name)
                for j in op.joins:
                    right = _prefix_keys(_materialize(j.segments or [j.video_id]), j.alias)
                    left = _nested_loop_join(left, right, j.kind, j.on)
                return left
            return _materialize(base_segments)

        rows = await loop.run_in_executor(None, _fetch)

        rows = _apply_where(rows, op.where)

        if op.aggregate:
            agg_lower = op.aggregate.upper()
            if "COUNT" in agg_lower:
                return QueryResult(
                    columns=["count"],
                    rows=[[len(rows)]],
                    command=f"SELECT 1",
                    tag="SELECT",
                )

        if op.orderings:
            rows = sorted(
                rows,
                key=functools.cmp_to_key(
                    lambda a, b, o=op.orderings: _compare_rows(a, b, o)
                ),
            )

        if op.limit is not None:
            rows = rows[: op.limit]

        col_names, data_rows = _project(rows, op.columns)
        return QueryResult(
            columns=col_names,
            rows=data_rows,
            command=f"SELECT {len(data_rows)}",
            tag="SELECT",
        )

    async def _execute_write(self, op: AppendCueOp) -> int:
        import asyncio

        raw_op = op.payload.get("_op", "INSERT")

        if raw_op == "DELETE":
            return await self._execute_delete_write(op)

        if raw_op == "UPDATE":
            return await self._execute_update_write(op)

        payload = dict(op.payload)

        if self._write_queue:
            await self._write_queue.submit(op.table_name, op.video_id, payload)
        else:
            await self._ensure_lsn(op.video_id)
            payload["_lsn"] = self._alloc_lsn(op.video_id)
            loop = asyncio.get_event_loop()
            await loop.run_in_executor(
                None, lambda: self._backend.append_cue(op.video_id, payload)
            )
        return 1

    async def _execute_delete_write(self, op: AppendCueOp) -> int:
        import asyncio

        loop = asyncio.get_event_loop()

        def _fetch_matching():
            from ytpg.vtt import parse_vtt, materialize
            segments = op.segments or [op.video_id]
            combined_log: list[dict] = []
            head_vtt = ""
            for i, seg in enumerate(segments):
                raw = self._backend.fetch_raw_vtt(seg)
                combined_log.extend(parse_vtt(raw))
                if i == len(segments) - 1:
                    head_vtt = raw
            rows = materialize(combined_log)
            where = op.payload.get("_where")
            return _apply_where(rows, where), head_vtt

        rows, current_vtt = await loop.run_in_executor(None, _fetch_matching)
        if not rows:
            return 0

        payloads = [{"id": row.get("id"), "_op": "DELETE"} for row in rows]

        if self._write_queue:
            await self._write_queue.submit_many(
                op.table_name, op.video_id, payloads, prefetched_vtt=current_vtt,
            )
        else:
            await self._ensure_lsn(op.video_id)
            for p in payloads:
                p["_lsn"] = self._alloc_lsn(op.video_id)
                await loop.run_in_executor(
                    None, lambda pp=p: self._backend.append_cue(op.video_id, pp)
                )
        return len(rows)

    async def _execute_update_write(self, op: AppendCueOp) -> int:
        import asyncio

        loop = asyncio.get_event_loop()

        def _fetch_matching():
            from ytpg.vtt import parse_vtt, materialize
            segments = op.segments or [op.video_id]
            combined_log: list[dict] = []
            head_vtt = ""
            for i, seg in enumerate(segments):
                raw = self._backend.fetch_raw_vtt(seg)
                combined_log.extend(parse_vtt(raw))
                if i == len(segments) - 1:
                    head_vtt = raw
            rows = materialize(combined_log)
            where = op.payload.get("_where")
            return _apply_where(rows, where), head_vtt

        rows, current_vtt = await loop.run_in_executor(None, _fetch_matching)
        if not rows:
            return 0

        set_values = {k: v for k, v in op.payload.items() if not k.startswith("_")}

        payloads: list[dict] = []
        for row in rows:
            payloads.append({"id": row.get("id"), "_op": "DELETE"})
            new_row = dict(row)
            new_row.update(set_values)
            new_row["_op"] = "INSERT"
            payloads.append(new_row)

        if self._write_queue:
            await self._write_queue.submit_many(
                op.table_name, op.video_id, payloads, prefetched_vtt=current_vtt,
            )
        else:
            await self._ensure_lsn(op.video_id)
            for p in payloads:
                p["_lsn"] = self._alloc_lsn(op.video_id)
                await loop.run_in_executor(
                    None, lambda pp=p: self._backend.append_cue(op.video_id, pp)
                )
        return len(rows)

    async def _execute_create(self, op: CreateTableOp) -> QueryResult:
        import asyncio
        from ytpg.schema import create_table

        loop = asyncio.get_event_loop()
        await loop.run_in_executor(
            None,
            lambda: create_table(self._backend, self._channel_id, op.table_name, op.columns),
        )
        return QueryResult([], [], "CREATE TABLE", "CREATE TABLE")

    async def _execute_drop(self, op: DropTableOp) -> QueryResult:
        import asyncio
        from ytpg.schema import drop_table

        loop = asyncio.get_event_loop()
        await loop.run_in_executor(
            None,
            lambda: drop_table(self._backend, self._channel_id, op.table_name),
        )
        return QueryResult([], [], "DROP TABLE", "DROP TABLE")

    async def _execute_explain(self, op: ExplainOp) -> QueryResult:
        lines = []
        for inner_op in op.inner:
            lines.append(repr(inner_op))

        if op.analyze:
            t0 = time.monotonic()
            await self._execute_ops(op.inner, "")
            elapsed = (time.monotonic() - t0) * 1000
            lines.append(f"Execution time: {elapsed:.1f}ms")

        return QueryResult(
            columns=["QUERY PLAN"],
            rows=[[line] for line in lines],
            command=f"EXPLAIN",
            tag="EXPLAIN",
        )

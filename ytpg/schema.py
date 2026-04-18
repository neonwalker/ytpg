from __future__ import annotations

import json
import logging
import time
from typing import Any

from ytpg.errors import PGError
from ytpg.manifest import register_table

logger = logging.getLogger(__name__)

_DESCRIPTION_LIMIT = 4_500

_RECYCLED_TITLE = "ytpg::__recycled__"


def _schema_description(table_name: str, columns: list[dict[str, Any]]) -> str:
    doc = {
        "ytpg_version": "0.1",
        "table": table_name,
        "schema": columns,
        "created_at": int(time.time()),
    }
    return json.dumps(doc, indent=2)


def _try_recycle(backend, channel_id: str, title: str, description: str) -> str | None:
    from ytpg.manifest import get_manifest, update_manifest

    manifest = get_manifest(backend._yt, channel_id)
    free = list(manifest.get("free", []))
    if not free:
        return None

    chosen: str | None = None
    new_free: list[str] = []

    for vid in free:
        if chosen is not None:
            new_free.append(vid)
            continue

        try:
            backend.get_video_snippet(vid)
        except PGError as exc:
            if exc.code == "42P01":
                logger.info("Pruning stale recycled video %s (manually deleted)", vid)
                continue
            raise

        try:
            backend.update_video_description(vid, title, description)
        except Exception as exc:
            logger.warning("Could not repurpose recycled video %s: %s", vid, exc)
            new_free.append(vid)
            continue

        chosen = vid

    if new_free != free:
        manifest["free"] = new_free
        update_manifest(backend._yt, channel_id, manifest)

    return chosen


def create_table(
    backend,
    channel_id: str,
    table_name: str,
    columns: list[dict[str, Any]],
) -> str:
    from ytpg.manifest import resolve_table

    try:
        resolve_table(backend._yt, channel_id, table_name)
        raise PGError(
            code="42P07",
            message=f'relation "{table_name}" already exists',
        )
    except PGError as e:
        if e.code != "42P01":
            raise

    title = f"ytpg::{table_name}"
    description = _schema_description(table_name, columns)
    if len(description) > _DESCRIPTION_LIMIT:
        raise PGError(code="54000", message=f"Schema for {table_name!r} is too large")

    video_id = _try_recycle(backend, channel_id, title, description)
    if video_id is not None:
        logger.info("Recycled video %s for table %s", video_id, table_name)
        try:
            backend.upload_vtt(video_id, "WEBVTT\n\n")
        except Exception as exc:
            logger.warning("Could not reset caption track on %s: %s", video_id, exc)
    else:
        logger.info("Creating table video: %s", title)
        video_id = backend.create_video(title=title, description=description)
        logger.info("Table video created: %s → %s", table_name, video_id)

    register_table(backend._yt, channel_id, table_name, video_id)
    logger.info("Table %s registered in manifest", table_name)
    return video_id


def drop_table(
    backend,
    channel_id: str,
    table_name: str,
) -> None:
    from ytpg.manifest import get_manifest, update_manifest

    manifest = get_manifest(backend._yt, channel_id)
    tables = manifest.get("tables", {})
    entry = tables.get(table_name)
    if entry is None:
        raise PGError(
            code="42P01",
            message=f'relation "{table_name}" does not exist',
        )
    video_id = entry["head"] if isinstance(entry, dict) else entry
    logger.info("Dropping table %s (video %s → recycled)", table_name, video_id)

    recycled_desc = json.dumps({
        "ytpg_version": "0.1",
        "recycled_at": int(time.time()),
        "prev_table": table_name,
    })
    try:
        backend.update_video_description(video_id, _RECYCLED_TITLE, recycled_desc)
    except Exception as exc:
        logger.warning("Could not rename %s to recycled: %s", video_id, exc)

    try:
        backend.upload_vtt(video_id, "WEBVTT\n\n")
    except Exception as exc:
        logger.warning("Could not clear caption track for %s: %s", video_id, exc)

    tables.pop(table_name, None)
    free = manifest.setdefault("free", [])
    if video_id not in free:
        free.append(video_id)
    update_manifest(backend._yt, channel_id, manifest)
    logger.info("Table %s unregistered; %s added to free list", table_name, video_id)


def get_table_schema(backend, channel_id: str, table_name: str) -> list[dict[str, Any]]:
    from ytpg.manifest import resolve_table

    video_id = resolve_table(backend._yt, channel_id, table_name)
    raw = backend.get_video_description(video_id)
    try:
        doc = json.loads(raw)
        return doc.get("schema", [])
    except json.JSONDecodeError as exc:
        raise PGError(code="XX000", message=f"Schema JSON corrupt for {table_name!r}: {exc}") from exc


def parse_column_defs(columns_ast) -> list[dict[str, Any]]:
    import sqlglot.expressions as exp

    result = []
    for col in columns_ast:
        name = col.name
        dtype = col.args.get("kind")
        type_str = dtype.sql() if dtype else "TEXT"
        nullable = True
        for constraint in col.args.get("constraints", []):
            if isinstance(constraint.args.get("kind"), exp.NotNullColumnConstraint):
                nullable = False
        result.append({"name": name, "type": type_str, "nullable": nullable})
    return result


_INT_TYPES = {"INT", "INTEGER", "BIGINT", "SMALLINT", "INT2", "INT4", "INT8",
              "SERIAL", "BIGSERIAL"}
_FLOAT_TYPES = {"FLOAT", "REAL", "DOUBLE", "DOUBLE PRECISION", "NUMERIC",
                "DECIMAL", "FLOAT4", "FLOAT8"}
_BOOL_TYPES = {"BOOL", "BOOLEAN"}
_TEXT_TYPES = {"TEXT", "VARCHAR", "CHAR", "CHARACTER", "CHARACTER VARYING",
               "STRING", "NVARCHAR", "NCHAR"}


def _base_type(decl_type: str) -> str:
    return decl_type.split("(", 1)[0].strip().upper()


def coerce_value(value: Any, decl_type: str) -> Any:
    if value is None:
        return None

    base = _base_type(decl_type)

    if base in _INT_TYPES:
        if isinstance(value, bool):
            return 1 if value else 0
        if isinstance(value, int):
            return value
        if isinstance(value, float):
            if value.is_integer():
                return int(value)
            raise PGError(
                code="22P02",
                message=f"invalid input syntax for type integer: {value}",
            )
        if isinstance(value, str):
            try:
                return int(value.strip())
            except ValueError:
                raise PGError(
                    code="22P02",
                    message=f'invalid input syntax for type integer: "{value}"',
                )
        raise PGError(
            code="22P02",
            message=f"cannot coerce {type(value).__name__} to integer",
        )

    if base in _FLOAT_TYPES:
        if isinstance(value, bool):
            return 1.0 if value else 0.0
        if isinstance(value, (int, float)):
            return float(value)
        if isinstance(value, str):
            try:
                return float(value.strip())
            except ValueError:
                raise PGError(
                    code="22P02",
                    message=f'invalid input syntax for type numeric: "{value}"',
                )
        raise PGError(
            code="22P02",
            message=f"cannot coerce {type(value).__name__} to numeric",
        )

    if base in _BOOL_TYPES:
        if isinstance(value, bool):
            return value
        if isinstance(value, int):
            return bool(value)
        if isinstance(value, str):
            s = value.strip().lower()
            if s in ("t", "true", "y", "yes", "on", "1"):
                return True
            if s in ("f", "false", "n", "no", "off", "0"):
                return False
            raise PGError(
                code="22P02",
                message=f'invalid input syntax for type boolean: "{value}"',
            )
        raise PGError(
            code="22P02",
            message=f"cannot coerce {type(value).__name__} to boolean",
        )

    if base in _TEXT_TYPES:
        if isinstance(value, bool):
            return "true" if value else "false"
        return str(value)

    return value


def coerce_payload(
    schema: list[dict[str, Any]],
    payload: dict[str, Any],
) -> dict[str, Any]:
    type_by_name = {c["name"]: c.get("type", "TEXT") for c in schema}
    out = dict(payload)
    for col, val in list(payload.items()):
        if col.startswith("_"):
            continue
        if col in type_by_name:
            out[col] = coerce_value(val, type_by_name[col])
    return out


def enforce_not_null(
    schema: list[dict[str, Any]],
    payload: dict[str, Any],
    *,
    check_missing: bool,
) -> None:
    for col in schema:
        if col.get("nullable", True):
            continue
        name = col["name"]
        if name in payload:
            if payload[name] is None:
                raise PGError(
                    code="23502",
                    message=f'null value in column "{name}" violates not-null constraint',
                )
        elif check_missing:
            raise PGError(
                code="23502",
                message=f'null value in column "{name}" violates not-null constraint',
            )

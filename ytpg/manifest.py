from __future__ import annotations

import json
import logging
import time
from pathlib import Path

from ytpg.errors import PGError

logger = logging.getLogger(__name__)

MANIFEST_TITLE = "ytpg::__manifest__"
_CONFIG_DIR = Path("~/.config/ytpg").expanduser()


def _bootstrap_path(channel_id: str) -> Path:
    return _CONFIG_DIR / f"{channel_id}.json"


def _find_manifest_video(youtube, channel_id: str) -> str:
    cache_path = _bootstrap_path(channel_id)
    if cache_path.exists():
        try:
            data = json.loads(cache_path.read_text())
            if vid := data.get("manifest_video_id"):
                return vid
        except Exception:
            pass

    logger.info("Manifest cache miss; running search.list recovery for channel %s", channel_id)
    resp = youtube.search().list(
        part="snippet",
        forMine=True,
        q=MANIFEST_TITLE,
        type="video",
        maxResults=5,
    ).execute()

    for item in resp.get("items", []):
        if item["snippet"]["title"] == MANIFEST_TITLE:
            video_id = item["id"]["videoId"]
            _save_bootstrap_cache(channel_id, manifest_video_id=video_id)
            return video_id

    raise PGError(
        code="3D000",
        message="channel not initialized, run 'ytpg init'",
    )


def _save_bootstrap_cache(
    channel_id: str,
    manifest_video_id: str | None = None,
) -> None:
    _CONFIG_DIR.mkdir(parents=True, exist_ok=True)
    cache_path = _bootstrap_path(channel_id)
    data: dict = {}
    if cache_path.exists():
        try:
            data = json.loads(cache_path.read_text())
        except Exception:
            pass
    if manifest_video_id:
        data["manifest_video_id"] = manifest_video_id
    cache_path.write_text(json.dumps(data))


def get_manifest(youtube, channel_id: str) -> dict:
    video_id = _find_manifest_video(youtube, channel_id)

    for attempt in range(5):
        resp = youtube.videos().list(part="snippet", id=video_id).execute()
        items = resp.get("items", [])
        if items:
            break
        if attempt < 4:
            logger.warning(
                "Manifest video %s not yet visible (attempt %d/5); retrying in %ds",
                video_id, attempt + 1, 2 ** attempt,
            )
            time.sleep(2 ** attempt)
    else:
        raise PGError(
            code="3D000",
            message=(
                f"Manifest video {video_id!r} not found or not yet accessible. "
                "If you just ran 'ytpg init', wait 30s and retry. "
                "Otherwise re-run 'ytpg init'."
            ),
        )

    raw = items[0]["snippet"]["description"]
    try:
        return json.loads(raw)
    except json.JSONDecodeError as exc:
        raise PGError(code="XX000", message=f"Manifest JSON corrupt: {exc}") from exc


def update_manifest(youtube, channel_id: str, manifest: dict) -> None:
    video_id = _find_manifest_video(youtube, channel_id)
    youtube.videos().update(
        part="snippet",
        body={
            "id": video_id,
            "snippet": {
                "title": MANIFEST_TITLE,
                "description": json.dumps(manifest, indent=2),
                "categoryId": "22",
            },
        },
    ).execute()


def resolve_table(youtube, channel_id: str, table_name: str) -> str:
    manifest = get_manifest(youtube, channel_id)
    tables = manifest.get("tables", {})
    entry = tables.get(table_name)
    if entry is None:
        raise PGError(
            code="42P01",
            message=f'relation "{table_name}" does not exist',
        )
    if isinstance(entry, dict):
        return entry["head"]
    return entry


def resolve_table_entry(youtube, channel_id: str, table_name: str) -> str | dict:
    manifest = get_manifest(youtube, channel_id)
    tables = manifest.get("tables", {})
    entry = tables.get(table_name)
    if entry is None:
        raise PGError(
            code="42P01",
            message=f'relation "{table_name}" does not exist',
        )
    return entry


def resolve_table_segments(youtube, channel_id: str, table_name: str) -> list[str]:
    entry = resolve_table_entry(youtube, channel_id, table_name)
    if isinstance(entry, dict):
        segs = entry.get("segments")
        if segs:
            return list(segs)
        return [entry["head"]]
    return [entry]


def list_tables(youtube, channel_id: str) -> list[str]:
    manifest = get_manifest(youtube, channel_id)
    return list(manifest.get("tables", {}).keys())


def register_table(youtube, channel_id: str, table_name: str, video_id: str) -> None:
    manifest = get_manifest(youtube, channel_id)
    manifest.setdefault("tables", {})[table_name] = video_id
    update_manifest(youtube, channel_id, manifest)


def make_empty_manifest(channel_id: str) -> dict:
    return {
        "ytpg_version": "0.1",
        "channel_id": channel_id,
        "tables": {},
        "created_at": int(time.time()),
    }

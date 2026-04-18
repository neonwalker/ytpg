from __future__ import annotations

import hashlib
import json
import time
from typing import Any


def canonical_json(obj: Any) -> bytes:
    return json.dumps(obj, sort_keys=True, separators=(",", ":"), ensure_ascii=False).encode("utf-8")


def _leaf(cue: dict[str, Any]) -> bytes:
    return hashlib.sha256(b"\x00" + canonical_json(cue)).digest()


def _node(left: bytes, right: bytes) -> bytes:
    return hashlib.sha256(b"\x01" + left + right).digest()


def merkle_root(cues: list[dict[str, Any]]) -> str:
    if not cues:
        return hashlib.sha256(b"\x00").hexdigest()

    ordered = sorted(cues, key=lambda c: c.get("_lsn", 0))
    layer: list[bytes] = [_leaf(c) for c in ordered]

    while len(layer) > 1:
        if len(layer) % 2 == 1:
            layer.append(layer[-1])
        layer = [_node(layer[i], layer[i + 1]) for i in range(0, len(layer), 2)]

    return layer[0].hex()


def cross_segment_root(segment_roots: list[str]) -> str:
    if not segment_roots:
        return hashlib.sha256(b"\x02").hexdigest()
    parts = b"".join(bytes.fromhex(r) for r in segment_roots)
    return hashlib.sha256(b"\x02" + parts).hexdigest()


def build_checkpoint(
    lsn: int,
    cues: list[dict[str, Any]],
    prev_root: str,
    prev_comment_id: str | None,
) -> dict[str, Any]:
    root = merkle_root(cues)
    return {
        "lsn": lsn,
        "root": root,
        "prev_root": prev_root,
        "leaf_count": len(cues),
        "prev_comment_id": prev_comment_id,
        "ts": int(time.time()),
    }

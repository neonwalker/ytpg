from __future__ import annotations

import json
import re
from typing import Any


_HH_MM_SS_US = re.compile(r"(\d{2,}):(\d{2}):(\d{2})\.(\d{3,6})")


def _ts_to_us(ts: str) -> int:
    m = _HH_MM_SS_US.match(ts.strip())
    if not m:
        raise ValueError(f"Bad VTT timestamp: {ts!r}")
    h, mn, s = int(m.group(1)), int(m.group(2)), int(m.group(3))
    frac = m.group(4).ljust(6, "0")
    us = int(frac)
    return ((h * 3600 + mn * 60 + s) * 1_000_000) + us


def _us_to_ts(us: int) -> str:
    total_ms, _ = divmod(us, 1_000)
    total_s, rem_ms = divmod(total_ms, 1_000)
    h, rem_s = divmod(total_s, 3600)
    mn, s = divmod(rem_s, 60)
    return f"{h:02d}:{mn:02d}:{s:02d}.{rem_ms:03d}"


class CueClock:
    def __init__(self, last_end_us: int = 0) -> None:
        self._last_end_us = last_end_us

    def next(self) -> tuple[int, int]:
        start = self._last_end_us + 1_000
        end = start + 1_000
        self._last_end_us = end
        return start, end

    @property
    def last_end_us(self) -> int:
        return self._last_end_us

    @last_end_us.setter
    def last_end_us(self, value: int) -> None:
        self._last_end_us = value


_WEBVTT_HEADER = "WEBVTT\n\n"


def _sanitize_payload(payload: dict[str, Any]) -> str:
    raw = json.dumps(payload, separators=(",", ":"), ensure_ascii=False)
    raw = raw.replace("-->", "--\\u003e")
    raw = raw.replace("\n", "\\n").replace("\r", "\\r")
    return raw


def encode_cue(start_us: int, end_us: int, payload: dict[str, Any]) -> str:
    ts_line = f"{_us_to_ts(start_us)} --> {_us_to_ts(end_us)}"
    return f"{ts_line}\n{_sanitize_payload(payload)}"


def build_vtt(cues: list[tuple[int, int, dict[str, Any]]]) -> str:
    blocks = [_WEBVTT_HEADER]
    for start_us, end_us, payload in cues:
        blocks.append(encode_cue(start_us, end_us, payload) + "\n\n")
    return "".join(blocks)


_CUE_ARROW = re.compile(r"(\S+)\s+-->\s+(\S+)")


def parse_vtt(vtt_str: str) -> list[dict[str, Any]]:
    text = vtt_str.lstrip("\ufeff")

    rows: list[dict[str, Any]] = []
    lines = text.splitlines()
    i = 0

    while i < len(lines) and not lines[i].startswith("WEBVTT"):
        i += 1
    i += 1

    while i < len(lines):
        line = lines[i]
        i += 1

        if not line.strip():
            continue

        timing_line = line
        if not _CUE_ARROW.search(line):
            if i < len(lines):
                timing_line = lines[i]
                i += 1
            else:
                continue

        m = _CUE_ARROW.search(timing_line)
        if not m:
            continue

        try:
            start_us = _ts_to_us(m.group(1))
        except ValueError:
            continue

        payload_lines: list[str] = []
        while i < len(lines) and lines[i].strip():
            payload_lines.append(lines[i])
            i += 1

        if not payload_lines:
            continue

        raw_json = " ".join(payload_lines)
        raw_json = raw_json.replace("--\\u003e", "-->")
        try:
            data = json.loads(raw_json)
        except json.JSONDecodeError:
            continue

        data["_ts"] = start_us
        rows.append(data)

    return rows


def materialize(log: list[dict[str, Any]]) -> list[dict[str, Any]]:
    ordered = sorted(log, key=lambda r: r.get("_lsn", r.get("_ts", 0)))

    state: dict[Any, dict[str, Any]] = {}
    for entry in ordered:
        pk = entry.get("id") if "id" in entry else entry.get("_ts")
        op = entry.get("_op", "INSERT")

        if op == "INSERT":
            state[pk] = {k: v for k, v in entry.items() if not k.startswith("_")}
        elif op == "UPDATE":
            if pk in state:
                state[pk].update(
                    {k: v for k, v in entry.items() if not k.startswith("_")}
                )
        elif op == "DELETE":
            state.pop(pk, None)

    return list(state.values())


def append_cue_to_vtt(
    existing_vtt: str,
    payload: dict[str, Any],
    clock: CueClock,
) -> tuple[str, int, int]:
    start_us, end_us = clock.next()
    new_cue = encode_cue(start_us, end_us, payload)

    if not existing_vtt.strip():
        new_vtt = _WEBVTT_HEADER + new_cue + "\n\n"
    else:
        base = existing_vtt.rstrip("\n")
        new_vtt = base + "\n\n" + new_cue + "\n\n"

    return new_vtt, start_us, end_us


def last_end_us_from_vtt(vtt_str: str) -> int:
    text = vtt_str.lstrip("\ufeff")
    last_end: int = 0
    for m in _CUE_ARROW.finditer(text):
        end_ts = m.group(2)
        try:
            last_end = max(last_end, _ts_to_us(end_ts))
        except ValueError:
            pass
    return last_end

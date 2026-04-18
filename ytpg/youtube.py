from __future__ import annotations

import io
import json
import logging
import os
import time
from pathlib import Path
from typing import Any

import google.auth.transport.requests
import google.oauth2.credentials
import googleapiclient.discovery
import googleapiclient.errors
import googleapiclient.http
from google_auth_oauthlib.flow import InstalledAppFlow

from ytpg.errors import PGError

logger = logging.getLogger(__name__)

SCOPES = [
    "https://www.googleapis.com/auth/youtube",
    "https://www.googleapis.com/auth/youtube.force-ssl",
]

CAPTION_TRACK_NAME = "ytpg_wal"
CAPTION_LANGUAGE = "en"

_CONFIG_DIR = Path("~/.config/ytpg").expanduser()


def _creds_path(channel_id: str) -> Path:
    return _CONFIG_DIR / f"{channel_id}.oauth.json"


def _bootstrap_path(channel_id: str) -> Path:
    return _CONFIG_DIR / f"{channel_id}.json"


def load_credentials(channel_id: str) -> google.oauth2.credentials.Credentials:
    path = _creds_path(channel_id)
    if not path.exists():
        raise PGError(
            code="28000",
            message=f"OAuth credentials not found for channel {channel_id}, run 'ytpg init'",
        )
    data = json.loads(path.read_text())
    return google.oauth2.credentials.Credentials(
        token=data.get("token"),
        refresh_token=data.get("refresh_token"),
        token_uri=data.get("token_uri", "https://oauth2.googleapis.com/token"),
        client_id=data.get("client_id"),
        client_secret=data.get("client_secret"),
        scopes=data.get("scopes", SCOPES),
    )


def save_credentials(channel_id: str, creds: google.oauth2.credentials.Credentials) -> None:
    _CONFIG_DIR.mkdir(parents=True, exist_ok=True)
    path = _creds_path(channel_id)
    data = {
        "token": creds.token,
        "refresh_token": creds.refresh_token,
        "token_uri": creds.token_uri,
        "client_id": creds.client_id,
        "client_secret": creds.client_secret,
        "scopes": list(creds.scopes or SCOPES),
    }
    path.write_text(json.dumps(data))
    path.chmod(0o600)


def run_oauth_flow(client_secrets_file: str, channel_id: str) -> google.oauth2.credentials.Credentials:
    flow = InstalledAppFlow.from_client_secrets_file(client_secrets_file, scopes=SCOPES)
    creds = flow.run_local_server(port=0)
    save_credentials(channel_id, creds)
    return creds


def _is_rate_limit(exc: googleapiclient.errors.HttpError) -> bool:
    try:
        details = json.loads(exc.content)
        reason = details["error"]["errors"][0]["reason"]
        return reason in ("rateLimitExceeded", "userRateLimitExceeded")
    except Exception:
        return exc.resp.status in (429, 403)


def _is_quota_exceeded(exc: googleapiclient.errors.HttpError) -> bool:
    try:
        details = json.loads(exc.content)
        reason = details["error"]["errors"][0]["reason"]
        return reason == "quotaExceeded"
    except Exception:
        return False


def _with_retry(fn, *, max_retries: int = 5):
    delay = 1.0
    for attempt in range(max_retries + 1):
        try:
            return fn()
        except googleapiclient.errors.HttpError as exc:
            if _is_quota_exceeded(exc):
                raise PGError(
                    code="53300",
                    message="YouTube daily quota exhausted",
                ) from exc
            if _is_rate_limit(exc) and attempt < max_retries:
                logger.warning("Rate limit hit; backing off %.1fs (attempt %d)", delay, attempt + 1)
                time.sleep(delay)
                delay = min(delay * 2, 60.0)
                continue
            raise


def build_youtube(creds: google.oauth2.credentials.Credentials):
    return googleapiclient.discovery.build(
        "youtube", "v3", credentials=creds, cache_discovery=False
    )


def _refresh_if_needed(creds: google.oauth2.credentials.Credentials) -> None:
    if creds.expired and creds.refresh_token:
        try:
            creds.refresh(google.auth.transport.requests.Request())
        except Exception as exc:
            raise PGError(
                code="28000",
                message="OAuth credentials invalid, re-run 'ytpg init --reauth'",
            ) from exc


class YouTubeBackend:
    def __init__(self, youtube, channel_id: str) -> None:
        self._yt = youtube
        self.channel_id = channel_id
        self._caption_id_cache: dict[str, str] = {}

    def _invalidate_caption_id(self, video_id: str) -> None:
        self._caption_id_cache.pop(video_id, None)

    def _get_caption_id(self, video_id: str) -> str | None:
        cached = self._caption_id_cache.get(video_id)
        if cached is not None:
            return cached

        resp = _with_retry(
            lambda: self._yt.captions().list(part="snippet", videoId=video_id).execute()
        )
        items = resp.get("items", [])
        logger.info(
            "_get_caption_id(%s): found %d tracks: %s",
            video_id, len(items),
            [(i["id"], i["snippet"].get("name"), i["snippet"].get("language"), i["snippet"].get("isDraft"))
             for i in items],
        )
        for item in items:
            if item["snippet"].get("name") == CAPTION_TRACK_NAME:
                self._caption_id_cache[video_id] = item["id"]
                return item["id"]
        return None

    def fetch_raw_vtt(self, video_id: str) -> str:
        caption_id = self._get_caption_id(video_id)
        if caption_id is None:
            logger.info("fetch_raw_vtt(%s): no ytpg_wal caption track found", video_id)
            return "WEBVTT\n\n"

        logger.info("fetch_raw_vtt(%s): downloading caption_id=%s", video_id, caption_id)
        for attempt in range(5):
            try:
                request = self._yt.captions().download(id=caption_id, tfmt="vtt")
                buffer = io.BytesIO()
                downloader = googleapiclient.http.MediaIoBaseDownload(buffer, request)
                done = False
                while not done:
                    _, done = downloader.next_chunk()
                raw = buffer.getvalue()
                logger.info(
                    "fetch_raw_vtt(%s): attempt %d returned %d bytes: %r",
                    video_id, attempt + 1, len(raw), raw[:300],
                )
                if raw:
                    return raw.decode("utf-8-sig")
                if attempt < 4:
                    logger.warning("Empty caption download (attempt %d/5); retrying in 2s", attempt + 1)
                    time.sleep(2)
                    continue
                logger.warning("Caption download empty after 5 attempts for video %s", video_id)
                return "WEBVTT\n\n"
            except googleapiclient.errors.HttpError as exc:
                logger.warning(
                    "fetch_raw_vtt(%s): HTTP %s on attempt %d: %s",
                    video_id, exc.resp.status, attempt + 1, exc,
                )
                if exc.resp.status == 404:
                    self._invalidate_caption_id(video_id)
                    if attempt < 4:
                        time.sleep(1)
                        continue
                raise
        return "WEBVTT\n\n"

    def upload_vtt(self, video_id: str, vtt_str: str) -> None:
        caption_id = self._get_caption_id(video_id)
        body_bytes = vtt_str.encode("utf-8")
        media = googleapiclient.http.MediaIoBaseUpload(
            io.BytesIO(body_bytes),
            mimetype="text/vtt",
            resumable=False,
        )

        if caption_id is None:
            result = _with_retry(
                lambda: self._yt.captions().insert(
                    part="snippet",
                    body={
                        "snippet": {
                            "videoId": video_id,
                            "language": CAPTION_LANGUAGE,
                            "name": CAPTION_TRACK_NAME,
                            "isDraft": True,
                        }
                    },
                    media_body=media,
                    sync=False,
                ).execute()
            )
            new_id = result.get("id") if result else None
            if new_id:
                self._caption_id_cache[video_id] = new_id
            logger.info(
                "upload_vtt(%s): captions.insert → id=%s isDraft=%s status=%s",
                video_id,
                new_id,
                result.get("snippet", {}).get("isDraft") if result else None,
                result.get("snippet", {}).get("status") if result else None,
            )
        else:
            result = _with_retry(
                lambda: self._yt.captions().update(
                    part="snippet",
                    body={
                        "id": caption_id,
                        "snippet": {
                            "videoId": video_id,
                            "language": CAPTION_LANGUAGE,
                            "name": CAPTION_TRACK_NAME,
                            "isDraft": True,
                        },
                    },
                    media_body=media,
                    sync=False,
                ).execute()
            )
            logger.info(
                "upload_vtt(%s): captions.update → id=%s isDraft=%s status=%s",
                video_id,
                result.get("id") if result else None,
                result.get("snippet", {}).get("isDraft") if result else None,
                result.get("snippet", {}).get("status") if result else None,
            )

    def append_cue(self, video_id: str, payload: dict, clock=None) -> tuple[int, int]:
        from ytpg.vtt import CueClock, append_cue_to_vtt, last_end_us_from_vtt

        raw = self.fetch_raw_vtt(video_id)
        if clock is None:
            clock = CueClock(last_end_us_from_vtt(raw))
        new_vtt, start_us, end_us = append_cue_to_vtt(raw, payload, clock)
        self.upload_vtt(video_id, new_vtt)
        return start_us, end_us

    def fetch_rows(self, video_id: str) -> list[dict]:
        from ytpg.vtt import parse_vtt, materialize

        raw = self.fetch_raw_vtt(video_id)
        log = parse_vtt(raw)
        return materialize(log)

    def fetch_log(self, video_id: str) -> list[dict]:
        from ytpg.vtt import parse_vtt

        return parse_vtt(self.fetch_raw_vtt(video_id))

    def fetch_vtt_size(self, video_id: str) -> int:
        raw = self.fetch_raw_vtt(video_id)
        return len(raw.encode("utf-8"))

    def create_video(
        self,
        title: str,
        description: str = "",
        category_id: str = "22",
    ) -> str:
        import tempfile

        video_bytes = _make_black_frame_video()
        with tempfile.NamedTemporaryFile(suffix=".mp4", delete=False) as f:
            f.write(video_bytes)
            tmp_path = f.name

        try:
            media = googleapiclient.http.MediaFileUpload(
                tmp_path, mimetype="video/mp4", resumable=True
            )
            resp = _with_retry(
                lambda: self._yt.videos().insert(
                    part="snippet,status",
                    body={
                        "snippet": {
                            "title": title,
                            "description": description,
                            "categoryId": category_id,
                        },
                        "status": {
                            "privacyStatus": "unlisted",
                            "madeForKids": False,
                        },
                    },
                    media_body=media,
                ).execute()
            )
        finally:
            os.unlink(tmp_path)

        return resp["id"]

    def update_video_description(self, video_id: str, title: str, description: str, category_id: str = "22") -> None:
        _with_retry(
            lambda: self._yt.videos().update(
                part="snippet",
                body={
                    "id": video_id,
                    "snippet": {
                        "title": title,
                        "description": description,
                        "categoryId": category_id,
                    },
                },
            ).execute()
        )

    def get_video_description(self, video_id: str) -> str:
        resp = _with_retry(
            lambda: self._yt.videos().list(part="snippet", id=video_id).execute()
        )
        items = resp.get("items", [])
        if not items:
            raise PGError(code="42P01", message=f"Video {video_id} not found")
        return items[0]["snippet"]["description"]

    def get_video_snippet(self, video_id: str) -> dict:
        resp = _with_retry(
            lambda: self._yt.videos().list(part="snippet", id=video_id).execute()
        )
        items = resp.get("items", [])
        if not items:
            raise PGError(code="42P01", message=f"Video {video_id} not found")
        return items[0]["snippet"]

    def delete_video(self, video_id: str) -> None:
        _with_retry(lambda: self._yt.videos().delete(id=video_id).execute())

    def search_videos(self, query: str, max_results: int = 5) -> list[dict]:
        resp = _with_retry(
            lambda: self._yt.search().list(
                part="snippet",
                forMine=True,
                q=query,
                type="video",
                maxResults=max_results,
            ).execute()
        )
        return [
            {"id": item["id"]["videoId"], "title": item["snippet"]["title"]}
            for item in resp.get("items", [])
        ]

    def get_pinned_comment(self, video_id: str) -> dict | None:
        page_token = None
        best: dict | None = None
        best_lsn = -1

        for _ in range(10):
            kwargs: dict[str, Any] = dict(
                part="snippet",
                videoId=video_id,
                maxResults=100,
                order="time",
            )
            if page_token:
                kwargs["pageToken"] = page_token

            resp = _with_retry(
                lambda: self._yt.commentThreads().list(**kwargs).execute()
            )
            for item in resp.get("items", []):
                text = item["snippet"]["topLevelComment"]["snippet"]["textDisplay"]
                try:
                    data = json.loads(text)
                    if "lsn" in data and data["lsn"] > best_lsn:
                        best_lsn = data["lsn"]
                        best = data
                        best["_comment_id"] = item["id"]
                except (json.JSONDecodeError, KeyError):
                    pass

            page_token = resp.get("nextPageToken")
            if not page_token:
                break

        return best

    def post_checkpoint(self, video_id: str, checkpoint: dict) -> str:
        payload = json.dumps(checkpoint, separators=(",", ":"))
        if len(payload.encode("utf-8")) > 9500:
            minimal = {k: checkpoint[k] for k in ("lsn", "root", "prev_root", "prev_comment_id", "leaf_count", "ts") if k in checkpoint}
            payload = json.dumps(minimal, separators=(",", ":"))

        resp = _with_retry(
            lambda: self._yt.commentThreads().insert(
                part="snippet",
                body={
                    "snippet": {
                        "videoId": video_id,
                        "topLevelComment": {
                            "snippet": {"textOriginal": payload}
                        },
                    }
                },
            ).execute()
        )
        return resp["id"]

    def delete_comment(self, comment_id: str) -> None:
        try:
            _with_retry(lambda: self._yt.comments().delete(id=comment_id).execute())
        except Exception as exc:
            logger.debug("Failed to delete comment %s: %s", comment_id, exc)


def _make_black_frame_video() -> bytes:
    data = _make_via_ffmpeg()
    if data:
        return data

    data = _make_via_opencv()
    if data:
        return data

    raise RuntimeError(
        "Cannot create a video file for YouTube upload.\n"
        "Install ffmpeg: brew install ffmpeg\n"
        "Then re-run: ytpg init --channel-id <id> --client-secrets <file>"
    )


def _make_via_ffmpeg() -> bytes | None:
    import subprocess
    import tempfile

    with tempfile.NamedTemporaryFile(suffix=".mp4", delete=False) as f:
        tmp = f.name

    try:
        result = subprocess.run(
            [
                "ffmpeg", "-y", "-loglevel", "error",
                "-f", "lavfi",
                "-i", "color=black:size=480x360:rate=25",
                "-t", "5",
                "-c:v", "libx264",
                "-pix_fmt", "yuv420p",
                "-profile:v", "baseline",
                "-level", "3.0",
                "-movflags", "+faststart",
                tmp,
            ],
            capture_output=True,
            timeout=60,
        )
        if result.returncode == 0 and os.path.getsize(tmp) > 5000:
            with open(tmp, "rb") as f:
                data = f.read()
            logger.info("Created placeholder video via ffmpeg (%d bytes)", len(data))
            return data
        logger.debug("ffmpeg returned %d: %s", result.returncode, result.stderr[:200])
    except FileNotFoundError:
        logger.debug("ffmpeg not found")
    except Exception as exc:
        logger.debug("ffmpeg failed: %s", exc)
    finally:
        if os.path.exists(tmp):
            os.unlink(tmp)
    return None


def _make_via_opencv() -> bytes | None:
    import tempfile

    try:
        import cv2
        import numpy as np
    except ImportError:
        return None

    fps = 25
    duration = 5
    width, height = 480, 360
    frame = np.zeros((height, width, 3), dtype=np.uint8)

    candidates = [
        ("avc1", ".mp4"),
        ("mp4v", ".mp4"),
        ("XVID", ".avi"),
        ("MJPG", ".avi"),
    ]

    for fourcc_str, suffix in candidates:
        with tempfile.NamedTemporaryFile(suffix=suffix, delete=False) as f:
            tmp = f.name
        try:
            fourcc = cv2.VideoWriter_fourcc(*fourcc_str)
            out = cv2.VideoWriter(tmp, fourcc, float(fps), (width, height))
            if not out.isOpened():
                os.unlink(tmp)
                continue
            for _ in range(fps * duration):
                out.write(frame)
            out.release()
            size = os.path.getsize(tmp)
            if size > 5000:
                with open(tmp, "rb") as f:
                    data = f.read()
                os.unlink(tmp)
                logger.info("Created placeholder video via OpenCV/%s (%d bytes)", fourcc_str, len(data))
                return data
            os.unlink(tmp)
        except Exception as exc:
            logger.debug("OpenCV/%s failed: %s", fourcc_str, exc)
            if os.path.exists(tmp):
                os.unlink(tmp)

    return None

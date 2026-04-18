from __future__ import annotations

import asyncio
import json
import logging
import sys
from pathlib import Path

import click

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s: %(message)s",
)
logger = logging.getLogger("ytpg.cli")


def _load_backend(channel_id: str):
    from ytpg.youtube import load_credentials, build_youtube, YouTubeBackend

    creds = load_credentials(channel_id)
    youtube = build_youtube(creds)
    backend = YouTubeBackend(youtube, channel_id)
    return youtube, backend


@click.group()
def cli():
    """ytpg — YouTube as a Postgres database."""


@cli.command()
@click.option("--channel-id", required=True, help="YouTube channel ID (UCxxxxxxxx)")
@click.option("--client-secrets", default="client_secrets.json", show_default=True,
              help="Path to Google OAuth client secrets JSON")
@click.option("--reauth", is_flag=True, help="Force re-authentication even if credentials exist")
def init(channel_id: str, client_secrets: str, reauth: bool):
    """Initialise ytpg for a YouTube channel.

    Creates the manifest video, runs OAuth flow, and writes bootstrap
    metadata to ~/.config/ytpg/<channel_id>.json.
    """
    from ytpg.youtube import (
        load_credentials, run_oauth_flow,
        build_youtube, YouTubeBackend, _creds_path,
    )
    from ytpg.manifest import (
        _find_manifest_video, _save_bootstrap_cache, make_empty_manifest,
        update_manifest, MANIFEST_TITLE,
    )
    from ytpg.errors import PGError

    creds_path = _creds_path(channel_id)
    if reauth or not creds_path.exists():
        click.echo(f"Running OAuth flow for channel {channel_id}…")
        if not Path(client_secrets).exists():
            click.echo(
                f"ERROR: client secrets file not found: {client_secrets}\n"
                "Download it from https://console.cloud.google.com/apis/credentials",
                err=True,
            )
            sys.exit(1)
        creds = run_oauth_flow(client_secrets, channel_id)
        click.echo("OAuth credentials saved.")
    else:
        creds = load_credentials(channel_id)

    youtube = build_youtube(creds)
    backend = YouTubeBackend(youtube, channel_id)

    try:
        manifest_id = _find_manifest_video(youtube, channel_id)
        click.echo(f"Manifest video already exists: {manifest_id}")
    except PGError:
        manifest_id = None

    if manifest_id is None:
        click.echo("Creating manifest video…")
        manifest_id = backend.create_video(
            title=MANIFEST_TITLE,
            description=json.dumps(make_empty_manifest(channel_id), indent=2),
        )
        click.echo(f"Manifest video created: {manifest_id}")

    _save_bootstrap_cache(channel_id, manifest_video_id=manifest_id)
    click.echo(
        f"\nInitialization complete.\n"
        f"  Manifest video : {manifest_id}\n"
        f"\nConnect with:\n"
        f"  postgresql://ytpg@localhost:5432/{channel_id}"
    )


@cli.command()
@click.option("--channel-id", required=True, help="YouTube channel ID")
@click.option("--host", default="localhost", show_default=True)
@click.option("--port", default=5432, show_default=True, type=int)
def serve(channel_id: str, host: str, port: int):
    """Start the ytpg Postgres wire protocol proxy."""
    from ytpg.proxy import YTPGProxy

    youtube, backend = _load_backend(channel_id)

    proxy = YTPGProxy(
        channel_id=channel_id,
        youtube=youtube,
        backend=backend,
        host=host,
        port=port,
    )

    async def _run():
        await proxy.start()
        click.echo(f"ytpg proxy running on {host}:{port} (channel={channel_id})")
        click.echo("Press Ctrl+C to stop.")
        try:
            await proxy.serve_forever()
        except asyncio.CancelledError:
            pass
        finally:
            await proxy.stop()

    try:
        asyncio.run(_run())
    except KeyboardInterrupt:
        click.echo("\nStopped.")


@cli.command()
@click.option("--channel-id", required=True)
def status(channel_id: str):
    """Display manifest tables."""
    from ytpg.manifest import get_manifest

    youtube, backend = _load_backend(channel_id)

    manifest = get_manifest(youtube, channel_id)
    tables = manifest.get("tables", {})
    free = manifest.get("free", [])

    click.echo(f"Channel : {channel_id}")
    click.echo(f"Version : {manifest.get('ytpg_version', '?')}")
    click.echo(f"Tables  : {len(tables)}")
    click.echo(f"Free    : {len(free)} recycled video(s) available")
    for name, entry in tables.items():
        vid = entry["head"] if isinstance(entry, dict) else entry
        segs = len(entry.get("segments", [])) if isinstance(entry, dict) else 1
        click.echo(f"  {name:30s}  video={vid}  segments={segs}")



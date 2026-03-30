#!/usr/bin/env python3
"""
Upload graph assets to DigitalOcean Spaces.

- Uploads atlas files used by the graph UI:
    atlas/atlas.webp
    atlas/atlas_manifest.json

- Uploads manually selected node face crops (flattened) from:
    images/node_faces_selected_optimized/**/*.webp
  (produced by ``13__optimize_node_faces.sh`` from ``node_faces_selected/``) to:
    faces/<basename>.webp
  (files in subfolders such as ``old/`` share the same flat ``faces/`` key by basename.)

When no ``*.webp`` files exist under ``node_faces_selected_optimized/``, the ``faces/`` prefix is not
synced (existing remote ``faces/*`` objects are left unchanged).

Note: document originals/thumbnails for the photo viewer are hosted separately.

DigitalOcean Spaces is S3-compatible. Configure via environment variables:

    EPSTEIN_SPACES_REGION   (e.g. "sfo3")
    EPSTEIN_SPACES_ENDPOINT (e.g. "https://sfo3.digitaloceanspaces.com")
    EPSTEIN_SPACES_BUCKET   (bucket name)
    EPSTEIN_SPACES_KEY      (access key)
    EPSTEIN_SPACES_SECRET   (secret key)
"""

from __future__ import annotations

import mimetypes
import os
from pathlib import Path

import boto3


SCRIPT_DIR = Path(__file__).resolve().parent
REPO_DIR = SCRIPT_DIR.parent

VIZ_DATA_DIR = REPO_DIR / "viz_data"

ATLAS_WEBP_PATH = REPO_DIR / "images" / "atlas.webp"
ATLAS_MANIFEST_PATH = VIZ_DATA_DIR / "atlas_manifest.json"
NODE_FACES_OPTIMIZED_DIR = REPO_DIR / "images" / "node_faces_selected_optimized"
SYNC_PREFIXES_ATLAS = ("atlas/",)
FORCE_UPLOAD_KEYS = {"atlas/atlas.webp", "atlas/atlas_manifest.json"}
ATLAS_IMAGE_CACHE_CONTROL = "public, max-age=3600, stale-while-revalidate=86400"
ATLAS_MANIFEST_CACHE_CONTROL = "no-cache, max-age=0, must-revalidate"
FACES_CACHE_CONTROL = "public, max-age=86400, stale-while-revalidate=604800"


def get_spaces_client():
    region = os.environ.get("EPSTEIN_SPACES_REGION")
    endpoint = os.environ.get("EPSTEIN_SPACES_ENDPOINT")
    bucket = os.environ.get("EPSTEIN_SPACES_BUCKET")
    key = os.environ.get("EPSTEIN_SPACES_KEY")
    secret = os.environ.get("EPSTEIN_SPACES_SECRET")

    missing = [name for name, val in [
        ("EPSTEIN_SPACES_REGION", region),
        ("EPSTEIN_SPACES_ENDPOINT", endpoint),
        ("EPSTEIN_SPACES_BUCKET", bucket),
        ("EPSTEIN_SPACES_KEY", key),
        ("EPSTEIN_SPACES_SECRET", secret),
    ] if not val]
    if missing:
        raise RuntimeError(f"Missing required environment variables for Spaces: {', '.join(missing)}")

    s3 = boto3.client(
        "s3",
        region_name=region,
        endpoint_url=endpoint,
        aws_access_key_id=key,
        aws_secret_access_key=secret,
    )
    return s3, bucket


def upload_file(
    s3,
    bucket: str,
    local_path: Path,
    key: str,
    cache_control: str | None = None,
) -> None:
    if not local_path.is_file():
        raise FileNotFoundError(f"Required file missing for upload: {local_path}")
    print(f"Uploading {local_path} -> s3://{bucket}/{key}")
    extra_args: dict[str, str] = {"ACL": "public-read"}
    content_type, _ = mimetypes.guess_type(str(local_path))
    if content_type is None:
        if local_path.suffix.lower() == ".webp":
            content_type = "image/webp"
        elif local_path.suffix.lower() in (".jpg", ".jpeg"):
            content_type = "image/jpeg"
        elif local_path.suffix.lower() == ".json":
            content_type = "application/json"
    if content_type is not None:
        extra_args["ContentType"] = content_type
    if cache_control is not None:
        extra_args["CacheControl"] = cache_control
    s3.upload_file(
        Filename=str(local_path),
        Bucket=bucket,
        Key=key,
        ExtraArgs=extra_args,
    )


def list_remote_objects(s3, bucket: str, prefixes: tuple[str, ...]) -> dict[str, int]:
    """Return {key: size_bytes} for all objects under the provided prefixes."""
    out: dict[str, int] = {}
    paginator = s3.get_paginator("list_objects_v2")
    for prefix in prefixes:
        for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
            for obj in page.get("Contents", []):
                out[obj["Key"]] = int(obj["Size"])
    return out


def collect_flat_face_webp_uploads() -> dict[str, Path]:
    """
    Map ``faces/<basename>`` -> local path for every ``*.webp`` under node_faces_selected_optimized.
    Errors if two files share the same basename in different folders.
    """
    if not NODE_FACES_OPTIMIZED_DIR.is_dir():
        return {}
    out: dict[str, Path] = {}
    for p in sorted(NODE_FACES_OPTIMIZED_DIR.rglob("*.webp")):
        key = f"faces/{p.name}"
        if key in out and out[key] != p:
            raise RuntimeError(
                f"Duplicate basename after flattening: {p.name!r}\n"
                f"  First: {out[key]}\n  Second: {p}"
            )
        out[key] = p
    return out


def delete_keys(s3, bucket: str, keys: list[str]) -> None:
    """Delete keys in batches (max 1000 per request)."""
    if not keys:
        return
    chunk_size = 1000
    for i in range(0, len(keys), chunk_size):
        chunk = keys[i:i + chunk_size]
        print(f"Deleting {len(chunk)} stale object(s) from s3://{bucket}/...")
        s3.delete_objects(
            Bucket=bucket,
            Delete={"Objects": [{"Key": k} for k in chunk], "Quiet": True},
        )


def main():
    s3, bucket = get_spaces_client()

    desired: dict[str, Path] = {}
    # Atlas: used by index.html to render per-node faces without many HTTP requests.
    desired["atlas/atlas.webp"] = ATLAS_WEBP_PATH
    desired["atlas/atlas_manifest.json"] = ATLAS_MANIFEST_PATH

    face_uploads = collect_flat_face_webp_uploads()
    desired.update(face_uploads)

    # Validate all desired local files before mutating remote state.
    for key, local_path in desired.items():
        if not local_path.is_file():
            raise FileNotFoundError(f"Required file missing for upload key={key}: {local_path}")

    list_prefixes: tuple[str, ...] = SYNC_PREFIXES_ATLAS
    if face_uploads:
        list_prefixes = ("atlas/", "faces/")

    remote = list_remote_objects(s3, bucket, list_prefixes)

    # Upload only missing/changed files.
    uploaded = 0
    skipped = 0
    for key, local_path in desired.items():
        if key in FORCE_UPLOAD_KEYS:
            # Atlas files are small and frequently replaced. Force upload to
            # avoid false "unchanged" skips when file size happens to match.
            cache_control = (
                ATLAS_IMAGE_CACHE_CONTROL
                if key == "atlas/atlas.webp"
                else ATLAS_MANIFEST_CACHE_CONTROL
            )
            upload_file(
                s3,
                bucket,
                local_path,
                key,
                cache_control=cache_control,
            )
            uploaded += 1
            continue

        if key.startswith("faces/"):
            local_size = local_path.stat().st_size
            remote_size = remote.get(key)
            if remote_size is not None and remote_size == local_size:
                skipped += 1
                continue
            upload_file(
                s3,
                bucket,
                local_path,
                key,
                cache_control=FACES_CACHE_CONTROL,
            )
            uploaded += 1
            continue

        local_size = local_path.stat().st_size
        remote_size = remote.get(key)
        if remote_size is not None and remote_size == local_size:
            skipped += 1
            continue
        upload_file(s3, bucket, local_path, key)
        uploaded += 1

    # Delete stale remote files in managed prefixes (faces/ only when we synced faces).
    desired_keys = set(desired.keys())
    remote_keys = set(remote.keys())
    stale_keys = sorted(remote_keys - desired_keys)
    delete_keys(s3, bucket, stale_keys)

    print(
        "Spaces sync complete. "
        f"desired={len(desired_keys)} uploaded={uploaded} skipped={skipped} deleted={len(stale_keys)}"
        + (f" (faces local files: {len(face_uploads)})" if face_uploads else "")
    )


if __name__ == "__main__":
    main()

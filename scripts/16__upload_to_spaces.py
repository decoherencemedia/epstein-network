#!/usr/bin/env python3
"""
Upload selected graph images and thumbnails to DigitalOcean Spaces.

- Reads image_data.json written by 10__create_graph.py:
    {"nodes": {name: filename_or_null, ...}, "edges": {"A-B": filename_or_null, ...}}
- Uploads originals from all_images/ to:   images/<filename>
- Uploads thumbnails from thumbnails/ to:  thumbnails/<stem>.webp

DigitalOcean Spaces is S3-compatible. Configure via environment variables:

    EPSTEIN_SPACES_REGION   (e.g. "sfo3")
    EPSTEIN_SPACES_ENDPOINT (e.g. "https://sfo3.digitaloceanspaces.com")
    EPSTEIN_SPACES_BUCKET   (bucket name)
    EPSTEIN_SPACES_KEY      (access key)
    EPSTEIN_SPACES_SECRET   (secret key)
"""

from __future__ import annotations

import json
import os
from pathlib import Path

import boto3

from config import IMAGE_DIR


SCRIPT_DIR = Path(__file__).resolve().parent
REPO_DIR = SCRIPT_DIR.parent

# Originals live in the pipeline's configured IMAGE_DIR (often outside the repo).
ALL_IMAGES_DIR = IMAGE_DIR

# These are produced inside this repo by the graph pipeline.
THUMBS_DIR = REPO_DIR / "images" / "thumbnails"
VIZ_DATA_DIR = REPO_DIR / "viz_data"
IMAGE_DATA_PATH = VIZ_DATA_DIR / "image_data.json"

ATLAS_WEBP_PATH = REPO_DIR / "images" / "atlas.webp"
ATLAS_MANIFEST_PATH = VIZ_DATA_DIR / "atlas_manifest.json"
SYNC_PREFIXES = ("images/", "thumbnails/", "atlas/")
FORCE_UPLOAD_KEYS = {"atlas/atlas.webp", "atlas/atlas_manifest.json"}
ATLAS_IMAGE_CACHE_CONTROL = "public, max-age=3600, stale-while-revalidate=86400"
ATLAS_MANIFEST_CACHE_CONTROL = "no-cache, max-age=0, must-revalidate"


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


def load_image_data():
    if not IMAGE_DATA_PATH.is_file():
        raise RuntimeError(f"image_data.json not found at {IMAGE_DATA_PATH}")
    with IMAGE_DATA_PATH.open("r", encoding="utf-8") as f:
        data = json.load(f)
    nodes = data.get("nodes") or {}
    edges = data.get("edges") or {}
    # nodes values are [filename, bbox] or null; edges values are [filename, bbox_a, bbox_b] or null.
    filenames: set[str] = set()
    for v in nodes.values():
        if v:
            filenames.add(v[0])
    for v in edges.values():
        if v:
            filenames.add(v[0])
    return sorted(filenames)


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
    filenames = load_image_data()

    desired: dict[str, Path] = {}
    for filename in filenames:
        # Original image
        orig_path = ALL_IMAGES_DIR / filename
        orig_key = f"images/{filename}"
        desired[orig_key] = orig_path

        # Thumbnail: thumbnails/<stem>.webp
        stem = Path(filename).stem
        thumb_path = THUMBS_DIR / f"{stem}.webp"
        thumb_key = f"thumbnails/{stem}.webp"
        desired[thumb_key] = thumb_path

    # Atlas: used by index.html to render per-node faces without many HTTP requests.
    desired["atlas/atlas.webp"] = ATLAS_WEBP_PATH
    desired["atlas/atlas_manifest.json"] = ATLAS_MANIFEST_PATH

    # Validate all desired local files before mutating remote state.
    for key, local_path in desired.items():
        if not local_path.is_file():
            raise FileNotFoundError(f"Required file missing for upload key={key}: {local_path}")

    remote = list_remote_objects(s3, bucket, SYNC_PREFIXES)

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

        local_size = local_path.stat().st_size
        remote_size = remote.get(key)
        if remote_size is not None and remote_size == local_size:
            skipped += 1
            continue
        upload_file(s3, bucket, local_path, key)
        uploaded += 1

    # Delete stale remote files in managed prefixes.
    desired_keys = set(desired.keys())
    remote_keys = set(remote.keys())
    stale_keys = sorted(remote_keys - desired_keys)
    delete_keys(s3, bucket, stale_keys)

    print(
        "Spaces sync complete. "
        f"desired={len(desired_keys)} uploaded={uploaded} skipped={skipped} deleted={len(stale_keys)}"
    )


if __name__ == "__main__":
    main()

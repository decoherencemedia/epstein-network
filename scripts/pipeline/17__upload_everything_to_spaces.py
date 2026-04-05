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


import json
import mimetypes
import os
from pathlib import Path

import boto3

from epstein_photos.config import IMAGE_DIR, NETWORK_ROOT

REPO_DIR = NETWORK_ROOT

# Originals live in the pipeline's configured IMAGE_DIR (often outside the repo).
ALL_IMAGES_DIR = Path("/home/tristan/Documents/misc/epstein/all_images_with_faces_webp/images")

# These are produced inside this repo by the graph pipeline.
THUMBS_DIR = Path("/home/tristan/Documents/misc/epstein/all_images_with_faces_thumbnails/images")
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
    content_type, _ = mimetypes.guess_type(str(local_path))
    if content_type is None:
        if local_path.suffix.lower() == ".webp":
            content_type = "image/webp"
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

    filenames = ALL_IMAGES_DIR.glob("*.webp")
    for filename in filenames:
        # Original image
        orig_path = filename
        orig_key = f"images/{filename.name}"
        print(orig_path, orig_key)
        upload_file(s3, bucket, orig_path, orig_key)


    thumbnails = THUMBS_DIR.glob("*.webp")
    for thumbnail in thumbnails:
        # Thumbnail image
        thumb_path = thumbnail
        thumb_key = f"thumbnails/{thumbnail.name}"
        print(thumb_path, thumb_key)
        upload_file(s3, bucket, thumb_path, thumb_key)

if __name__ == "__main__":
    main()

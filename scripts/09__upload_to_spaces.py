#!/usr/bin/env python3
"""
Upload selected graph images and thumbnails to DigitalOcean Spaces.

- Reads image_data.json written by 07__create_graph.py:
    {"nodes": {name: filename_or_null, ...}, "edges": {"A-B": filename_or_null, ...}}
- Uploads originals from all_images/ to:   images/<filename>
- Uploads thumbnails from thumbnails/ to:  thumbnails/<stem>.webp

DigitalOcean Spaces is S3-compatible. Configure via environment variables:

    SPACES_REGION   (e.g. "sfo3")
    SPACES_ENDPOINT (e.g. "https://sfo3.digitaloceanspaces.com")
    SPACES_BUCKET   (bucket name)
    SPACES_KEY      (access key)
    SPACES_SECRET   (secret key)
"""

from __future__ import annotations

import json
import os
from pathlib import Path

import boto3


ROOT_DIR = Path(__file__).resolve().parents[1]
ALL_IMAGES_DIR = ROOT_DIR / "all_images"
THUMBS_DIR = ROOT_DIR / "thumbnails"
IMAGE_DATA_PATH = ROOT_DIR / "image_data.json"


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
    filenames: set[str] = set()
    for fn in nodes.values():
        if fn:
            filenames.add(fn)
    for fn in edges.values():
        if fn:
            filenames.add(fn)
    return sorted(filenames)


def upload_file_if_exists(s3, bucket: str, local_path: Path, key: str) -> None:
    if not local_path.is_file():
        return
    print(f"Uploading {local_path} -> s3://{bucket}/{key}")
    s3.upload_file(
        Filename=str(local_path),
        Bucket=bucket,
        Key=key,
        ExtraArgs={"ACL": "public-read"},
    )


def main():
    s3, bucket = get_spaces_client()
    filenames = load_image_data()

    for filename in filenames:
        # Original image
        orig_path = ALL_IMAGES_DIR / filename
        orig_key = f"images/{filename}"
        upload_file_if_exists(s3, bucket, orig_path, orig_key)

        # Thumbnail: thumbnails/<stem>.webp
        stem = Path(filename).stem
        thumb_path = THUMBS_DIR / f"{stem}.webp"
        thumb_key = f"thumbnails/{stem}.webp"
        upload_file_if_exists(s3, bucket, thumb_path, thumb_key)

    print("Done uploading originals and thumbnails to Spaces.")


if __name__ == "__main__":
    main()

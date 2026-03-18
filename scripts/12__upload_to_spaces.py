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
THUMBS_DIR = REPO_DIR / "thumbnails"
IMAGE_DATA_PATH = REPO_DIR / "image_data.json"


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


def upload_file(s3, bucket: str, local_path: Path, key: str) -> None:
    if not local_path.is_file():
        raise FileNotFoundError(f"Required file missing for upload: {local_path}")
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
        upload_file(s3, bucket, orig_path, orig_key)

        # Thumbnail: thumbnails/<stem>.webp
        stem = Path(filename).stem
        thumb_path = THUMBS_DIR / f"{stem}.webp"
        thumb_key = f"thumbnails/{stem}.webp"
        upload_file(s3, bucket, thumb_path, thumb_key)

    print("Done uploading originals and thumbnails to Spaces.")


if __name__ == "__main__":
    main()

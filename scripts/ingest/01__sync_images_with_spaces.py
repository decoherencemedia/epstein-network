#!/usr/bin/env python3
"""
Compare DigitalOcean Spaces ``images/`` and ``thumbnails/`` to SQLite, then upload gaps.

For every canonical image with ``has_face = 1``, the site expects WebP assets::

    images/<stem>.webp
    thumbnails/<stem>.webp

(where ``stem`` is the basename of ``images.image_name`` without its original extension).

This script:

1. Lists remote objects under ``images/`` and ``thumbnails/`` (basenames only).
2. Loads ``image_name`` from ``images`` where ``duplicate_of IS NULL`` and ``has_face = 1``.
3. For any row whose WebP basename is missing from Spaces, runs ``cwebp`` on the local file
   (full-size WebP). Thumbnails use ``epstein_photos.webp`` (ImageMagick resize + cwebp), same
   stack as ``pipeline/11__create_thumbnails.py``.

Requires: ``cwebp``, ``boto3``, and ImageMagick (``magick`` or ``convert``) for thumbnails.
Same env vars as ``16__upload_to_spaces.py``::

    EPSTEIN_SPACES_REGION
    EPSTEIN_SPACES_ENDPOINT
    EPSTEIN_SPACES_BUCKET
    EPSTEIN_SPACES_KEY
    EPSTEIN_SPACES_SECRET

Run (after ``pip install -e .`` from the ``network/`` repo root)::

    python3 scripts/ingest/01__sync_images_with_spaces.py

Edit constants below (e.g. ``DRY_RUN``) as needed.
"""

import io
import os
import shutil
import sqlite3
from pathlib import Path

import boto3

from epstein_photos.config import DB_PATH, IMAGE_DIR
from epstein_photos.webp import full_image_to_webp_bytes, thumbnail_to_webp_bytes

# ----- run configuration -----

DRY_RUN = False
# If set, use this DB instead of ``epstein_photos.config.DB_PATH``.
SYNC_DB_PATH: Path | None = None
# Local tree of originals (must contain every ``image_name`` row from SQLite).
SYNC_IMAGE_DIR: Path = IMAGE_DIR

FULL_WEBP_QUALITY = 90
THUMB_MAX_PX = 500
THUMB_WEBP_QUALITY = 82
IMAGE_CACHE_CONTROL = "public, max-age=86400, stale-while-revalidate=604800"


def get_spaces_client():
    region = os.environ.get("EPSTEIN_SPACES_REGION")
    endpoint = os.environ.get("EPSTEIN_SPACES_ENDPOINT")
    bucket = os.environ.get("EPSTEIN_SPACES_BUCKET")
    key = os.environ.get("EPSTEIN_SPACES_KEY")
    secret = os.environ.get("EPSTEIN_SPACES_SECRET")

    missing = [
        name
        for name, val in [
            ("EPSTEIN_SPACES_REGION", region),
            ("EPSTEIN_SPACES_ENDPOINT", endpoint),
            ("EPSTEIN_SPACES_BUCKET", bucket),
            ("EPSTEIN_SPACES_KEY", key),
            ("EPSTEIN_SPACES_SECRET", secret),
        ]
        if not val
    ]
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


def list_remote_basenames(s3, bucket: str, prefix: str) -> set[str]:
    """Basenames (e.g. ``foo.webp``) under ``prefix`` (e.g. ``images/``)."""
    out: set[str] = set()
    paginator = s3.get_paginator("list_objects_v2")
    for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
        for obj in page.get("Contents", []):
            key = obj["Key"]
            if key.endswith("/"):
                continue
            out.add(Path(key).name)
    return out


def db_has_face_image_names(conn: sqlite3.Connection) -> list[str]:
    c = conn.cursor()
    c.execute(
        """
        SELECT image_name FROM images
        WHERE duplicate_of IS NULL AND has_face = 1
        ORDER BY image_name
        """
    )
    return [str(row[0]) for row in c.fetchall()]


def webp_basename(image_name: str) -> str:
    return f"{Path(image_name).stem}.webp"


def upload_bytes(
    s3,
    bucket: str,
    key: str,
    data: bytes,
    *,
    dry_run: bool,
) -> None:
    extra: dict[str, str] = {
        "ACL": "public-read",
        "ContentType": "image/webp",
        "CacheControl": IMAGE_CACHE_CONTROL,
    }
    if dry_run:
        print(f"  [dry-run] would upload {len(data)} bytes -> s3://{bucket}/{key}")
        return
    print(f"  uploading {len(data)} bytes -> s3://{bucket}/{key}")
    s3.upload_fileobj(io.BytesIO(data), Bucket=bucket, Key=key, ExtraArgs=extra)


def main() -> None:
    if not shutil.which("cwebp"):
        raise RuntimeError(
            "cwebp not found in PATH (install webp package / same as scripts/pipeline/13__optimize_node_faces.py)"
        )

    db_path = (SYNC_DB_PATH or DB_PATH).resolve()
    if not db_path.is_file():
        raise FileNotFoundError(f"Database not found: {db_path}")

    image_dir = SYNC_IMAGE_DIR.resolve()
    if not image_dir.is_dir():
        raise NotADirectoryError(f"Not a directory: {image_dir}")

    s3, bucket = get_spaces_client()
    remote_img = list_remote_basenames(s3, bucket, "images/")
    remote_thumb = list_remote_basenames(s3, bucket, "thumbnails/")

    conn = sqlite3.connect(str(db_path))
    try:
        names = db_has_face_image_names(conn)
    finally:
        conn.close()

    print(f"Database: {db_path}")
    print(f"IMAGE_DIR:  {image_dir}")
    print(f"Remote images/:      {len(remote_img)} object(s)")
    print(f"Remote thumbnails/:  {len(remote_thumb)} object(s)")
    print(f"DB rows (has_face=1, canonical): {len(names)}")

    need_work: list[tuple[str, str, bool, bool]] = []
    for image_name in names:
        wname = webp_basename(image_name)
        have_i = wname in remote_img
        have_t = wname in remote_thumb
        if have_i and have_t:
            continue
        need_work.append((image_name, wname, not have_i, not have_t))

    print(f"To sync (missing full and/or thumb): {len(need_work)}")
    if not need_work:
        print("Nothing to do.")
        return

    for image_name, wname, need_full, need_thumb in need_work:
        local = image_dir / image_name
        if not local.is_file():
            raise FileNotFoundError(
                f"SQLite references image_name={image_name!r} but file is missing: {local}"
            )
        print(f"{image_name} -> {wname}  (full={need_full}, thumb={need_thumb})")
        if DRY_RUN:
            if need_full:
                print(f"  [dry-run] would cwebp (q={FULL_WEBP_QUALITY}) -> images/{wname}")
            if need_thumb:
                print(
                    f"  [dry-run] would magick resize + cwebp (q={THUMB_WEBP_QUALITY}) -> thumbnails/{wname}"
                )
            continue
        if need_full:
            upload_bytes(
                s3,
                bucket,
                f"images/{wname}",
                full_image_to_webp_bytes(local, FULL_WEBP_QUALITY),
                dry_run=False,
            )
        if need_thumb:
            upload_bytes(
                s3,
                bucket,
                f"thumbnails/{wname}",
                thumbnail_to_webp_bytes(
                    local,
                    max_px=THUMB_MAX_PX,
                    quality=THUMB_WEBP_QUALITY,
                ),
                dry_run=False,
            )

    print("Done.")


if __name__ == "__main__":
    main()

#!/usr/bin/env python3
"""
Upload full-size WebP originals and thumbnails to DigitalOcean Spaces.

- Originals from ``epstein_photos.config.ALL_IMAGES_WITH_FACES_WEBP_DIR`` → ``images/<basename>``
- Thumbnails from ``ALL_IMAGES_WITH_FACES_THUMBNAILS_DIR`` → ``thumbnails/<basename>``

Edit those config paths if your corpus lives elsewhere (they default to under ``EPSTEIN_ROOT``).

DigitalOcean Spaces is S3-compatible. Configure via environment variables:

    EPSTEIN_SPACES_REGION   (e.g. "sfo3")
    EPSTEIN_SPACES_ENDPOINT (e.g. "https://sfo3.digitaloceanspaces.com")
    EPSTEIN_SPACES_BUCKET   (bucket name)
    EPSTEIN_SPACES_KEY      (access key)
    EPSTEIN_SPACES_SECRET   (secret key)
"""

from pathlib import Path

from epstein_photos.config import (
    ALL_IMAGES_WITH_FACES_THUMBNAILS_DIR,
    ALL_IMAGES_WITH_FACES_WEBP_DIR,
)
from epstein_photos.spaces import get_spaces_client, upload_file


def main() -> None:
    s3, bucket = get_spaces_client()

    all_images = ALL_IMAGES_WITH_FACES_WEBP_DIR
    thumbs = ALL_IMAGES_WITH_FACES_THUMBNAILS_DIR

    if not all_images.is_dir():
        raise NotADirectoryError(f"WebP originals dir not found: {all_images}")
    if not thumbs.is_dir():
        raise NotADirectoryError(f"Thumbnails dir not found: {thumbs}")

    for filename in sorted(all_images.glob("*.webp")):
        orig_key = f"images/{filename.name}"
        print(filename, orig_key)
        upload_file(s3, bucket, filename, orig_key)

    for thumbnail in sorted(thumbs.glob("*.webp")):
        thumb_key = f"thumbnails/{thumbnail.name}"
        print(thumbnail, thumb_key)
        upload_file(s3, bucket, thumbnail, thumb_key)


if __name__ == "__main__":
    main()

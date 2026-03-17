#!/usr/bin/env python3
"""
Find exact duplicate image files in a directory by content hash.
Populates the `images` table:
- One row per file
- `duplicate_of` is NULL for canonical files
- `duplicate_of` is the canonical filename for byte-identical duplicates

No files are deleted or moved.
"""

import hashlib
from pathlib import Path

from config import IMAGE_DIR
from faces_db import init_db, upsert_image_duplicate_of

# ---------------- CONFIG ----------------

DRY_RUN = False
ALL_EXTENSIONS = False

IMAGE_EXTENSIONS = frozenset(
    {".jpg", ".jpeg", ".png", ".gif", ".webp", ".bmp", ".tiff", ".tif", ".ppm"}
)

# --------------------------------------


def file_hash(path: Path, block_size: int = 65536) -> str:
    """Compute SHA256 hash of file contents."""
    h = hashlib.sha256()
    with open(path, "rb") as f:
        while chunk := f.read(block_size):
            h.update(chunk)
    return h.hexdigest()


def main() -> None:
    directory = IMAGE_DIR.resolve()
    if not directory.is_dir():
        raise NotADirectoryError(f"Not a directory: {directory}")

    # Deterministic canonical choice: first file by sorted path order.
    first_for_hash: dict[str, str] = {}
    duplicates = 0

    paths = sorted(directory.iterdir()) if directory.is_dir() else []
    paths = [p for p in paths if p.is_file()]
    if not ALL_EXTENSIONS:
        paths = [p for p in paths if p.suffix.lower() in IMAGE_EXTENSIONS]

    conn = init_db()
    try:
        for path in paths:
            digest = file_hash(path)
            canonical = first_for_hash.get(digest)
            if canonical is None:
                canonical = path.name
                first_for_hash[digest] = canonical
                duplicate_of = None
            else:
                duplicate_of = canonical
                duplicates += 1

            if DRY_RUN:
                if duplicate_of is None:
                    continue
                print(f"Duplicate: {path.name} -> {duplicate_of}")
                continue

            upsert_image_duplicate_of(conn, path.name, duplicate_of, commit=False)
        if not DRY_RUN:
            conn.commit()
    finally:
        conn.close()

    if DRY_RUN:
        print(f"Would mark {duplicates} duplicate(s) in DB.")
    else:
        print(f"Marked {duplicates} duplicate(s) in DB.")


if __name__ == "__main__":
    main()

#!/usr/bin/env python3
"""
Find exact duplicate image files in a directory by content hash.
Keeps the first occurrence of each unique file and removes later duplicates.
"""

import argparse
import hashlib
import sys
from pathlib import Path

# Common image extensions
IMAGE_EXTENSIONS = frozenset(
    {".jpg", ".jpeg", ".png", ".gif", ".webp", ".bmp", ".tiff", ".tif", ".ppm"}
)


def file_hash(path: Path, block_size: int = 65536) -> str:
    """Compute SHA256 hash of file contents."""
    h = hashlib.sha256()
    with open(path, "rb") as f:
        while chunk := f.read(block_size):
            h.update(chunk)
    return h.hexdigest()


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Remove exact duplicate image files (by content hash). Keeps first, deletes rest."
    )
    parser.add_argument(
        "directory",
        type=Path,
        help="Directory to scan for duplicate images",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Only report duplicates, do not delete",
    )
    parser.add_argument(
        "--all",
        action="store_true",
        help="Consider all files, not just common image extensions",
    )
    args = parser.parse_args()

    directory = args.directory.resolve()
    if not directory.is_dir():
        print(f"Not a directory: {directory}", file=sys.stderr)
        sys.exit(1)

    seen_hashes: set[str] = set()
    removed = 0

    # Sort paths for deterministic "first occurrence" (keeps lexicographically first)
    paths = sorted(directory.iterdir()) if directory.is_dir() else []
    paths = [p for p in paths if p.is_file()]
    if not args.all:
        paths = [p for p in paths if p.suffix.lower() in IMAGE_EXTENSIONS]

    for path in paths:
        try:
            digest = file_hash(path)
        except OSError as e:
            print(f"Skip (read error): {path} -- {e}", file=sys.stderr)
            continue

        if digest in seen_hashes:
            if args.dry_run:
                print(f"Would remove duplicate: {path.name}")
            else:
                try:
                    path.unlink()
                    print(f"Removed: {path.name}")
                    removed += 1
                except OSError as e:
                    print(f"Failed to remove {path}: {e}", file=sys.stderr)
        else:
            seen_hashes.add(digest)

    if args.dry_run:
        print(f"Would remove {removed} duplicate(s).")
    else:
        print(f"Removed {removed} duplicate(s).")


if __name__ == "__main__":
    main()

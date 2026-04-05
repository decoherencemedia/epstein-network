#!/usr/bin/env python3
"""
Create WebP thumbnails (max edge in px) mirroring subdirectory structure.

Uses ImageMagick resize + ``cwebp`` (same stack as ``01__sync_images_with_spaces.py``).

Requires: ``cwebp``, ``magick`` or ``convert``.

Edit paths and parameters below.
"""

import shutil
from pathlib import Path

from epstein_photos.config import EPSTEIN_ROOT
from epstein_photos.webp import magick_bin, thumbnail_to_webp_bytes

# ----- run configuration -----
IN_DIR = EPSTEIN_ROOT / "all_images_with_faces"
OUT_DIR = EPSTEIN_ROOT / "all_images_with_faces_thumbnails"
MAX_PX = 500
QUALITY = 82
# If True, delete OUT_DIR before writing (old shell script always did this).
CLEAN_OUTPUT = False

THUMB_SUFFIXES = (
    ".jpg",
    ".jpeg",
    ".png",
    ".tif",
    ".tiff",
    ".bmp",
    ".gif",
    ".webp",
    ".heic",
    ".avif",
)


def main() -> None:
    in_dir = IN_DIR.resolve()
    out_dir = OUT_DIR.resolve()

    if str(out_dir) in ("", ".", "..", "/"):
        raise ValueError(f"refused to use OUT_DIR={out_dir!r}")

    if not shutil.which("cwebp"):
        raise RuntimeError("cwebp not found in PATH (install webp package)")

    if magick_bin() is None:
        raise RuntimeError("ImageMagick (magick or convert) not found in PATH")

    if not in_dir.is_dir():
        raise NotADirectoryError(f"Not a directory: {in_dir}")

    if CLEAN_OUTPUT and out_dir.exists():
        shutil.rmtree(out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)

    n_written = n_skip = 0
    for src in sorted(in_dir.rglob("*")):
        if not src.is_file():
            continue
        if src.suffix.lower() not in THUMB_SUFFIXES:
            continue
        rel = src.relative_to(in_dir)
        rel_dir = rel.parent
        stem = src.stem
        dst = out_dir / rel_dir / f"{stem}.webp"

        if dst.is_file() and dst.stat().st_mtime_ns >= src.stat().st_mtime_ns:
            n_skip += 1
            continue

        dst.parent.mkdir(parents=True, exist_ok=True)
        data = thumbnail_to_webp_bytes(src, max_px=MAX_PX, quality=QUALITY)
        dst.write_bytes(data)
        n_written += 1

    print(f"Done. Thumbnails written to: {out_dir}/")
    print(f"Written: {n_written}, skipped (up to date): {n_skip}")


if __name__ == "__main__":
    main()

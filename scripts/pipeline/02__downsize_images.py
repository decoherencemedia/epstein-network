#!/usr/bin/env python3
"""
Find files larger than 5 MiB under INPUT_DIR, copy originals to INPUT_DIR/images_downsized/,
then replace files in place: PNG → JPEG (ImageMagick), then jpegoptim to target size.

Requires: ``file``, ``jpegoptim``, and ``magick`` or ``convert`` (for PNG sources).

Rekognition limit is 5 MiB; jpegoptim uses ``--size=5000`` KiB (same idea as the old shell script).

Edit ``INPUT_DIR`` / ``BACKUP_SUBDIR`` below if needed.
"""

import shutil
import subprocess
from pathlib import Path

from epstein_photos.config import IMAGE_DIR
from epstein_photos.webp import magick_bin

# ----- run configuration -----
INPUT_DIR = IMAGE_DIR
BACKUP_SUBDIR = "images_downsized"

MAX_BYTES = 5 * 1024 * 1024
TARGET_KB = 5000


def _file_mime(path: Path) -> str:
    r = subprocess.run(
        ["file", "-b", "--mime-type", "--", str(path)],
        capture_output=True,
        text=True,
    )
    if r.returncode != 0:
        return ""
    return (r.stdout or "").strip()


def _png_to_jpeg_inplace(path: Path, magick: list[str]) -> None:
    tmp = path.with_suffix(path.suffix + ".tmp.jpg")
    r = subprocess.run(
        magick
        + [str(path), "-strip", "-interlace", "Plane", "-quality", "92", str(tmp)],
        capture_output=True,
        text=True,
    )
    if r.returncode != 0:
        raise RuntimeError(f"ImageMagick PNG→JPEG failed for {path}: {r.stderr or r.stdout}")
    tmp.replace(path)


def main() -> None:
    input_dir = INPUT_DIR.resolve()
    if not input_dir.is_dir():
        raise NotADirectoryError(f"Not a directory: {input_dir}")

    if not shutil.which("jpegoptim"):
        raise RuntimeError("Missing dependency: jpegoptim (install it first)")
    if not shutil.which("file"):
        raise RuntimeError("Missing dependency: file (install it first)")

    out_dir = (input_dir / BACKUP_SUBDIR).resolve()
    out_dir.mkdir(parents=True, exist_ok=True)

    magick = magick_bin()

    count_found = count_copied = count_optimized = count_png = 0

    print(f"Scanning: {input_dir}")
    print(f"Backups:  {out_dir}")
    print(f"Max bytes: {MAX_BYTES} (jpegoptim --size={TARGET_KB} --force)")
    print()

    for src in sorted(input_dir.rglob("*")):
        if not src.is_file():
            continue
        try:
            rel = src.relative_to(input_dir)
        except ValueError:
            continue
        if rel.parts and rel.parts[0] == BACKUP_SUBDIR:
            continue
        if src.stat().st_size <= MAX_BYTES:
            continue

        count_found += 1
        backup = out_dir / rel
        backup.parent.mkdir(parents=True, exist_ok=True)
        shutil.copy2(src, backup)
        count_copied += 1

        mime = _file_mime(src)
        if mime == "image/png":
            if magick is None:
                raise RuntimeError(f"PNG over size limit but ImageMagick not in PATH: {rel}")
            print(f"PNG → JPEG (in place): {rel}")
            _png_to_jpeg_inplace(src, magick)
            count_png += 1

        mime2 = _file_mime(src)
        if mime2 != "image/jpeg":
            raise RuntimeError(
                f"After processing, expected image/jpeg for {rel}, got {mime2!r} (cannot jpegoptim)"
            )

        print(f"Optimizing (in place): {rel}")
        r = subprocess.run(
            [
                "jpegoptim",
                "--quiet",
                "--force",
                "--strip-all",
                "--all-progressive",
                f"--size={TARGET_KB}",
                "--",
                str(src),
            ],
            capture_output=True,
            text=True,
        )
        if r.returncode != 0:
            raise RuntimeError(
                f"jpegoptim failed for {rel}: {r.stderr or r.stdout or 'no output'}"
            )
        count_optimized += 1

        sz = src.stat().st_size
        if sz > MAX_BYTES:
            mib = sz / (1024 * 1024)
            print(
                f"Warning: still > 5 MiB after jpegoptim: {rel} ({mib:.2f} MiB)",
                flush=True,
            )

    print()
    print("Done.")
    print(f"Found >5MiB: {count_found}")
    print(f"Copied:      {count_copied}")
    print(f"PNG→JPEG:    {count_png}")
    print(f"Optimized:   {count_optimized}")


if __name__ == "__main__":
    main()

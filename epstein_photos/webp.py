"""WebP encoding and ImageMagick resize helpers shared by pipeline and ingest scripts."""


import shutil
import subprocess
import tempfile
from pathlib import Path


def magick_bin() -> list[str] | None:
    if shutil.which("magick"):
        return ["magick"]
    if shutil.which("convert"):
        return ["convert"]
    return None


def run_cwebp(src: Path, dst: Path, quality: int) -> None:
    r = subprocess.run(
        ["cwebp", "-quiet", "-q", str(quality), str(src), "-o", str(dst)],
        capture_output=True,
        text=True,
    )
    if r.returncode != 0:
        raise RuntimeError(f"cwebp failed: {r.stderr or r.stdout}")


def full_image_to_webp_bytes(src: Path, quality: int) -> bytes:
    with tempfile.NamedTemporaryFile(suffix=".webp", delete=False) as f:
        out = Path(f.name)
    try:
        run_cwebp(src, out, quality)
        return out.read_bytes()
    finally:
        out.unlink(missing_ok=True)


def magick_resize_max_edge_to_png(
    src: Path,
    dst_png: Path,
    max_edge_px: int,
    *,
    strip: bool = False,
) -> None:
    magick = magick_bin()
    if magick is None:
        raise RuntimeError(
            "ImageMagick (magick or convert) required in PATH for resize "
            "(install imagemagick; same as pipeline 11 / 13)."
        )
    dst_png.parent.mkdir(parents=True, exist_ok=True)
    cmd = magick + [str(src), "-resize", f"{max_edge_px}x{max_edge_px}>"]
    if strip:
        cmd.append("-strip")
    cmd.append(str(dst_png))
    r = subprocess.run(cmd, capture_output=True, text=True)
    if r.returncode != 0:
        raise RuntimeError(f"ImageMagick resize failed: {r.stderr or r.stdout}")


def thumbnail_to_webp_bytes(
    local_src: Path,
    *,
    max_px: int,
    quality: int,
) -> bytes:
    """Resize (max edge) with ImageMagick, encode as WebP via ``cwebp``."""
    with tempfile.TemporaryDirectory() as td:
        td = Path(td)
        resized = td / "resized.png"
        magick_resize_max_edge_to_png(local_src, resized, max_px, strip=True)
        out_webp = td / "thumb.webp"
        run_cwebp(resized, out_webp, quality)
        return out_webp.read_bytes()

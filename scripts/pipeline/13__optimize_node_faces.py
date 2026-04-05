#!/usr/bin/env python3
"""
Convert images under ``images/node_faces_*`` input dirs to flat ``.webp`` outputs.

Duplicate basenames anywhere under an input tree are rejected (same rule as graph upload).

Requires: ``cwebp``; ImageMagick (``magick`` or ``convert``) strongly recommended for resize.

Environment overrides (same as the old shell script)::

    NODE_INPUT_DIR, NODE_OUTPUT_DIR — node tiles (unoptimized → optimized)
    PEOPLE_INPUT_DIR, PEOPLE_OUTPUT_DIR — people crops (selected → selected_optimized)
"""

import os
import shutil
import tempfile
from collections import Counter
from pathlib import Path

from epstein_photos.config import NETWORK_ROOT
from epstein_photos.webp import magick_bin, run_cwebp, magick_resize_max_edge_to_png

CWEBP_QUALITY = 90
MAX_EDGE_PX = 1024

ALLOWED_OUTPUTS = frozenset(
    {
        NETWORK_ROOT / "images" / "node_faces_optimized",
        NETWORK_ROOT / "images" / "node_faces_selected_optimized",
    }
)


def _optimize_dir(label: str, input_dir: Path, output_dir: Path) -> None:
    if not input_dir.is_dir():
        raise FileNotFoundError(f"{label} INPUT_DIR does not exist: {input_dir}")

    if output_dir.resolve() not in {p.resolve() for p in ALLOWED_OUTPUTS}:
        raise ValueError(f"Refusing unexpected OUTPUT_DIR={output_dir!r}")

    exts = {".jpg", ".jpeg", ".png", ".webp"}
    found = sorted(
        p
        for p in input_dir.rglob("*")
        if p.is_file() and p.suffix.lower() in exts
    )
    if not found:
        print(f"No images under {input_dir} for {label}.")
        return

    name_counts = Counter(p.name for p in found)
    dup = sorted(name for name, n in name_counts.items() if n > 1)
    if dup:
        raise RuntimeError(
            f"Duplicate basename(s) under {input_dir} for {label}:\n" + "\n".join(dup)
        )

    output_dir.mkdir(parents=True, exist_ok=True)
    if magick_bin() is None:
        raise RuntimeError(
            "ImageMagick (magick or convert) required in PATH for resize before cwebp "
            f"({label})"
        )
    n_conv = n_skip = 0

    with tempfile.TemporaryDirectory() as tmp_root:
        tmp_root_p = Path(tmp_root)
        for i, src in enumerate(found):
            stem = src.stem
            out_webp = output_dir / f"{stem}.webp"
            if out_webp.is_file():
                n_skip += 1
                continue
            tmp_proc = tmp_root_p / f"{label}.{i}.proc.png"
            magick_resize_max_edge_to_png(src, tmp_proc, MAX_EDGE_PX, strip=False)
            run_cwebp(tmp_proc, out_webp, CWEBP_QUALITY)
            n_conv += 1

    print(
        f"Converted {n_conv} image(s) to WebP in {output_dir} for {label} "
        f"(skipped {n_skip} existing; max edge {MAX_EDGE_PX}px when ImageMagick available)"
    )


def main() -> None:
    if not shutil.which("cwebp"):
        raise RuntimeError("cwebp not found in PATH.")

    node_in = Path(os.environ.get("NODE_INPUT_DIR", NETWORK_ROOT / "images" / "node_faces_unoptimized"))
    node_out = Path(os.environ.get("NODE_OUTPUT_DIR", NETWORK_ROOT / "images" / "node_faces_optimized"))
    people_in = Path(os.environ.get("PEOPLE_INPUT_DIR", NETWORK_ROOT / "images" / "node_faces_selected"))
    people_out = Path(
        os.environ.get("PEOPLE_OUTPUT_DIR", NETWORK_ROOT / "images" / "node_faces_selected_optimized")
    )

    _optimize_dir("node_faces", node_in, node_out)
    _optimize_dir("people_faces", people_in, people_out)


if __name__ == "__main__":
    main()

#!/usr/bin/env python3
"""
Load one face by face_id from faces.db, draw its Rekognition bbox (normalized 0..1) on the image,
save a preview next to this script, and open the default image viewer.

Configure only the variables in the block below. Run from anywhere; paths use config / this file.
"""

# ---------------------------------------------------------------------------
# face_id from the `faces` table (AWS Rekognition UUID string).
FACE_ID = "a3e136ad-66c8-4451-afbf-c17f76c8b9d1"
# Preview image written next to this script.
OUTPUT_FILENAME = "_face_bbox_preview.png"
# ---------------------------------------------------------------------------

import sqlite3
from pathlib import Path

from PIL import Image, ImageDraw

from epstein_photos.config import DB_PATH, IMAGE_DIR

SCRIPT_DIR = Path(__file__).resolve().parent
DB_FILE = Path(DB_PATH)
OUT_FILE = SCRIPT_DIR / OUTPUT_FILENAME

IMAGE_EXTENSIONS = (".jpg", ".jpeg", ".webp", ".png")


def resolve_image_file(image_name: str) -> Path:
    """Return path to the image on disk; try alternate extensions if missing."""
    direct = IMAGE_DIR / image_name
    if direct.is_file():
        return direct
    stem = Path(image_name).stem
    for ext in IMAGE_EXTENSIONS:
        p = IMAGE_DIR / f"{stem}{ext}"
        if p.is_file():
            return p
    return direct


def load_face_row(conn: sqlite3.Connection, face_id: str) -> sqlite3.Row | None:
    cur = conn.execute(
        """
        SELECT face_id, image_name, person_id, "left", top, width, height
        FROM faces
        WHERE face_id = ?
        """,
        (face_id,),
    )
    return cur.fetchone()


def draw_normalized_bbox(
    im: Image.Image,
    left: float,
    top: float,
    width: float,
    height: float,
    *,
    outline: str = "#00ff88",
    width_px: int = 4,
) -> Image.Image:
    """Rekognition-style box: left, top, width, height in 0..1 relative to image size."""
    out = im.convert("RGB").copy()
    w, h = out.size
    x0 = left * w
    y0 = top * h
    x1 = (left + width) * w
    y1 = (top + height) * h
    draw = ImageDraw.Draw(out)
    draw.rectangle([x0, y0, x1, y1], outline=outline, width=width_px)
    return out


def main() -> None:
    fid = FACE_ID.strip()
    if not fid or fid == "00000000-0000-0000-0000-000000000000":
        raise ValueError("Set FACE_ID at the top of view_face_bbox.py")

    if not DB_FILE.is_file():
        raise FileNotFoundError(f"Database not found: {DB_FILE}")

    conn = sqlite3.connect(DB_FILE)
    conn.row_factory = sqlite3.Row
    try:
        row = load_face_row(conn, fid)
    finally:
        conn.close()

    if row is None:
        raise LookupError(f"No face with face_id={fid!r}")

    image_name = row["image_name"]
    left = float(row["left"])
    top = float(row["top"])
    w = float(row["width"])
    h = float(row["height"])
    person_id = row["person_id"]

    path = resolve_image_file(str(image_name))
    if not path.is_file():
        raise FileNotFoundError(
            f"Image file not found for image_name={image_name!r} (tried {path}); IMAGE_DIR={IMAGE_DIR}"
        )

    im = Image.open(path)
    out = draw_normalized_bbox(im, left, top, w, h)
    out.save(OUT_FILE)
    print(
        f"face_id={fid}\n"
        f"image_name={image_name}\n"
        f"person_id={person_id}\n"
        f"bbox (norm): left={left:.6f} top={top:.6f} width={w:.6f} height={h:.6f}\n"
        f"Saved: {OUT_FILE}"
    )
    out.show()


if __name__ == "__main__":
    main()

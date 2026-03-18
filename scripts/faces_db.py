"""
Shared DB schema and helpers for the face preprocessing + Rekognition pipeline.

Used by preprocess (writes has_face), cluster (writes indexed + faces.person_id), and celebrity (writes faces.celebrity_*).
"""

import json
import math
import sqlite3
from pathlib import Path
from typing import List, Optional, Tuple

from PIL import Image

from config import DB_PATH, IMAGE_DIR


def init_db():
    """Create/ensure all tables: images, faces (person_id and celebrity_* on faces; people table removed)."""
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()

    c.execute("""
        CREATE TABLE IF NOT EXISTS images (
            image_name TEXT PRIMARY KEY,
            duplicate_of TEXT, -- if non-NULL, this file is a byte-identical duplicate of duplicate_of
            has_face INTEGER,   -- 1 if local detector saw at least one face, 0 if not, NULL if unknown
            indexed  INTEGER    -- 1 if sent to Rekognition, 0 or NULL otherwise
        )
    """)

    c.execute("""
        CREATE TABLE IF NOT EXISTS faces (
            face_id TEXT PRIMARY KEY,
            image_name TEXT,
            left REAL,
            top REAL,
            width REAL,
            height REAL,
            searched INTEGER DEFAULT 0,
            person_id TEXT,
            celebrity_name TEXT,
            celebrity_id TEXT,
            celebrity_confidence REAL,
            age_range_low INTEGER,
            age_range_high INTEGER,
            index_face_record TEXT,
            search_faces_result TEXT
        )
    """)
    _ensure_faces_person_celebrity_columns(conn)
    _ensure_faces_index_response_columns(conn)
    _ensure_images_moderation_column(conn)
    _ensure_images_duplicate_of_column(conn)
    _ensure_images_width_height_columns(conn)
    _migrate_people_to_faces_if_exists(conn)
    c.execute("DROP TABLE IF EXISTS people")
    conn.commit()
    return conn


def _ensure_faces_person_celebrity_columns(conn):
    """Add person_id and celebrity columns to faces if missing (for DBs created before)."""
    for sql in (
        "ALTER TABLE faces ADD COLUMN person_id TEXT",
        "ALTER TABLE faces ADD COLUMN celebrity_name TEXT",
        "ALTER TABLE faces ADD COLUMN celebrity_id TEXT",
        "ALTER TABLE faces ADD COLUMN celebrity_confidence REAL",
    ):
        try:
            conn.execute(sql)
        except sqlite3.OperationalError as e:
            if "duplicate" not in str(e).lower():
                raise


def _ensure_faces_index_response_columns(conn):
    """Add age range, raw IndexFaces record, and SearchFaces result to faces if missing."""
    for sql in (
        "ALTER TABLE faces ADD COLUMN age_range_low INTEGER",
        "ALTER TABLE faces ADD COLUMN age_range_high INTEGER",
        "ALTER TABLE faces ADD COLUMN index_face_record TEXT",
        "ALTER TABLE faces ADD COLUMN search_faces_result TEXT",
    ):
        try:
            conn.execute(sql)
        except sqlite3.OperationalError as e:
            if "duplicate" not in str(e).lower():
                raise


def _ensure_images_moderation_column(conn):
    """Add moderation_result (JSON) to images if missing."""
    try:
        conn.execute("ALTER TABLE images ADD COLUMN moderation_result TEXT")
    except sqlite3.OperationalError as e:
        if "duplicate" not in str(e).lower():
            raise


def _ensure_images_duplicate_of_column(conn):
    """Add duplicate_of column to images if missing."""
    try:
        conn.execute("ALTER TABLE images ADD COLUMN duplicate_of TEXT")
    except sqlite3.OperationalError as e:
        if "duplicate" not in str(e).lower():
            raise


def _ensure_images_width_height_columns(conn):
    """Add width_px and height_px to images if missing."""
    for sql in (
        "ALTER TABLE images ADD COLUMN width_px INTEGER",
        "ALTER TABLE images ADD COLUMN height_px INTEGER",
    ):
        try:
            conn.execute(sql)
        except sqlite3.OperationalError as e:
            if "duplicate" not in str(e).lower():
                raise


def _migrate_people_to_faces_if_exists(conn):
    """If people table exists, copy person_id and celebrity_* into faces then drop people."""
    c = conn.cursor()
    c.execute(
        "SELECT name FROM sqlite_master WHERE type='table' AND name='people'"
    )
    if not c.fetchone():
        return
    c.execute("""
        UPDATE faces SET
            person_id = (SELECT person_id FROM people p WHERE p.face_id = faces.face_id),
            celebrity_name = (SELECT celebrity_name FROM people p WHERE p.face_id = faces.face_id),
            celebrity_id = (SELECT celebrity_id FROM people p WHERE p.face_id = faces.face_id),
            celebrity_confidence = (SELECT celebrity_confidence FROM people p WHERE p.face_id = faces.face_id)
        WHERE face_id IN (SELECT face_id FROM people)
    """)
    conn.commit()


def get_image_status(conn, image_name):
    """Return (has_face, indexed) or (None, None) if not in DB."""
    c = conn.cursor()
    c.execute("SELECT has_face, indexed FROM images WHERE image_name = ?", (image_name,))
    row = c.fetchone()
    if row is None:
        return None, None
    return row[0], row[1]


def get_already_has_face(conn):
    """Return set of image_name that already have has_face set (one query for batch skip)."""
    c = conn.cursor()
    c.execute("SELECT image_name FROM images WHERE duplicate_of IS NULL AND has_face IS NOT NULL")
    return {row[0] for row in c.fetchall()}


def upsert_image_status(conn, image_name, has_face=None, indexed=None, *, commit=True):
    """Insert or update images row; None means leave that column unchanged. If commit=False, caller must commit (for batching)."""
    c = conn.cursor()
    c.execute("SELECT has_face, indexed FROM images WHERE image_name = ?", (image_name,))
    row = c.fetchone()
    cur_has_face, cur_indexed = (row if row is not None else (None, None))
    if has_face is None:
        has_face = cur_has_face
    if indexed is None:
        indexed = cur_indexed
    c.execute(
        """
        INSERT INTO images (image_name, duplicate_of, has_face, indexed)
        VALUES (?, NULL, ?, ?)
        ON CONFLICT(image_name) DO UPDATE SET
            has_face = COALESCE(?, images.has_face),
            indexed  = COALESCE(?, images.indexed)
        """,
        (image_name, has_face, indexed, has_face, indexed),
    )
    if commit:
        conn.commit()


def get_images_to_index(conn):
    """Return list of image_name where has_face=1 and not yet indexed (for Rekognition)."""
    c = conn.cursor()
    c.execute(
        "SELECT image_name FROM images WHERE duplicate_of IS NULL AND has_face = 1 AND (indexed IS NULL OR indexed = 0)"
    )
    return [row[0] for row in c.fetchall()]


def get_images_for_moderation(conn):
    """Return list of image_name that are indexed and not yet moderated."""
    c = conn.cursor()
    c.execute(
        """
        SELECT image_name FROM images
        WHERE duplicate_of IS NULL
          AND indexed = 1
          AND (moderation_result IS NULL OR moderation_result = '')
        """
    )
    return [row[0] for row in c.fetchall()]


def upsert_image_duplicate_of(conn, image_name: str, duplicate_of: str | None, *, commit: bool = True) -> None:
    """Insert image row if missing; set duplicate_of (NULL for canonical)."""
    conn.execute(
        """
        INSERT INTO images (image_name, duplicate_of, has_face, indexed)
        VALUES (?, ?, NULL, NULL)
        ON CONFLICT(image_name) DO UPDATE SET
            duplicate_of = excluded.duplicate_of
        """,
        (image_name, duplicate_of),
    )
    if commit:
        conn.commit()


def upsert_image_dimensions(conn, image_name: str, width_px: int, height_px: int, *, commit: bool = True) -> None:
    """Set width_px and height_px for an existing images row (e.g. after dedup)."""
    conn.execute(
        "UPDATE images SET width_px = ?, height_px = ? WHERE image_name = ?",
        (width_px, height_px, image_name),
    )
    if commit:
        conn.commit()


def upsert_image_moderation(conn, image_name, moderation_result_json, *, commit=True):
    """Store DetectModerationLabels result (JSON string) for an image."""
    conn.execute(
        "UPDATE images SET moderation_result = ? WHERE image_name = ?",
        (moderation_result_json, image_name),
    )
    if commit:
        conn.commit()


# --------------- Best-face selection (shared by 07 and 09) ---------------

def get_appearances_for_person(conn, person_id: str) -> List[Tuple]:
    """Return list of (image_name, left, top, width, height, index_face_record) for that person."""
    c = conn.cursor()
    c.execute(
        "SELECT image_name, left, top, width, height, index_face_record FROM faces WHERE person_id = ?",
        (person_id,),
    )
    return [row for row in c.fetchall()]


def _clamp(x: float, lo: float, hi: float) -> float:
    return lo if x < lo else hi if x > hi else x


def _parse_index_face_record(raw: str) -> dict:
    if not raw:
        raise ValueError("Missing index_face_record")
    return json.loads(raw)


def _index_quality_multiplier(index_record: dict) -> float:
    fd = (index_record or {}).get("FaceDetail") or {}
    quality = fd.get("Quality") or {}
    pose = fd.get("Pose") or {}
    occluded = (fd.get("FaceOccluded") or {}).get("Value")

    sharpness = float(quality.get("Sharpness") or 0.0)
    brightness = float(quality.get("Brightness") or 0.0)
    yaw = abs(float(pose.get("Yaw") or 0.0))
    pitch = abs(float(pose.get("Pitch") or 0.0))

    sharp_mult = 0.35 + _clamp(sharpness / 20.0, 0.0, 1.5)
    pose_penalty = math.exp(-((yaw + pitch) / 35.0))
    bright_penalty = 1.0 - _clamp(abs(brightness - 55.0) / 90.0, 0.0, 0.45)
    occ_penalty = 0.2 if occluded is True else 1.0

    mult = sharp_mult * pose_penalty * bright_penalty * occ_penalty
    return _clamp(mult, 0.08, 2.25)


def _image_size(path: Path) -> Tuple[int, int]:
    with Image.open(path) as im:
        return im.size


def _crop_face(path: Path, left: float, top: float, width: float, height: float) -> Image.Image:
    with Image.open(path) as im:
        W, H = im.size
        x1 = left * W
        y1 = top * H
        x2 = (left + width) * W
        y2 = (top + height) * H
        crop = im.crop((int(x1), int(y1), int(x2), int(y2)))
        return crop.convert("RGB")


def _dhash(im: Image.Image, hash_size: int = 8) -> int:
    g = im.convert("L").resize((hash_size + 1, hash_size), resample=Image.Resampling.LANCZOS)
    pixels = list(g.getdata())
    h = 0
    bit = 0
    for row in range(hash_size):
        row_start = row * (hash_size + 1)
        for col in range(hash_size):
            if pixels[row_start + col] > pixels[row_start + col + 1]:
                h |= 1 << bit
            bit += 1
    return h


def _hamming_distance(a: int, b: int) -> int:
    return (a ^ b).bit_count()


def pick_best_images(
    conn,
    person_id: str,
    n: int = 3,
    *,
    image_dir: Optional[Path] = None,
    pool_size: int = 25,
    dhash_size: int = 8,
    min_hamming: int = 10,
) -> List[Tuple[str, float, float, float, float]]:
    """
    Return up to n best (image_name, left, top, width, height) for this person.

    Selection: pool by size (bbox area × image pixels), rerank by size × quality^0.6,
    then greedily add by dHash diversity.
    """
    image_dir = image_dir or IMAGE_DIR
    appearances = get_appearances_for_person(conn, person_id)
    if not appearances:
        return []

    by_size: List[Tuple[float, Tuple]] = []
    for image_name, left, top, width, height, index_face_record in appearances:
        path = image_dir / image_name
        if not path.is_file():
            raise FileNotFoundError(f"Image file missing for person {person_id}: {path}")
        w, h = _image_size(path)
        base = (float(width) * float(height)) * (w * h)
        by_size.append(
            (base, (image_name, float(left), float(top), float(width), float(height), index_face_record))
        )
    by_size.sort(key=lambda x: x[0], reverse=True)
    pool = [t for _, t in by_size[: max(n, pool_size)]]

    scored: List[Tuple[float, Tuple[str, float, float, float, float]]] = []
    for image_name, left, top, width, height, index_face_record in pool:
        path = image_dir / image_name
        w, h = _image_size(path)
        base = (width * height) * (w * h)
        idx = _parse_index_face_record(index_face_record)
        mult = _index_quality_multiplier(idx)
        score = base * (mult ** 0.6)
        scored.append((score, (image_name, left, top, width, height)))
    scored.sort(key=lambda x: x[0], reverse=True)

    selected: List[Tuple[str, float, float, float, float]] = []
    selected_hashes: List[int] = []
    for _, app in scored:
        image_name, left, top, width, height = app
        src = image_dir / image_name
        if not src.is_file():
            continue
        crop = _crop_face(src, left, top, width, height)
        h = _dhash(crop, hash_size=dhash_size)
        if any(_hamming_distance(h, hh) < min_hamming for hh in selected_hashes):
            continue
        selected.append(app)
        selected_hashes.append(h)
        if len(selected) >= n:
            break

    if len(selected) < n:
        for _, app in scored:
            if app in selected:
                continue
            selected.append(app)
            if len(selected) >= n:
                break
    return selected[:n]

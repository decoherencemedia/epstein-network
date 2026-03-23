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

# Face bbox area in pixels (width*height*image_width*image_height) statistics from this dataset.
MEDIAN_BASE_PX = 40000

def init_db():
    """Create/ensure all tables: images, faces, people (sheet sync + celebrity_check_done)."""
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
    _migrate_legacy_face_people_to_faces_if_exists(conn)
    _ensure_people_table_and_migrate_celebrity_done(conn)
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


def _migrate_legacy_face_people_to_faces_if_exists(conn):
    """
    Legacy: an old `people` table keyed by face_id. Copy into faces, then drop.
    Skip if `people` already has the new schema (person_id PK, no face_id).
    """
    c = conn.cursor()
    c.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='people'")
    if not c.fetchone():
        return
    c.execute("PRAGMA table_info(people)")
    cols = [row[1] for row in c.fetchall()]
    if "face_id" not in cols:
        return
    c.execute("""
        UPDATE faces SET
            person_id = (SELECT person_id FROM people p WHERE p.face_id = faces.face_id),
            celebrity_name = (SELECT celebrity_name FROM people p WHERE p.face_id = faces.face_id),
            celebrity_id = (SELECT celebrity_id FROM people p WHERE p.face_id = faces.face_id),
            celebrity_confidence = (SELECT celebrity_confidence FROM people p WHERE p.face_id = faces.face_id)
        WHERE face_id IN (SELECT face_id FROM people)
    """)
    c.execute("DROP TABLE people")
    conn.commit()


def _ensure_people_table_and_migrate_celebrity_done(conn):
    """
    New `people` table: one row per Rekognition person_id.
    - celebrity_check_done: 1 after 07__recognize_celebrities has processed this person
      (replaces old person_celebrity_check_done).
    - name, include_in_network: filled by sync_people_from_google_sheets (and 09).
    """
    c = conn.cursor()
    c.execute("""
        CREATE TABLE IF NOT EXISTS people (
            person_id TEXT PRIMARY KEY,
            celebrity_check_done INTEGER NOT NULL DEFAULT 0,
            name TEXT,
            include_in_network INTEGER NOT NULL DEFAULT 0
        )
    """)
    c.execute(
        "SELECT name FROM sqlite_master WHERE type='table' AND name='person_celebrity_check_done'"
    )
    if c.fetchone():
        c.execute("SELECT person_id FROM person_celebrity_check_done")
        for (pid,) in c.fetchall():
            c.execute(
                """
                INSERT INTO people (person_id, celebrity_check_done, name, include_in_network)
                VALUES (?, 1, NULL, 0)
                ON CONFLICT(person_id) DO UPDATE SET celebrity_check_done = 1
                """,
                (pid,),
            )
        c.execute("DROP TABLE person_celebrity_check_done")
    conn.commit()


def sync_people_from_google_sheets(conn, gc) -> None:
    """
    Upsert `people` from Matches / Unknowns / Ignore sheets (see sheets_common).
    - name: from Matches (person_id -> display name)
    - include_in_network: 1 if person_id appears on Matches or Unknowns, else 0 (e.g. Ignore only)
    Does not clear celebrity_check_done.
    """
    from sheets_common import load_ignore, load_names, load_person_ids_matches_and_unknowns

    names = load_names(gc)
    include_ids = load_person_ids_matches_and_unknowns(gc)
    ignore_ids = load_ignore(gc)

    all_ids = set(names.keys()) | include_ids | ignore_ids
    c = conn.cursor()
    for pid in all_ids:
        if pid in ignore_ids:
            inc = 0
        elif pid in include_ids:
            inc = 1
        else:
            inc = 0
        nm = names.get(pid)
        c.execute(
            """
            INSERT INTO people (person_id, celebrity_check_done, name, include_in_network)
            VALUES (?, 0, ?, ?)
            ON CONFLICT(person_id) DO UPDATE SET
                name = excluded.name,
                include_in_network = excluded.include_in_network
            """,
            (pid, nm, inc),
        )
    conn.commit()


def upsert_celebrity_check_done(conn, person_id: str) -> None:
    """Mark person_id as processed by 07__recognize_celebrities (insert row if missing)."""
    c = conn.cursor()
    c.execute(
        """
        INSERT INTO people (person_id, celebrity_check_done, name, include_in_network)
        VALUES (?, 1, NULL, 0)
        ON CONFLICT(person_id) DO UPDATE SET celebrity_check_done = 1
        """,
        (person_id,),
    )


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
    eyes_open = (fd.get("EyesOpen") or {}).get("Value")
    eye_dir = fd.get("EyeDirection") or {}

    sharpness = float(quality.get("Sharpness") or 0.0)
    brightness = float(quality.get("Brightness") or 0.0)
    yaw = abs(float(pose.get("Yaw") or 0.0))
    pitch = abs(float(pose.get("Pitch") or 0.0))
    eye_yaw = abs(float(eye_dir.get("Yaw") or 0.0))
    eye_pitch = abs(float(eye_dir.get("Pitch") or 0.0))

    sharp_mult = 0.35 + _clamp(sharpness / 20.0, 0.0, 1.5)
    pose_penalty = math.exp(-((yaw + pitch) / 35.0))
    bright_penalty = 1.0 - _clamp(abs(brightness - 55.0) / 90.0, 0.0, 0.45)
    # Strong penalty for occluded faces (obscured by hair, another person, etc.).
    sunglasses = (fd.get("Sunglasses") or {}).get("Value")

    if occluded and sunglasses:
        occ_penalty = 0.55  # Sunglasses occlusion — face is visible, just eyes obscured
    elif occluded:
        occ_penalty = 0.15  # Structural occlusion — hair, hand, other person, crop
    else:
        occ_penalty = 1.0
    # Eyes closed is often a poor node thumbnail even when sharp/frontal.
    eyes_penalty = 0.22 if eyes_open is False else 1.0
    # Strong off-camera gaze also tends to be worse for thumbnails.
    gaze = eye_yaw + eye_pitch
    gaze_penalty = math.exp(-(gaze / 55.0))

    mult = sharp_mult * pose_penalty * bright_penalty * occ_penalty * eyes_penalty * gaze_penalty
    return _clamp(mult, 0.08, 2.25)


def _index_quality_components(index_record: dict) -> dict:
    """Return dict of quality multiplier components (for debugging)."""
    fd = (index_record or {}).get("FaceDetail") or {}
    quality = fd.get("Quality") or {}
    pose = fd.get("Pose") or {}
    occluded = (fd.get("FaceOccluded") or {}).get("Value")
    eyes_open = (fd.get("EyesOpen") or {}).get("Value")
    eye_dir = fd.get("EyeDirection") or {}

    sharpness = float(quality.get("Sharpness") or 0.0)
    brightness = float(quality.get("Brightness") or 0.0)
    yaw = abs(float(pose.get("Yaw") or 0.0))
    pitch = abs(float(pose.get("Pitch") or 0.0))
    eye_yaw = abs(float(eye_dir.get("Yaw") or 0.0))
    eye_pitch = abs(float(eye_dir.get("Pitch") or 0.0))

    sharp_mult = 0.35 + _clamp(sharpness / 20.0, 0.0, 1.5)
    pose_penalty = math.exp(-((yaw + pitch) / 35.0))
    bright_penalty = 1.0 - _clamp(abs(brightness - 55.0) / 90.0, 0.0, 0.45)
    occ_penalty = 0.05 if occluded is True else 1.0
    eyes_penalty = 0.22 if eyes_open is False else 1.0
    gaze = eye_yaw + eye_pitch
    gaze_penalty = math.exp(-(gaze / 55.0))
    mult = sharp_mult * pose_penalty * bright_penalty * occ_penalty * eyes_penalty * gaze_penalty
    return {
        "sharp_mult": sharp_mult,
        "pose_penalty": pose_penalty,
        "bright_penalty": bright_penalty,
        "occ_penalty": occ_penalty,
        "eyes_penalty": eyes_penalty,
        "gaze_penalty": gaze_penalty,
        "eye_yaw": eye_yaw,
        "eye_pitch": eye_pitch,
        "eyes_open": eyes_open,
        "occluded": occluded,
        "mult": _clamp(mult, 0.08, 2.25),
    }


def _edge_penalty(left: float, top: float, width: float, height: float) -> float:
    """
    Penalize faces close to the image edge (likely cut off when adding head margin).
    Returns a multiplier in (0, 1]; 1.0 when there is enough margin on all sides.
    """
    right = 1.0 - (left + width)
    bottom = 1.0 - (top + height)
    min_margin = min(left, right, top, bottom)
    if min_margin >= 0.15:
        return 1.0
    if min_margin >= 0.10:
        return 0.55
    if min_margin >= 0.05:
        return 0.2
    return 0.06


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


def _min_margin(left: float, top: float, width: float, height: float) -> float:
    right = 1.0 - (left + width)
    bottom = 1.0 - (top + height)
    return min(left, right, top, bottom)



def _size_term(base_px: float) -> float:
    """
    Saturating size score:
    - strong penalty for very small faces
    - gradual differences once faces are medium/large
    """
    if base_px <= 0:
        return 0.0
    return base_px / (base_px + MEDIAN_BASE_PX)


def pick_best_images(
    conn,
    person_id: str,
    n: int = 3,
    *,
    image_dir: Optional[Path] = None,
    pool_size: int = 25,
    dhash_size: int = 8,
    min_hamming: int = 10,
    debug_person_id: Optional[str] = None,
) -> List[Tuple[str, float, float, float, float]]:
    """
    Return up to n best (image_name, left, top, width, height) for this person.

    Selection: pool by size (bbox area × image pixels), rerank by size_term × (quality × edge_penalty)^0.6,
    then greedily add by dHash diversity. Quality includes sharpness, pose, brightness, and a strong
    penalty for FaceOccluded; edge_penalty down-ranks faces near the image border (cut off risk).
    """
    image_dir = image_dir or IMAGE_DIR
    appearances = get_appearances_for_person(conn, person_id)
    if not appearances:
        return []

    image_names = sorted({str(row[0]) for row in appearances})
    if not image_names:
        return []
    placeholders = ",".join("?" for _ in image_names)
    rows = conn.execute(
        f"SELECT image_name, width_px, height_px FROM images WHERE image_name IN ({placeholders})",
        image_names,
    ).fetchall()
    image_dims = {str(name): (w, h) for name, w, h in rows}
    missing_dims = [
        name for name in image_names
        if name not in image_dims
        or image_dims[name][0] is None
        or image_dims[name][1] is None
    ]
    if missing_dims:
        examples = ", ".join(missing_dims[:10])
        more = " ..." if len(missing_dims) > 10 else ""
        raise RuntimeError(
            "Missing width_px/height_px in images table for pick_best_images. "
            "Run scripts/01__dedup_images.py first. "
            f"Examples: {examples}{more}"
        )

    by_size: List[Tuple[float, Tuple]] = []
    for image_name, left, top, width, height, index_face_record in appearances:
        path = image_dir / image_name
        if not path.is_file():
            raise FileNotFoundError(f"Image file missing for person {person_id}: {path}")
        w, h = image_dims[str(image_name)]
        w = int(w)
        h = int(h)
        base = (float(width) * float(height)) * (w * h)
        by_size.append(
            (base, (image_name, float(left), float(top), float(width), float(height), index_face_record))
        )
    by_size.sort(key=lambda x: x[0], reverse=True)
    pool = [t for _, t in by_size[: max(n, pool_size)]]

    scored: List[Tuple[float, Tuple[str, float, float, float, float]]] = []
    debug_rows: List[dict] = []
    for image_name, left, top, width, height, index_face_record in pool:
        path = image_dir / image_name
        w, h = image_dims[str(image_name)]
        w = int(w)
        h = int(h)
        base = (width * height) * (w * h)
        idx = _parse_index_face_record(index_face_record)
        mult = _index_quality_multiplier(idx)
        edge = _edge_penalty(left, top, width, height)
        size = _size_term(base)
        score = size * ((mult * edge) ** 0.6)
        scored.append((score, (image_name, left, top, width, height)))
        if debug_person_id is not None and person_id == debug_person_id:
            comp = _index_quality_components(idx)
            debug_rows.append({
                "image_name": image_name,
                "left": left, "top": top, "width": width, "height": height,
                "base": base, "size": size, "edge": edge, "min_margin": _min_margin(left, top, width, height),
                **comp,
                "score": score,
            })
    scored.sort(key=lambda x: x[0], reverse=True)

    if debug_person_id is not None and person_id == debug_person_id and debug_rows:
        # Print by score rank (desc)
        by_score = sorted(debug_rows, key=lambda r: r["score"], reverse=True)
        print(f"\n--- pick_best_images debug: {person_id} ({len(debug_rows)} in pool) ---")
        for rank, row in enumerate(by_score, 1):
            print(f"  #{rank} score={row['score']:.4f} base={row['base']:.0f} size={row['size']:.3f} mult={row['mult']:.3f} edge={row['edge']:.3f} "
                  f"min_margin={row['min_margin']:.3f} occluded={row['occluded']}")
            print(
                f"      sharp={row['sharp_mult']:.3f} pose={row['pose_penalty']:.3f} bright={row['bright_penalty']:.3f} "
                f"occ={row['occ_penalty']:.3f} eyes={row['eyes_penalty']:.3f} gaze={row['gaze_penalty']:.3f} "
                f"eye_yaw={row['eye_yaw']:.1f} eye_pitch={row['eye_pitch']:.1f} eyes_open={row['eyes_open']}"
            )
            print(f"      bbox left={row['left']:.3f} top={row['top']:.3f} w={row['width']:.3f} h={row['height']:.3f}")
            print(f"      image_name={row['image_name']}")
        print("---\n")

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

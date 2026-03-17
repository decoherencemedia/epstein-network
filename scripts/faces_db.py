"""
Shared DB schema and helpers for the face preprocessing + Rekognition pipeline.

Used by preprocess (writes has_face), cluster (writes indexed + faces.person_id), and celebrity (writes faces.celebrity_*).
"""

import sqlite3

DB_PATH = "faces.db"


def init_db():
    """Create/ensure all tables: images, faces (person_id and celebrity_* on faces; people table removed)."""
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()

    c.execute("""
        CREATE TABLE IF NOT EXISTS images (
            image_name TEXT PRIMARY KEY,
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
            celebrity_confidence REAL
        )
    """)
    _ensure_faces_person_celebrity_columns(conn)
    _ensure_faces_index_response_columns(conn)
    _ensure_images_moderation_column(conn)
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
    """Add age range and raw IndexFaces record to faces if missing."""
    for sql in (
        "ALTER TABLE faces ADD COLUMN age_range_low INTEGER",
        "ALTER TABLE faces ADD COLUMN age_range_high INTEGER",
        "ALTER TABLE faces ADD COLUMN index_face_record TEXT",
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
    c.execute("SELECT image_name FROM images WHERE has_face IS NOT NULL")
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
        INSERT INTO images (image_name, has_face, indexed)
        VALUES (?, ?, ?)
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
        "SELECT image_name FROM images WHERE has_face = 1 AND (indexed IS NULL OR indexed = 0)"
    )
    return [row[0] for row in c.fetchall()]


def get_images_for_moderation(conn):
    """Return list of image_name that are indexed and not yet moderated."""
    c = conn.cursor()
    c.execute(
        """
        SELECT image_name FROM images
        WHERE indexed = 1 AND (moderation_result IS NULL OR moderation_result = '')
        """
    )
    return [row[0] for row in c.fetchall()]


def upsert_image_moderation(conn, image_name, moderation_result_json, *, commit=True):
    """Store DetectModerationLabels result (JSON string) for an image."""
    conn.execute(
        "UPDATE images SET moderation_result = ? WHERE image_name = ?",
        (moderation_result_json, image_name),
    )
    if commit:
        conn.commit()

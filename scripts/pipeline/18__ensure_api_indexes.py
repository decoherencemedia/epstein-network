"""
Create/refresh SQLite indexes used by API /photos queries.

Run:
  cd scripts
  python3 18__ensure_api_indexes.py
"""
import sqlite3

from epstein_photos.config import DB_PATH


def ensure_api_indexes(conn: sqlite3.Connection) -> None:
    conn.execute(
        "CREATE INDEX IF NOT EXISTS idx_faces_person_image ON faces(person_id, image_name)"
    )
    conn.execute(
        "CREATE INDEX IF NOT EXISTS idx_faces_image_person ON faces(image_name, person_id)"
    )
    conn.execute(
        "CREATE INDEX IF NOT EXISTS idx_images_duplicate_of ON images(duplicate_of)"
    )
    conn.execute(
        "CREATE INDEX IF NOT EXISTS idx_images_dup_explicit ON images(duplicate_of, is_explicit)"
    )
    conn.execute(
        "CREATE INDEX IF NOT EXISTS idx_images_contains_victim ON images(contains_victim)"
    )
    conn.commit()


def main() -> None:
    conn = sqlite3.connect(DB_PATH)
    try:
        ensure_api_indexes(conn)
    finally:
        conn.close()
    print("API indexes ensured.")


if __name__ == "__main__":
    main()

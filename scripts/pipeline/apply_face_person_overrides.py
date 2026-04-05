#!/usr/bin/env python3
"""
Apply ground-truth face -> person_id overrides from JSON.

Values are lists of face_id (UUID) strings. Each key is interpreted as:

- The literal "none": set faces.person_id to NULL for those face_ids.
- Any key that contains the substring "person_": treat the full key as an existing
  person_id; those faces are assigned to it. The person must already exist in `people`
  (otherwise the script raises before writing).
- Any other non-empty key: treat as a display name. If `people.name` already equals
  that key (exactly one row), those faces are assigned to that row’s `person_id` and
  no new row is inserted (safe to re-run). Otherwise the script allocates the smallest
  unused person_<digits>, inserts (name=key, include_in_network=1, celebrity_check_done=1),
  and assigns the faces. If more than one row has the same `name`, the script raises.

Avoid display names that contain the substring "person_" (they would be treated as an
existing person_id).

Raises before any DB write if:
  - any key is empty
  - any face_id appears under more than one key
  - any face_id is missing from faces
  - any "person_..." key does not match an existing people.person_id

Edit ``OVERRIDE_JSON`` below if the file lives elsewhere.

Example overrides.json:
  {
    "none": ["uuid-to-unassign"],
    "Jane Doe": ["uuid-new-person-face"],
    "person_2": ["uuid-existing-cluster-face"]
  }
"""


import json
import re
import sqlite3
from pathlib import Path
from typing import Any

from epstein_photos.config import DB_PATH, SCRIPTS_DIR

# ---------------- CONFIG ----------------

OVERRIDE_JSON = SCRIPTS_DIR / "face_person_overrides.json"

# ---------------------------------------

KEY_NONE = "none"
PERSON_ID_NUM_RE = re.compile(r"^person_(\d+)$")


def _db_path() -> Path:
    return Path(DB_PATH)


def _is_existing_person_key(key: str) -> bool:
    return key != KEY_NONE and "person_" in key


def _validate_key(key: str) -> None:
    if not isinstance(key, str):
        raise TypeError(f"Override keys must be strings, got {type(key).__name__}")
    if key == KEY_NONE:
        return
    if not key.strip():
        raise ValueError("Override key must not be empty or whitespace-only")


def _load_and_validate_payload(raw: Any) -> dict[str, list[str]]:
    if not isinstance(raw, dict):
        raise TypeError("Override file must be a JSON object at the top level")

    for key in raw:
        _validate_key(key)

    face_first_key: dict[str, str] = {}
    normalized: dict[str, list[str]] = {}

    for key, value in raw.items():
        if not isinstance(value, list):
            raise TypeError(f"Value for key {key!r} must be a JSON array, got {type(value).__name__}")
        ids: list[str] = []
        for i, item in enumerate(value):
            if not isinstance(item, str) or not item.strip():
                raise ValueError(f"Key {key!r}: item at index {i} must be a non-empty string face_id")
            fid = item.strip()
            if fid in face_first_key:
                raise ValueError(
                    f"Duplicate face_id {fid!r}: listed under {face_first_key[fid]!r} and {key!r}"
                )
            face_first_key[fid] = key
            ids.append(fid)
        normalized[key] = ids

    return normalized


def assert_all_faces_exist(conn: sqlite3.Connection, all_face_ids: list[str]) -> None:
    if not all_face_ids:
        return
    placeholders = ",".join("?" * len(all_face_ids))
    found = {
        str(row[0])
        for row in conn.execute(
            f"SELECT face_id FROM faces WHERE face_id IN ({placeholders})",
            all_face_ids,
        ).fetchall()
    }
    missing = [fid for fid in all_face_ids if fid not in found]
    if missing:
        raise ValueError(f"{len(missing)} face_id(s) not found in faces table, e.g. {missing[:5]!r}")


def assert_existing_person_keys_in_db(conn: sqlite3.Connection, overrides: dict[str, list[str]]) -> None:
    for key in overrides:
        if not _is_existing_person_key(key):
            continue
        row = conn.execute("SELECT 1 FROM people WHERE person_id = ?", (key,)).fetchone()
        if row is None:
            raise ValueError(
                f"Override key {key!r} is treated as an existing person_id, but no row exists in people"
            )


def next_free_person_id(conn: sqlite3.Connection) -> str:
    used: set[int] = set()
    for (pid,) in conn.execute("SELECT person_id FROM people").fetchall():
        m = PERSON_ID_NUM_RE.match(str(pid))
        if m:
            used.add(int(m.group(1)))
    n = 1
    while n in used:
        n += 1
    return f"person_{n}"


def person_id_for_display_name(conn: sqlite3.Connection, display_name: str) -> str | None:
    """
    Return person_id if exactly one `people` row has name == display_name; None if none.
    Raises if multiple rows share that name.
    """
    rows = conn.execute(
        "SELECT person_id FROM people WHERE name = ?",
        (display_name,),
    ).fetchall()
    if len(rows) > 1:
        pids = [str(r[0]) for r in rows]
        raise ValueError(
            f"Ambiguous name {display_name!r}: {len(rows)} people rows share this name: {pids}"
        )
    if len(rows) == 1:
        return str(rows[0][0])
    return None


def apply_overrides(
    conn: sqlite3.Connection, overrides: dict[str, list[str]]
) -> tuple[list[tuple[str, str]], list[tuple[str, str]]]:
    """
    Insert new people when needed and update faces. Caller must BEGIN/COMMIT or ROLLBACK.
    Returns (created, reused): each is a list of (person_id, display_name) for name-key
    groups that inserted a row vs reused an existing row by name.
    """
    c = conn.cursor()
    created: list[tuple[str, str]] = []
    reused: list[tuple[str, str]] = []

    for person_key, face_ids in overrides.items():
        if not face_ids:
            continue
        ph = ",".join("?" * len(face_ids))
        if person_key == KEY_NONE:
            c.execute(
                f"UPDATE faces SET person_id = NULL WHERE face_id IN ({ph})",
                face_ids,
            )
        elif _is_existing_person_key(person_key):
            c.execute(
                f"UPDATE faces SET person_id = ? WHERE face_id IN ({ph})",
                [person_key, *face_ids],
            )
        else:
            existing_pid = person_id_for_display_name(conn, person_key)
            if existing_pid is not None:
                reused.append((existing_pid, person_key))
                target_pid = existing_pid
            else:
                target_pid = next_free_person_id(conn)
                c.execute(
                    """
                INSERT INTO people (person_id, celebrity_check_done, name, include_in_network, is_victim)
                VALUES (?, 1, ?, 1, 0)
                    """,
                    (target_pid, person_key),
                )
                created.append((target_pid, person_key))
            c.execute(
                f"UPDATE faces SET person_id = ? WHERE face_id IN ({ph})",
                [target_pid, *face_ids],
            )

    return created, reused


def main() -> None:
    json_path = OVERRIDE_JSON.resolve()
    if not json_path.is_file():
        raise FileNotFoundError(f"Override JSON not found: {json_path}")

    raw = json.loads(json_path.read_text(encoding="utf-8"))
    overrides = _load_and_validate_payload(raw)

    db = _db_path()
    if not db.is_file():
        raise FileNotFoundError(f"Database not found: {db}")

    all_face_ids = [fid for ids in overrides.values() for fid in ids]

    conn = sqlite3.connect(str(db))
    try:
        assert_all_faces_exist(conn, all_face_ids)
        assert_existing_person_keys_in_db(conn, overrides)
        conn.execute("BEGIN IMMEDIATE")
        created, reused = apply_overrides(conn, overrides)
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()

    total = sum(len(v) for v in overrides.values())
    print(f"Applied overrides from {json_path}: {len(overrides)} key(s), {total} face row(s) updated.")
    for pid, name in created:
        print(f"  New person {pid!r} (name {name!r})")
    for pid, name in reused:
        print(f"  Reused existing person {pid!r} (name {name!r})")


if __name__ == "__main__":
    main()

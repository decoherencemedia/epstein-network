#!/usr/bin/env python3
"""
Auto-populate `people.best_face_id` where it is still empty.

- Targets non-victim rows with NULL/empty `best_face_id`.
- Uses `faces_db.pick_best_images()` for the score ordering; then takes the first
  candidate whose source image passes the disallowed filter (when enabled).

When `FILTER_DISALLOWED_IMAGES` is True, many people will have **no** candidate
after filtering (only explicit/minor photos). That is expected: those rows are
left unchanged, not treated as errors.

Run after `09__sheets_rekognition.py` has synced people into SQLite.
"""

from __future__ import annotations

import json
import sqlite3

from config import DB_PATH, IMAGE_DIR
from faces_db import pick_best_images


# ----------------------------- Configuration -----------------------------

# If True, print intended updates but do not write to SQLite.
DRY_RUN = False

# If >0, only process up to N people (for quick iteration).
LIMIT = 0

# pick_best_images settings
PICK_N = 25
PICK_POOL_SIZE = 80

# Skip candidate faces that come from images flagged as explicit or containing minor faces.
# Set to False if you want "always pick the top face" regardless of flags.
FILTER_DISALLOWED_IMAGES = True


def _is_explicit_moderation(moderation_result: str | None) -> bool:
    if not moderation_result:
        return False
    data = json.loads(moderation_result)
    labels = data.get("ModerationLabels") or []
    for lb in labels:
        name = lb.get("Name")
        parent = lb.get("ParentName")
        if name == "Explicit" or parent == "Explicit":
            return True
    return False


def main() -> None:
    conn = sqlite3.connect(DB_PATH)

    disallowed_images: set[str] = set()
    if FILTER_DISALLOWED_IMAGES:
        for (image_name,) in conn.execute(
            "SELECT DISTINCT image_name FROM faces WHERE age_range_low < 18 OR age_range_high < 18"
        ).fetchall():
            disallowed_images.add(str(image_name))
        for image_name, moderation_result in conn.execute(
            "SELECT image_name, moderation_result FROM images WHERE moderation_result IS NOT NULL"
        ).fetchall():
            if _is_explicit_moderation(moderation_result):
                disallowed_images.add(str(image_name))
        for (image_name,) in conn.execute(
            "SELECT image_name FROM images WHERE COALESCE(is_explicit, 0) = 1"
        ).fetchall():
            disallowed_images.add(str(image_name))

    rows = conn.execute(
        """
        SELECT person_id, name
        FROM people
        WHERE is_victim = 0
          AND (best_face_id IS NULL OR best_face_id = '')
        ORDER BY person_id
        """
    ).fetchall()

    if LIMIT and LIMIT > 0:
        rows = rows[:LIMIT]

    updated = 0
    skipped_no_selectable_face = 0
    for person_id, name in rows:
        pid = str(person_id)
        best_list = pick_best_images(
            conn, pid, n=PICK_N, pool_size=PICK_POOL_SIZE, image_dir=IMAGE_DIR
        )
        chosen_face_id: str | None = None
        for image_name, _l, _t, _w, _h, face_id in best_list:
            if not face_id:
                continue
            if FILTER_DISALLOWED_IMAGES and str(image_name) in disallowed_images:
                continue
            chosen_face_id = str(face_id)
            break

        if not chosen_face_id:
            skipped_no_selectable_face += 1
            continue

        if DRY_RUN:
            print(f"DRY RUN: {pid} ({name}) -> {chosen_face_id}")
            updated += 1
            continue

        conn.execute(
            "UPDATE people SET best_face_id = ? WHERE person_id = ?",
            (chosen_face_id, pid),
        )
        updated += 1

    if not DRY_RUN:
        conn.commit()

    print(
        f"Updated best_face_id for {updated} person_id(s); "
        f"left unchanged (no selectable face after filters): {skipped_no_selectable_face}."
    )

    conn.close()


if __name__ == "__main__":
    main()

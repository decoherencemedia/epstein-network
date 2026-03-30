"""
Export one cropped face image per graph node to `images/node_faces/`.

This script recomputes the \"best\" face per node using the current pick_best_images scoring,
so you can iterate on the algorithm quickly without rerunning 10__create_graph.py.

Each crop is saved as ``<SanitizedLabel>_<face_id>.jpg`` where ``face_id`` is the Rekognition
UUID from ``faces.face_id`` (stable across renames). Top-K exports use distinct face_ids per file.

Safety: skips any image that is explicit (images.moderation_result) or contains a minor face
(any face in that image with age_range_low/high < 18).
"""
import os
import sqlite3
import json
from pathlib import Path
from shutil import rmtree
from collections import defaultdict
from typing import Any

from PIL import Image

from config import DB_PATH, IMAGE_DIR
from faces_db import parse_node_face_export_stem, pick_best_images
from sheets_common import (
    get_sheet_client,
    load_ignore,
    load_names,
    load_person_ids_matches_and_unknowns,
)


SCRIPT_DIR = Path(__file__).resolve().parent
IMAGES_DIR = SCRIPT_DIR.parent / "images"
TOP_FACES_DIR = IMAGES_DIR / "node_faces_top5"
SELECTED_FACES_DIR = IMAGES_DIR / "node_faces_selected"
TOP_K = 5
IMAGE_EXTENSIONS = {".jpg", ".jpeg", ".png", ".webp"}

# Face bbox is typically eyes-to-chin; expand more above for full head. Fraction of bbox dimension.
MARGIN_TOP = 0.65
MARGIN_SIDES = 0.45
MARGIN_BOTTOM = 0.35

DEBUG_PERSON_ID = None


def _is_explicit_moderation(moderation_result: str | None) -> bool:
    if not moderation_result:
        return False
    data: Any = json.loads(moderation_result)
    labels = data.get("ModerationLabels") or []
    for lb in labels:
        name = lb.get("Name")
        parent = lb.get("ParentName")
        if name == "Explicit" or parent == "Explicit":
            return True
    return False


def _extract_roll(index_face_record: str | None) -> float | None:
    if not index_face_record:
        return None
    return json.loads(index_face_record)["FaceDetail"]["Pose"]["Roll"]


def _rotation_for_roll(roll: float | None) -> int:
    if roll is None:
        return 0
    if abs(roll) < 45:
        return 0
    return -90 if roll > 0 else 90


def _rotate_bbox(
    left: float, top: float, width: float, height: float, rotation: int
) -> tuple[float, float, float, float]:
    if rotation == 0:
        return (left, top, width, height)
    if rotation == -90:  # CCW
        return (top, 1 - left - width, height, width)
    if rotation == 90:   # CW
        return (1 - top - height, left, height, width)
    raise ValueError(f"Unsupported rotation: {rotation}")


def _best_roll_for_bbox(
    con: sqlite3.Connection,
    person_id: str,
    image_name: str,
    left: float,
    top: float,
    width: float,
    height: float,
) -> float | None:
    """
    Find the closest face record for this (person_id, image_name) and return its roll.
    We match by minimizing L1 distance in bbox params.
    """
    rows = con.execute(
        "SELECT left, top, width, height, index_face_record "
        "FROM faces WHERE person_id = ? AND image_name = ?",
        (person_id, image_name),
    ).fetchall()
    if not rows:
        return None
    best = None
    best_d = 1e9
    for l, t, w, h, rec in rows:
        d = abs(float(l) - left) + abs(float(t) - top) + abs(float(w) - width) + abs(float(h) - height)
        if d < best_d:
            best_d = d
            best = rec
    return _extract_roll(best)

def _sanitize_label_for_filename(label: str) -> str:
    """Safe filename from node label."""
    s = "".join(c if c.isalnum() or c in "._- " else "" for c in label)
    return s.strip().replace(" ", "_").replace("/", "_") or "node"


def _square_crop_region(
    left: float, top: float, width: float, height: float,
    img_w: int, img_h: int,
) -> tuple[int, int, int, int]:
    """
    Return (x0, y0, x1, y1) pixel coords for a square crop around the face.
    Uses asymmetric margin (more above) so the crop includes the full head.
    Always returns a square region; if the ideal square would exceed image bounds,
    shifts (or shrinks) so the crop stays inside the image.
    """
    cx = left + width / 2
    cy = top + height / 2
    expand_w = width * (1 + 2 * MARGIN_SIDES)
    expand_h_top = height * MARGIN_TOP
    expand_h_bottom = height * MARGIN_BOTTOM
    expand_h = height + expand_h_top + expand_h_bottom
    # Center of expanded box (shifted up so more room above).
    cy_exp = top + (height / 2) + (expand_h_bottom - expand_h_top) / 2
    side_norm = max(expand_w, expand_h)
    x0_n = cx - side_norm / 2
    y0_n = cy_exp - side_norm / 2
    x1_n = x0_n + side_norm
    y1_n = y0_n + side_norm
    # Convert to pixels; ensure integer square.
    side_px = min(
        int(round(side_norm * img_w)),
        int(round(side_norm * img_h)),
        img_w,
        img_h,
    )
    if side_px <= 0:
        return (0, 0, 1, 1)
    cx_px = int(round(cx * img_w))
    cy_exp_px = int(round(cy_exp * img_h))
    x0 = cx_px - side_px // 2
    y0 = cy_exp_px - side_px // 2
    x1 = x0 + side_px
    y1 = y0 + side_px
    # Shift square into bounds (keep side_px so crop stays square).
    if x0 < 0:
        x0 = 0
        x1 = side_px
    if x1 > img_w:
        x1 = img_w
        x0 = img_w - side_px
    if y0 < 0:
        y0 = 0
        y1 = side_px
    if y1 > img_h:
        y1 = img_h
        y0 = img_h - side_px
    return (x0, y0, x1, y1)


def _label_from_export_filename(
    path: Path,
    sanitized_to_label: dict[str, str],
) -> str | None:
    """
    Parse legacy ``Label__NNN`` or ``Label_<face_id>`` stems; map sanitized base to display label.
    Returns None if filename does not match expected pattern or label unknown.
    """
    parsed = parse_node_face_export_stem(path.stem)
    if parsed is None:
        return None
    base_stem, _sort = parsed
    return sanitized_to_label.get(base_stem)


def _face_id_from_export_stem(stem: str) -> str | None:
    """
    Best-effort: stems are either legacy ``Label__NNN`` (no face_id) or ``Label_<uuid>``.
    For the uuid form, return the suffix after the last underscore if it looks like a UUID.
    """
    if "__" in stem:
        return None
    # uuid is always after the last "_" in our export naming.
    suffix = stem.rsplit("_", 1)[-1].strip()
    if len(suffix) == 36 and suffix.count("-") == 4:
        return suffix
    return None


def _export_crop(
    *,
    con: sqlite3.Connection,
    person_id: str,
    safe_label: str,
    img_name: str,
    left: float,
    top: float,
    width: float,
    height: float,
    face_id: str,
    roll: float | None,
    out_dir: Path,
) -> bool:
    """
    Export one crop using the exact same rotation + square crop convention as top-K.
    Returns True if written.
    """
    src_path = IMAGE_DIR / img_name
    if not src_path.is_file():
        return False
    rotation = _rotation_for_roll(roll)
    bbox = _rotate_bbox(left, top, width, height, rotation)
    transpose = None
    if rotation == -90:
        transpose = Image.Transpose.ROTATE_90
    elif rotation == 90:
        transpose = Image.Transpose.ROTATE_270
    with Image.open(src_path) as img:
        img.load()
        if transpose is not None:
            img = img.transpose(transpose)
        w, h = img.size
        x0, y0, x1, y1 = _square_crop_region(bbox[0], bbox[1], bbox[2], bbox[3], w, h)
        if x1 <= x0 or y1 <= y0:
            return False
        crop = img.crop((x0, y0, x1, y1))
        out = f"{safe_label}_{face_id}.jpg"
        crop.save(out_dir / out, "JPEG", quality=92)
        return True


if __name__ == "__main__":
    con = sqlite3.connect(DB_PATH)

    # Compute global disallowed images.
    disallowed_images = set(
        r[0]
        for r in con.execute(
            "SELECT DISTINCT image_name FROM faces WHERE age_range_low < 18 OR age_range_high < 18"
        ).fetchall()
    )
    for image_name, moderation_result in con.execute(
        "SELECT image_name, moderation_result FROM images WHERE moderation_result IS NOT NULL"
    ).fetchall():
        if _is_explicit_moderation(moderation_result):
            disallowed_images.add(image_name)

    # Graph / top-K: Matches + Unknowns minus Ignore. Selected crops: anyone with people.best_face_id.
    gc = get_sheet_client()
    names = load_names(gc)
    ignore = load_ignore(gc)
    include_person_ids = load_person_ids_matches_and_unknowns(gc) - ignore
    people_names = {str(r[0]): r[1] for r in con.execute("SELECT person_id, name FROM people").fetchall()}
    best_face_ids: dict[str, str] = {}
    for pid, fid in con.execute(
        "SELECT person_id, best_face_id FROM people WHERE best_face_id IS NOT NULL AND best_face_id != ''"
    ).fetchall():
        best_face_ids[str(pid)] = str(fid)
    union_person_ids = include_person_ids | set(best_face_ids.keys())
    if not union_person_ids:
        raise RuntimeError(
            "Nothing to export: no person_ids on Matches/Unknowns (after ignore) and no people.best_face_id set."
        )

    TOP_FACES_DIR.mkdir(parents=True, exist_ok=True)
    SELECTED_FACES_DIR.mkdir(parents=True, exist_ok=True)

    def _display_label(person_id: str) -> str:
        sheet = (names.get(person_id) or "").strip()
        if sheet:
            return sheet
        dbn = people_names.get(person_id)
        if dbn is not None and str(dbn).strip():
            return str(dbn).strip()
        return person_id

    # Build stable mapping between display labels and sanitized filename labels.
    label_by_person_id = {pid: _display_label(pid) for pid in union_person_ids}
    sanitized_to_label: dict[str, str] = {}
    for label in label_by_person_id.values():
        s = _sanitize_label_for_filename(label)
        prev = sanitized_to_label.get(s)
        if prev is not None and prev != label:
            raise RuntimeError(
                "Sanitized label collision while mapping existing exports: "
                f"{prev!r} and {label!r} both map to {s!r}"
            )
        sanitized_to_label[s] = label

    # Determine which labels already have exported top-K images.
    existing_labels: set[str] = set()
    existing_selected_face_ids_by_label: dict[str, set[str]] = defaultdict(set)
    for p in TOP_FACES_DIR.iterdir():
        if not p.is_file() or p.suffix.lower() not in IMAGE_EXTENSIONS:
            continue
        lbl = _label_from_export_filename(p, sanitized_to_label)
        if lbl is not None:
            existing_labels.add(lbl)
    for p in SELECTED_FACES_DIR.iterdir():
        if not p.is_file() or p.suffix.lower() not in IMAGE_EXTENSIONS:
            continue
        lbl = _label_from_export_filename(p, sanitized_to_label)
        if lbl is None:
            continue
        fid = _face_id_from_export_stem(p.stem)
        if fid:
            existing_selected_face_ids_by_label[lbl].add(fid)

    top_count = 0
    exported_people_count = 0
    skipped_existing = 0
    selected_count = 0
    failed_exports: list[tuple[str, str, str]] = []
    for person_id in sorted(union_person_ids):
        label = label_by_person_id[person_id]
        safe_label = _sanitize_label_for_filename(label)

        # Export DB-selected face_id (synced earlier by 09) even if top-K already exists for this person/label.
        selected_face_id = (best_face_ids.get(person_id) or "").strip()
        if selected_face_id and selected_face_id not in existing_selected_face_ids_by_label.get(label, set()):
            row = con.execute(
                """
                SELECT image_name, left, top, width, height, index_face_record
                FROM faces
                WHERE person_id = ? AND face_id = ?
                LIMIT 1
                """,
                (person_id, selected_face_id),
            ).fetchone()
            if row is None:
                raise ValueError(
                    "people.best_face_id does not exist in faces table for person_id: "
                    f"{person_id} -> {selected_face_id}"
                )
            img_name_s, left_s, top_s, width_s, height_s, index_face_record_s = row
            roll_s = _extract_roll(index_face_record_s)
            if _export_crop(
                con=con,
                person_id=person_id,
                safe_label=safe_label,
                img_name=str(img_name_s),
                left=float(left_s),
                top=float(top_s),
                width=float(width_s),
                height=float(height_s),
                face_id=selected_face_id,
                roll=roll_s,
                out_dir=SELECTED_FACES_DIR,
            ):
                existing_selected_face_ids_by_label[label].add(selected_face_id)
                selected_count += 1

        # Top-K only for people on Matches/Unknowns (graph nodes); not for off-sheet people with only best_face_id.
        if person_id not in include_person_ids:
            continue

        # If we already exported the top-K set for this label, skip recomputing them.
        if label in existing_labels:
            skipped_existing += 1
            continue

        if DEBUG_PERSON_ID and person_id == DEBUG_PERSON_ID:
            pick_best_images(
                con,
                person_id,
                n=10,
                pool_size=60,
                debug_person_id=person_id,
                image_dir=IMAGE_DIR,
            )

        best_list = pick_best_images(con, person_id, n=25, pool_size=80, image_dir=IMAGE_DIR)
        chosen = None
        top_candidates: list[tuple[str, float, float, float, float, str]] = []
        for image_name, left, top, width, height, face_id in best_list:
            if image_name in disallowed_images:
                continue
            if not face_id:
                continue
            cand = (image_name, float(left), float(top), float(width), float(height), str(face_id))
            if chosen is None:
                chosen = cand
            if len(top_candidates) < TOP_K:
                top_candidates.append(cand)
            if len(top_candidates) >= TOP_K and chosen is not None:
                break
        if not chosen:
            failed_exports.append((person_id, label, "no allowed candidate image after disallowed filters"))
            continue

        # Also export top-K similarly cropped faces per person.
        exported_for_person = 0
        for img_name_k, left_k, top_k, width_k, height_k, face_id_k in top_candidates:
            # For top-K, compute roll via nearest bbox match to keep behavior stable.
            roll_k = _best_roll_for_bbox(con, person_id, img_name_k, left_k, top_k, width_k, height_k)
            if _export_crop(
                con=con,
                person_id=person_id,
                safe_label=safe_label,
                img_name=img_name_k,
                left=left_k,
                top=top_k,
                width=width_k,
                height=height_k,
                face_id=face_id_k,
                roll=roll_k,
                out_dir=TOP_FACES_DIR,
            ):
                exported_for_person += 1
                top_count += 1
        if exported_for_person == 0:
            failed_exports.append((person_id, label, "no top-K crops could be exported"))
        else:
            exported_people_count += 1

    if failed_exports:
        print("FAILED NODE FACE EXPORTS:")
        for pid, lbl, reason in failed_exports:
            print(f"  - {pid} ({lbl}): {reason}")
        print(
            f"WARNING: {len(failed_exports)} person_id(s) had no exportable node faces. "
            "See list above."
        )

    print(f"Skipped {skipped_existing} person_id(s) with existing top-K exports in {TOP_FACES_DIR}.")
    print(
        f"Wrote {top_count} top-{TOP_K} node face crops to {TOP_FACES_DIR} "
        f"({exported_people_count} people) and {selected_count} best_face_id crop(s) to {SELECTED_FACES_DIR}."
    )

"""
Export one cropped face image per graph node to node_faces/.

This script recomputes the \"best\" face per node using the current pick_best_images scoring,
so you can iterate on the algorithm quickly without rerunning 10__create_graph.py.

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
from faces_db import pick_best_images
from sheets_common import (
    get_sheet_client,
    load_ignore,
    load_names,
    load_person_ids_matches_and_unknowns,
)


SCRIPT_DIR = Path(__file__).resolve().parent
NODE_FACES_DIR = SCRIPT_DIR.parent / "node_faces"
TOP_FACES_DIR = SCRIPT_DIR.parent / "node_faces_top5"
TOP_K = 5

# Face bbox is typically eyes-to-chin; expand more above for full head. Fraction of bbox dimension.
MARGIN_TOP = 0.65
MARGIN_SIDES = 0.45
MARGIN_BOTTOM = 0.35

DEBUG_PERSON_ID = "person_1820"


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

    # Node set and display labels from Sheets (Matches + Unknowns minus Ignore).
    gc = get_sheet_client()
    names = load_names(gc)
    ignore = load_ignore(gc)
    include_person_ids = load_person_ids_matches_and_unknowns(gc) - ignore
    if not include_person_ids:
        raise RuntimeError("No person_ids loaded from Matches/Unknowns sheets (after ignore list).")

    if NODE_FACES_DIR.exists():
        rmtree(NODE_FACES_DIR)
    NODE_FACES_DIR.mkdir(parents=True)
    if TOP_FACES_DIR.exists():
        rmtree(TOP_FACES_DIR)
    TOP_FACES_DIR.mkdir(parents=True)

    count = 0
    top_count = 0
    for person_id in sorted(include_person_ids):
        label = names.get(person_id) or person_id

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
        top_candidates: list[tuple[str, float, float, float, float]] = []
        for image_name, left, top, width, height in best_list:
            if image_name in disallowed_images:
                continue
            cand = (image_name, float(left), float(top), float(width), float(height))
            if chosen is None:
                chosen = cand
            if len(top_candidates) < TOP_K:
                top_candidates.append(cand)
            if len(top_candidates) >= TOP_K and chosen is not None:
                break
        if not chosen:
            continue

        image_name, left, top, width, height = chosen
        roll = _best_roll_for_bbox(con, person_id, image_name, left, top, width, height)
        rotation = _rotation_for_roll(roll)
        bbox = _rotate_bbox(left, top, width, height, rotation)

        src_path = IMAGE_DIR / image_name
        if not src_path.is_file():
            continue
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
                continue
            crop = img.crop((x0, y0, x1, y1))
            out_name = _sanitize_label_for_filename(label) + ".jpg"
            crop.save(NODE_FACES_DIR / out_name, "JPEG", quality=92)
            count += 1

        # Also export top-K similarly cropped faces per person.
        safe_label = _sanitize_label_for_filename(label)
        for idx, (img_name_k, left_k, top_k, width_k, height_k) in enumerate(top_candidates):
            roll_k = _best_roll_for_bbox(con, person_id, img_name_k, left_k, top_k, width_k, height_k)
            rotation_k = _rotation_for_roll(roll_k)
            bbox_k = _rotate_bbox(left_k, top_k, width_k, height_k, rotation_k)
            src_path_k = IMAGE_DIR / img_name_k
            if not src_path_k.is_file():
                continue
            transpose_k = None
            if rotation_k == -90:
                transpose_k = Image.Transpose.ROTATE_90
            elif rotation_k == 90:
                transpose_k = Image.Transpose.ROTATE_270
            with Image.open(src_path_k) as img_k:
                img_k.load()
                if transpose_k is not None:
                    img_k = img_k.transpose(transpose_k)
                w_k, h_k = img_k.size
                x0_k, y0_k, x1_k, y1_k = _square_crop_region(
                    bbox_k[0], bbox_k[1], bbox_k[2], bbox_k[3], w_k, h_k
                )
                if x1_k <= x0_k or y1_k <= y0_k:
                    continue
                crop_k = img_k.crop((x0_k, y0_k, x1_k, y1_k))
                out_k = f"{safe_label}__{idx:03d}.jpg"
                crop_k.save(TOP_FACES_DIR / out_k, "JPEG", quality=92)
                top_count += 1

    print(f"Wrote {count} node face crops to {NODE_FACES_DIR}.")
    print(f"Wrote {top_count} top-{TOP_K} node face crops to {TOP_FACES_DIR}.")

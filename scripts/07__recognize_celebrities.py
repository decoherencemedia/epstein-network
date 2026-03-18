#!/usr/bin/env python3
"""
Compare indexed people to AWS Rekognition's celebrity database.

Processes by person: for each person_id, picks the 3 best appearances (largest,
highest quality), runs RecognizeCelebrities on each until one reaches ≥99%
confidence (early exit) or all 3 are tried. If all are ≤95%, no match. If any
are in 95.1–98.9%, the highest confidence celebrity is used for that person.
Writes celebrity_name / celebrity_id / celebrity_confidence to all faces for
that person_id.

Run after clustering. Up to 3 RecognizeCelebrities calls per person (Group 2 pricing).
"""

import json
import math
import time
from pathlib import Path
from typing import List, Optional, Tuple

import boto3
from PIL import Image

from config import IMAGE_DIR, REKOGNITION_REGION
from faces_db import init_db

# ---------------- CONFIG ----------------

API_DELAY_SECONDS = 0.2

MIN_IOU = 0.3
MIN_CELEBRITY_CONFIDENCE = 95.0   # below this = no match
CELEBRITY_CONFIDENCE_HIGH = 99.0  # ≥ this = satisfactory, stop trying more faces
NUM_BEST_FACES = 3

# Best-face selection improvements (uses IndexFaces metadata + perceptual diversity)
BEST_FACE_RERANK_POOL_SIZE = 25
DHASH_SIZE = 8  # 8 => 64-bit hash
MIN_DHASH_HAMMING_DISTANCE = 10

REKOGNITION_MAX_BYTES = 5 * 1024 * 1024

DRY_RUN = False
PROCESS_ALL_PEOPLE = False

# --------------------------------------

rekognition = boto3.client("rekognition", region_name=REKOGNITION_REGION)


def load_image_bytes(path: Path) -> bytes:
    """Load image bytes for Rekognition (expects file <= 5 MiB)."""
    with open(path, "rb") as f:
        raw = f.read()
    if len(raw) > REKOGNITION_MAX_BYTES:
        raise ValueError(
            f"File exceeds 5 MiB Rekognition limit: {len(raw)} bytes"
        )
    return raw


def image_size(path: Path) -> Tuple[int, int]:
    """Return (width, height) in pixels."""
    with Image.open(path) as im:
        return im.size


def bbox_iou(a: dict, b: dict) -> float:
    """Intersection-over-union of two Rekognition-style boxes (Left, Top, Width, Height, 0-1)."""
    def box_area(x):
        return x["Width"] * x["Height"]
    l1, t1, w1, h1 = a["Left"], a["Top"], a["Width"], a["Height"]
    l2, t2, w2, h2 = b["Left"], b["Top"], b["Width"], b["Height"]
    xi1 = max(l1, l2)
    yi1 = max(t1, t2)
    xi2 = min(l1 + w1, l2 + w2)
    yi2 = min(t1 + h1, t2 + h2)
    inter_w = max(0, xi2 - xi1)
    inter_h = max(0, yi2 - yi1)
    inter = inter_w * inter_h
    union = box_area(a) + box_area(b) - inter
    return inter / union if union > 0 else 0.0


def ensure_tables(conn):
    c = conn.cursor()
    c.execute("""
        CREATE TABLE IF NOT EXISTS person_celebrity_check_done (
            person_id TEXT PRIMARY KEY
        )
    """)
    conn.commit()


def get_person_ids(conn, skip_already_done=True):
    """Return list of person_id in decreasing frequency (most appearances first).
    If skip_already_done, omit those in person_celebrity_check_done.
    """
    c = conn.cursor()
    if skip_already_done:
        c.execute("""
            SELECT f.person_id
            FROM faces f
            LEFT JOIN person_celebrity_check_done d ON f.person_id = d.person_id
            WHERE f.person_id IS NOT NULL AND d.person_id IS NULL
            GROUP BY f.person_id
            ORDER BY COUNT(*) DESC
        """)
    else:
        c.execute("""
            SELECT person_id FROM faces
            WHERE person_id IS NOT NULL
            GROUP BY person_id
            ORDER BY COUNT(*) DESC
        """)
    return [row[0] for row in c.fetchall()]


def get_appearances_for_person(conn, person_id):
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


def _crop_face(path: Path, left: float, top: float, width: float, height: float) -> Image.Image:
    with Image.open(path) as im:
        W, H = im.size
        x1 = left * W
        y1 = top * H
        x2 = (left + width) * W
        y2 = (top + height) * H
        crop = im.crop((int(x1), int(y1), int(x2), int(y2)))
        return crop.convert("RGB")


def _dhash(im: Image.Image, hash_size: int = DHASH_SIZE) -> int:
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
    conn, person_id: str, n: int = NUM_BEST_FACES
) -> List[Tuple[str, float, float, float, float]]:
    """
    Return up to n best (image_name, left, top, width, height) for this person.

    Selection:
    - Pool: take the largest faces by size (bbox area × image pixels).
    - Rerank: size × (IndexFaces-based quality multiplier ** 0.6).
    - Diversity: greedily skip near-duplicate face crops by dHash distance.
    """
    appearances = get_appearances_for_person(conn, person_id)
    if not appearances:
        return []

    by_size: List[Tuple[float, Tuple[str, float, float, float, float, str]]] = []
    for image_name, left, top, width, height, index_face_record in appearances:
        path = IMAGE_DIR / image_name
        if not path.is_file():
            raise FileNotFoundError(f"Image file missing for person {person_id}: {path}")
        w, h = image_size(path)
        base = (float(width) * float(height)) * (w * h)
        by_size.append(
            (base, (image_name, float(left), float(top), float(width), float(height), index_face_record))
        )
    by_size.sort(key=lambda x: x[0], reverse=True)
    pool = [t for _, t in by_size[: max(n, BEST_FACE_RERANK_POOL_SIZE)]]

    scored: List[Tuple[float, Tuple[str, float, float, float, float]]] = []
    for image_name, left, top, width, height, index_face_record in pool:
        path = IMAGE_DIR / image_name
        w, h = image_size(path)
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
        src = IMAGE_DIR / image_name
        if not src.is_file():
            continue
        crop = _crop_face(src, left, top, width, height)
        h = _dhash(crop)
        if any(_hamming_distance(h, hh) < MIN_DHASH_HAMMING_DISTANCE for hh in selected_hashes):
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


def _match_celebrity_in_response(
    response: dict, our_bbox: dict
) -> Optional[Tuple[dict, float]]:
    """
    Find the celebrity in RecognizeCelebrities response that best matches our_bbox (by IoU).
    Return (celeb_dict, confidence) or None if no CelebrityFaces or no IoU >= MIN_IOU.
    """
    celebrity_faces = response.get("CelebrityFaces", [])
    if not celebrity_faces:
        return None
    best_celeb = None
    best_iou = 0.0
    for celeb in celebrity_faces:
        iou = bbox_iou(our_bbox, celeb["Face"]["BoundingBox"])
        if iou >= MIN_IOU and iou > best_iou:
            best_iou = iou
            best_celeb = celeb
    if best_celeb is None:
        return None
    confidence = best_celeb.get("MatchConfidence") or 0.0
    return (best_celeb, confidence)


def process_person(person_id: str, conn) -> Optional[str]:
    """
    Get up to 3 best appearances for this person. Run RecognizeCelebrities on each:
    - If any returns ≥99% confidence: use that celebrity and stop.
    - If all return ≤95%: no match.
    - If any in 95.1–98.9%: keep the highest confidence; after trying all 3, use it.
    Update faces.celebrity_* for this person_id. Return celebrity name if matched, else None.
    """
    best_appearances = pick_best_images(conn, person_id, n=NUM_BEST_FACES)
    if not best_appearances:
        c = conn.cursor()
        c.execute(
            "UPDATE faces SET celebrity_name = NULL, celebrity_id = NULL, celebrity_confidence = NULL WHERE person_id = ?",
            (person_id,),
        )
        c.execute(
            "INSERT OR IGNORE INTO person_celebrity_check_done (person_id) VALUES (?)",
            (person_id,),
        )
        conn.commit()
        return None

    best_celeb: Optional[dict] = None
    best_confidence = 0.0

    for image_name, left, top, width, height in best_appearances:
        path = IMAGE_DIR / image_name
        if not path.is_file():
            raise FileNotFoundError(f"Image file missing: {path}")
        image_bytes = load_image_bytes(path)
        response = rekognition.recognize_celebrities(Image={"Bytes": image_bytes})
        time.sleep(API_DELAY_SECONDS)

        our_bbox = {"Left": left, "Top": top, "Width": width, "Height": height}
        matched = _match_celebrity_in_response(response, our_bbox)
        if matched is None:
            continue
        celeb, confidence = matched

        if confidence >= CELEBRITY_CONFIDENCE_HIGH:
            best_celeb = celeb
            best_confidence = confidence
            break
        if confidence > MIN_CELEBRITY_CONFIDENCE and confidence > best_confidence:
            best_celeb = celeb
            best_confidence = confidence

    c = conn.cursor()
    if best_celeb is not None and best_confidence >= MIN_CELEBRITY_CONFIDENCE:
        c.execute(
            """
            UPDATE faces
            SET celebrity_name = ?, celebrity_id = ?, celebrity_confidence = ?
            WHERE person_id = ?
            """,
            (
                best_celeb["Name"],
                best_celeb.get("Id"),
                best_celeb.get("MatchConfidence"),
                person_id,
            ),
        )
        result_name = best_celeb["Name"]
    else:
        c.execute(
            "UPDATE faces SET celebrity_name = NULL, celebrity_id = NULL, celebrity_confidence = NULL WHERE person_id = ?",
            (person_id,),
        )
        result_name = None
    c.execute(
        "INSERT OR IGNORE INTO person_celebrity_check_done (person_id) VALUES (?)",
        (person_id,),
    )
    conn.commit()
    return result_name


def main():
    conn = init_db()
    ensure_tables(conn)
    if not IMAGE_DIR.is_dir():
        raise RuntimeError(f"IMAGE_DIR not a directory: {IMAGE_DIR}")

    person_ids = get_person_ids(conn, skip_already_done=not PROCESS_ALL_PEOPLE)
    print(f"{len(person_ids)} person(s) to check")

    if DRY_RUN:
        print("Dry run: exiting.")
        conn.close()
        return

    total_matched = 0
    for i, person_id in enumerate(person_ids, start=1):
        celebrity_name = process_person(person_id, conn)
        if celebrity_name:
            total_matched += 1
            print(f"{i}/{len(person_ids)} {person_id} -> celebrity match: {celebrity_name}")
        time.sleep(API_DELAY_SECONDS)

    print(f"Done. People matched to a celebrity: {total_matched}")
    conn.close()


if __name__ == "__main__":
    main()

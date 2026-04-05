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

from epstein_photos.utils import rekognition_bbox_iou
from epstein_photos.config import IMAGE_DIR, REKOGNITION_REGION
from epstein_photos.faces_db import init_db, pick_best_images, upsert_celebrity_check_done

# ---------------- CONFIG ----------------

API_DELAY_SECONDS = 0.2

MIN_IOU = 0.3
MIN_CELEBRITY_CONFIDENCE = 95.0   # below this = no match
CELEBRITY_CONFIDENCE_HIGH = 99.0  # ≥ this = satisfactory, stop trying more faces
NUM_BEST_FACES = 3

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


def get_person_ids(conn, skip_already_done=True):
    """Return list of person_id in decreasing frequency (most appearances first).
    If skip_already_done, omit those with people.celebrity_check_done = 1.
    """
    c = conn.cursor()
    if skip_already_done:
        c.execute("""
            SELECT f.person_id
            FROM faces f
            LEFT JOIN people p ON f.person_id = p.person_id
            WHERE f.person_id IS NOT NULL
              AND (p.person_id IS NULL OR p.celebrity_check_done = 0)
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
        iou = rekognition_bbox_iou(our_bbox, celeb["Face"]["BoundingBox"])
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
        upsert_celebrity_check_done(conn, person_id)
        conn.commit()
        return None

    best_celeb: Optional[dict] = None
    best_confidence = 0.0

    for image_name, left, top, width, height, _face_id in best_appearances:
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
    upsert_celebrity_check_done(conn, person_id)
    conn.commit()
    return result_name


def main():
    conn = init_db()
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

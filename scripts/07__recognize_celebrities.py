#!/usr/bin/env python3
"""
Compare indexed people to AWS Rekognition's celebrity database.

Processes by person: for each person_id, picks the image where their face is
largest (bbox area × image resolution), calls RecognizeCelebrities once,
then writes celebrity_name / celebrity_id / celebrity_confidence to all
faces rows for that person_id.

Run after clustering. One RecognizeCelebrities call per person (Group 2 pricing).
"""

import time
from pathlib import Path
from typing import Tuple

import boto3
from PIL import Image

from config import IMAGE_DIR
from faces_db import init_db

# ---------------- CONFIG ----------------

REGION = "us-east-1"
API_DELAY_SECONDS = 0.2

MIN_IOU = 0.3
MIN_CELEBRITY_CONFIDENCE = 95.0
REKOGNITION_MAX_BYTES = 5 * 1024 * 1024

DRY_RUN = False
PROCESS_ALL_PEOPLE = False

# --------------------------------------

rekognition = boto3.client("rekognition", region_name=REGION)


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
    """Return list of (image_name, left, top, width, height) for that person."""
    c = conn.cursor()
    c.execute(
        "SELECT image_name, left, top, width, height FROM faces WHERE person_id = ?",
        (person_id,),
    )
    return [row for row in c.fetchall()]


def pick_best_image(conn, person_id):
    """
    Return (image_name, left, top, width, height) for the appearance where
    this person's face is largest (bbox area × image pixel count).
    Returns None if no valid image found.
    """
    appearances = get_appearances_for_person(conn, person_id)
    if not appearances:
        return None
    best = None
    best_score = -1.0
    for image_name, left, top, width, height in appearances:
        path = IMAGE_DIR / image_name
        if not path.is_file():
            raise FileNotFoundError(f"Image file missing for person {person_id}: {path}")
        w, h = image_size(path)
        # Normalized bbox area × pixel count = approximate face size in pixels
        score = (width * height) * (w * h)
        if score > best_score:
            best_score = score
            best = (image_name, left, top, width, height)
    return best


def process_person(person_id, conn):
    """
    Pick best image for this person, call RecognizeCelebrities, match by IoU,
    update faces.celebrity_* for this person_id. Return celebrity name if matched, else None.
    """
    best = pick_best_image(conn, person_id)
    if best is None:
        return False
    image_name, left, top, width, height = best
    path = IMAGE_DIR / image_name
    if not path.is_file():
        raise FileNotFoundError(f"Image file missing: {path}")
    image_bytes = load_image_bytes(path)
    response = rekognition.recognize_celebrities(Image={"Bytes": image_bytes})

    celebrity_faces = response.get("CelebrityFaces", [])
    if not celebrity_faces:
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
        return False

    our_bbox = {"Left": left, "Top": top, "Width": width, "Height": height}
    best_iou = 0.0
    best_celeb = None
    for celeb in celebrity_faces:
        iou = bbox_iou(our_bbox, celeb["Face"]["BoundingBox"])
        if iou > best_iou and iou >= MIN_IOU:
            best_iou = iou
            best_celeb = celeb

    c = conn.cursor()
    confidence = (best_celeb or {}).get("MatchConfidence") or 0.0
    if best_celeb is not None and confidence >= MIN_CELEBRITY_CONFIDENCE:
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
            print(f"[{i}/{len(person_ids)}] {person_id} -> celebrity match: {celebrity_name}")
        time.sleep(API_DELAY_SECONDS)

    print(f"Done. People matched to a celebrity: {total_matched}")
    conn.close()


if __name__ == "__main__":
    main()

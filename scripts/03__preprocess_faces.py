#!/usr/bin/env python3
"""
Preprocess images with a local face detector to decide which files contain faces.

This script:
- Scans IMAGE_DIR for image files
- Runs a local InsightFace detector (buffalo_l / SCRFD) on each image
- Populates the `images` table in faces.db with:
    image_name TEXT PRIMARY KEY,
    has_face   INTEGER  -- 1 if local detector saw at least one face, 0 if not

It does NOT call AWS Rekognition. Run 01__cluster_faces.py afterwards to index with Rekognition
and perform clustering using the precomputed `has_face` flags.
"""

import os
import time

import cv2  # pip install opencv-python
from insightface.app import FaceAnalysis  # pip install insightface onnxruntime-gpu

from faces_db import get_already_has_face, init_db, upsert_image_status

# ---------------- CONFIG ----------------

IMAGE_DIR = "../../../all_images"

DETECTOR_CTX_ID = 0  # 0 = first CUDA GPU; set to -1 for CPU
DETECTOR_INPUT_SIZE = (640, 640)
MIN_FACE_SCORE = 0.5
MIN_IMAGE_DIM = 10  # skip images with width or height smaller than this (avoids RetinaFace/OpenCV resize errors)

API_DELAY_SECONDS = 0.0  # optional sleep between images
COMMIT_BATCH = 500  # commit every N images (reduces DB commit overhead; progress saved on interrupt)

# --------------------------------------

_detector = None


def _init_detector():
    """Initialize InsightFace FaceAnalysis once, using GPU if available."""
    global _detector
    if _detector is not None:
        return _detector

    app = FaceAnalysis(name="buffalo_l")
    app.prepare(ctx_id=DETECTOR_CTX_ID, det_size=DETECTOR_INPUT_SIZE)
    _detector = app
    return _detector


def image_has_face_local(image_path):
    detector = _init_detector()
    img = cv2.imread(image_path)
    if img is None:
        return False
    h, w = img.shape[:2]
    if w < MIN_IMAGE_DIM or h < MIN_IMAGE_DIM:
        return False

    faces = detector.get(img)
    for f in faces:
        score = getattr(f, "det_score", None)
        if score is None or score >= MIN_FACE_SCORE:
            return True
    return False


def _format_eta(sec):
    if sec < 60:
        return f"{sec:.0f}s"
    if sec < 3600:
        return f"{sec / 60:.1f}m"
    return f"{sec / 3600:.1f}h"


def preprocess_all_images(conn):
    entries = os.listdir(IMAGE_DIR)
    print(f"{len(entries)} files found in {IMAGE_DIR}")
    if len(entries) == 0:
        raise RuntimeError(f"IMAGE_DIR is empty: {IMAGE_DIR}")

    # One query: skip all images that already have has_face set (avoids 10k+ get_image_status calls).
    already_done = get_already_has_face(conn)
    image_exts = (".jpg", ".jpeg", ".png", ".ppm")
    todo_count = sum(
        1 for f in entries
        if f.lower().endswith(image_exts) and f not in already_done
    )
    skipped = sum(1 for f in entries if f.lower().endswith(image_exts) and f in already_done)
    print(f"Already have has_face (will skip): {len(already_done)}")
    print(f"To process this run: {todo_count}")

    processed = 0
    batch_start = None

    for filename in entries:

        if not filename.lower().endswith(image_exts):
            continue

        if filename in already_done:
            continue

        # print(filename)

        if batch_start is None:
            batch_start = time.perf_counter()

        path = os.path.join(IMAGE_DIR, filename)
        has_face_flag = 1 if image_has_face_local(path) else 0
        upsert_image_status(conn, filename, has_face=has_face_flag, commit=False)
        processed += 1

        if processed % COMMIT_BATCH == 0:
            conn.commit()
            batch_elapsed = time.perf_counter() - batch_start
            rate = batch_elapsed / COMMIT_BATCH
            remaining = todo_count - processed
            eta_sec = remaining * rate if remaining > 0 else 0
            print(f"{processed}/{todo_count} done. Last batch: {batch_elapsed:.1f}s ({rate:.3f} s/img). {remaining} left, ETA ~{_format_eta(eta_sec)}")
            batch_start = time.perf_counter()

        if API_DELAY_SECONDS:
            time.sleep(API_DELAY_SECONDS)

    conn.commit()
    if processed % COMMIT_BATCH != 0 and processed > 0:
        batch_elapsed = time.perf_counter() - batch_start
        rate = batch_elapsed / (processed % COMMIT_BATCH)
        print(f"{processed}/{todo_count} done. Last batch: {batch_elapsed:.1f}s ({rate:.3f} s/img).")
    print("Preprocessing complete.")
    print(f"Images with has_face previously set (skipped): {skipped}")
    print(f"Images newly processed this run: {processed}")


def main():
    conn = init_db()
    try:
        preprocess_all_images(conn)
    finally:
        conn.close()


if __name__ == "__main__":
    main()

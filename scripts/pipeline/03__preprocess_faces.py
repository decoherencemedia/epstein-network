#!/usr/bin/env python3
"""
Preprocess images with a local face detector to decide which files contain faces.

This script:
- Scans IMAGE_DIR for image files
- Runs a local InsightFace detector (buffalo_l / SCRFD) on each image
- Populates the `images` table in faces.db with:
    image_name TEXT PRIMARY KEY,
    has_face   INTEGER  -- 1 if local detector saw at least one face, 0 if not

It does NOT call AWS Rekognition. Run 04__index_faces.py then 05__cluster_faces.py to index and cluster.

Only processes canonical rows (``duplicate_of IS NULL``) where ``has_face`` is still unknown
(``has_face IS NULL``). Images with ``has_face`` already set (0 or 1) are skipped entirely.
"""

import os
import time

import cv2
from insightface.app import FaceAnalysis
import onnxruntime as ort

from epstein_photos.faces_db import init_db, upsert_image_status

# ---------------- CONFIG ----------------

from epstein_photos.config import IMAGE_DIR

IMAGE_DIR = str(IMAGE_DIR)  # os.listdir / os.path.join expect str in this script

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

    if DETECTOR_CTX_ID >= 0:
        providers = ort.get_available_providers()
        if "CUDAExecutionProvider" not in providers:
            raise RuntimeError(
                "GPU requested (DETECTOR_CTX_ID >= 0) but ONNXRuntime CUDA provider is not available.\n"
                f"onnxruntime providers: {providers}\n"
                "Fix: install a GPU build of onnxruntime (onnxruntime-gpu) and ensure CUDA libraries are installed.\n"
                "If you want CPU, set DETECTOR_CTX_ID = -1."
            )

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
    # 01__dedup_images.py is expected to populate `images` table (and duplicate_of).
    # Preprocess only canonical images (duplicate_of IS NULL) that still need has_face.
    c = conn.cursor()
    c.execute(
        "SELECT image_name FROM images WHERE duplicate_of IS NULL AND has_face IS NULL"
    )
    entries = [row[0] for row in c.fetchall()]
    skipped = c.execute(
        "SELECT COUNT(*) FROM images WHERE duplicate_of IS NULL AND has_face IS NOT NULL"
    ).fetchone()[0]
    canonical_total = c.execute(
        "SELECT COUNT(*) FROM images WHERE duplicate_of IS NULL"
    ).fetchone()[0]
    print(f"{canonical_total} canonical image(s) in DB (duplicate_of IS NULL)")
    print(f"Already have has_face set (skipped): {skipped}")
    if len(entries) == 0:
        print("No canonical images left to preprocess (all have has_face set, or run 01__dedup_images.py first).")
        return

    image_exts = (".jpg", ".jpeg", ".png", ".ppm")
    todo_count = sum(1 for f in entries if f.lower().endswith(image_exts))
    print(f"To process this run (has_face IS NULL, supported extension): {todo_count}")

    processed = 0
    batch_start = None

    for filename in entries:

        if not filename.lower().endswith(image_exts):
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
    print(f"Canonical images that already had has_face before this run: {skipped}")
    print(f"Images newly processed this run: {processed}")


def main():
    conn = init_db()
    try:
        preprocess_all_images(conn)
    finally:
        conn.close()


if __name__ == "__main__":
    main()

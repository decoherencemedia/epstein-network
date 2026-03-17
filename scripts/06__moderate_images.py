"""
Run Rekognition DetectModerationLabels on each indexed image and store the raw JSON response in images.moderation_result.
filter_explicit_labels() is available for downstream use when you only care about Explicit (ParentName == "Explicit").
Run after 04__index_faces (indexing). Skips images that already have moderation_result set.
"""

import json
import time

import boto3

from config import IMAGE_DIR
from faces_db import get_images_for_moderation, init_db, upsert_image_moderation

# ---------------- CONFIG ----------------

REGION = "us-east-1"
API_DELAY_SECONDS = 0.2

# --------------------------------------


def filter_explicit_labels(moderation_labels):
    """Keep only labels whose top-level category is Explicit (ParentName == 'Explicit')."""
    if not moderation_labels:
        return []
    return [
        {"Confidence": lb.get("Confidence"), "Name": lb.get("Name"), "ParentName": lb.get("ParentName"), "TaxonomyLevel": lb.get("TaxonomyLevel")}
        for lb in moderation_labels
        if lb.get("ParentName") == "Explicit"
    ]


def main():
    rekognition = boto3.client("rekognition", region_name=REGION)
    conn = init_db()

    to_moderate = get_images_for_moderation(conn)
    print(f"{len(to_moderate)} image(s) to moderate (indexed, no moderation_result yet)")

    for image_name in to_moderate:
        path = IMAGE_DIR / image_name
        if not path.is_file():
            raise FileNotFoundError(f"Image file missing: {path}")
        with open(path, "rb") as f:
            image_bytes = f.read()
        response = rekognition.detect_moderation_labels(Image={"Bytes": image_bytes})
        result_json = json.dumps(response, default=str)
        upsert_image_moderation(conn, image_name, result_json, commit=True)
        print(image_name)
        time.sleep(API_DELAY_SECONDS)

    conn.close()


if __name__ == "__main__":
    main()

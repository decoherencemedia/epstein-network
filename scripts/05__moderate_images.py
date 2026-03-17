"""
Run Rekognition DetectModerationLabels on each indexed image and store the raw JSON response in images.moderation_result.
filter_explicit_labels() is available for downstream use when you only care about Explicit (ParentName == "Explicit").
Run after 04__cluster_faces (indexing). Skips images that already have moderation_result set.
"""

import argparse
import json
import os
import time

import boto3

from faces_db import get_images_for_moderation, init_db, upsert_image_moderation

REGION = "us-east-1"
IMAGE_DIR = "../../../all_images"
API_DELAY_SECONDS = 0.2


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
    ap = argparse.ArgumentParser(description="DetectModerationLabels for indexed images, store only Explicit labels in DB.")
    ap.add_argument("--image-dir", default=IMAGE_DIR, help="Directory containing image files")
    ap.add_argument("--delay", type=float, default=API_DELAY_SECONDS, help="Seconds between API calls")
    args = ap.parse_args()

    rekognition = boto3.client("rekognition", region_name=REGION)
    conn = init_db()

    to_moderate = get_images_for_moderation(conn)
    print(f"{len(to_moderate)} image(s) to moderate (indexed, no moderation_result yet)")

    for image_name in to_moderate:
        path = os.path.join(args.image_dir, image_name)
        if not os.path.isfile(path):
            print(f"  Skipping (file missing): {image_name}")
            continue
        with open(path, "rb") as f:
            image_bytes = f.read()
        try:
            response = rekognition.detect_moderation_labels(Image={"Bytes": image_bytes})
        except Exception as e:
            print(f"  Error {image_name}: {e}")
            continue
        # Store full raw response in DB; filter_explicit_labels available for query/display
        result_json = json.dumps(response, default=str)
        upsert_image_moderation(conn, image_name, result_json, commit=True)
        print(image_name)
        time.sleep(args.delay)

    conn.close()


if __name__ == "__main__":
    main()

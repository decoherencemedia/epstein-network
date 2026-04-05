"""
Index faces into a Rekognition collection (IndexFaces). Populates faces table and marks images as indexed.
Run 05__cluster_faces.py afterwards to run SearchFaces and assign person_id.
"""

import time
from decimal import Decimal
from pathlib import Path

import boto3

from epstein_photos.utils import dumps_aws_response
from epstein_photos.config import IMAGE_DIR, REKOGNITION_COLLECTION_ID, REKOGNITION_REGION
from epstein_photos.faces_db import get_images_to_index, init_db, upsert_image_status

# ---------------- CONFIG ----------------

API_DELAY_SECONDS = 0.2

# --------------------------------------

rekognition = boto3.client("rekognition", region_name=REKOGNITION_REGION)


def ensure_collection():
    try:
        rekognition.describe_collection(CollectionId=REKOGNITION_COLLECTION_ID)
    except rekognition.exceptions.ResourceNotFoundException:
        rekognition.create_collection(CollectionId=REKOGNITION_COLLECTION_ID)


def index_image(image_path: Path, conn):
    if not isinstance(image_path, Path):
        raise TypeError(f"image_path must be pathlib.Path, got {type(image_path).__name__}")
    with open(image_path, "rb") as f:
        image_bytes = f.read()

    response = rekognition.index_faces(
        CollectionId=REKOGNITION_COLLECTION_ID,
        Image={"Bytes": image_bytes},
        ExternalImageId=image_path.name,
        DetectionAttributes=["ALL"],
    )

    c = conn.cursor()
    image_name = image_path.name

    for record in response.get("FaceRecords", []):
        face = record["Face"]
        bb = face["BoundingBox"]
        detail = record.get("FaceDetail") or {}
        age_range = detail.get("AgeRange") or {}
        age_low = age_range.get("Low")
        age_high = age_range.get("High")
        if age_low is not None and isinstance(age_low, Decimal):
            age_low = int(age_low)
        if age_high is not None and isinstance(age_high, Decimal):
            age_high = int(age_high)
        index_record_json = dumps_aws_response(record)

        c.execute("""
            INSERT OR IGNORE INTO faces
            (face_id, image_name, left, top, width, height, age_range_low, age_range_high, index_face_record)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (
            face["FaceId"],
            image_name,
            float(bb["Left"]),
            float(bb["Top"]),
            float(bb["Width"]),
            float(bb["Height"]),
            age_low,
            age_high,
            index_record_json,
        ))

    conn.commit()


def index_all_images(conn):
    to_index = get_images_to_index(conn)
    print(f"{len(to_index)} image(s) to index (has_face=1, not yet indexed)")

    for image_name in to_index:
        path = IMAGE_DIR / image_name
        if not path.is_file():
            raise FileNotFoundError(f"Image file missing: {path}")
        print(image_name)
        index_image(path, conn)
        upsert_image_status(conn, image_name, indexed=1)
        time.sleep(API_DELAY_SECONDS)


def main():
    conn = init_db()
    ensure_collection()
    index_all_images(conn)
    conn.close()
    print("Done!")


if __name__ == "__main__":
    main()

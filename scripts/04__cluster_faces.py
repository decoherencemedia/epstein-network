import boto3
import argparse
import os
import time
from collections import defaultdict

from faces_db import get_images_to_index, init_db, upsert_image_status

# ---------------- CONFIG ----------------

REGION = "us-east-1"
COLLECTION_ID = "epstein-doj-rerun"
IMAGE_DIR = "../../../all_images"

SIMILARITY_THRESHOLD = 99.0
MAX_FACES_PER_SEARCH = 100
API_DELAY_SECONDS = 0.2  # rate limiting

# --------------------------------------

rekognition = boto3.client("rekognition", region_name=REGION)

# ---------------- COLLECTION ----------------

def ensure_collection():
    try:
        rekognition.describe_collection(CollectionId=COLLECTION_ID)
    except rekognition.exceptions.ResourceNotFoundException:
        rekognition.create_collection(CollectionId=COLLECTION_ID)

# ---------------- INDEXING ----------------

def index_image(image_path, conn):
    with open(image_path, "rb") as f:
        image_bytes = f.read()

    response = rekognition.index_faces(
        CollectionId=COLLECTION_ID,
        Image={"Bytes": image_bytes},
        ExternalImageId=os.path.basename(image_path),
        DetectionAttributes=[]
    )

    c = conn.cursor()

    for record in response.get("FaceRecords", []):
        face = record["Face"]
        bb = face["BoundingBox"]

        c.execute("""
            INSERT OR IGNORE INTO faces
            (face_id, image_name, left, top, width, height, searched)
            VALUES (?, ?, ?, ?, ?, ?, 0)
        """, (
            face["FaceId"],
            os.path.basename(image_path),
            bb["Left"],
            bb["Top"],
            bb["Width"],
            bb["Height"]
        ))

    conn.commit()


def index_all_images(conn):
    # Get all images that have a face and are not yet sent to Rekognition (one DB query).
    to_index = get_images_to_index(conn)
    print(f"{len(to_index)} image(s) to index (has_face=1, not yet indexed)")

    for image_name in to_index:
        path = os.path.join(IMAGE_DIR, image_name)
        if not os.path.isfile(path):
            print(f"  Skipping (file missing): {image_name}")
            continue
        print(image_name)
        index_image(path, conn)
        upsert_image_status(conn, image_name, indexed=1)
        time.sleep(API_DELAY_SECONDS)

# ---------------- UNION FIND ----------------

class UnionFind:
    def __init__(self):
        self.parent = {}

    def find(self, x):
        self.parent.setdefault(x, x)
        if self.parent[x] != x:
            self.parent[x] = self.find(self.parent[x])
        return self.parent[x]

    def union(self, x, y):
        self.parent[self.find(x)] = self.find(y)

# ---------------- CLUSTERING ----------------

def cluster_faces(conn):
    uf = UnionFind()
    c = conn.cursor()

    c.execute("SELECT face_id FROM faces")
    face_ids = [row[0] for row in c.fetchall()]

    for face_id in face_ids:
        uf.find(face_id)

    # Only search faces that haven't already been returned as a match (we already know their cluster).
    matched = set()
    for face_id in face_ids:
        if face_id in matched:
            continue
        response = rekognition.search_faces(
            CollectionId=COLLECTION_ID,
            FaceId=face_id,
            FaceMatchThreshold=SIMILARITY_THRESHOLD,
            MaxFaces=MAX_FACES_PER_SEARCH
        )

        for match in response["FaceMatches"]:
            other = match["Face"]["FaceId"]
            uf.union(face_id, other)
            matched.add(other)

        time.sleep(API_DELAY_SECONDS)

    clusters = defaultdict(list)
    for face_id in face_ids:
        clusters[uf.find(face_id)].append(face_id)

    return clusters

# ---------------- OUTPUT ----------------

def assign_person_ids(clusters, conn):
    """Write person_id onto each face in faces (clears existing person_id first)."""
    c = conn.cursor()
    c.execute("UPDATE faces SET person_id = NULL")
    for i, face_ids in enumerate(clusters.values(), start=1):
        person_id = f"person_{i}"
        for face_id in face_ids:
            c.execute(
                "UPDATE faces SET person_id = ? WHERE face_id = ?",
                (person_id, face_id),
            )
    conn.commit()

# ---------------- MAIN ----------------

def main():
    parser = argparse.ArgumentParser(
        description="Index faces into a Rekognition collection, and optionally cluster (SearchFaces) once at the end."
    )
    parser.add_argument(
        "--cluster",
        action="store_true",
        help="After indexing, run clustering (SearchFaces) and assign person_id on faces. This is the expensive step.",
    )
    parser.add_argument(
        "--cluster-only",
        action="store_true",
        help="Skip indexing; only run clustering + assign person_id using already-indexed faces in faces.db.",
    )
    args = parser.parse_args()

    conn = init_db()
    ensure_collection()

    if not args.cluster_only:
        print("Indexing images...")
        index_all_images(conn)

    if args.cluster or args.cluster_only:
        print("Clustering faces...")
        clusters = cluster_faces(conn)

        print("Assigning person_id to faces...")
        assign_person_ids(clusters, conn)
    else:
        print("Indexing complete. Run again with --cluster when you're ready to cluster once at the end.")

    print("Done!")

    conn.close()

if __name__ == "__main__":
    main()

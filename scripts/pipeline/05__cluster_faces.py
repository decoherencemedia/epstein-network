"""
Run SearchFaces on indexed faces, cluster by similarity (UnionFind), and assign person_id.
Run after 04__index_faces.py. Requires faces already indexed in Rekognition and in faces table.
"""

import time
from collections import defaultdict

import boto3

from epstein_photos.faces_db import init_db
from epstein_photos.config import REKOGNITION_COLLECTION_ID, REKOGNITION_REGION
from epstein_photos.utils import UnionFind, dumps_aws_response

# ---------------- CONFIG ----------------

# Store richer SearchFaces neighborhoods for offline analysis/debugging.
SEARCH_FACES_STORE_THRESHOLD = 0.0
MAX_FACES_PER_SEARCH = 100
API_DELAY_SECONDS = 0.2

# Use this stricter threshold for cluster edge creation.
CLUSTER_SIMILARITY_THRESHOLD = 99.0

# --------------------------------------

rekognition = boto3.client("rekognition", region_name=REKOGNITION_REGION)


def cluster_faces(conn):
    uf = UnionFind()
    c = conn.cursor()

    c.execute("SELECT face_id FROM faces")
    face_ids = [row[0] for row in c.fetchall()]

    for face_id in face_ids:
        uf.find(face_id)

    for i, face_id in enumerate(face_ids):
        print(f"{i}/{len(face_ids)}:   {face_id}")
        response = rekognition.search_faces(
            CollectionId=REKOGNITION_COLLECTION_ID,
            FaceId=face_id,
            FaceMatchThreshold=SEARCH_FACES_STORE_THRESHOLD,
            MaxFaces=MAX_FACES_PER_SEARCH,
        )

        # Persist the raw SearchFaces response for later analysis / debugging.
        c.execute(
            "UPDATE faces SET searched = 1, search_faces_result = ? WHERE face_id = ?",
            (dumps_aws_response(response), face_id),
        )

        for match in response["FaceMatches"]:
            similarity = float(match.get("Similarity") or 0.0)
            if similarity < CLUSTER_SIMILARITY_THRESHOLD:
                continue
            other = match["Face"]["FaceId"]
            uf.union(face_id, other)

        time.sleep(API_DELAY_SECONDS)

    conn.commit()

    clusters = defaultdict(list)
    for face_id in face_ids:
        clusters[uf.find(face_id)].append(face_id)

    return clusters


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


def main():
    conn = init_db()
    print("Clustering faces...")
    clusters = cluster_faces(conn)
    print("Assigning person_id to faces...")
    assign_person_ids(clusters, conn)
    conn.close()
    print("Done!")


if __name__ == "__main__":
    main()

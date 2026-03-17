"""
Run SearchFaces on indexed faces, cluster by similarity (UnionFind), and assign person_id.
Run after 04__index_faces.py. Requires faces already indexed in Rekognition and in faces table.
"""

import time
from collections import defaultdict

import boto3

from faces_db import init_db

# ---------------- CONFIG ----------------

REGION = "us-east-1"
COLLECTION_ID = "epstein-doj-rerun"
SIMILARITY_THRESHOLD = 99.0
MAX_FACES_PER_SEARCH = 100
API_DELAY_SECONDS = 0.2

# --------------------------------------

rekognition = boto3.client("rekognition", region_name=REGION)


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


def cluster_faces(conn):
    uf = UnionFind()
    c = conn.cursor()

    c.execute("SELECT face_id FROM faces")
    face_ids = [row[0] for row in c.fetchall()]

    for face_id in face_ids:
        uf.find(face_id)

    matched = set()
    for face_id in face_ids:
        if face_id in matched:
            continue
        response = rekognition.search_faces(
            CollectionId=COLLECTION_ID,
            FaceId=face_id,
            FaceMatchThreshold=SIMILARITY_THRESHOLD,
            MaxFaces=MAX_FACES_PER_SEARCH,
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

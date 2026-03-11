import boto3
import os
import sqlite3
import time
from collections import defaultdict

# ---------------- CONFIG ----------------

REGION = "us-east-1"
COLLECTION_ID = "epstein-doj"
IMAGE_DIR = "../../all_images_parallel"

SIMILARITY_THRESHOLD = 99.0
MAX_FACES_PER_SEARCH = 100
API_DELAY_SECONDS = 0.2  # rate limiting

DB_PATH = "faces.db"

# --------------------------------------

rekognition = boto3.client("rekognition", region_name=REGION)

# ---------------- DATABASE ----------------

def init_db():
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()

    c.execute("""
        CREATE TABLE IF NOT EXISTS faces (
            face_id TEXT PRIMARY KEY,
            image_name TEXT,
            left REAL,
            top REAL,
            width REAL,
            height REAL,
            searched INTEGER DEFAULT 0
        )
    """)

    c.execute("""
        CREATE TABLE IF NOT EXISTS people (
            person_id TEXT,
            image_name TEXT,
            face_id TEXT,
            left REAL,
            top REAL,
            width REAL,
            height REAL,
            PRIMARY KEY (person_id, face_id)
        )
    """)

    conn.commit()
    return conn

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

def is_image_indexed(image_name, conn):
    c = conn.cursor()
    c.execute("SELECT COUNT(*) FROM faces WHERE image_name = ?", (image_name,))
    count = c.fetchone()[0]
    return count > 0

def index_all_images(conn):
    for filename in os.listdir(IMAGE_DIR):
        print(filename)
        if not filename.lower().endswith((".jpg", ".jpeg", ".png")):
            continue

        if is_image_indexed(filename, conn):
            continue

        path = os.path.join(IMAGE_DIR, filename)
        index_image(path, conn)
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

    # c.execute("SELECT face_id FROM faces WHERE searched = 0")
    c.execute("SELECT face_id FROM faces")

    unsearched = [row[0] for row in c.fetchall()]

    for face_id in unsearched:
        response = rekognition.search_faces(
            CollectionId=COLLECTION_ID,
            FaceId=face_id,
            FaceMatchThreshold=SIMILARITY_THRESHOLD,
            MaxFaces=MAX_FACES_PER_SEARCH
        )

        for match in response["FaceMatches"]:
            other = match["Face"]["FaceId"]
            uf.union(face_id, other)

        c.execute(
            "UPDATE faces SET searched = 1 WHERE face_id = ?",
            (face_id,)
        )

        conn.commit()
        time.sleep(API_DELAY_SECONDS)

    clusters = defaultdict(list)
    for face_id in face_ids:
        clusters[uf.find(face_id)].append(face_id)

    return clusters

# ---------------- OUTPUT ----------------

def build_people_table(clusters, conn):
    c = conn.cursor()

    # Clear existing people table
    c.execute("DELETE FROM people")

    for i, face_ids in enumerate(clusters.values(), start=1):
        person_id = f"person_{i}"

        for face_id in face_ids:
            c.execute("""
                SELECT image_name, left, top, width, height
                FROM faces
                WHERE face_id = ?
            """, (face_id,))

            row = c.fetchone()
            if row:
                image_name, left, top, width, height = row
                c.execute("""
                    INSERT INTO people
                    (person_id, image_name, face_id, left, top, width, height)
                    VALUES (?, ?, ?, ?, ?, ?, ?)
                """, (person_id, image_name, face_id, left, top, width, height))

    conn.commit()

# ---------------- MAIN ----------------

def main():
    conn = init_db()
    ensure_collection()

    # print("Indexing images...")
    # index_all_images(conn)

    print("Clustering faces...")
    clusters = cluster_faces(conn)

    print("Building people table...")
    build_people_table(clusters, conn)

    print("Done!")

    conn.close()

if __name__ == "__main__":
    main()

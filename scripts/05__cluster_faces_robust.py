"""
Robust face clustering with quality-first processing and edge gating.

Differences from 05__cluster_faces.py:
- ranks faces by quality and queries higher-quality faces first
- stores broad SearchFaces payloads for diagnostics
- only creates cluster edges when matches are:
  1) above similarity threshold
  2) mutual top-k neighbors
  3) supported by shared strong-neighbor context
"""

import json
import time
from collections import defaultdict

import boto3

from config import REKOGNITION_COLLECTION_ID, REKOGNITION_REGION
from faces_db import init_db

# ---------------- CONFIG ----------------

# Save rich neighborhoods; clustering still uses stricter gating.
SEARCH_FACES_STORE_THRESHOLD = 0.0
MAX_FACES_PER_SEARCH = 100
API_DELAY_SECONDS = 0.2

# Strong edge requirements.
EDGE_SIMILARITY_THRESHOLD = 99.0
MUTUAL_TOP_K = 10
MIN_SHARED_STRONG_NEIGHBORS = 1

# Quality ranking weights.
AREA_WEIGHT = 1.0
CONFIDENCE_WEIGHT = 0.25
POSE_WEIGHT = 0.15
OCCLUSION_PENALTY = 0.4

# ---------------------------------------

rekognition = boto3.client("rekognition", region_name=REKOGNITION_REGION)


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


def _face_quality_score(index_face_record: str, width: float, height: float) -> float:
    area = float(width) * float(height)
    payload = json.loads(index_face_record) if index_face_record else {}
    face_obj = payload.get("Face") or {}
    face_detail = payload.get("FaceDetail") or {}
    pose = face_detail.get("Pose") or {}
    occluded = bool((face_detail.get("FaceOccluded") or {}).get("Value"))

    conf = float(face_obj.get("Confidence") or 0.0) / 100.0
    yaw = abs(float(pose.get("Yaw") or 0.0))
    pitch = abs(float(pose.get("Pitch") or 0.0))
    pose_penalty = min(1.0, (yaw + pitch) / 90.0)

    score = area * AREA_WEIGHT
    score += conf * CONFIDENCE_WEIGHT
    score -= pose_penalty * POSE_WEIGHT
    if occluded:
        score -= OCCLUSION_PENALTY
    return score


def _extract_strong_neighbors(response: dict) -> list[tuple[str, float]]:
    out = []
    for match in response.get("FaceMatches") or []:
        other = str(((match.get("Face") or {}).get("FaceId") or "")).strip()
        if not other:
            continue
        sim = float(match.get("Similarity") or 0.0)
        if sim >= EDGE_SIMILARITY_THRESHOLD:
            out.append((other, sim))
    out.sort(key=lambda x: x[1], reverse=True)
    return out


def _query_and_store(conn, ordered_face_ids: list[str]) -> dict[str, list[tuple[str, float]]]:
    c = conn.cursor()
    strong_neighbors_by_face: dict[str, list[tuple[str, float]]] = {}
    total = len(ordered_face_ids)

    for i, face_id in enumerate(ordered_face_ids, start=1):
        print(f"{i}/{total}: {face_id}")
        response = rekognition.search_faces(
            CollectionId=REKOGNITION_COLLECTION_ID,
            FaceId=face_id,
            FaceMatchThreshold=SEARCH_FACES_STORE_THRESHOLD,
            MaxFaces=MAX_FACES_PER_SEARCH,
        )

        c.execute(
            "UPDATE faces SET searched = 1, search_faces_result = ? WHERE face_id = ?",
            (json.dumps(response, default=str), face_id),
        )

        strong_neighbors_by_face[face_id] = _extract_strong_neighbors(response)
        time.sleep(API_DELAY_SECONDS)

    conn.commit()
    return strong_neighbors_by_face


def _build_approved_edges(strong_neighbors_by_face: dict[str, list[tuple[str, float]]]) -> list[tuple[str, str]]:
    topk_sets: dict[str, set[str]] = {}
    strong_sets: dict[str, set[str]] = {}
    for face_id, neighbors in strong_neighbors_by_face.items():
        ids = [n for n, _ in neighbors]
        topk_sets[face_id] = set(ids[:MUTUAL_TOP_K])
        strong_sets[face_id] = set(ids)

    edges: list[tuple[str, str]] = []
    seen: set[tuple[str, str]] = set()

    for src, neighbors in strong_neighbors_by_face.items():
        src_top = topk_sets.get(src) or set()
        src_strong = strong_sets.get(src) or set()
        for dst, _sim in neighbors:
            if dst == src:
                continue

            # Gate 1: mutual top-k.
            dst_top = topk_sets.get(dst) or set()
            if dst not in src_top or src not in dst_top:
                continue

            # Gate 2: shared-neighbor support.
            dst_strong = strong_sets.get(dst) or set()
            shared = (src_strong & dst_strong) - {src, dst}
            if len(shared) < MIN_SHARED_STRONG_NEIGHBORS:
                continue

            pair = (src, dst) if src < dst else (dst, src)
            if pair in seen:
                continue
            seen.add(pair)
            edges.append(pair)

    return edges


def cluster_faces_robust(conn):
    uf = UnionFind()
    c = conn.cursor()

    rows = c.execute(
        """
        SELECT face_id, index_face_record, width, height
        FROM faces
        """
    ).fetchall()

    ranked = []
    for row in rows:
        face_id = str(row[0])
        score = _face_quality_score(
            index_face_record=str(row[1] or ""),
            width=float(row[2] or 0.0),
            height=float(row[3] or 0.0),
        )
        ranked.append((face_id, score))
        uf.find(face_id)

    ranked.sort(key=lambda x: x[1], reverse=True)
    ordered_face_ids = [face_id for face_id, _score in ranked]

    strong_neighbors_by_face = _query_and_store(conn=conn, ordered_face_ids=ordered_face_ids)
    approved_edges = _build_approved_edges(strong_neighbors_by_face=strong_neighbors_by_face)

    print(f"Approved robust edges: {len(approved_edges)}")
    for a, b in approved_edges:
        uf.union(a, b)

    clusters = defaultdict(list)
    for face_id in ordered_face_ids:
        clusters[uf.find(face_id)].append(face_id)
    return clusters


def assign_person_ids(clusters, conn):
    c = conn.cursor()
    c.execute("UPDATE faces SET person_id = NULL")

    # Stable ordering: largest clusters first, then lexicographically.
    cluster_list = list(clusters.values())
    cluster_list.sort(key=lambda ids: (-len(ids), ids[0]))

    for i, face_ids in enumerate(cluster_list, start=1):
        person_id = f"person_{i}"
        for face_id in face_ids:
            c.execute("UPDATE faces SET person_id = ? WHERE face_id = ?", (person_id, face_id))
    conn.commit()


def main():
    conn = init_db()
    print("Robust clustering faces...")
    clusters = cluster_faces_robust(conn)
    print("Assigning person_id to faces...")
    assign_person_ids(clusters, conn)
    conn.close()
    print("Done!")


if __name__ == "__main__":
    main()


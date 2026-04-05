#!/usr/bin/env python3
"""
Assign person_id to **new** faces only (incremental), without clearing existing clusters.

Targets faces with ``person_id IS NULL`` whose ``image_name`` matches configured prefixes
(``BIRTHDAY_BOOK_*``, ``HOUSE_OVERSIGHT_*`` by default). Runs SearchFaces like ``05__cluster_faces``,
unions matches among those new faces, and attaches to an existing ``person_id`` when a match
points at an already-assigned face in SQLite. Otherwise allocates a new ``person_N`` id.

Skips **low-quality** faces: normalized bbox area (``width * height``) below ``--min-bbox-area``
so junk crops stay un-clustered.

Requires: faces indexed in Rekognition (``04__index_faces``), rows in ``faces`` with
``index_face_record`` / bbox columns populated.

Does **not** run full-graph recluster; do not use this and ``05__cluster_faces`` interchangeably
on the same dataset without understanding that ``05`` wipes all ``person_id`` values.
"""
from __future__ import annotations

import argparse
import json
import re
import sqlite3
import time
from collections import defaultdict
from decimal import Decimal

import boto3

from config import REKOGNITION_COLLECTION_ID, REKOGNITION_REGION
from faces_db import init_db

# Same defaults as 05__cluster_faces.py
SEARCH_FACES_STORE_THRESHOLD = 0.0
MAX_FACES_PER_SEARCH = 100
API_DELAY_SECONDS = 0.2
CLUSTER_SIMILARITY_THRESHOLD = 99.0

# Normalized bbox area (0..1); below this, leave person_id NULL
DEFAULT_MIN_BBOX_AREA = 0.001

DEFAULT_PREFIXES = ("BIRTHDAY_BOOK_", "HOUSE_OVERSIGHT_")

rekognition = boto3.client("rekognition", region_name=REKOGNITION_REGION)


class UnionFind:
    def __init__(self) -> None:
        self.parent: dict[str, str] = {}

    def find(self, x: str) -> str:
        self.parent.setdefault(x, x)
        if self.parent[x] != x:
            self.parent[x] = self.find(self.parent[x])
        return self.parent[x]

    def union(self, x: str, y: str) -> None:
        self.parent[self.find(x)] = self.find(y)


def _json_default(obj):
    if isinstance(obj, Decimal):
        return float(obj)
    raise TypeError


def next_free_person_id(conn: sqlite3.Connection) -> str:
    max_n = 0
    c = conn.cursor()
    for table in ("faces", "people"):
        c.execute(f"SELECT person_id FROM {table} WHERE person_id GLOB 'person_[0-9]*'")
        for (pid,) in c.fetchall():
            m = re.match(r"^person_(\d+)$", str(pid).strip())
            if m:
                max_n = max(max_n, int(m.group(1)))
    return f"person_{max_n + 1}"


def load_target_face_ids(
    conn: sqlite3.Connection,
    *,
    min_bbox_area: float,
    prefixes: tuple[str, ...],
) -> list[str]:
    c = conn.cursor()
    ph = " OR ".join(["f.image_name LIKE ?" for _ in prefixes])
    params = [f"{p}%" for p in prefixes]
    c.execute(
        f"""
        SELECT f.face_id
        FROM faces f
        INNER JOIN images i ON i.image_name = f.image_name
        WHERE f.person_id IS NULL
          AND i.duplicate_of IS NULL
          AND COALESCE(i.indexed, 0) = 1
          AND ({ph})
          AND (f.width * f.height) >= ?
        """,
        (*params, min_bbox_area),
    )
    return [row[0] for row in c.fetchall()]


def incremental_assign(conn: sqlite3.Connection, face_ids: list[str]) -> None:
    if not face_ids:
        print("No target faces (check prefixes, indexed images, min bbox area).")
        return

    F = set(face_ids)
    uf = UnionFind()
    for fid in face_ids:
        uf.find(fid)

    # face_id -> (person_id, similarity) best external match for this face
    best_external: dict[str, tuple[str, float]] = {}
    c = conn.cursor()

    for i, face_id in enumerate(face_ids):
        print(f"{i + 1}/{len(face_ids)}  {face_id}")
        response = rekognition.search_faces(
            CollectionId=REKOGNITION_COLLECTION_ID,
            FaceId=face_id,
            FaceMatchThreshold=SEARCH_FACES_STORE_THRESHOLD,
            MaxFaces=MAX_FACES_PER_SEARCH,
        )
        c.execute(
            "UPDATE faces SET searched = 1, search_faces_result = ? WHERE face_id = ?",
            (json.dumps(response, default=_json_default), face_id),
        )

        for match in response.get("FaceMatches") or []:
            similarity = float(match.get("Similarity") or 0.0)
            if similarity < CLUSTER_SIMILARITY_THRESHOLD:
                continue
            other = match.get("Face") or {}
            oid = other.get("FaceId")
            if not oid or oid == face_id:
                continue
            if oid in F:
                uf.union(face_id, oid)
                continue
            row = c.execute(
                "SELECT person_id FROM faces WHERE face_id = ?",
                (oid,),
            ).fetchone()
            if not row or not row[0]:
                continue
            pid = str(row[0]).strip()
            prev = best_external.get(face_id)
            if prev is None or similarity > prev[1]:
                best_external[face_id] = (pid, similarity)

        time.sleep(API_DELAY_SECONDS)

    # Components
    by_root: dict[str, list[str]] = defaultdict(list)
    for fid in face_ids:
        by_root[uf.find(fid)].append(fid)

    assigned = 0
    for _root, members in by_root.items():
        # Best (person_id, sim) across any member with an external match
        candidates: list[tuple[str, float]] = []
        for fid in members:
            if fid in best_external:
                candidates.append(best_external[fid])
        if not candidates:
            pid = next_free_person_id(conn)
            for fid in members:
                c.execute("UPDATE faces SET person_id = ? WHERE face_id = ?", (pid, fid))
            assigned += len(members)
            print(f"  new cluster -> {pid} ({len(members)} face(s))")
            continue
        best_pid = max(candidates, key=lambda x: x[1])[0]
        for fid in members:
            c.execute("UPDATE faces SET person_id = ? WHERE face_id = ?", (best_pid, fid))
        assigned += len(members)
        print(f"  merged -> {best_pid} ({len(members)} face(s))")

    conn.commit()
    print(f"Done. Assigned person_id to {assigned} face(s) in {len(by_root)} component(s).")


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument(
        "--min-bbox-area",
        type=float,
        default=DEFAULT_MIN_BBOX_AREA,
        help=f"Min normalized bbox area width*height (default: {DEFAULT_MIN_BBOX_AREA})",
    )
    ap.add_argument(
        "--prefix",
        action="append",
        dest="prefixes",
        metavar="PREFIX",
        help="Image name prefix (repeatable). Default: BIRTHDAY_BOOK_ and HOUSE_OVERSIGHT_",
    )
    args = ap.parse_args()
    prefixes = tuple(args.prefixes) if args.prefixes else DEFAULT_PREFIXES

    conn = init_db()
    try:
        face_ids = load_target_face_ids(
            conn, min_bbox_area=args.min_bbox_area, prefixes=prefixes
        )
        print(f"Prefixes: {prefixes!r}")
        print(f"min_bbox_area: {args.min_bbox_area}")
        print(f"Candidate faces: {len(face_ids)}")
        incremental_assign(conn, face_ids)
    finally:
        conn.close()


if __name__ == "__main__":
    main()

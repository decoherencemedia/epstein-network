#!/usr/bin/env python3
"""
Count distinct k-person subsets (k = 1..MAX_K) that co-occur on at least one image.

This is the classic **frequent itemset** / **market-basket** setup: each image is a
"transaction", each person_id is an "item". You want the number of distinct itemsets
of size k with **support ≥ 1** (at least one image contains all k people).

Related: **hyperedges** in a hypergraph whose vertices are people and whose hyperedges
are the person-sets per image; you're counting distinct subsets of size k that appear
as a subset of some hyperedge (equivalently: k-uniform patterns covered by at least one edge).

Only **in-network** people are counted: ``people.include_in_network = 1`` (Matches/Unknowns
minus Ignore, as maintained by sheet sync). Faces whose ``person_id`` is missing from
``people`` or is out-of-network are excluded.

Run from repo root or scripts/::

  cd scripts && python3 count_shared_image_person_combinations.py
  python3 scripts/count_shared_image_person_combinations.py --db /path/to/faces.db --max-k 4
"""
from __future__ import annotations

import argparse
import sqlite3
from collections import defaultdict
from itertools import combinations
from pathlib import Path


def main() -> None:
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument(
        "--db",
        type=Path,
        default=Path(__file__).resolve().parent / "faces.db",
        help="SQLite database path (default: scripts/faces.db)",
    )
    p.add_argument(
        "--max-k",
        type=int,
        default=10,
        help="Maximum subset size K (default: 4)",
    )
    p.add_argument(
        "--all-faces",
        action="store_true",
        help="Count all faces with a person_id (ignore include_in_network).",
    )
    args = p.parse_args()
    max_k = args.max_k
    if max_k < 1:
        raise SystemExit("--max-k must be >= 1")

    if not args.db.is_file():
        raise SystemExit(f"Database not found: {args.db}")

    conn = sqlite3.connect(str(args.db))
    try:
        if args.all_faces:
            rows = conn.execute(
                """
                SELECT f.image_name, f.person_id
                FROM faces f
                WHERE f.person_id IS NOT NULL AND TRIM(f.person_id) != ''
                """
            ).fetchall()
        else:
            rows = conn.execute(
                """
                SELECT f.image_name, f.person_id
                FROM faces f
                INNER JOIN people p ON p.person_id = f.person_id
                WHERE f.person_id IS NOT NULL AND TRIM(f.person_id) != ''
                  AND COALESCE(p.include_in_network, 0) = 1
                """
            ).fetchall()
    finally:
        conn.close()

    by_image: dict[str, set[str]] = defaultdict(set)
    for image_name, person_id in rows:
        by_image[str(image_name)].add(str(person_id).strip())

    # Distinct k-subsets that appear as a subset of at least one image's person set.
    distinct: dict[int, set[frozenset[str]]] = {k: set() for k in range(1, max_k + 1)}

    max_m = 0
    for _img, pids in by_image.items():
        m = len(pids)
        max_m = max(max_m, m)
        plist = sorted(pids)
        upper = min(max_k, m)
        for k in range(1, upper + 1):
            for combo in combinations(plist, k):
                distinct[k].add(frozenset(combo))

    print(f"Database: {args.db.resolve()}")
    print(f"Filter: {'all faces with person_id' if args.all_faces else 'in-network people only (include_in_network=1)'}")
    print(f"Images with ≥1 qualifying face (distinct person_id): {len(by_image)}")
    print(f"Max distinct people on a single image: {max_m}")
    print()
    for k in range(1, max_k + 1):
        print(f"Distinct non-empty {k}-person combinations with ≥1 shared image: {len(distinct[k])}")


if __name__ == "__main__":
    main()

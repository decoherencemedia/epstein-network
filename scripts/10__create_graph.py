import sqlite3
import json
import re
from collections import defaultdict
from pathlib import Path
from typing import Any

import pandas as pd
from itertools import combinations

import networkx as nx

from sheets_common import (
    get_sheet_client,
    load_categories,
    load_ignore,
    load_names,
    load_person_ids_matches_and_unknowns,
)
from config import DB_PATH
from faces_db import pick_best_images


SCRIPT_DIR = Path(__file__).resolve().parent
REPO_DIR = SCRIPT_DIR.parent
VIZ_DATA_DIR = REPO_DIR / "viz_data"

OUTPUT_GRAPHML = REPO_DIR / "graphml" / "epstein_photo_people.graphml"

# Manual node face crops: run ``13__optimize_node_faces.sh`` to build WebPs from
# ``node_faces_selected/`` into this directory. Same naming as node_faces_top5: ``Label__NNN.*``.
NODE_FACES_OPTIMIZED_DIR = REPO_DIR / "images" / "node_faces_selected_optimized"
FACE_IDX_SUFFIX_RE = re.compile(r"^(.*)__(\d{3})$")


def _is_explicit_moderation(moderation_result: str | None) -> bool:
    if not moderation_result:
        return False
    data: Any = json.loads(moderation_result)
    labels = data.get("ModerationLabels") or []
    for lb in labels:
        name = lb.get("Name")
        parent = lb.get("ParentName")
        # Treat any Explicit L1 or any child of Explicit as explicit content.
        if name == "Explicit" or parent == "Explicit":
            return True
    return False


def _to_webp_filename(image_name: str) -> str:
    p = Path(image_name)
    return str(p.with_suffix(".webp"))


def _has_minor_face(age_low: Any, age_high: Any) -> bool:
    if not isinstance(age_low, int) or not isinstance(age_high, int):
        raise TypeError(f"age bounds must be int, got age_range_low={age_low!r}, age_range_high={age_high!r}")
    # Under 18: if either bound implies <18, treat as minor.
    if age_low < 18:
        return True
    if age_high < 18:
        return True
    return False


def _sanitize_label_for_filename(label: str) -> str:
    """Match ``12__export_node_faces`` / atlas: safe filename stem from graph node label."""
    s = "".join(c if c.isalnum() or c in "._- " else "" for c in label)
    return s.strip().replace(" ", "_").replace("/", "_") or "node"


def _stem_to_label_from_graph(nodes: Any) -> dict[str, str]:
    """Map sanitized stem -> graph label for all nodes in G."""
    stem_to_label: dict[str, str] = {}
    for label in nodes:
        stem = _sanitize_label_for_filename(str(label))
        stem_to_label[stem] = str(label)
    return stem_to_label


def _collect_best_selected_face_webp_by_label(stem_to_label: dict[str, str]) -> dict[str, str]:
    """
    For each graph label, pick the best ``*.webp`` under NODE_FACES_OPTIMIZED_DIR (recursive).
    Filenames use ``Stem__NNN.webp``; lowest NNN wins. Keys in image_data are ``faces/<basename>.webp``.

    Returns: label -> basename (e.g. ``Brooke_Visoski__001.webp``).
    """
    if not NODE_FACES_OPTIMIZED_DIR.is_dir():
        return {}
    paths = list(NODE_FACES_OPTIMIZED_DIR.rglob("*.webp"))
    if not paths:
        return {}

    by_label: dict[str, tuple[int, str]] = {}
    seen_basenames: dict[str, Path] = {}

    for path in sorted(paths):
        if path.name in seen_basenames and seen_basenames[path.name] != path:
            raise RuntimeError(
                f"Duplicate basename under node_faces_selected_optimized (flattened upload): {path.name!r}\n"
                f"  First: {seen_basenames[path.name]}\n  Second: {path}"
            )
        seen_basenames[path.name] = path

        stem = path.stem
        base_stem = stem
        face_idx = 0
        m = FACE_IDX_SUFFIX_RE.match(stem)
        if m:
            base_stem = m.group(1)
            face_idx = int(m.group(2))

        label = stem_to_label.get(base_stem)
        if label is None:
            print(
                f"WARNING: no graph node for selected face stem {base_stem!r} ({path.name}); skipped"
            )
            continue

        prev = by_label.get(label)
        if prev is None or face_idx < prev[0]:
            by_label[label] = (face_idx, path.name)

    return {lbl: fn for lbl, (_i, fn) in by_label.items()}


def _load_victim_person_ids_and_labels(
    conn: sqlite3.Connection, candidate_ids: set[str]
) -> tuple[set[str], dict[str, str]]:
    """
    From ``people.is_victim``, return victim person_ids (intersect candidate_ids) and
    person_id -> display label. Labels come from ``people.name`` (set by sheet sync as
    ``Victim 1`` … ``Victim N``); run 09 before 10 if names are missing.
    """
    if not candidate_ids:
        return set(), {}
    placeholders = ",".join("?" * len(candidate_ids))
    cur = conn.execute(
        f"SELECT person_id, name FROM people WHERE is_victim = 1 AND person_id IN ({placeholders})",
        tuple(sorted(candidate_ids)),
    )
    victims: set[str] = set()
    label_by_pid: dict[str, str] = {}
    for row in cur.fetchall():
        pid = str(row[0])
        name = row[1]
        victims.add(pid)
        if name and str(name).strip():
            label_by_pid[pid] = str(name).strip()
        else:
            raise RuntimeError(
                f"Victim {pid!r} has empty people.name; run scripts/09__sheets_rekognition.py "
                "to sync victim flags and anonymized names before building the graph."
            )
    return victims, label_by_pid


if __name__ == "__main__":
    # Names and ignore list from the shared Google Spreadsheet (Matches / Ignore sheets).
    gc = get_sheet_client()
    PEOPLE_NAMES = load_names(gc)
    PEOPLE_TO_SKIP = load_ignore(gc)
    INCLUDE_PERSON_IDS = load_person_ids_matches_and_unknowns(gc)
    INCLUDE_PERSON_IDS = INCLUDE_PERSON_IDS - PEOPLE_TO_SKIP

    con = sqlite3.connect(DB_PATH)

    if not INCLUDE_PERSON_IDS:
        raise RuntimeError("No person_ids loaded from Matches/Unknowns sheets (after ignore list).")

    _, victim_label_by_person = _load_victim_person_ids_and_labels(con, INCLUDE_PERSON_IDS)
    victim_labels = set(victim_label_by_person.values())

    # Fetch all faces for those person_ids (including victims — they stay in the graph).
    placeholders_top = ",".join("?" for _ in INCLUDE_PERSON_IDS)
    df = pd.read_sql_query(
        "SELECT f.person_id, f.image_name, f.left, f.top, f.width, f.height, "
        "f.celebrity_name, f.celebrity_confidence, "
        "f.age_range_low, f.age_range_high, "
        "f.index_face_record, "
        "i.moderation_result, "
        "i.width_px, i.height_px, "
        "COALESCE(i.contains_victim, 0) AS contains_victim "
        f"FROM faces f "
        f"LEFT JOIN images i ON i.image_name = f.image_name "
        f"WHERE f.person_id IN ({placeholders_top})",
        con,
        params=list(INCLUDE_PERSON_IDS),
    )

    if df.empty:
        raise RuntimeError("No faces found in DB for person_ids from Matches/Unknowns.")

    # Disallow images that are explicit or contain any face under 18.
    df["is_explicit"] = df["moderation_result"].apply(_is_explicit_moderation)
    df["is_minor_face"] = df.apply(
        lambda r: _has_minor_face(r.get("age_range_low"), r.get("age_range_high")),
        axis=1,
    )
    disallowed_images = set(df.loc[df["is_explicit"] | df["is_minor_face"], "image_name"].tolist())

    # Images safe to show in node/edge previews (no victim faces in frame).
    safe_preview_images = set(
        df.loc[df["contains_victim"] == 0, "image_name"].dropna().unique().tolist()
    )

    # Compute face area (normalized bbox area × image pixels), using dimensions from DB.
    if df["width_px"].isna().any() or df["height_px"].isna().any():
        missing = (
            df.loc[df["width_px"].isna() | df["height_px"].isna(), "image_name"]
            .dropna()
            .unique()
            .tolist()
        )
        raise RuntimeError(
            "Missing image dimensions in DB for some images. "
            "Re-run scripts/01__dedup_images.py to populate images.width_px/height_px. "
            f"Examples: {', '.join(missing[:20])}" + (" ..." if len(missing) > 20 else "")
        )
    df["face_area"] = df["width"] * df["height"] * df["width_px"] * df["height_px"]

    # Fail loudly if sheets contain IDs not in DB.
    found_ids = set(df["person_id"].unique().tolist())
    missing_ids = sorted(INCLUDE_PERSON_IDS - found_ids)
    if missing_ids:
        raise RuntimeError(
            f"{len(missing_ids)} person_id(s) from Matches/Unknowns not found in DB: {', '.join(missing_ids[:50])}"
            + (" ..." if len(missing_ids) > 50 else "")
        )

    # Victims: anonymized labels; others: Matches sheet name or person_id.
    def _label_for_person_id(pid: str) -> str:
        if pid in victim_label_by_person:
            return victim_label_by_person[pid]
        return PEOPLE_NAMES.get(pid) or pid

    df["label"] = df["person_id"].apply(_label_for_person_id)

    # Build per (image_name, label) -> bbox of the largest face.
    best_face_idx = df.groupby(["image_name", "label"])["face_area"].idxmax()
    face_bbox_lookup: dict[tuple[str, str], tuple[float, float, float, float]] = (
        df.loc[best_face_idx]
        .set_index(["image_name", "label"])[["left", "top", "width", "height"]]
        .apply(tuple, axis=1)
        .to_dict()
    )

    # For each (label, image), keep the largest face_area.
    per_label_image = (
        df.groupby(["label", "image_name"])["face_area"]
        .max()
        .reset_index()
    )

    # Build mapping image -> {label: area} for edge image scoring.
    image_label_area: dict[str, dict[str, float]] = defaultdict(dict)
    for _, row in per_label_image.iterrows():
        image_label_area[row["image_name"]][row["label"]] = row["face_area"]

    # Build co-occurrence edges and collect all (product, image) per edge for ranking.
    edges = defaultdict(int)
    edge_candidates: dict[tuple[str, str], list[tuple[float, str]]] = defaultdict(list)

    for image, label_area in image_label_area.items():
        labels = list(label_area.keys())
        for a, b in combinations(sorted(labels), 2):
            edges[(a, b)] += 1
            prod = label_area[a] * label_area[b]
            edge_candidates[(a, b)].append((prod, image))

    edge_list = [(*k, v) for k, v in edges.items()]

    G = nx.Graph()
    G.add_weighted_edges_from(edge_list)

    selected_face_webp_by_label = _collect_best_selected_face_webp_by_label(
        _stem_to_label_from_graph(G.nodes())
    )

    # Node images: optimized manual crops (13 → node_faces_selected_optimized) take precedence;
    # else best-face
    # from DB (same scoring as earlier pipeline steps).
    label_to_person = df.groupby("label")["person_id"].first().to_dict()
    node_images = {}
    for label in G.nodes():
        person_id = label_to_person.get(label)
        if person_id is None:
            node_images[label] = None
            continue
        # No thumbnails for victim nodes in image_data.json (privacy).
        if label in victim_labels:
            node_images[label] = None
            continue
        basename = selected_face_webp_by_label.get(label)
        if basename:
            node_images[label] = [f"faces/{basename}", None]
            continue
        best_list = pick_best_images(con, person_id, n=10)
        for image_name, left, top, width, height in best_list:
            if image_name not in disallowed_images and image_name in safe_preview_images:
                bbox = [left, top, width, height]
                node_images[label] = [_to_webp_filename(image_name), bbox]
                break
        else:
            node_images[label] = None

    # Attach node attributes: degree_root_2, total image count, and category.
    degree_root_2 = {k: v ** 0.5 for k, v in dict(G.degree()).items()}
    degree_root_3 = {k: v ** (1/3.0) for k, v in dict(G.degree()).items()}
    nx.set_node_attributes(G=G, values=degree_root_2, name="degree_root_2")
    nx.set_node_attributes(G=G, values=degree_root_3, name="degree_root_3")
    nx.set_node_attributes(G=G, values=label_to_person, name="person_id")

    # Total number of distinct images associated with each person/label.
    total_images_by_label = (
        df.groupby("label")["image_name"].nunique().to_dict()
    )
    total_images_root_3 = {k : v ** (1/3.0) for k, v in total_images_by_label.items()}

    nx.set_node_attributes(G=G, values=total_images_by_label, name="total")
    nx.set_node_attributes(G=G, values=total_images_root_3, name="total_root_3")

    # Categories from the Matches sheet (column H).
    name_to_category = load_categories(gc)
    for vl in sorted(victim_labels):
        name_to_category[vl] = "Victim"

    # Only require an explicit category for non-generic names (victim labels filled above).
    required = {name for name in node_images.keys() if not name.startswith("person_")}
    missing = sorted(required - set(name_to_category.keys()))
    if missing:
        raise RuntimeError(
            f"The following node names are missing from the Matches sheet (category column): {', '.join(missing)}"
        )

    nx.set_node_attributes(G=G, values=name_to_category, name="category")
    nx.write_graphml(G, OUTPUT_GRAPHML)

    # Edge images: per edge, ranked by product of areas desc; pick highest-res image that is allowed.
    # edge = (a, b) where a < b alphabetically; bboxes are in the same order.
    edge_images = {}
    for edge, candidates in edge_candidates.items():
        a, b = edge
        candidates = sorted(candidates, key=lambda x: x[0], reverse=True)
        for _prod, img in candidates:
            # No edge preview images that depict victims (or any contains_victim image).
            if img in disallowed_images or img not in safe_preview_images:
                continue
            raw_a = face_bbox_lookup[(img, a)]
            raw_b = face_bbox_lookup[(img, b)]
            bbox_a = list(raw_a)
            bbox_b = list(raw_b)
            edge_images["-".join(edge)] = [_to_webp_filename(img), bbox_a, bbox_b]
            break
        else:
            edge_images["-".join(edge)] = None

    image_data = {
        "nodes": node_images,
        "edges": edge_images,
    }

    VIZ_DATA_DIR.mkdir(parents=True, exist_ok=True)
    with open(VIZ_DATA_DIR / "image_data.json", "w") as f:
        json.dump(image_data, f)
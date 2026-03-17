import sqlite3
import json
from collections import defaultdict
from pathlib import Path
from functools import cache
from shutil import copy, rmtree
from typing import Any

import pandas as pd
from itertools import combinations

import networkx as nx
from PIL import Image

from sheets_common import get_sheet_client, load_categories, load_names, load_ignore
from config import DB_PATH, IMAGE_DIR


MAX_RANK = 292

SCRIPT_DIR = Path(__file__).resolve().parent
FILTERED_IMAGE_DIR = SCRIPT_DIR.parent / "images"

OUTPUT_GRAPHML = SCRIPT_DIR.parent / "graphml" / "epstein_photo_people.graphml"


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


def _has_minor_face(age_low: Any, age_high: Any) -> bool:
    # Under 18: if either bound implies <18, treat as minor.
    try:
        if age_low is not None and int(age_low) < 18:
            return True
    except Exception:
        pass
    try:
        if age_high is not None and int(age_high) < 18:
            return True
    except Exception:
        pass
    return False


@cache
def get_image_dims(filename: str) -> tuple[int, int]:
    image = Image.open(IMAGE_DIR / filename)
    w, h = image.size
    image.close()
    return w, h


if __name__ == "__main__":
    if FILTERED_IMAGE_DIR.exists():
        rmtree(FILTERED_IMAGE_DIR)

    # Names and ignore list from the shared Google Spreadsheet (Matches / Ignore sheets).
    gc = get_sheet_client()
    PEOPLE_NAMES = load_names(gc)
    PEOPLE_TO_SKIP = load_ignore(gc)

    con = sqlite3.connect(DB_PATH)

    # Step 1: in SQL, get the MAX_RANK most common people (no ignore list here).
    top_ids_df = pd.read_sql_query(
        """
        SELECT person_id
        FROM faces
        WHERE person_id IS NOT NULL
        GROUP BY person_id
        ORDER BY COUNT(*) DESC
        LIMIT ?
        """,
        con,
        params=[MAX_RANK],
    )
    top_ids = set(top_ids_df["person_id"].tolist())
    if not top_ids:
        raise RuntimeError("No person_ids found for graph.")

    # Step 2: fetch all faces for those top_ids.
    placeholders_top = ",".join("?" for _ in top_ids)
    df = pd.read_sql_query(
        "SELECT f.person_id, f.image_name, f.left, f.top, f.width, f.height, "
        "f.celebrity_name, f.celebrity_confidence, "
        "f.age_range_low, f.age_range_high, "
        "i.moderation_result "
        f"FROM faces f "
        f"LEFT JOIN images i ON i.image_name = f.image_name "
        f"WHERE f.person_id IN ({placeholders_top})",
        con,
        params=list(top_ids),
    )

    # Step 3: apply ignore list AFTER rank selection.
    df = df[~df["person_id"].isin(PEOPLE_TO_SKIP)]

    # Disallow images that are explicit or contain any face under 18.
    df["is_explicit"] = df["moderation_result"].apply(_is_explicit_moderation)
    df["is_minor_face"] = df.apply(
        lambda r: _has_minor_face(r.get("age_range_low"), r.get("age_range_high")),
        axis=1,
    )
    disallowed_images = set(df.loc[df["is_explicit"] | df["is_minor_face"], "image_name"].tolist())

    # Attach image dimensions and face area (normalized box × pixels).
    df[["img_w", "img_h"]] = df["image_name"].apply(
        lambda fn: pd.Series(get_image_dims(fn))
    )
    df["face_area"] = df["width"] * df["height"] * df["img_w"] * df["img_h"]

    # Keep only the top MAX_RANK people by number of face appearances.
    counts = df.groupby("person_id").size().sort_values(ascending=False)
    top_ids = set(counts.head(MAX_RANK).index)
    df = df[df["person_id"].isin(top_ids)]

    # Resolve a display name per person_id:
    # 1) If the person_id is in PEOPLE_NAMES, ALWAYS use that (manual label wins).
    # 2) Otherwise, if any celebrity_name exists with confidence >= 95, use that.
    # 3) Otherwise, fall back to the raw person_id.
    celeb_df = df.dropna(subset=["celebrity_name"])
    celeb_df = celeb_df.sort_values("celebrity_confidence", ascending=False)
    # For each person, take (name, confidence) of the highest-confidence celebrity.
    celeb_best = (
        celeb_df.groupby("person_id")[["celebrity_name", "celebrity_confidence"]]
        .first()
        .to_dict(orient="index")
    )

    def resolve_name(pid: str) -> str:
        # Manual mapping takes priority over Rekognition celebrity labels.
        manual = PEOPLE_NAMES.get(pid)
        if manual is not None:
            return manual
        info = celeb_best.get(pid)
        if info is not None and info.get("celebrity_confidence", 0) >= 95.0:
            return info.get("celebrity_name", pid)
        return pid

    df["label"] = df["person_id"].apply(resolve_name)

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

    # Node images: per label, ranked by face_area desc; pick highest-res image that is not nude.
    node_candidates = (
        per_label_image.sort_values("face_area", ascending=False)
        .groupby("label", sort=False)["image_name"]
        .apply(list)
        .to_dict()
    )
    node_images = {}
    for label, filenames in node_candidates.items():
        for fn in filenames:
            if fn not in disallowed_images:
                node_images[label] = fn
                break
        else:
            node_images[label] = None

    # Attach node attributes: degree_root_2, total image count, and category.
    degree_root_2 = {k: v ** 0.5 for k, v in dict(G.degree()).items()}
    nx.set_node_attributes(G=G, values=degree_root_2, name="degree_root_2")

    # Total number of distinct images associated with each person/label.
    total_images_by_label = (
        df.groupby("label")["image_name"].nunique().to_dict()
    )
    nx.set_node_attributes(G=G, values=total_images_by_label, name="total")

    # Categories from the Matches sheet (column H).
    name_to_category = load_categories(gc)

    # Only require an explicit category for non-generic names.
    required = {name for name in node_images.keys() if not name.startswith("person_")}
    missing = sorted(required - set(name_to_category.keys()))
    if missing:
        raise RuntimeError(
            f"The following node names are missing from the Matches sheet (category column): {', '.join(missing)}"
        )

    nx.set_node_attributes(G=G, values=name_to_category, name="category")
    nx.write_graphml(G, OUTPUT_GRAPHML)

    # Edge images: per edge, ranked by product of areas desc; pick highest-res image that is allowed.
    edge_images = {}
    for edge, candidates in edge_candidates.items():
        # Sort by product descending (best first).
        candidates = sorted(candidates, key=lambda x: x[0], reverse=True)
        for _prod, img in candidates:
            if img not in disallowed_images:
                edge_images["-".join(edge)] = img
                break
        else:
            edge_images["-".join(edge)] = None

    FILTERED_IMAGE_DIR.mkdir(exist_ok=True)

    for filename in set(node_images.values()):
        if filename is not None:
            copy(IMAGE_DIR / filename, FILTERED_IMAGE_DIR / filename)

    for filename in set(edge_images.values()):
        if filename is not None:
            copy(IMAGE_DIR / filename, FILTERED_IMAGE_DIR / filename)

    image_data = {
        "nodes": node_images,
        "edges": edge_images,
    }

    with open(SCRIPT_DIR.parent / "image_data.json", "w") as f:
        json.dump(image_data, f)
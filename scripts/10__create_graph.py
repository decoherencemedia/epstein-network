import sqlite3
import json
from collections import defaultdict
from pathlib import Path
from shutil import copy, rmtree
from typing import Any

from PIL import Image

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
from config import DB_PATH, IMAGE_DIR
from faces_db import pick_best_images


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


def _extract_roll(index_face_record: str | None) -> float | None:
    if not index_face_record:
        return None
    return json.loads(index_face_record)["FaceDetail"]["Pose"]["Roll"]


def _rotation_for_image(rolls: list[float]) -> int:
    """Return rotation code to correct a sideways image (0, 90, or -90)."""
    if not rolls:
        return 0
    s = sorted(rolls)
    median = (s[len(s) // 2] + s[(len(s) - 1) // 2]) / 2
    if abs(median) < 45:
        return 0
    return -90 if median > 0 else 90


def _rotate_bbox(
    left: float, top: float, width: float, height: float, rotation: int
) -> tuple[float, float, float, float]:
    """Adjust normalized bbox for image rotation."""
    if rotation == 0:
        return (left, top, width, height)
    if rotation == -90:  # PIL ROTATE_90 (CCW): (x,y) -> (y, 1-x)
        return (top, 1 - left - width, height, width)
    if rotation == 90:   # PIL ROTATE_270 (CW): (x,y) -> (1-y, x)
        return (1 - top - height, left, height, width)
    raise ValueError(f"Unsupported rotation: {rotation}")


def _copy_image(src: Path, dst: Path, rotation: int) -> None:
    if rotation == 0:
        copy(src, dst)
        return
    transpose = Image.Transpose.ROTATE_90 if rotation == -90 else Image.Transpose.ROTATE_270
    with Image.open(src) as img:
        img.transpose(transpose).save(dst)


def _has_minor_face(age_low: Any, age_high: Any) -> bool:
    if not isinstance(age_low, int) or not isinstance(age_high, int):
        raise TypeError(f"age bounds must be int, got age_range_low={age_low!r}, age_range_high={age_high!r}")
    # Under 18: if either bound implies <18, treat as minor.
    if age_low < 18:
        return True
    if age_high < 18:
        return True
    return False


if __name__ == "__main__":
    if FILTERED_IMAGE_DIR.exists():
        rmtree(FILTERED_IMAGE_DIR)

    # Names and ignore list from the shared Google Spreadsheet (Matches / Ignore sheets).
    gc = get_sheet_client()
    PEOPLE_NAMES = load_names(gc)
    PEOPLE_TO_SKIP = load_ignore(gc)
    INCLUDE_PERSON_IDS = load_person_ids_matches_and_unknowns(gc)
    INCLUDE_PERSON_IDS = INCLUDE_PERSON_IDS - PEOPLE_TO_SKIP

    con = sqlite3.connect(DB_PATH)

    if not INCLUDE_PERSON_IDS:
        raise RuntimeError("No person_ids loaded from Matches/Unknowns sheets (after ignore list).")

    # Fetch all faces for those person_ids.
    placeholders_top = ",".join("?" for _ in INCLUDE_PERSON_IDS)
    df = pd.read_sql_query(
        "SELECT f.person_id, f.image_name, f.left, f.top, f.width, f.height, "
        "f.celebrity_name, f.celebrity_confidence, "
        "f.age_range_low, f.age_range_high, "
        "f.index_face_record, "
        "i.moderation_result, "
        "i.width_px, i.height_px "
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

    # Ground truth names from Matches sheet; unnamed person_ids keep their ID as label.
    df["label"] = df["person_id"].apply(lambda pid: PEOPLE_NAMES.get(pid) or pid)

    # Build per-image rotation correction from face Roll angles.
    image_rolls: dict[str, list[float]] = defaultdict(list)
    for _, row in df.iterrows():
        roll = _extract_roll(row.get("index_face_record"))
        if roll is not None:
            image_rolls[row["image_name"]].append(roll)
    image_rotation = {img: _rotation_for_image(rolls) for img, rolls in image_rolls.items()}

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

    # Node images: same best-face selection as 07 (size × quality, dHash diversity).
    label_to_person = df.groupby("label")["person_id"].first().to_dict()
    node_images = {}
    for label in G.nodes():
        person_id = label_to_person.get(label)
        if person_id is None:
            node_images[label] = None
            continue
        best_list = pick_best_images(con, person_id, n=10)
        for image_name, left, top, width, height in best_list:
            if image_name not in disallowed_images:
                rotation = image_rotation.get(image_name, 0)
                bbox = list(_rotate_bbox(left, top, width, height, rotation))
                node_images[label] = [image_name, bbox]
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
    # edge = (a, b) where a < b alphabetically; bboxes are in the same order.
    edge_images = {}
    for edge, candidates in edge_candidates.items():
        a, b = edge
        candidates = sorted(candidates, key=lambda x: x[0], reverse=True)
        for _prod, img in candidates:
            if img not in disallowed_images:
                rotation = image_rotation.get(img, 0)
                raw_a = face_bbox_lookup[(img, a)]
                raw_b = face_bbox_lookup[(img, b)]
                bbox_a = list(_rotate_bbox(*raw_a, rotation))
                bbox_b = list(_rotate_bbox(*raw_b, rotation))
                edge_images["-".join(edge)] = [img, bbox_a, bbox_b]
                break
        else:
            edge_images["-".join(edge)] = None

    FILTERED_IMAGE_DIR.mkdir(exist_ok=True)

    all_filenames = set()
    for v in node_images.values():
        if v is not None:
            all_filenames.add(v[0])
    for v in edge_images.values():
        if v is not None:
            all_filenames.add(v[0])

    rotated = sum(1 for f in all_filenames if image_rotation.get(f, 0) != 0)
    if rotated:
        print(f"Rotating {rotated} sideways image(s).")

    for filename in all_filenames:
        _copy_image(IMAGE_DIR / filename, FILTERED_IMAGE_DIR / filename, image_rotation.get(filename, 0))

    image_data = {
        "nodes": node_images,
        "edges": edge_images,
    }

    with open(SCRIPT_DIR / "image_data.json", "w") as f:
        json.dump(image_data, f)
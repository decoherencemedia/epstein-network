#!/usr/bin/env python3
"""
Map old person_id -> new person_id by matching face bounding boxes.

Assumes both SQLite DBs have a `faces` table with (at minimum):
  - face_id (TEXT)
  - image_name (TEXT)
  - left, top, width, height (REAL)   # Rekognition BoundingBox (normalized 0..1)
  - person_id (TEXT)                  # assigned by clustering step

Strategy:
  - Pick the top N old person_id values by number of faces.
  - For each old face row, look at new faces in the same image_name and pick the best bbox match
    using IoU (preferred) and a center-distance fallback.
  - Vote (weighted by IoU) for the matched new person_id.
  - Emit a mapping with confidence stats.
"""


import csv
import json
import math
import re
import sqlite3
import sys
from collections import Counter, defaultdict
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Iterable, Iterator

from PIL import Image

from epstein_photos.utils import normalized_box_iou
from epstein_photos.config import DB_PATH, IMAGE_DIR, SCRIPTS_DIR

_SCRIPTS = SCRIPTS_DIR
OLD_DB_PATH = _SCRIPTS / "faces_20260324__before_reclustering.db"
NEW_DB_PATH = DB_PATH
OUT_PREFIX: Path | None = _SCRIPTS / "person_id_map"  # writes .json + .csv; set None to print JSON

TOP_N = 400

# Old used `{document}-{number:0>4}`, new used `{document}-{number:0>5}`
OLD_IMAGE_PAD = 4
NEW_IMAGE_PAD = 5

MIN_IOU = 0.5
MAX_CENTER_DIST = 0.03
MAX_SIZE_RATIO = 2.5
MIN_VOTES = 5

# Verification image output
GENERATE_VERIFICATION_IMAGES = True
VERIFY_DIRNAME_SUFFIX = "_verify"  # appended to OUT_PREFIX name
VERIFY_PROGRESS_EVERY = 10  # print every N mapped pairs processed

# Set these to point at the two runs' image directories.
# If both runs used the same underlying images, these can be the same directory.
OLD_IMAGE_DIR = IMAGE_DIR
NEW_IMAGE_DIR = IMAGE_DIR

# The DB stores image_name strings; the filesystem may use different padding.
# In your case, the *new run* extracted files with 5 digits, so even old DB
# image_name values (4 digits) need to be translated to open the on-disk file.
OLD_IMAGE_FILES_PAD = NEW_IMAGE_PAD
NEW_IMAGE_FILES_PAD = NEW_IMAGE_PAD


@dataclass(frozen=True)
class BBox:
    left: float
    top: float
    width: float
    height: float

    @property
    def right(self) -> float:
        return self.left + self.width

    @property
    def bottom(self) -> float:
        return self.top + self.height

    @property
    def cx(self) -> float:
        return self.left + self.width / 2.0

    @property
    def cy(self) -> float:
        return self.top + self.height / 2.0

    @property
    def area(self) -> float:
        a = self.width * self.height
        return a if a > 0 else 0.0


def iou(a: BBox, b: BBox) -> float:
    return normalized_box_iou(
        a.left, a.top, a.width, a.height, b.left, b.top, b.width, b.height
    )


def center_distance(a: BBox, b: BBox) -> float:
    return math.hypot(a.cx - b.cx, a.cy - b.cy)


def size_ratio(a: BBox, b: BBox) -> float:
    # ratio in (0..inf); 1.0 means same area
    if a.area <= 0 or b.area <= 0:
        return float("inf")
    return max(a.area, b.area) / min(a.area, b.area)


def connect(db_path: Path) -> sqlite3.Connection:
    conn = sqlite3.connect(str(db_path))
    conn.row_factory = sqlite3.Row
    return conn


def require_faces_schema(conn: sqlite3.Connection, label: str) -> None:
    cur = conn.execute("PRAGMA table_info(faces)")
    cols = {row["name"] for row in cur.fetchall()}
    required = {"face_id", "image_name", "left", "top", "width", "height", "person_id"}
    missing = required - cols
    if missing:
        raise RuntimeError(
            f"{label} DB missing required faces columns: {sorted(missing)}. "
            f"Found: {sorted(cols)}"
        )


_IMAGE_NAME_RE = re.compile(r"^(?P<prefix>.+)-(?P<num>\d+)(?P<ext>\.[^.]+)?$")


def translate_image_name_padding(image_name: str, *, from_pad: int, to_pad: int) -> str:
    """
    Convert `{document}-{number:0>from_pad}{ext}` -> `{document}-{number:0>to_pad}{ext}`.

    Only applies when the suffix is purely digits and has length == from_pad.
    If it already has to_pad digits (or any other format), returns unchanged.
    """
    if from_pad == to_pad:
        return image_name
    m = _IMAGE_NAME_RE.match(image_name)
    if not m:
        return image_name
    num = m.group("num")
    if len(num) != from_pad:
        return image_name
    prefix = m.group("prefix")
    ext = m.group("ext") or ""
    return f"{prefix}-{str(int(num)).zfill(to_pad)}{ext}"


def iter_faces_for_person_ids(
    conn: sqlite3.Connection, person_ids: Iterable[str]
) -> Iterator[sqlite3.Row]:
    person_ids = list(person_ids)
    if not person_ids:
        return iter(())
    placeholders = ",".join(["?"] * len(person_ids))
    q = f"""
        SELECT face_id, image_name, left, top, width, height, person_id
        FROM faces
        WHERE person_id IN ({placeholders})
    """
    cur = conn.execute(q, person_ids)
    yield from cur.fetchall()


def top_person_ids(conn: sqlite3.Connection, n: int) -> list[str]:
    cur = conn.execute(
        """
        SELECT person_id, COUNT(*) AS cnt
        FROM faces
        WHERE person_id IS NOT NULL AND person_id != ''
        GROUP BY person_id
        ORDER BY cnt DESC
        LIMIT ?
        """,
        (n,),
    )
    return [row["person_id"] for row in cur.fetchall()]


def build_new_index(
    conn_new: sqlite3.Connection,
    *,
    only_images: set[str] | None = None,
) -> dict[str, list[tuple[str, str, BBox]]]:
    """
    Returns: image_name -> list of (face_id, person_id, bbox)
    """
    params: list[Any] = []
    where = ["person_id IS NOT NULL", "person_id != ''"]
    if only_images:
        placeholders = ",".join(["?"] * len(only_images))
        where.append(f"image_name IN ({placeholders})")
        params.extend(sorted(only_images))
    where_sql = " AND ".join(where)

    cur = conn_new.execute(
        f"""
        SELECT face_id, person_id, image_name, left, top, width, height
        FROM faces
        WHERE {where_sql}
        """,
        params,
    )

    by_image: dict[str, list[tuple[str, str, BBox]]] = defaultdict(list)
    for row in cur.fetchall():
        by_image[row["image_name"]].append(
            (
                row["face_id"],
                row["person_id"],
                BBox(
                    float(row["left"]),
                    float(row["top"]),
                    float(row["width"]),
                    float(row["height"]),
                ),
            )
        )
    return dict(by_image)


def best_match_in_image(
    old_bbox: BBox,
    candidates: list[tuple[str, str, BBox]],
    *,
    min_iou: float,
    max_center_dist: float,
    max_size_ratio: float,
) -> tuple[str, str, float] | None:
    """
    Returns best (new_face_id, new_person_id, match_score) or None.
    match_score is IoU when IoU-based match passes; otherwise 0 (center-distance based match).
    """
    best: tuple[str, str, float] | None = None
    best_iou = 0.0
    best_center: tuple[str, str, float] | None = None
    best_dist = float("inf")

    for new_face_id, new_person_id, new_bbox in candidates:
        if size_ratio(old_bbox, new_bbox) > max_size_ratio:
            continue
        this_iou = iou(old_bbox, new_bbox)
        if this_iou > best_iou:
            best_iou = this_iou
            best = (new_face_id, new_person_id, this_iou)
        d = center_distance(old_bbox, new_bbox)
        if d < best_dist:
            best_dist = d
            best_center = (new_face_id, new_person_id, d)

    if best is not None and best_iou >= min_iou:
        return best

    if best_center is not None and best_center[2] <= max_center_dist:
        new_face_id, new_person_id, _d = best_center
        return (new_face_id, new_person_id, 0.0)

    return None


def _clamp(v: float, lo: float, hi: float) -> float:
    return lo if v < lo else hi if v > hi else v


def crop_face(image_path: Path, bbox: BBox) -> tuple[Image.Image, int]:
    """
    Crop bbox from image (bbox in normalized 0..1 coords).

    Returns: (cropped_image_rgb, pixel_area)
    """
    if not image_path.is_file():
        raise FileNotFoundError(f"Missing image file: {image_path}")

    with Image.open(image_path) as im:
        im = im.convert("RGB")
        w, h = im.size
        x1 = int(round(_clamp(bbox.left, 0.0, 1.0) * w))
        y1 = int(round(_clamp(bbox.top, 0.0, 1.0) * h))
        x2 = int(round(_clamp(bbox.right, 0.0, 1.0) * w))
        y2 = int(round(_clamp(bbox.bottom, 0.0, 1.0) * h))
        # Ensure non-empty crop
        if x2 <= x1:
            x2 = min(w, x1 + 1)
        if y2 <= y1:
            y2 = min(h, y1 + 1)
        crop = im.crop((x1, y1, x2, y2))
        area = max(0, x2 - x1) * max(0, y2 - y1)
        return crop, area


def concat_horiz(a: Image.Image, b: Image.Image) -> Image.Image:
    """Concatenate two RGB images side-by-side, vertically centered."""
    a = a.convert("RGB")
    b = b.convert("RGB")
    out_w = a.size[0] + b.size[0]
    out_h = max(a.size[1], b.size[1])
    out = Image.new("RGB", (out_w, out_h), (0, 0, 0))
    ay = (out_h - a.size[1]) // 2
    by = (out_h - b.size[1]) // 2
    out.paste(a, (0, ay))
    out.paste(b, (a.size[0], by))
    return out


def _safe_filename_component(s: str) -> str:
    return re.sub(r"[^A-Za-z0-9_.-]+", "_", s)


def _get_image_size_cached(
    cache: dict[tuple[Path, str], tuple[int, int]], image_dir: Path, image_name: str
) -> tuple[int, int]:
    key = (image_dir, image_name)
    if key in cache:
        return cache[key]
    path = image_dir / image_name
    if not path.is_file():
        raise FileNotFoundError(f"Missing image file: {path}")
    with Image.open(path) as im:
        w, h = im.size
    cache[key] = (w, h)
    return w, h


def _iter_faces_for_person_id(conn: sqlite3.Connection, person_id: str) -> Iterator[sqlite3.Row]:
    cur = conn.execute(
        """
        SELECT face_id, image_name, left, top, width, height, person_id
        FROM faces
        WHERE person_id = ?
        """,
        (person_id,),
    )
    yield from cur.fetchall()


def _largest_face_row(
    conn: sqlite3.Connection,
    person_id: str,
    *,
    image_dir: Path,
    db_image_pad: int,
    files_image_pad: int,
    image_size_cache: dict[tuple[Path, str], tuple[int, int]],
) -> tuple[str, BBox, int]:
    """
    Find the face row for this person_id with the largest pixel area:
      bbox_pixel_area = (bbox_norm_area) * (image_w * image_h)

    Returns: (image_name_on_disk, bbox, pixel_area)
    """
    best: tuple[str, BBox, int] | None = None
    best_area = -1

    for row in _iter_faces_for_person_id(conn, person_id):
        image_name_db = row["image_name"]
        image_name_on_disk = translate_image_name_padding(
            image_name_db, from_pad=db_image_pad, to_pad=files_image_pad
        )
        bbox = BBox(
            float(row["left"]),
            float(row["top"]),
            float(row["width"]),
            float(row["height"]),
        )
        w, h = _get_image_size_cached(image_size_cache, image_dir, image_name_on_disk)
        area = int(round(bbox.area * (w * h)))
        if area > best_area:
            best_area = area
            best = (image_name_on_disk, bbox, area)

    if best is None:
        raise RuntimeError(f"No faces found for person_id={person_id!r}")
    return best


def write_verification_images(
    report: dict[str, Any],
    conn_old: sqlite3.Connection,
    conn_new: sqlite3.Connection,
    *,
    out_prefix: Path,
    old_image_dir: Path,
    new_image_dir: Path,
    old_image_pad: int,
    new_image_pad: int,
) -> None:
    if not old_image_dir.is_dir():
        raise FileNotFoundError(f"OLD_IMAGE_DIR not found: {old_image_dir}")
    if not new_image_dir.is_dir():
        raise FileNotFoundError(f"NEW_IMAGE_DIR not found: {new_image_dir}")

    verify_dir = out_prefix.with_name(out_prefix.name + VERIFY_DIRNAME_SUFFIX)
    verify_dir.mkdir(parents=True, exist_ok=True)

    # Build a new index for relevant images (translated to new padding).
    mapped = [m for m in report["mappings"] if m.get("status") == "mapped" and m.get("new_person_id")]
    if not mapped:
        print("No mapped person_id pairs; skipping verification images.", flush=True)
        return

    print(f"Writing verification images to: {verify_dir}", flush=True)
    print(f"Mapped pairs to verify: {len(mapped)}", flush=True)
    print(f"OLD_IMAGE_DIR: {old_image_dir}", flush=True)
    print(f"NEW_IMAGE_DIR: {new_image_dir}", flush=True)

    image_size_cache: dict[tuple[Path, str], tuple[int, int]] = {}

    for rank, m in enumerate(mapped, start=1):
        old_pid = m["old_person_id"]
        new_pid_target = m["new_person_id"]

        if VERIFY_PROGRESS_EVERY > 0 and (rank == 1 or rank % VERIFY_PROGRESS_EVERY == 0):
            print(f"[{rank}/{len(mapped)}] {old_pid} -> {new_pid_target}", flush=True)

        old_image_name, old_bbox, _old_area = _largest_face_row(
            conn_old,
            old_pid,
            image_dir=old_image_dir,
            db_image_pad=old_image_pad,
            files_image_pad=OLD_IMAGE_FILES_PAD,
            image_size_cache=image_size_cache,
        )
        new_image_name, new_bbox, _new_area = _largest_face_row(
            conn_new,
            new_pid_target,
            image_dir=new_image_dir,
            db_image_pad=new_image_pad,
            files_image_pad=NEW_IMAGE_FILES_PAD,
            image_size_cache=image_size_cache,
        )

        old_crop, _ = crop_face(old_image_dir / old_image_name, old_bbox)
        new_crop, _ = crop_face(new_image_dir / new_image_name, new_bbox)
        out_img = concat_horiz(old_crop, new_crop)
        out_name = (
            f"{rank:03d}__{_safe_filename_component(old_pid)}__{_safe_filename_component(new_pid_target)}.jpg"
        )
        out_path = verify_dir / out_name
        out_img.save(out_path, format="JPEG", quality=92, optimize=True)
        if VERIFY_PROGRESS_EVERY > 0 and (rank == 1 or rank % VERIFY_PROGRESS_EVERY == 0):
            print(f"  wrote {out_path.name}", flush=True)


def map_person_ids(
    conn_old: sqlite3.Connection,
    conn_new: sqlite3.Connection,
    *,
    n: int,
    old_image_pad: int,
    new_image_pad: int,
    min_iou: float,
    max_center_dist: float,
    max_size_ratio: float,
    min_votes: int,
) -> dict[str, Any]:
    old_person_ids = top_person_ids(conn_old, n)
    if not old_person_ids:
        raise RuntimeError("No non-empty person_id values found in old DB faces table.")

    # Limit new-index to only images present among the old top-N persons for speed.
    cur = conn_old.execute(
        f"""
        SELECT DISTINCT image_name
        FROM faces
        WHERE person_id IN ({",".join(["?"] * len(old_person_ids))})
        """,
        old_person_ids,
    )
    relevant_images_old = {row["image_name"] for row in cur.fetchall()}
    relevant_images_new = {
        translate_image_name_padding(name, from_pad=old_image_pad, to_pad=new_image_pad)
        for name in relevant_images_old
    }
    new_by_image = build_new_index(conn_new, only_images=relevant_images_new)

    # Gather old faces for those people.
    old_faces = list(iter_faces_for_person_ids(conn_old, old_person_ids))
    old_faces_by_person: dict[str, list[sqlite3.Row]] = defaultdict(list)
    for row in old_faces:
        old_faces_by_person[row["person_id"]].append(row)

    results: dict[str, Any] = {
        "params": {
            "n": n,
            "old_image_pad": old_image_pad,
            "new_image_pad": new_image_pad,
            "min_iou": min_iou,
            "max_center_dist": max_center_dist,
            "max_size_ratio": max_size_ratio,
            "min_votes": min_votes,
        },
        "mappings": [],
    }

    for old_pid in old_person_ids:
        votes_weighted: Counter[str] = Counter()
        votes_raw: Counter[str] = Counter()
        matches = 0
        unmatched = 0
        iou_scores: list[float] = []

        for row in old_faces_by_person.get(old_pid, []):
            image_name_old = row["image_name"]
            image_name_new = translate_image_name_padding(
                image_name_old, from_pad=old_image_pad, to_pad=new_image_pad
            )
            candidates = new_by_image.get(image_name_new)
            if not candidates:
                unmatched += 1
                continue

            old_bbox = BBox(
                float(row["left"]),
                float(row["top"]),
                float(row["width"]),
                float(row["height"]),
            )
            m = best_match_in_image(
                old_bbox,
                candidates,
                min_iou=min_iou,
                max_center_dist=max_center_dist,
                max_size_ratio=max_size_ratio,
            )
            if m is None:
                unmatched += 1
                continue
            _new_face_id, new_pid, score = m
            matches += 1
            votes_raw[new_pid] += 1
            # Weight IoU matches; center-distance fallback contributes a small constant.
            w = score if score > 0 else 0.05
            votes_weighted[new_pid] += w
            if score > 0:
                iou_scores.append(score)

        if matches < min_votes or not votes_raw:
            results["mappings"].append(
                {
                    "old_person_id": old_pid,
                    "new_person_id": None,
                    "status": "insufficient_matches",
                    "old_face_count": len(old_faces_by_person.get(old_pid, [])),
                    "matched_faces": matches,
                    "unmatched_faces": unmatched,
                    "top_candidates": votes_raw.most_common(5),
                }
            )
            continue

        new_pid_best, raw_votes_best = votes_raw.most_common(1)[0]
        weighted_best = float(votes_weighted[new_pid_best])
        raw_total = sum(votes_raw.values())
        confidence = raw_votes_best / raw_total if raw_total else 0.0
        avg_iou = (sum(iou_scores) / len(iou_scores)) if iou_scores else None

        results["mappings"].append(
            {
                "old_person_id": old_pid,
                "new_person_id": new_pid_best,
                "status": "mapped",
                "old_face_count": len(old_faces_by_person.get(old_pid, [])),
                "matched_faces": matches,
                "unmatched_faces": unmatched,
                "raw_votes_best": raw_votes_best,
                "raw_votes_total": raw_total,
                "confidence_raw": confidence,
                "weighted_score_best": weighted_best,
                "avg_iou": avg_iou,
                "top_candidates": votes_raw.most_common(5),
            }
        )

    return results


def write_outputs(report: dict[str, Any], out_prefix: Path | None) -> None:
    mappings = report["mappings"]
    simple_map = {m["old_person_id"]: m.get("new_person_id") for m in mappings}
    if out_prefix is None:
        print(json.dumps(simple_map, indent=2, sort_keys=False))
        return

    out_prefix.parent.mkdir(parents=True, exist_ok=True)
    json_path = out_prefix.with_suffix(".json")
    csv_path = out_prefix.with_suffix(".csv")
    report_json_path = out_prefix.with_name(out_prefix.name + "_report").with_suffix(".json")

    # Primary output: old_person_id -> new_person_id (or null if insufficient matches).
    json_path.write_text(
        json.dumps(simple_map, indent=2, sort_keys=False) + "\n", encoding="utf-8"
    )
    # Extra debug output: full votes/stats per old_person_id.
    report_json_path.write_text(
        json.dumps(report, indent=2, sort_keys=False) + "\n", encoding="utf-8"
    )

    with csv_path.open("w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(
            [
                "old_person_id",
                "new_person_id",
                "status",
                "old_face_count",
                "matched_faces",
                "unmatched_faces",
                "confidence_raw",
                "avg_iou",
                "top_candidates",
            ]
        )
        for m in mappings:
            w.writerow(
                [
                    m.get("old_person_id"),
                    m.get("new_person_id"),
                    m.get("status"),
                    m.get("old_face_count"),
                    m.get("matched_faces"),
                    m.get("unmatched_faces"),
                    m.get("confidence_raw"),
                    m.get("avg_iou"),
                    json.dumps(m.get("top_candidates"), ensure_ascii=False),
                ]
            )

    print(f"Wrote {json_path}")
    print(f"Wrote {report_json_path}")
    print(f"Wrote {csv_path}")


def _resolve_paths() -> tuple[Path, Path, Path | None]:
    """
    Policy: prefer constants, but allow overriding via 2-3 positional args:
      map_person_ids.py [old_db] [new_db] [out_prefix]
    """
    old_db = OLD_DB_PATH
    new_db = NEW_DB_PATH
    out_prefix = OUT_PREFIX

    argv = sys.argv[1:]
    if len(argv) in (1,) or len(argv) > 3:
        raise ValueError(
            "Usage: python3 scripts/map_person_ids.py [old_db] [new_db] [out_prefix]\n"
            "Or edit constants at top of file.\n"
            "Examples:\n"
            "  python3 scripts/map_person_ids.py\n"
            "  python3 scripts/map_person_ids.py old.db new.db person_id_map\n"
        )
    if len(argv) >= 2:
        old_db = Path(argv[0])
        new_db = Path(argv[1])
    if len(argv) == 3:
        out_prefix = Path(argv[2]) if argv[2].lower() != "none" else None

    # Resolve paths relative to network/scripts/ (not this file's remapping/ dir).
    scripts_dir = SCRIPTS_DIR
    if not old_db.is_absolute():
        old_db = (scripts_dir / old_db).resolve()
    if not new_db.is_absolute():
        new_db = (scripts_dir / new_db).resolve()
    if out_prefix is not None and not out_prefix.is_absolute():
        out_prefix = (scripts_dir / out_prefix).resolve()

    return old_db, new_db, out_prefix


def main() -> None:
    old_db, new_db, out_prefix = _resolve_paths()
    if not old_db.is_file():
        raise FileNotFoundError(f"Old DB not found: {old_db}")
    if not new_db.is_file():
        raise FileNotFoundError(f"New DB not found: {new_db}")

    conn_old = connect(old_db)
    conn_new = connect(new_db)
    try:
        require_faces_schema(conn_old, "Old")
        require_faces_schema(conn_new, "New")
        report = map_person_ids(
            conn_old,
            conn_new,
            n=TOP_N,
            old_image_pad=OLD_IMAGE_PAD,
            new_image_pad=NEW_IMAGE_PAD,
            min_iou=MIN_IOU,
            max_center_dist=MAX_CENTER_DIST,
            max_size_ratio=MAX_SIZE_RATIO,
            min_votes=MIN_VOTES,
        )
        if GENERATE_VERIFICATION_IMAGES and out_prefix is not None:
            write_verification_images(
                report,
                conn_old,
                conn_new,
                out_prefix=out_prefix,
                old_image_dir=OLD_IMAGE_DIR,
                new_image_dir=NEW_IMAGE_DIR,
                old_image_pad=OLD_IMAGE_PAD,
                new_image_pad=NEW_IMAGE_PAD,
            )
    finally:
        conn_old.close()
        conn_new.close()

    write_outputs(report, out_prefix)


if __name__ == "__main__":
    main()

"""
Build a single image atlas from many small images (e.g. node face thumbnails).

This script is intentionally single-purpose and dataset-specific:
- Reads images from `INPUT_DIR`
- Reads node labels from `DATASET_PATH`
- Writes:
  - `OUTPUT_DIR/atlas.webp`
  - `OUTPUT_DIR/atlas_manifest.json`

The manifest keys are node labels (so the frontend can use `d.label`).

Exports under ``node_faces*`` use **sheet display names** in filenames (see ``12__export_node_faces``),
not anonymized graph labels like ``Victim 1``. This script therefore skips any file whose
sanitized stem matches a ``person_id`` with ``people.is_victim = 1`` (same stem rule as export),
and still skips manifest keys whose label matches ``Victim N`` when those rows map through
``dataset.json``.
"""
from __future__ import annotations

import json
import re
import sqlite3
from pathlib import Path

from PIL import Image

from config import DB_PATH
from sheets_common import get_sheet_client, load_names


SCRIPT_DIR = Path(__file__).resolve().parent
REPO_DIR = SCRIPT_DIR.parent

# Run configuration (repo-root paths)
INPUT_DIR = REPO_DIR / "images" / "node_faces_optimized"
VICTIM_IMAGE = INPUT_DIR / "victim.png",
OUTPUT_DIR = REPO_DIR / "images"
VIZ_DATA_DIR = REPO_DIR / "viz_data"
DATASET_PATH = VIZ_DATA_DIR / "dataset.json"

# Atlas layout configuration
CELL_SIZE_PX = 100
PADDING_PX = 0
ATLAS_FORMAT = "webp"  # keep constant: this is a small pipeline helper

# Supported image extensions
IMAGE_EXTENSIONS = {".jpg", ".jpeg", ".png", ".webp", ".gif"}

FACE_IDX_SUFFIX_RE = re.compile(r"^(.*)__(\d{3})$")
# Matches anonymized victim labels from 10__create_graph.py (e.g. "Victim 1").
VICTIM_LABEL_RE = re.compile(r"^Victim \d+$")


def is_victim_label(label: str) -> bool:
    return bool(VICTIM_LABEL_RE.match(label.strip()))


def sanitize_label_for_filename(label: str) -> str:
    """Match 11__export_node_faces: safe filename stem from node label."""
    s = "".join(c if c.isalnum() or c in "._- " else "" for c in label)
    return s.strip().replace(" ", "_").replace("/", "_") or "node"


def load_victim_sanitized_stems(conn: sqlite3.Connection, gc) -> set[str]:
    """
    Stems used in ``12__export_node_faces`` file names: sanitize(Matches name) or person_id.
    Same person_ids as ``people.is_victim = 1`` must be excluded from the atlas even when
    graph labels are anonymized.
    """
    names = load_names(gc)
    cur = conn.execute("SELECT person_id FROM people WHERE is_victim = 1")
    stems: set[str] = set()
    for (pid,) in cur.fetchall():
        pid_s = str(pid).strip()
        if not pid_s:
            continue
        display = names.get(pid_s) or pid_s
        stems.add(sanitize_label_for_filename(str(display)))
    return stems


def load_dataset_labels(dataset_path: Path) -> dict[str, str] | None:
    """
    Load dataset.json and return mapping from filename_stem -> label
    so we can key the manifest by label.
    """
    if not dataset_path.is_file():
        raise FileNotFoundError(f"DATASET_PATH not found: {dataset_path}")
    with open(dataset_path, encoding="utf-8") as f:
        data = json.load(f)
    nodes = data.get("nodes") or []
    stem_to_label: dict[str, str] = {}
    for n in nodes:
        label = n.get("label")
        if label is None:
            continue
        stem = sanitize_label_for_filename(str(label))
        stem_to_label[stem] = str(label)
    return stem_to_label


def collect_images(input_dir: Path, extensions: set[str]) -> list[tuple[Path, str]]:
    """Return list of (path, stem) for each image file."""
    if not input_dir.is_dir():
        raise FileNotFoundError(f"INPUT_DIR not found: {input_dir}")
    out: list[tuple[Path, str]] = []
    for p in sorted(input_dir.iterdir()):
        if p.suffix.lower() in extensions:
            out.append((p, p.stem))
    return out


def resolve_victim_image_path() -> Path | None:
    """Return victim placeholder image path if present in known locations."""
    return INPUT_DIR / "victim.webp"


def build_atlas(
    image_list: list[tuple[Path, str]],
    cell_size: int,
    padding: int,
    stem_to_label: dict[str, str] | None,
    output_dir: Path,
    atlas_format: str,
    victim_sanitized_stems: set[str],
    victim_image_path: Path | None,
) -> tuple[Path, Path]:
    """
    Pack images into a grid atlas. All cells are cell_size x cell_size (images
    are resized to fit). Returns (atlas_path, manifest_path).
    """
    if stem_to_label is None:
        raise ValueError("stem_to_label must be provided to build_atlas()")

    if not image_list:
        raise ValueError("No images to pack")

    # First map image files to graph node labels and pick one best image per label.
    unmatched_inputs: list[str] = []
    victim_skipped_files: list[str] = []
    best_by_label: dict[str, tuple[int, Path]] = {}
    for path, stem in image_list:
        base_stem = stem
        face_idx = 0
        m = FACE_IDX_SUFFIX_RE.match(stem)
        if m:
            base_stem = m.group(1)
            face_idx = int(m.group(2))

        # Filenames use sheet names (e.g. ``Jane_Doe__000``), not graph labels (``Victim_1``).
        if base_stem in victim_sanitized_stems:
            victim_skipped_files.append(path.name)
            continue

        key = stem_to_label.get(base_stem)
        if key is None:
            unmatched_inputs.append(path.name)
            continue
        if is_victim_label(key):
            victim_skipped_files.append(path.name)
            continue

        prev = best_by_label.get(key)
        if prev is None or face_idx < prev[0]:
            # Prefer the best face for each node base-stem (lowest `__NNN`).
            best_by_label[key] = (face_idx, path)

    selected: list[tuple[str, Path]] = [
        (label, best_by_label[label][1]) for label in sorted(best_by_label.keys())
    ]
    victim_labels_present = sorted({lbl for lbl in stem_to_label.values() if is_victim_label(lbl)})
    if victim_labels_present:
        if victim_image_path is None:
            raise FileNotFoundError(
                "Victim labels exist in dataset, but victim placeholder image was not found. "
                f"Tried: {', '.join(str(p) for p in VICTIM_IMAGE_CANDIDATES)}"
            )
        # Pack one placeholder tile; all victim labels will point to this same atlas cell.
        selected.append(("__victim_placeholder__", victim_image_path))
    if not selected:
        raise ValueError(
            "No mappable face images for graph nodes (all inputs were unmatched, victim-only, or filtered)."
        )

    n = len(selected)
    # Grid: aim for roughly square
    cols = max(1, int(n**0.5 + 0.5))
    rows = (n + cols - 1) // cols

    atlas_w = cols * (cell_size + padding) + padding
    atlas_h = rows * (cell_size + padding) + padding

    atlas = Image.new("RGBA", (atlas_w, atlas_h), (0, 0, 0, 0))
    manifest: dict[str, dict[str, int]] = {}

    victim_cell: dict[str, int] | None = None
    for idx, (label, path) in enumerate(selected):
        row, col = divmod(idx, cols)
        px = padding + col * (cell_size + padding)
        py = padding + row * (cell_size + padding)

        with Image.open(path) as im:
            im = im.convert("RGBA")
            # Resize to fit cell (letterbox if not square)
            im.thumbnail((cell_size, cell_size), Image.Resampling.LANCZOS)
            w, h = im.size
            # Center in cell
            x = px + (cell_size - w) // 2
            y = py + (cell_size - h) // 2
            atlas.paste(im, (x, y), im if im.mode == "RGBA" else None)

        cell = {"x": px, "y": py, "w": cell_size, "h": cell_size}
        if label == "__victim_placeholder__":
            victim_cell = cell
        else:
            manifest[label] = cell

    if victim_cell is not None:
        for lbl in victim_labels_present:
            manifest[lbl] = dict(victim_cell)

    output_dir.mkdir(parents=True, exist_ok=True)
    atlas_name = f"atlas.{atlas_format}"
    atlas_path = output_dir / atlas_name
    if atlas_format.lower() == "webp":
        atlas.save(atlas_path, "WEBP", quality=90, method=6)
    else:
        atlas.save(atlas_path, "PNG")

    VIZ_DATA_DIR.mkdir(parents=True, exist_ok=True)
    manifest_path = VIZ_DATA_DIR / "atlas_manifest.json"
    with open(manifest_path, "w", encoding="utf-8") as f:
        json.dump(manifest, f)

    # Warn when graph nodes are missing a corresponding face in the atlas (victim labels excluded by design).
    if victim_skipped_files:
        victim_skipped_sorted = sorted(victim_skipped_files)
        print(
            f"WARNING: {len(victim_skipped_sorted)} face image(s) match victim person_id(s) "
            "(people.is_victim / sheet name stem) or anonymized Victim N labels; "
            "excluded from the atlas:"
        )
        for fn in victim_skipped_sorted:
            print(f"  - {fn}")

    expected_labels = (
        {lbl for lbl in stem_to_label.values() if not is_victim_label(lbl)}
        if stem_to_label is not None
        else set()
    )
    missing_labels = sorted(expected_labels - set(manifest.keys()))
    if missing_labels:
        print(f"WARNING: {len(missing_labels)} graph node(s) have no atlas face:")
        for label in missing_labels:
            print(f"  - {label}")
    elif expected_labels:
        print("All graph nodes have atlas faces.")

    # Secondary warning: face files that did not map to any graph node label.
    if unmatched_inputs:
        unmatched_inputs_sorted = sorted(unmatched_inputs)
        print(f"WARNING: {len(unmatched_inputs)} input face file(s) were not mapped to graph nodes:")
        for filename in unmatched_inputs_sorted:
            print(f"  - {filename}")

    return atlas_path, manifest_path


def main() -> None:
    stem_to_label = load_dataset_labels(DATASET_PATH)
    image_list = collect_images(INPUT_DIR, IMAGE_EXTENSIONS)
    if not image_list:
        raise ValueError(
            f"No images found in INPUT_DIR={INPUT_DIR} (extensions: {sorted(IMAGE_EXTENSIONS)})"
        )

    conn = sqlite3.connect(DB_PATH)
    try:
        victim_stems = load_victim_sanitized_stems(conn, get_sheet_client())
    finally:
        conn.close()
    victim_image_path = resolve_victim_image_path()

    atlas_path, manifest_path = build_atlas(
        image_list=image_list,
        cell_size=CELL_SIZE_PX,
        padding=PADDING_PX,
        stem_to_label=stem_to_label,
        output_dir=OUTPUT_DIR,
        atlas_format=ATLAS_FORMAT,
        victim_sanitized_stems=victim_stems,
        victim_image_path=victim_image_path,
    )
    print(f"Atlas: {atlas_path} ({len(image_list)} images)")
    print(f"Manifest: {manifest_path}")


if __name__ == "__main__":
    main()

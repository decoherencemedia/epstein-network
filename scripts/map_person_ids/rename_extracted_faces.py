#!/usr/bin/env python3
"""
Rename new extracted_faces folders using:
  - Human-curated names (and IGNORE) from the old extracted_faces folder
  - The old->new person_id mapping (person_id_map.json)

Old folders (reference):  000__person_2__Jeffrey_Epstein, 018__person_251__Lady_Victoria_Hervey__IGNORE
New folders (08 pipeline): 037__person_30, 042__person_55__Some_Celeb

We keep the NEW folder's rank (do not change it). We only add/replace the name and IGNORE from the old folder.

Example:
  new folder:   037__person_30
  mapping:      person_2 -> person_30  =>  old folder had person_2, name "Jeffrey_Epstein"
  renamed to:   037__person_30__Jeffrey_Epstein   (rank 037 unchanged)
"""

from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path
from typing import Optional


# CONFIG (edit these to your environment)

SCRIPT_DIR = Path(__file__).resolve().parent

# Old run: folders with human-curated names / ranks.
OLD_EXTRACTED_DIR = SCRIPT_DIR.parent.parent / "extracted_faces__intitial"

# New run: folders created by the current pipeline (mostly person_id only).
NEW_EXTRACTED_DIR = SCRIPT_DIR.parent.parent / "extracted_faces"

# Mapping JSON: old_person_id -> new_person_id (as produced by map_person_ids.py)
PERSON_ID_MAP_JSON = SCRIPT_DIR / "person_id_map.json"

# Safety: start as dry-run to see what would be renamed.
DRY_RUN = False


@dataclass(frozen=True)
class FolderInfo:
    basename: str
    rank: int
    person_id: str
    name: Optional[str]
    to_ignore: bool


def process_basename(basename: str) -> FolderInfo:
    """
    Mirror of standardize_lists.process_basename, but typed and reusable here.
    """
    parts = basename.split("__")
    to_ignore = parts[-1] == "IGNORE"

    match parts:
        case [rank, person_id, "IGNORE"]:
            name = None
        case [rank, person_id, name, "IGNORE"]:
            name = None
        case [rank, person_id]:
            name = None
        case [rank, person_id, name]:
            name = name
        case _:
            raise ValueError(f"Folder name not structured as expected: {basename!r}")

    return FolderInfo(
        basename=basename,
        rank=int(rank),
        person_id=person_id,
        name=name,
        to_ignore=to_ignore,
    )


def build_new_basename(new_rank: int, new_person_id: str, name: Optional[str], to_ignore: bool) -> str:
    """Keep new_rank (from new folder); add name and IGNORE from old folder."""
    rank_str = f"{new_rank:03d}"
    pieces: list[str] = [rank_str, new_person_id]
    if name is not None:
        pieces.append(name)
    if to_ignore:
        pieces.append("IGNORE")
    return "__".join(pieces)


def parse_new_folder_basename(basename: str) -> tuple[int, str]:
    """Parse new pipeline folder name: NNN__person_XXX or NNN__person_XXX__rest. Returns (rank, person_id)."""
    parts = basename.split("__")
    if len(parts) < 2:
        raise ValueError(f"New folder name must be at least rank__person_id: {basename!r}")
    rank_s = parts[0]
    person_id = parts[1]
    if not rank_s.isdigit():
        raise ValueError(f"New folder rank must be numeric: {basename!r}")
    return int(rank_s), person_id


def load_person_id_map(path: Path) -> dict[str, str | None]:
    if not path.is_file():
        raise FileNotFoundError(f"Mapping JSON not found: {path}")
    with path.open("r", encoding="utf-8") as f:
        data = json.load(f)
    if not isinstance(data, dict):
        raise TypeError(f"Expected mapping JSON to be an object, got {type(data).__name__}")
    return data


def main() -> None:
    if not OLD_EXTRACTED_DIR.is_dir():
        raise FileNotFoundError(f"OLD_EXTRACTED_DIR not found: {OLD_EXTRACTED_DIR}")
    if not NEW_EXTRACTED_DIR.is_dir():
        raise FileNotFoundError(f"NEW_EXTRACTED_DIR not found: {NEW_EXTRACTED_DIR}")

    mapping = load_person_id_map(PERSON_ID_MAP_JSON)

    # Reverse map: new_person_id -> old_person_id (first old that maps to this new wins)
    new_to_old: dict[str, str] = {}
    for old_pid, new_pid in mapping.items():
        if new_pid is not None and new_pid not in new_to_old:
            new_to_old[new_pid] = old_pid

    # Index old folders by old_person_id -> (name, to_ignore). Rank from old folder is not used.
    old_pid_to_info: dict[str, tuple[Optional[str], bool]] = {}
    for old_folder in sorted(OLD_EXTRACTED_DIR.iterdir()):
        if not old_folder.is_dir():
            continue
        info = process_basename(old_folder.name)
        old_pid_to_info[info.person_id] = (info.name, info.to_ignore)

    planned_renames: list[tuple[Path, Path]] = []

    for new_folder in sorted(NEW_EXTRACTED_DIR.iterdir()):
        if not new_folder.is_dir():
            continue
        new_rank, new_pid = parse_new_folder_basename(new_folder.name)

        old_pid = new_to_old.get(new_pid)
        if old_pid is None:
            continue

        name_ignore = old_pid_to_info.get(old_pid)
        if name_ignore is None:
            continue
        name, to_ignore = name_ignore

        new_basename = build_new_basename(new_rank, new_pid, name, to_ignore)
        target = new_folder.with_name(new_basename)

        if target.exists() and target != new_folder:
            raise FileExistsError(
                f"Target folder already exists and is different: {target} (from {new_folder})"
            )

        planned_renames.append((new_folder, target))

    if not planned_renames:
        print("No folders to rename (check mapping and extracted_faces directories).")
        return

    print(f"Planned renames ({len(planned_renames)}):")
    for src, dst in planned_renames:
        print(f"  {src.name}  ->  {dst.name}")

    if DRY_RUN:
        print("\nDRY_RUN is True; no changes were made.")
        return

    for src, dst in planned_renames:
        src.rename(dst)
    print(f"Applied {len(planned_renames)} renames.")


if __name__ == "__main__":
    main()

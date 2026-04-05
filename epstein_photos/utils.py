"""Miscellaneous shared helpers (JSON, geometry, labels, clustering, external name maps)."""

import json
from dataclasses import dataclass
from decimal import Decimal
from typing import Any, Mapping, Optional, SupportsFloat


# --- AWS / boto3 JSON for SQLite TEXT ---


def _aws_json_default(o: object) -> object:
    if isinstance(o, Decimal):
        return float(o)
    return str(o)


def dumps_aws_response(obj: Any, **kwargs: Any) -> str:
    """
    ``json.dumps`` with a default suitable for Rekognition / boto3 payloads:
    ``Decimal`` → ``float``; anything else non-JSON-native → ``str`` (UUID, datetime, etc.).
    Pass through ``json.dumps`` kwargs (e.g. ``separators=``).
    """
    return json.dumps(obj, default=_aws_json_default, **kwargs)


# --- Normalized (0..1) box IoU ---


def normalized_box_iou(
    l1: SupportsFloat,
    t1: SupportsFloat,
    w1: SupportsFloat,
    h1: SupportsFloat,
    l2: SupportsFloat,
    t2: SupportsFloat,
    w2: SupportsFloat,
    h2: SupportsFloat,
) -> float:
    """IoU for two boxes (left, top, width, height), same coordinate system."""
    a_left = float(l1)
    a_top = float(t1)
    a_w = float(w1)
    a_h = float(h1)
    b_left = float(l2)
    b_top = float(t2)
    b_w = float(w2)
    b_h = float(h2)

    a_right = a_left + a_w
    a_bottom = a_top + a_h
    b_right = b_left + b_w
    b_bottom = b_top + b_h

    a_area = a_w * a_h
    b_area = b_w * b_h

    ix1 = max(a_left, b_left)
    iy1 = max(a_top, b_top)
    ix2 = min(a_right, b_right)
    iy2 = min(a_bottom, b_bottom)
    iw = max(0.0, ix2 - ix1)
    ih = max(0.0, iy2 - iy1)
    inter = iw * ih
    if inter <= 0:
        return 0.0
    union = a_area + b_area - inter
    if union <= 0:
        return 0.0
    return inter / union


def rekognition_bbox_iou(a: Mapping[str, SupportsFloat], b: Mapping[str, SupportsFloat]) -> float:
    """IoU for Rekognition-style dicts with ``Left``, ``Top``, ``Width``, ``Height``."""
    return normalized_box_iou(
        a["Left"],
        a["Top"],
        a["Width"],
        a["Height"],
        b["Left"],
        b["Top"],
        b["Width"],
        b["Height"],
    )


# --- Graph / export filename stems ---


def sanitize_label_for_filename(label: str) -> str:
    """Safe filename stem from a graph node label (matches export / atlas conventions)."""
    s = "".join(c if c.isalnum() or c in "._- " else "" for c in str(label))
    return s.strip().replace(" ", "_").replace("/", "_") or "node"


# --- ``extracted_faces`` folder names: ``NNN__person_X`` / optional name / ``IGNORE`` ---


@dataclass(frozen=True)
class FolderInfo:
    basename: str
    rank: int
    person_id: str
    name: Optional[str]
    to_ignore: bool


def process_basename(basename: str) -> FolderInfo:
    parts = basename.split("__")
    to_ignore = parts[-1] == "IGNORE"

    match parts:
        case [rank, person_id, "IGNORE"]:
            name = None
        case [rank, person_id, _ignored_name, "IGNORE"]:
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


# --- Tommy gallery URL → local basename ---


def tommy_to_me(s: str) -> str:
    filename = s.strip("/").split("/")[-1]
    doc = filename.split("_")[0]
    pg = int(filename.split("_p")[1].split("_")[0])
    return doc + f"-{pg-1:>05}.jpg"


# --- Clustering ---


class UnionFind:
    """Disjoint-set (union–find) for face_id clustering."""

    def __init__(self) -> None:
        self.parent: dict[str, str] = {}

    def find(self, x: str) -> str:
        self.parent.setdefault(x, x)
        if self.parent[x] != x:
            self.parent[x] = self.find(self.parent[x])
        return self.parent[x]

    def union(self, x: str, y: str) -> None:
        self.parent[self.find(x)] = self.find(y)

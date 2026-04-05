#!/usr/bin/env python3
"""Google Sheet + AWS Rekognition: maintain a spreadsheet of people with face comparison results.

- Syncs Matches / Unknowns / Ignore into the SQLite `people` table (name, include_in_network, is_victim).
  Column I (Victim) on Matches: ``1`` sets ``people.is_victim``; then ``images.contains_victim`` is recomputed.
- Sheet ``Restricted Images`` (column A, header row): each listed basename gets ``images.is_explicit = 1``
  (separate from Rekognition moderation; does not write moderation JSON).
- Initializes the sheet with columns and formatting (frozen header, style).
- For each row missing Confidence: downloads reference image, runs CompareFaces
  (reference vs local Image Path), writes Confidence and full JSON to the sheet.
- When the reference has multiple faces, each face is compared and the highest similarity is used.
"""
from __future__ import annotations

import io
import json
import os
import shutil
from urllib.parse import urlparse, parse_qs
import time
from pathlib import Path

import boto3
import gspread
import requests
from PIL import Image, ImageDraw
from gspread_formatting import format_cell_range, set_frozen, CellFormat, TextFormat, Color

from faces_db import (
    apply_restricted_images_explicit,
    init_db,
    parse_node_face_export_stem,
    sync_people_from_google_sheets,
)
from sheets_common import (
    get_sheet_client,
    get_workbook,
    load_names,
    load_person_ids_matches_and_unknowns,
    load_restricted_image_names,
)

try:
    import pillow_avif  # noqa: F401 — registers AVIF support with Pillow if installed
except ImportError:
    pass

# ----------------------------- Constants -----------------------------

INIT_ONLY = False   # if True, only set headers/format then exit
DRY_RUN = False     # if True, only list rows missing Confidence, no API calls
SHOW_MATCH = True  # if True, display target image with face bounding boxes (matches=green, unmatched=red)

SCRIPT_DIR = Path(__file__).resolve().parent
IMAGE_DIR = SCRIPT_DIR.parent.parent.parent / "all_images"
NODE_FACES_SELECTED_DIR = SCRIPT_DIR.parent / "images" / "node_faces_selected"

COLUMNS = [
    "Name",
    "Person ID",
    "Image Path",
    "Reference Image URL",
    "Archived Reference Image URL",
    "Confidence",
    "JSON Response",
    "Category",
    "Victim",
    "Best Face ID",
    "Links",
    "Notes",
    "Previously Reported",
    "Jmail"
]

REKOGNITION_REGION = os.environ.get("AWS_REGION", "us-east-1")
REKOGNITION_MAX_BYTES = 5 * 1024 * 1024  # 5 MiB
REKOGNITION_MIN_DIM = 80  # minimum width/height in pixels
API_DELAY_SECONDS = 0.3
FACE_CROP_PADDING = 0.2  # add 20% on each side when cropping reference to a face

# ----------------------------- Sheet -----------------------------


def init_spreadsheet(worksheet):
    header = worksheet.row_values(1)
    if header != COLUMNS:
        worksheet.update("A1", [COLUMNS], value_input_option="RAW")
    set_frozen(worksheet, rows=1)
    fmt = CellFormat(
        textFormat=TextFormat(bold=True),
        backgroundColor=Color(0.95, 0.95, 0.95),
    )
    format_cell_range(worksheet, "A1:N1", fmt)


def get_or_create_sheet(gc: gspread.Client):
    """Return the first worksheet of the shared workbook (main Rekognition sheet)."""
    return get_workbook(gc).sheet1


def _sanitize_label_for_filename(label: str) -> str:
    s = "".join(c if c.isalnum() or c in "._- " else "" for c in str(label))
    return s.strip().replace(" ", "_").replace("/", "_") or "node"


def _copy_person_id_node_faces_to_name_form(gc: gspread.Client) -> int:
    """
    If `node_faces_selected/` has manual faces for `<person_id>_*` but lacks `<Name>_*`,
    copy the best existing file to the standard name-based stem used by `10__create_graph`.
    """
    if not NODE_FACES_SELECTED_DIR.is_dir():
        return 0

    image_exts = {".jpg", ".jpeg", ".png", ".webp", ".avif"}
    parsed: list[tuple[Path, str, tuple[int, int | str]]] = []
    for p in NODE_FACES_SELECTED_DIR.iterdir():
        if not p.is_file() or p.suffix.lower() not in image_exts:
            continue
        out = parse_node_face_export_stem(p.stem)
        if out is None:
            continue
        base_stem, sort_key = out
        parsed.append((p, base_stem, sort_key))

    by_stem: dict[str, list[tuple[Path, tuple[int, int | str]]]] = {}
    for p, base_stem, sort_key in parsed:
        by_stem.setdefault(base_stem, []).append((p, sort_key))
    for k in list(by_stem.keys()):
        by_stem[k].sort(key=lambda x: x[1])

    names = load_names(gc)
    include_ids = load_person_ids_matches_and_unknowns(gc)
    copied = 0
    for pid in sorted(include_ids):
        target_stem = _sanitize_label_for_filename(names.get(pid) or pid)
        # Already has a correctly named manual file.
        if by_stem.get(target_stem):
            continue
        source_list = by_stem.get(pid)
        if not source_list:
            continue
        src, sk = source_list[0]
        if sk[0] == 0:
            new_stem = f"{target_stem}__{int(sk[1]):03d}"
        else:
            new_stem = f"{target_stem}_{str(sk[1]).lower()}"
        dst = src.with_name(new_stem + src.suffix.lower())
        if dst.exists():
            continue
        shutil.copy2(src, dst)
        copied += 1

    return copied


# ----------------------------- Rekognition -----------------------------


def _normalize_reddit_url(url: str) -> str:
    """
    Reddit image links are sometimes indirect, e.g. `/media?url=...` which serves an HTML
    landing page with the real `preview.redd.it` image in `og:image`. For Rekognition we
    want the direct image URL, so unwrap those when possible.
    """
    parsed = urlparse(url)
    if parsed.netloc.endswith("reddit.com") and parsed.path == "/media":
        qs = parse_qs(parsed.query)
        candidates = qs.get("url") or []
        if candidates:
            return candidates[0]
    return url


def download_image_bytes_reddit(url: str, session: requests.Session) -> bytes:
    """
    Download image bytes from Reddit-hosted URLs, handling indirection like `/media?url=...`
    and making sure we end up with real image bytes (not HTML wrappers).
    """
    url = _normalize_reddit_url(url)
    # Be defensive: this header might not exist if the session is reused elsewhere.
    session.headers.pop("Accept-Language", None)

    r = session.get(url, timeout=30)
    r.raise_for_status()

    content_type = (r.headers.get("Content-Type") or "").split(";", 1)[0].strip().lower()
    if content_type and not content_type.startswith("image/"):
        raise ValueError(f"Reddit URL did not return an image (Content-Type={content_type!r})")

    data = r.content
    if len(data) > REKOGNITION_MAX_BYTES:
        raise ValueError(f"Downloaded image too large for Rekognition: {len(data)} bytes")
    return data


def download_image_bytes(url: str, session: requests.Session) -> bytes:
    """
    Download image bytes from an arbitrary URL, enforcing a size limit suitable for
    Rekognition. Reddit-specific quirks live in `download_image_bytes_reddit`.
    """
    if "reddit.com" in url:
        return download_image_bytes_reddit(url, session)

    r = session.get(url, timeout=30)
    r.raise_for_status()

    content_type = (r.headers.get("Content-Type") or "").split(";", 1)[0].strip().lower()
    if content_type and not content_type.startswith("image/"):
        raise ValueError(f"URL did not return an image (Content-Type={content_type!r})")

    data = r.content
    if len(data) > REKOGNITION_MAX_BYTES:
        raise ValueError(f"Downloaded image too large for Rekognition: {len(data)} bytes")
    return data


def ensure_rekognition_compatible(image_bytes: bytes) -> bytes:
    """Convert image bytes to JPEG if needed. Rekognition accepts only JPEG and PNG."""
    if len(image_bytes) >= 3 and image_bytes[:3] == b"\xff\xd8\xff":
        return image_bytes
    if len(image_bytes) >= 8 and image_bytes[:8] == b"\x89PNG\r\n\x1a\n":
        return image_bytes
    img = Image.open(io.BytesIO(image_bytes)).convert("RGB")
    buf = io.BytesIO()
    img.save(buf, format="JPEG", quality=95)
    out = buf.getvalue()
    if len(out) > REKOGNITION_MAX_BYTES:
        buf = io.BytesIO()
        img.save(buf, format="JPEG", quality=70)
        out = buf.getvalue()
        if len(out) > REKOGNITION_MAX_BYTES:
            raise ValueError(
                f"Converted image still too large for Rekognition: {len(out)} bytes (max {REKOGNITION_MAX_BYTES})"
            )
    return out


def resolve_image_path(image_path_value: str) -> Path:
    p = Path(image_path_value)
    if not p.is_absolute():
        p = IMAGE_DIR / p
    return p


def detect_faces(rekognition_client, image_bytes: bytes) -> list:
    """Return list of FaceDetail dicts (each has BoundingBox) from the image."""
    resp = rekognition_client.detect_faces(Image={"Bytes": image_bytes})
    return resp.get("FaceDetails") or []


def crop_to_face(image_bytes: bytes, box: dict, padding: float = FACE_CROP_PADDING) -> bytes | None:
    """Crop image to the face bounding box (normalized 0–1) with padding. Returns None if crop would be smaller than Rekognition's minimum (80px)."""
    img = Image.open(io.BytesIO(image_bytes)).convert("RGB")
    w, h = img.size
    left = box["Left"]
    top = box["Top"]
    width = box["Width"]
    height = box["Height"]
    pad_w = width * padding
    pad_h = height * padding
    left = max(0, left - pad_w)
    top = max(0, top - pad_h)
    width = min(1 - left, width + 2 * pad_w)
    height = min(1 - top, height + 2 * pad_h)
    x0 = int(w * left)
    y0 = int(h * top)
    x1 = int(w * (left + width))
    y1 = int(h * (top + height))
    cw = x1 - x0
    ch = y1 - y0
    if cw < REKOGNITION_MIN_DIM or ch < REKOGNITION_MIN_DIM:
        return None
    cropped = img.crop((x0, y0, x1, y1))
    buf = io.BytesIO()
    cropped.save(buf, format="JPEG", quality=95)
    out = buf.getvalue()
    if len(out) > REKOGNITION_MAX_BYTES:
        buf = io.BytesIO()
        cropped.save(buf, format="JPEG", quality=70)
        out = buf.getvalue()
    return out


def _similarity_from_response(response: dict) -> float:
    matches = response.get("FaceMatches") or []
    return matches[0]["Similarity"] if matches else 0.0


def compare_faces(rekognition_client, source_bytes: bytes, target_path: Path) -> dict:
    with open(target_path, "rb") as f:
        target_bytes = f.read()
    if len(target_bytes) > REKOGNITION_MAX_BYTES:
        raise ValueError(f"Target image too large: {target_path}")
    return rekognition_client.compare_faces(
        SourceImage={"Bytes": source_bytes},
        TargetImage={"Bytes": target_bytes},
        SimilarityThreshold=0,
    )


def compare_faces_best_of(
    rekognition_client, source_bytes: bytes, target_path: Path
) -> tuple[dict, dict | None, list | None]:
    """
    When the reference has multiple faces, compare each face against the target and return
    the response with the highest similarity. Returns (response, source_face_box_for_display, all_source_boxes_or_none).
    """
    faces = detect_faces(rekognition_client, source_bytes)
    if len(faces) <= 1:
        response = compare_faces(rekognition_client, source_bytes, target_path)
        source_box = None
        if response.get("SourceImageFace") and "BoundingBox" in response["SourceImageFace"]:
            source_box = response["SourceImageFace"]["BoundingBox"]
        return (response, source_box, None)
    best_response = None
    best_similarity = -1.0
    best_box = None
    for face in faces:
        box = face["BoundingBox"]
        crop_bytes = crop_to_face(source_bytes, box)
        if crop_bytes is None:
            continue  # face too small for Rekognition, skip
        response = compare_faces(rekognition_client, crop_bytes, target_path)
        sim = _similarity_from_response(response)
        if sim > best_similarity:
            best_similarity = sim
            best_response = response
            best_box = box
        time.sleep(API_DELAY_SECONDS)
    # if all faces were too small, fall back to full-image comparison (uses largest face)
    if best_response is None:
        response = compare_faces(rekognition_client, source_bytes, target_path)
        source_box = None
        if response.get("SourceImageFace") and "BoundingBox" in response["SourceImageFace"]:
            source_box = response["SourceImageFace"]["BoundingBox"]
        return (response, source_box, None)
    all_boxes = [f["BoundingBox"] for f in faces]
    return (best_response, best_box, all_boxes)


def extract_confidence(response: dict) -> str:
    matches = response.get("FaceMatches") or []
    if not matches:
        return ""
    return f"{matches[0]['Similarity']:.6f}"


def show_bounding_boxes(image_bytes: bytes, box_sets: list, colors: list) -> None:
    """
    Draws bounding boxes on an image and shows it with the default image viewer.

    :param image_bytes: The image to draw, as bytes.
    :param box_sets: A list of lists of bounding boxes to draw on the image.
    :param colors: A list of colors to use to draw the bounding boxes.
    """
    image = Image.open(io.BytesIO(image_bytes))
    draw = ImageDraw.Draw(image)
    for boxes, color in zip(box_sets, colors):
        for box in boxes:
            left = image.width * box["Left"]
            top = image.height * box["Top"]
            right = (image.width * box["Width"]) + left
            bottom = (image.height * box["Height"]) + top
            draw.rectangle([left, top, right, bottom], outline=color, width=4)
    image.show()


def show_match(
    source_bytes: bytes,
    target_path: Path,
    response: dict,
    source_face_box: dict | None = None,
    all_source_face_boxes: list | None = None,
) -> None:
    """Display both images: reference with best face (green/yellow) / other faces (red); target with matches (green/yellow) / unmatched (red)."""
    # Choose highlight color based on similarity
    similarity = _similarity_from_response(response)
    highlight_color = "green" if similarity >= 99.0 else "yellow"

    # Reference/source image: best (highest-confidence) face in one color, other faces in red
    box = source_face_box
    if box is None:
        source_face = response.get("SourceImageFace")
        if source_face and "BoundingBox" in source_face:
            box = source_face["BoundingBox"]
    if box is not None:
        if all_source_face_boxes:
            other_boxes = [b for b in all_source_face_boxes if b != box]
            show_bounding_boxes(
                source_bytes,
                [[box], other_boxes],
                [highlight_color, "red"],
            )
        else:
            show_bounding_boxes(
                source_bytes,
                [[box]],
                [highlight_color],
            )
    else:
        Image.open(io.BytesIO(source_bytes)).show()
    # Target image: matched faces (green/yellow), unmatched faces (red)
    target_bytes = target_path.read_bytes()
    matches = response.get("FaceMatches") or []
    unmatched = response.get("UnmatchedFaces") or []
    unmatched_boxes = [u["BoundingBox"] for u in unmatched]

    # With SimilarityThreshold=0, Rekognition can return many FaceMatches.
    # For visualization, show ONLY the best match in green; everything else in red.
    best_match_box = matches[0]["Face"]["BoundingBox"] if matches else None
    other_match_boxes = [m["Face"]["BoundingBox"] for m in matches[1:]] if len(matches) > 1 else []
    non_match_boxes = other_match_boxes + unmatched_boxes

    if best_match_box is not None:
        show_bounding_boxes(
            target_bytes,
            [[best_match_box], non_match_boxes],
            [highlight_color, "red"],
        )
    else:
        show_bounding_boxes(
            target_bytes,
            [non_match_boxes],
            ["red"],
        )


# ----------------------------- Main -----------------------------


def main():
    gc = get_sheet_client()
    worksheet = get_or_create_sheet(gc)
    init_spreadsheet(worksheet)

    if INIT_ONLY:
        print("Sheet initialized.")
        return

    rows = worksheet.get_all_values()
    if not rows:
        print("No rows in sheet.")
        return

    header = rows[0]
    col_index = {h.strip().lower().replace(" ", ""): i for i, h in enumerate(header)}
    idx_name = col_index["name"]
    idx_person_id = col_index["personid"]
    idx_image_path = col_index["imagepath"]
    idx_ref_url = col_index["referenceimageurl"]
    idx_archived_url = col_index["archivedreferenceimageurl"]
    idx_confidence = col_index["confidence"]
    idx_json_response = col_index["jsonresponse"]

    # Gate new person_ids: only allow inserting/updating new rows in `people` once the compare-faces
    # confidence exists and is >99%. This helps prevent speculative matches from polluting the DB.
    allow_new_person_ids: set[str] = set()
    for i in range(1, len(rows)):
        row = rows[i]
        pid = (row[idx_person_id] if idx_person_id < len(row) else "").strip()
        if not pid:
            continue
        conf_raw = (row[idx_confidence] if idx_confidence < len(row) else "").strip()
        if not conf_raw:
            continue
        try:
            conf = float(conf_raw)
        except ValueError:
            continue
        if conf > 99.0:
            allow_new_person_ids.add(pid)

    conn = init_db()
    try:
        sync_people_from_google_sheets(conn, gc, allow_new_person_ids=allow_new_person_ids)
        copied = _copy_person_id_node_faces_to_name_form(gc)
        if copied:
            print(f"node_faces_selected: copied {copied} person_id-named file(s) to name-based stems.")
        restricted = load_restricted_image_names(gc)
        if restricted:
            updated, missing = apply_restricted_images_explicit(conn, restricted)
            conn.commit()
            print(
                f"Restricted Images sheet: set is_explicit=1 on {updated} image row(s); "
                f"{len(missing)} listed name(s) not found in images table."
            )
            if missing:
                preview = missing if len(missing) <= 25 else missing[:25] + ["..."]
                print(f"  Not in DB: {preview}")
        else:
            print("Restricted Images sheet: no filenames, sheet missing, or only header row.")
    finally:
        conn.close()

    missing = []
    for i in range(1, len(rows)):
        row = rows[i]
        confidence_val = (row[idx_confidence] if idx_confidence < len(row) else "").strip()
        if confidence_val:
            continue
        ref_url = (row[idx_ref_url] if idx_ref_url < len(row) else "").strip()
        archived_url = (row[idx_archived_url] if idx_archived_url < len(row) else "").strip()
        image_path_val = (row[idx_image_path] if idx_image_path < len(row) else "").strip()
        if not image_path_val or (not ref_url and not archived_url):
            continue
        missing.append((i + 1, row, ref_url, archived_url, image_path_val))

    if not missing:
        print("No rows missing Confidence.")
        return

    print(f"Found {len(missing)} row(s) missing Confidence.")
    if DRY_RUN:
        for row_1based, _, ref_url, archived_url, image_path_val in missing:
            print(f"  Row {row_1based}: ref={ref_url or archived_url!r} target={image_path_val}")
        return

    rekognition = boto3.client("rekognition", region_name=REKOGNITION_REGION)

    session = requests.Session()
    session.headers.update({
        "User-Agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/145.0.0.0 Safari/537.36",
        "Accept": "image/webp,image/apng,image/*,*/*;q=0.8",
        "Accept-Language": "en-US,en;q=0.9",
    })

    for row_1based, row, ref_url, archived_url, image_path_val in missing:
        name = row[idx_name] if idx_name < len(row) else ""
        person_id = row[idx_person_id] if idx_person_id < len(row) else ""
        print(f"Row {row_1based} ({name or person_id or '?'}): ", end="", flush=True)

        target_path = resolve_image_path(image_path_val)
        source_bytes = None
        if ref_url:
            source_bytes = download_image_bytes(ref_url, session)
        if source_bytes is None and archived_url:
            source_bytes = download_image_bytes(archived_url, session)
        if source_bytes is None:
            raise RuntimeError(f"Row {row_1based}: could not download reference image")

        source_bytes = ensure_rekognition_compatible(source_bytes)
        response, source_face_box, all_source_boxes = compare_faces_best_of(
            rekognition, source_bytes, target_path
        )
        if SHOW_MATCH:
            show_match(
                source_bytes, target_path, response, source_face_box, all_source_boxes
            )
        confidence = extract_confidence(response)
        json_str = json.dumps(response, separators=(",", ":"))

        worksheet.update_cell(row_1based, idx_confidence + 1, confidence)
        worksheet.update_cell(row_1based, idx_json_response + 1, json_str)
        print(confidence)
        time.sleep(API_DELAY_SECONDS)

    print("Done.")


if __name__ == "__main__":
    main()

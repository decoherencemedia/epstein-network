"""
Shared paths and DB for the pipeline.

Install the package editable (``pip install -e .`` from the ``network/`` repo root) so scripts
anywhere can ``import epstein_photos``.
"""
from pathlib import Path

# network/ — repository root for this project (parent of ``epstein_photos/``).
NETWORK_ROOT = Path(__file__).resolve().parent.parent

# Alias used by many scripts for repo-relative paths (images/, viz_data/, graphml/).
REPO_DIR = NETWORK_ROOT

# Parent of network/ — ``facial_grouping/`` (sibling dirs like ``extracted_faces/`` live here).
FACIAL_GROUPING_ROOT = NETWORK_ROOT.parent

# Epstein corpus root: ``network/../../`` = ``.../epstein/`` (contains ``all_images/``).
EPSTEIN_ROOT = NETWORK_ROOT.parent.parent

SCRIPTS_DIR = NETWORK_ROOT / "scripts"
# Legacy name: the ``scripts/`` directory (not an individual pipeline script file).
SCRIPT_DIR = SCRIPTS_DIR

IMAGE_DIR = EPSTEIN_ROOT / "all_images"
# WebP originals + thumbnails tree (often under ``EPSTEIN_ROOT``; used by ``17__upload_everything_to_spaces``).
ALL_IMAGES_WITH_FACES_WEBP_DIR = EPSTEIN_ROOT / "all_images_with_faces_webp" / "images"
ALL_IMAGES_WITH_FACES_THUMBNAILS_DIR = EPSTEIN_ROOT / "all_images_with_faces_thumbnails" / "images"
DB_PATH = SCRIPTS_DIR / "faces.db"

# ---------------- Rekognition ----------------

REKOGNITION_REGION = "us-east-1"
REKOGNITION_COLLECTION_ID = "epstein-doj-20260317"

# ---------------- Repo layout under network/ ----------------

IMAGES_DIR = NETWORK_ROOT / "images"
NODE_FACES_SELECTED_DIR = IMAGES_DIR / "node_faces_selected"

# Remapping: extracted face folders live next to ``network/`` under ``facial_grouping/``.
EXTRACTED_FACES_DIR = FACIAL_GROUPING_ROOT / "extracted_faces"
EXTRACTED_FACES_INITIAL_DIR = FACIAL_GROUPING_ROOT / "extracted_faces__intitial"

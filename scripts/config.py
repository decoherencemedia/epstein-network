"""
Shared paths and DB for the pipeline. All scripts assume CWD is scripts/ when using DB_PATH.
"""
from pathlib import Path

SCRIPT_DIR = Path(__file__).resolve().parent
# Repo root: images live at root/all_images (same as 07's parent.parent.parent)
ROOT_DIR = SCRIPT_DIR.parent.parent.parent
IMAGE_DIR = ROOT_DIR / "all_images"
DB_PATH = "faces.db"

# ---------------- Rekognition ----------------

REKOGNITION_REGION = "us-east-1"
REKOGNITION_COLLECTION_ID = "epstein-doj-20260317"

"""
Shared paths and DB for the pipeline. All scripts assume CWD is scripts/ when using DB_PATH.
"""
from pathlib import Path

SCRIPT_DIR = Path(__file__).resolve().parent
ROOT_DIR = SCRIPT_DIR.parent.parent.parent  # scripts/ -> network/ -> facial_grouping/ -> epstein/
IMAGE_DIR = ROOT_DIR / "all_images"
DB_PATH = "faces.db"

# ---------------- Rekognition ----------------

REKOGNITION_REGION = "us-east-1"
REKOGNITION_COLLECTION_ID = "epstein-doj-20260317"

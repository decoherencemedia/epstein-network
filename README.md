# Epstein Photo Network Visualization

## Python package (pipeline)

The shared library lives in **`epstein_photos/`** (import name). The distribution name on PyPI-style installs is **`epstein-photos`**.

From the **`network/`** directory (this repo root):

```bash
pip install -e ".[pipeline]"
```

That makes `import epstein_photos` work from any working directory. Configuration and paths are in `epstein_photos.config` (e.g. `IMAGE_DIR`, `DB_PATH`, `NETWORK_ROOT`).

The SQLite database is **`scripts/faces.db`** by default (`epstein_photos.config.DB_PATH`).

## Website (static site)

HTML is assembled from fragments under `site/`:

- **`site/partials/`** — shared head, nav (per route), footer, closing tags
- **`site/pages/`** — page bodies (`home-inner.html` is the D3 graph; edit there for viz changes)
- **`site/build.sh`** — concatenates into `dist/` and copies `styles.css`, `favicon.svg`, `viz_data/`, `js/`; optionally copies `images/atlas.webp` into `dist/images/` when present (local `http.server` preview only — production loads the atlas from Spaces, not from `dist/`)

Run from repo root:

```bash
./site/build.sh
```

Preview locally (needs `viz_data/` inside `dist/`):

```bash
cd dist && python3 -m http.server 8000
```

GitHub Actions runs `./site/build.sh` and deploys **`dist/`** to GitHub Pages.

## Pipeline layout

| Location | Contents |
|----------|----------|
| **`scripts/pipeline/`** | Numbered main pipeline steps (mostly `.py`; `00__extract_pdf_images.sh` remains), utilities (`apply_face_person_overrides.py`, etc.) |
| **`scripts/ingest/`** | New / incremental images: assign person IDs, sync WebP to Spaces |
| **`scripts/remapping/`** | Person-id remapping helpers |
| **`scripts/external/`** | One-off / external list tools |
| **`scripts/face_person_overrides.json`** | Override file read by `apply_face_person_overrides.py` |

Run scripts with `python3 scripts/pipeline/01__dedup_images.py` (paths in `epstein_photos.config` do not depend on current working directory once the package is installed).

## Pipeline order (main)

| Step | Script | Description |
|------|--------|-------------|
| 00 | `pipeline/00__extract_pdf_images.sh` | Extract images from PDFs (recursive; uses `_00__extract_pdf_images_parallel.py`). |
| 01 | `pipeline/01__dedup_images.py` | Remove exact duplicate images by content hash. |
| 02 | `pipeline/02__downsize_images.py` | Downsize images above 5MB. |
| 03 | `pipeline/03__preprocess_faces.py` | Local face detection (InsightFace), set `has_face` in DB. |
| 04 | `pipeline/04__index_faces.py` | Index faces in Rekognition (IndexFaces), populate faces table. |
| 05 | `pipeline/05__cluster_faces.py` | SearchFaces + UnionFind, assign `person_id` to faces. |
| 06 | `pipeline/06__moderate_images.py` | DetectModerationLabels, store raw JSON in `images.moderation_result`. |
| 07 | `pipeline/07__recognize_celebrities.py` | RecognizeCelebrities per person, write `celebrity_*` on faces. |
| 08 | `pipeline/08__extract_faces.py` | Write cropped face images to `extracted_faces/`. |
| 09 | `pipeline/09__sheets_rekognition.py` | Google Sheets + CompareFaces sync. |
| 10 | `pipeline/10__create_graph.py` | Build co-occurrence graph (GraphML), output under `graphml/`, `viz_data/`. |
| 11 | `pipeline/11__create_thumbnails.py` | WebP thumbnails (`--clean` wipes output dir; defaults use `EPSTEIN_ROOT` paths). |
| 12 | `pipeline/12__export_node_faces.py` | Export node face crops. |
| 13 | `pipeline/13__optimize_node_faces.py` | WebP optimize node / people face dirs for upload. |
| 14 | `pipeline/14__visualize_graph.py` | GraphML → D3 `dataset.json`. |
| 15 | `pipeline/15__build_atlas.py` | Build image atlas. |
| 16 | `pipeline/16__upload_to_spaces.py` | Upload site assets to Spaces. |
| 17 | `pipeline/17__upload_everything_to_spaces.py` | Broader Spaces upload. |
| 18 | `pipeline/18__ensure_api_indexes.py` | API index helpers. |

## Ingest (new images / incremental)

Run after the main steps that apply to your new data (e.g. moderation, graph rebuild as needed).

| Order | Script | Description |
|------|--------|-------------|
| 0 | `ingest/00__assign_person_ids.py` | Incremental `person_id` assignment for new faces. |
| 1 | `ingest/01__sync_images_with_spaces.py` | Sync missing full/thumbnail WebP to Spaces for `has_face` rows (edit `DRY_RUN` / paths at top of file). |

Shared WebP helpers: `epstein_photos.webp` (used by ingest sync and pipeline 11 / 13).

## TODO

- add "Unknown" page
- separate repos for scripts and web
- double check pdfimages to make sure pbm and non-jpg images are kept
- change API such that search returns duplicated images of documents correctly

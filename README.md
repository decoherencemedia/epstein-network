# Epstein Photo Network Visualization

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

## Pipeline order

Run from `scripts/` (or set paths accordingly). `all_images` is at repo root.

| Step | Script | Description |
|------|--------|-------------|
| 00 | `00__extract_pdf_images.sh` | Extract images from PDFs (recursive). |
| 01 | `01__dedup_images.py` | Remove exact duplicate images by content hash. |
| 02 | `02__downsize_images.sh` | Downsize images above 5MB. |
| 03 | `03__preprocess_faces.py` | Local face detection (InsightFace), set `has_face` in DB. |
| 04 | `04__index_faces.py` | Index faces in Rekognition (IndexFaces), populate faces table. |
| 05 | `05__cluster_faces.py` | SearchFaces + UnionFind, assign `person_id` to faces. |
| 06 | `06__moderate_images.py` | DetectModerationLabels, store raw JSON in `images.moderation_result`. |
| 07 | `07__recognize_celebrities.py` | RecognizeCelebrities per person, write `celebrity_*` on faces. |
| 08 | `08__extract_faces.py` | Write cropped face images to `extracted_faces/`. |
| 09 | `09__create_graph.py` | Build co-occurrence graph (GraphML), filter nude (NudeNet), output `image_data.json`. |
| 10 | `10__create_thumbnails.sh` | Create thumbnails for graph assets. |
| 11 | `11__upload_to_spaces.py` | Upload images/thumbnails to DigitalOcean Spaces (env: `EPSTEIN_SPACES_*`). |
| 12 | `12__visualize_graph.py` | Turn laid-out GraphML into D3 dataset (e.g. `dataset.json`). |

Shared config: `config.py` (IMAGE_DIR, DB_PATH). DB: `faces.db` in `scripts/`.

## TODO
- add "Unknown" page
- update scripts and descriptions in README
- separate repos for scripts and web
- double check pdfimages to make sure pbm and non-jpg images are kept
- change API such that search returns duplicated images of documents correctly
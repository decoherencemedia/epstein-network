# epstein-web

Static site for the Epstein photo network graph (D3, atlas, search, people). Lives **next to** `epstein-pipeline` and `epstein-api` under the shared **`network/`** folder.

## Build

HTML is assembled from fragments under **`site/`**:

- **`site/partials/`** — shared head, nav (per route), footer, closing tags
- **`site/pages/`** — page bodies (`home-inner.html` holds the D3 graph)
- **`site/build.sh`** — concatenates into **`dist/`** and copies `styles.css`, `favicon.svg`, `viz_data/`, `js/`; downloads **`imagesloaded`** into **`site/js/imagesloaded.pkgd.min.js`** (gitignored, pinned URL) so the Search page script isn’t blocked on a CDN; optionally copies **`../images/atlas.webp`** (umbrella `network/images/`, same as pipeline `FACES_IMAGE_DIR`) into `dist/images/` when present (local preview only — production often loads the atlas from Spaces)

Generated JSON (`dataset.json`, `image_data.json`, `atlas_manifest.json`, …) is written by **`epstein-pipeline`** into **`viz_data/`** in **this** repo (`epstein_photos.config.VIZ_DATA_DIR` → `epstein-web/viz_data/`). **`site/build.sh`** copies **`viz_data/`** into **`dist/`** for preview and deploy.

From **this** repo root:

```bash
./site/build.sh
```

Preview locally (needs `viz_data/` inside `dist/` after build):

```bash
cd dist && python3 -m http.server 8000
```

GitHub Actions (if configured) runs `./site/build.sh` and deploys **`dist/`** to GitHub Pages.

## TODO

- add fuzzy search to People page (with filters for category?)
- fix original PDF link for non-EFTA

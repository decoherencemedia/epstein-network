#!/usr/bin/env bash
# Assemble static HTML from partials into dist/. Run from repo root: ./site/build.sh
set -euo pipefail
ROOT="$(cd "$(dirname "$0")/.." && pwd)"
SITE="$ROOT/site"
DIST="$ROOT/dist"

rm -rf "$DIST"
mkdir -p "$DIST/people" "$DIST/search" "$DIST/about"

# Home: graph (no footer — full-viewport viz)
cat "$SITE/partials/head-root.html" \
  "$SITE/partials/nav-home.html" \
  "$SITE/pages/home-inner.html" \
  "$SITE/partials/close.html" > "$DIST/index.html"

# Search (/search)
cat "$SITE/partials/head-people.html" \
  "$SITE/partials/nav-people.html" \
  "$SITE/pages/people-inner.html" \
  "$SITE/partials/footer.html" \
  "$SITE/partials/close.html" > "$DIST/search/index.html"

# People gallery (/people) — data from /faces API
cat "$SITE/partials/head-browse.html" \
  "$SITE/partials/nav-browse.html" \
  "$SITE/pages/browse-inner.html" \
  "$SITE/partials/footer.html" \
  "$SITE/partials/close.html" > "$DIST/people/index.html"

# About
cat "$SITE/partials/head-about.html" \
  "$SITE/partials/nav-about.html" \
  "$SITE/pages/about-inner.html" \
  "$SITE/partials/footer.html" \
  "$SITE/partials/close.html" > "$DIST/about/index.html"

cp "$ROOT/styles.css" "$DIST/"
cp "$ROOT/favicon.svg" "$DIST/"

# Pipeline writes JSON under epstein-web/viz_data/ (see epstein_photos.config.VIZ_DATA_DIR).
if [[ -d "$ROOT/viz_data" ]]; then
  cp -r "$ROOT/viz_data" "$DIST/"
else
  mkdir -p "$DIST/viz_data"
  echo "warning: no viz_data/ in repo root (run pipeline steps that write dataset.json, etc.)" >&2
fi

cp -r "$SITE/js" "$DIST/"

# Local dev: atlas from pipeline output (``network/images/``, see FACES_IMAGE_DIR), else in-repo copy.
ATLAS_PIPELINE="$ROOT/../images/atlas.webp"
if [[ -f "$ATLAS_PIPELINE" ]]; then
  mkdir -p "$DIST/images"
  cp "$ATLAS_PIPELINE" "$DIST/images/"
elif [[ -f "$ROOT/images/atlas.webp" ]]; then
  mkdir -p "$DIST/images"
  cp "$ROOT/images/atlas.webp" "$DIST/images/"
fi

echo "Built static site → $DIST"

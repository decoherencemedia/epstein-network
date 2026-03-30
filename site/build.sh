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
cp -r "$ROOT/viz_data" "$DIST/"
cp -r "$SITE/js" "$DIST/"

# Local dev only: home-inner uses ./images/atlas.webp when IS_LOCAL_DEV; production uses
# the DigitalOcean Spaces URL and never loads atlas from dist/. Optional copy so
# `cd dist && python3 -m http.server` can show the atlas after 15__build_atlas.py.
if [[ -f "$ROOT/images/atlas.webp" ]]; then
  mkdir -p "$DIST/images"
  cp "$ROOT/images/atlas.webp" "$DIST/images/"
fi

echo "Built static site → $DIST"

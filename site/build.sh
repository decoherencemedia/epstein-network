#!/usr/bin/env bash
# Assemble static HTML from partials into dist/. Run from repo root: ./site/build.sh
set -euo pipefail
ROOT="$(cd "$(dirname "$0")/.." && pwd)"
SITE="$ROOT/site"
DIST="$ROOT/dist"

# Preserve outputs from epstein-web/scripts/generate_static_search_pages.py across full dist/ resets.
STASH=""
if [[ -d "$DIST" ]]; then
  STASH="$(mktemp -d "${TMPDIR:-/tmp}/epstein-web-build-stash.XXXXXX")"
  if [[ -d "$DIST/search/people" ]]; then
    mkdir -p "$STASH/search/people"
    cp -a "$DIST/search/people/." "$STASH/search/people/"
  fi
  [[ -f "$DIST/sitemap.xml" ]] && cp -a "$DIST/sitemap.xml" "$STASH/"
  [[ -f "$DIST/search-people-pages.json" ]] && cp -a "$DIST/search-people-pages.json" "$STASH/"
fi

rm -rf "$DIST"
mkdir -p "$DIST/people" "$DIST/search" "$DIST/about" "$DIST/explore"

# Regenerate per-page <head> partials from site/site_metadata.json (single source
# of truth for OG/Twitter/canonical tags shared across pages). Pure stdlib Python.
python3 "$ROOT/scripts/render_head_partials.py"

# Shared header + nav for every page. nav.js adds `site-nav-active` /
# aria-current="page" based on location.pathname, so no per-page partial needed.
NAV="$SITE/partials/nav.html"
# First-visit disclaimer gate. Same markup on every page; disclaimer.js (loaded
# from the head partials) reads localStorage to decide whether to show it.
DISCLAIMER="$SITE/partials/disclaimer.html"

# Home: graph (no footer — full-viewport viz)
cat "$SITE/partials/head-root.html" \
  "$NAV" \
  "$SITE/pages/home-inner.html" \
  "$DISCLAIMER" \
  "$SITE/partials/close.html" > "$DIST/index.html"

# Search (/search)
cat "$SITE/partials/head-search.html" \
  "$NAV" \
  "$SITE/pages/search-inner.html" \
  "$SITE/partials/footer.html" \
  "$DISCLAIMER" \
  "$SITE/partials/close.html" > "$DIST/search/index.html"

# People gallery (/people) — data from /faces API
cat "$SITE/partials/head-people.html" \
  "$NAV" \
  "$SITE/pages/people-inner.html" \
  "$SITE/partials/footer.html" \
  "$DISCLAIMER" \
  "$SITE/partials/close.html" > "$DIST/people/index.html"

# About
cat "$SITE/partials/head-about.html" \
  "$NAV" \
  "$SITE/pages/about-inner.html" \
  "$SITE/partials/footer.html" \
  "$DISCLAIMER" \
  "$SITE/partials/close.html" > "$DIST/about/index.html"

# Explore (/explore) — UMAP scatter viewer (d3 canvas). No footer (full-viewport viz).
cat "$SITE/partials/head-explore.html" \
  "$NAV" \
  "$SITE/pages/explore-inner.html" \
  "$DISCLAIMER" \
  "$SITE/partials/close.html" > "$DIST/explore/index.html"

cp "$ROOT/styles.css" "$DIST/"
cp "$ROOT/favicon.svg" "$DIST/"
cp "$ROOT/robots.txt" "$DIST/"

# Pipeline writes JSON under epstein-web/viz_data/ (see epstein_photos.config.VIZ_DATA_DIR).
if [[ -d "$ROOT/viz_data" ]]; then
  cp -r "$ROOT/viz_data" "$DIST/"
else
  mkdir -p "$DIST/viz_data"
  echo "warning: no viz_data/ in repo root (run pipeline steps that write dataset.json, etc.)" >&2
fi

# /explore/ needs both umap_viz.json and viz_data/umap/thumbs/ populated. Both are gitignored
# (regenerated per pipeline run, too large or too numerous for git) and uploaded to Spaces
# by scripts/umap_viz/04__upload_to_spaces.py, so CI and clean clones pull them from the
# CDN. Locally, developers who just ran the pipeline already have both and we skip the fetch.
UMAP_DIR="$DIST/viz_data/umap"
SPACES_CDN_BASE="${EPSTEIN_SPACES_CDN_BASE:-https://epstein.sfo3.cdn.digitaloceanspaces.com}"
mkdir -p "$UMAP_DIR"

if [[ ! -f "$UMAP_DIR/umap_viz.json" ]]; then
  echo "Fetching UMAP umap_viz.json from ${SPACES_CDN_BASE}/umap/umap_viz.json ..."
  curl -fsSL "${SPACES_CDN_BASE}/umap/umap_viz.json" -o "$UMAP_DIR/umap_viz.json"
else
  echo "Using local umap_viz.json at $UMAP_DIR/umap_viz.json."
fi

if [[ ! -d "$UMAP_DIR/thumbs" ]] || [[ -z "$(ls -A "$UMAP_DIR/thumbs" 2>/dev/null)" ]]; then
  echo "Fetching UMAP thumbs.zip from ${SPACES_CDN_BASE}/umap/thumbs.zip ..."
  curl -fsSL "${SPACES_CDN_BASE}/umap/thumbs.zip" -o "$UMAP_DIR/thumbs.zip"
  ( cd "$UMAP_DIR" && rm -rf thumbs && unzip -q thumbs.zip && rm -f thumbs.zip )
else
  echo "Using local UMAP thumbs from $UMAP_DIR/thumbs."
fi

# Third-party script (not committed): fetch at build time so /search doesn’t block on unpkg at runtime.
IMAGELOADED_URL="https://unpkg.com/imagesloaded@5.0.0/imagesloaded.pkgd.min.js"
IMAGELOADED_OUT="$SITE/js/imagesloaded.pkgd.min.js"
curl -fsSL "$IMAGELOADED_URL" -o "$IMAGELOADED_OUT"

# latinize: pin 1.x (2.x is ESM-only). IIFE-wrap the bundle with a local ``module``
# shim so the Node ``module.exports = latinize;`` trailer is a harmless assignment
# in-browser, without leaking ``window.module`` (which would hijack the UMD wrapper
# in imagesloaded.pkgd.min.js and break ``window.imagesLoaded``).
LATINIZE_URL="https://unpkg.com/latinize@1.0.0/latinize.js"
LATINIZE_TMP="$SITE/js/latinize.js.tmp"
LATINIZE_OUT="$SITE/js/latinize.js"
curl -fsSL "$LATINIZE_URL" -o "$LATINIZE_TMP"
{
  printf '(function(){\n'
  printf 'var module={exports:{}};\n'
  cat "$LATINIZE_TMP"
  printf '\nwindow.latinize = latinize;\n'
  printf '})();\n'
} > "$LATINIZE_OUT"
rm -f "$LATINIZE_TMP"

cp -r "$SITE/js" "$DIST/"

# Copy static site images (icons, etc.) into dist/images.
if [[ -d "$SITE/images" ]]; then
  cp -r "$SITE/images" "$DIST/"
fi

# Local dev: atlas from the sibling ``network/images/`` produced by epstein-pipeline
# (``FACES_IMAGE_DIR``), falling back to the in-repo copy in prod / fresh clones.
ATLAS_PIPELINE="$ROOT/../images/atlas.webp"
if [[ -f "$ATLAS_PIPELINE" ]]; then
  mkdir -p "$DIST/images"
  cp "$ATLAS_PIPELINE" "$DIST/images/"
elif [[ -f "$ROOT/images/atlas.webp" ]]; then
  mkdir -p "$DIST/images"
  cp "$ROOT/images/atlas.webp" "$DIST/images/"
fi

if [[ -n "$STASH" ]]; then
  if [[ -d "$STASH/search/people" ]]; then
    mkdir -p "$DIST/search/people"
    cp -a "$STASH/search/people/." "$DIST/search/people/"
  fi
  [[ -f "$STASH/sitemap.xml" ]] && cp -a "$STASH/sitemap.xml" "$DIST/"
  [[ -f "$STASH/search-people-pages.json" ]] && cp -a "$STASH/search-people-pages.json" "$DIST/"
  rm -rf "$STASH"
fi

echo "Built static site → $DIST"

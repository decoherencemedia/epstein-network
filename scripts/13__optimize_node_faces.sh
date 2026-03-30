#!/usr/bin/env bash
set -euo pipefail

# Convert all images under node_faces_selected/ (any subdirectory) to WebP in
# node_faces_selected_optimized/ with flat filenames (same basename as the source file,
# extension replaced with .webp).
#
# Duplicate basenames in different folders are rejected (same rule as graph upload).
#
# Requirements:
# - cwebp
# - ImageMagick (magick, or convert+identify) for resizing — optional but recommended;
#   without it, cwebp is invoked on the original file with no resize.
#
# Usage:
#   bash scripts/13__optimize_node_faces.sh

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_DIR="$(cd "$SCRIPT_DIR/.." && pwd)"

NODE_INPUT_DIR="${NODE_INPUT_DIR:-$REPO_DIR/images/node_faces_unoptimized}"
NODE_OUTPUT_DIR="${NODE_OUTPUT_DIR:-$REPO_DIR/images/node_faces_optimized}"

PEOPLE_INPUT_DIR="${PEOPLE_INPUT_DIR:-$REPO_DIR/images/node_faces_selected}"
PEOPLE_OUTPUT_DIR="${PEOPLE_OUTPUT_DIR:-$REPO_DIR/images/node_faces_selected_optimized}"

CWEBP_QUALITY=90
MAX_EDGE_PX=1024

# Safety: refuse unexpected output locations (we no longer wipe them, but keep a guardrail).
for out in "$NODE_OUTPUT_DIR" "$PEOPLE_OUTPUT_DIR"; do
  case "$out" in
    "$REPO_DIR/images/node_faces_optimized" | "$REPO_DIR/images/node_faces_selected_optimized" ) ;;
    * ) echo "ERROR: Refusing unexpected OUTPUT_DIR='$out'" >&2; exit 1 ;;
  esac
done

if ! command -v cwebp >/dev/null 2>&1; then
  echo "ERROR: cwebp not found in PATH." >&2
  exit 1
fi

MAGICK_RESIZE=()
if command -v magick >/dev/null 2>&1; then
  MAGICK_RESIZE=(magick)
elif command -v convert >/dev/null 2>&1; then
  MAGICK_RESIZE=(convert)
fi

tmp_root="$(mktemp -d)"
cleanup() {
  rm -rf "$tmp_root"
}
trap cleanup EXIT

optimize_dir() {
  local label="$1"
  local input_dir="$2"
  local output_dir="$3"

  if [[ ! -d "$input_dir" ]]; then
    echo "WARNING: $label INPUT_DIR does not exist (skipping): $input_dir" >&2
    return 0
  fi

  mapfile -t found < <(
    find "$input_dir" -type f \( \
      -iname '*.jpg' -o -iname '*.jpeg' -o -iname '*.png' -o -iname '*.webp' \
    \) | sort
  )

  if [[ ${#found[@]} -eq 0 ]]; then
    echo "No images found under $input_dir (recursive). Nothing to convert for $label."
    return 0
  fi

  local dup
  dup=$(for src in "${found[@]}"; do basename "$src"; done | sort | uniq -d)
  if [[ -n "$dup" ]]; then
    echo "ERROR: Duplicate basename(s) under $input_dir for $label (flattened output would collide):" >&2
    echo "$dup" >&2
    exit 1
  fi

  mkdir -p "$output_dir"

  local count=0
  local skipped=0
  for src in "${found[@]}"; do
    local base stem out_webp tmp_proc
    base=$(basename "$src")
    stem="${base%.*}"
    out_webp="$output_dir/${stem}.webp"
    tmp_proc="$tmp_root/${label}.${count}.proc.png"

    if [[ -f "$out_webp" ]]; then
      skipped=$((skipped + 1))
      continue
    fi

    if [[ ${#MAGICK_RESIZE[@]} -gt 0 ]]; then
      "${MAGICK_RESIZE[@]}" "$src" -resize "${MAX_EDGE_PX}x${MAX_EDGE_PX}>" "$tmp_proc"
      cwebp -quiet -q "$CWEBP_QUALITY" "$tmp_proc" -o "$out_webp"
    else
      echo "WARNING: ImageMagick not found; running cwebp without resize (max edge ${MAX_EDGE_PX} ignored): $src" >&2
      cwebp -quiet -q "$CWEBP_QUALITY" "$src" -o "$out_webp"
    fi
    count=$((count + 1))
  done

  echo "Converted $count image(s) to WebP in $output_dir for $label (skipped $skipped existing; max edge ${MAX_EDGE_PX}px when ImageMagick is available)"
}

# Node tiles used in the visualization (often grayscale / bg-removed).
optimize_dir "node_faces" "$NODE_INPUT_DIR" "$NODE_OUTPUT_DIR"
# People crops used for people/ page + sidebar.
optimize_dir "people_faces" "$PEOPLE_INPUT_DIR" "$PEOPLE_OUTPUT_DIR"

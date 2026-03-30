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
INPUT_DIR="$REPO_DIR/images/node_faces_selected"
OUTPUT_DIR="$REPO_DIR/images/node_faces_selected_optimized"

CWEBP_QUALITY=90
MAX_EDGE_PX=1024

# Safety: refuse to wipe unexpected output locations.
case "$OUTPUT_DIR" in
  "$REPO_DIR/images/node_faces_selected_optimized" ) ;;
  * ) echo "ERROR: Refusing unexpected OUTPUT_DIR='$OUTPUT_DIR'" >&2; exit 1 ;;
esac

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

if [[ ! -d "$INPUT_DIR" ]]; then
  echo "ERROR: INPUT_DIR does not exist: $INPUT_DIR" >&2
  exit 1
fi

mapfile -t found < <(
  find "$INPUT_DIR" -type f \( \
    -iname '*.jpg' -o -iname '*.jpeg' -o -iname '*.png' -o -iname '*.webp' \
  \) | sort
)

if [[ ${#found[@]} -eq 0 ]]; then
  echo "No images found under $INPUT_DIR (recursive). Nothing to convert."
  exit 0
fi

dup=$(for src in "${found[@]}"; do basename "$src"; done | sort | uniq -d)
if [[ -n "$dup" ]]; then
  echo "ERROR: Duplicate basename(s) under $INPUT_DIR (flattened output would collide):" >&2
  echo "$dup" >&2
  exit 1
fi

mkdir -p "$OUTPUT_DIR"
# Empty output directory (contents only).
if [[ -d "$OUTPUT_DIR" ]]; then
  rm -rf "$OUTPUT_DIR"/* "$OUTPUT_DIR"/.[!.]* "$OUTPUT_DIR"/..?* 2>/dev/null || true
fi

tmp_root="$(mktemp -d)"
cleanup() {
  rm -rf "$tmp_root"
}
trap cleanup EXIT

count=0
for src in "${found[@]}"; do
  base=$(basename "$src")
  stem="${base%.*}"
  out_webp="$OUTPUT_DIR/${stem}.webp"
  tmp_proc="$tmp_root/${count}.proc.png"

  if [[ ${#MAGICK_RESIZE[@]} -gt 0 ]]; then
    "${MAGICK_RESIZE[@]}" "$src" -resize "${MAX_EDGE_PX}x${MAX_EDGE_PX}>" "$tmp_proc"
    cwebp -quiet -q "$CWEBP_QUALITY" "$tmp_proc" -o "$out_webp"
  else
    echo "WARNING: ImageMagick not found; running cwebp without resize (max edge ${MAX_EDGE_PX} ignored): $src" >&2
    cwebp -quiet -q "$CWEBP_QUALITY" "$src" -o "$out_webp"
  fi
  count=$((count + 1))
done

echo "Converted $count image(s) to WebP in $OUTPUT_DIR (max edge ${MAX_EDGE_PX}px when ImageMagick is available)"

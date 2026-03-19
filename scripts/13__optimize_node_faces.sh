#!/usr/bin/env bash
set -euo pipefail

# Convert all .png files from INPUT_DIR to square RESIZE_PX webp files in OUTPUT_DIR.
# Fails loudly if any input image is not square.
#
# Requirements:
# - cwebp
# - ImageMagick identify/convert (or magick)
#
# Usage:
#   bash scripts/13__optimize_node_faces.sh

INPUT_DIR="../images/node_faces_unoptimized/"
OUTPUT_DIR="../images/node_faces_optimized"
RESIZE_PX=100
CWEBP_QUALITY=90
MAX_ASPECT_DIFF_PERCENT=10

# Safety: refuse to wipe unexpected output locations.
case "$OUTPUT_DIR" in
  "../images/node_faces_optimized" | "../images/node_faces_optimized/" ) ;;
  * ) echo "ERROR: Refusing to wipe unexpected OUTPUT_DIR='$OUTPUT_DIR'" >&2; exit 1 ;;
esac

if ! command -v cwebp >/dev/null 2>&1; then
  echo "ERROR: cwebp not found in PATH." >&2
  exit 1
fi

IDENTIFY_CMD=""
CONVERT_CMD=""
if command -v identify >/dev/null 2>&1 && command -v convert >/dev/null 2>&1; then
  IDENTIFY_CMD="identify"
  CONVERT_CMD="convert"
elif command -v magick >/dev/null 2>&1; then
  IDENTIFY_CMD="magick identify"
  CONVERT_CMD="magick convert"
else
  echo "ERROR: ImageMagick identify/convert (or magick) not found in PATH." >&2
  exit 1
fi

if [[ ! -d "$INPUT_DIR" ]]; then
  echo "ERROR: INPUT_DIR does not exist: $INPUT_DIR" >&2
  exit 1
fi

# Empty output directory at start (contents only).
if [[ -d "$OUTPUT_DIR" ]]; then
  rm -rf "$OUTPUT_DIR"/* "$OUTPUT_DIR"/.[!.]* "$OUTPUT_DIR"/..?* || true
fi
mkdir -p "$OUTPUT_DIR"

shopt -s nullglob
png_files=("$INPUT_DIR"/*.png "$INPUT_DIR"/*.PNG)

if [[ ${#png_files[@]} -eq 0 ]]; then
  echo "ERROR: No PNG files found in $INPUT_DIR" >&2
  exit 1
fi

tmp_dir="$(mktemp -d)"
cleanup() {
  rm -rf "$tmp_dir"
}
trap cleanup EXIT

count=0
for src in "${png_files[@]}"; do
  base="$(basename "$src")"
  stem="${base%.*}"
  tmp_png="$tmp_dir/${stem}.png"
  out_webp="$OUTPUT_DIR/${stem}.webp"

  dims="$($IDENTIFY_CMD -format '%w %h' "$src")"
  w="${dims%% *}"
  h="${dims##* }"
  # Allow small non-square differences; fail only if aspect diff > MAX_ASPECT_DIFF_PERCENT.
  if (( w > h )); then
    max_dim="$w"
    min_dim="$h"
  else
    max_dim="$h"
    min_dim="$w"
  fi
  diff=$((max_dim - min_dim))
  if (( diff * 100 > max_dim * MAX_ASPECT_DIFF_PERCENT )); then
    echo "ERROR: Image too non-square (> ${MAX_ASPECT_DIFF_PERCENT}%): $src (${w}x${h})" >&2
    exit 1
  fi

  # Center-crop to square using the smaller dimension, then resize.
  $CONVERT_CMD "$src" \
    -gravity center \
    -crop "${min_dim}x${min_dim}+0+0" +repage \
    -resize "${RESIZE_PX}x${RESIZE_PX}!" \
    "$tmp_png"
  cwebp -quiet -q "$CWEBP_QUALITY" "$tmp_png" -o "$out_webp"
  count=$((count + 1))
done

echo "Converted $count PNG image(s) to ${RESIZE_PX}x${RESIZE_PX} WebP in $OUTPUT_DIR"

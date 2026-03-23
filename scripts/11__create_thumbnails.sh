#!/usr/bin/env bash
set -euo pipefail

# Create WebP thumbnails (max 500px on either side) for all images in images/filtered_images/
# Output mirrors subfolders into images/thumbnails/
#
# Requires ImageMagick (magick) with WebP support.
# Ubuntu: sudo apt-get update && sudo apt-get install -y imagemagick webp

IN_DIR="${1:-../../../all_images_with_faces}"   # pass a different input dir as $1 if you want
OUT_DIR="${2:-../../../all_images_with_faces_thumbnails}"       # pass a different output dir as $2 if you want
MAX_PX="${MAX_PX:-500}"          # override: MAX_PX=500 ./make_thumbnails.sh
QUALITY="${QUALITY:-82}"         # override: QUALITY=82 ./make_thumbnails.sh

# Refuse to rm -rf obviously dangerous paths (default "thumbnails" is safe).
case "$OUT_DIR" in
  ''|.|..|/) echo "create_thumbnails.sh: refused to use OUT_DIR='$OUT_DIR'" >&2; exit 1 ;;
  *) ;;
esac

rm -rf "$OUT_DIR"
mkdir -p "$OUT_DIR"

# Find common image types (case-insensitive) and convert to WebP.
# - auto-orient: respects EXIF orientation
# - strip: removes metadata (smaller files)
# - resize "${MAX_PX}x${MAX_PX}>": only shrink if larger than MAX_PX
find "$IN_DIR" -type f \( \
  -iname '*.jpg' -o -iname '*.jpeg' -o -iname '*.png' -o -iname '*.tif' -o -iname '*.tiff' -o \
  -iname '*.bmp' -o -iname '*.gif' -o -iname '*.webp' -o -iname '*.heic' -o -iname '*.avif' \
\) -print0 |
while IFS= read -r -d '' src; do
  rel="${src#"$IN_DIR"/}"                 # path relative to input dir
  rel_dir="$(dirname "$rel")"
  base="$(basename "$rel")"
  name="${base%.*}"

  mkdir -p "$OUT_DIR/$rel_dir"
  dst="$OUT_DIR/$rel_dir/$name.webp"

  # Skip if output exists and is newer than input
  if [[ -f "$dst" && "$dst" -nt "$src" ]]; then
    continue
  fi

  convert "$src" \
    -auto-orient \
    -resize "${MAX_PX}x${MAX_PX}>" \
    -strip \
    -quality "$QUALITY" \
    "$dst"
done

echo "Done. Thumbnails written to: $OUT_DIR/"
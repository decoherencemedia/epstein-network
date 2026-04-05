#!/usr/bin/env bash
set -euo pipefail

# Finds files > 5 MiB in INPUT_DIR, backs up originals to images_downsized/,
# then modifies the files in INPUT_DIR in place (convert PNG->JPEG, then
# jpegoptim to get under 5 MiB).
#
# - Originals are copied to INPUT_DIR/images_downsized/ (same relative path).
# - INPUT_DIR files are overwritten with the downsized/JPEG version.
#
# Requirements:
#   jpegoptim, file, magick (ImageMagick) or convert

# ---------------- CONFIG ----------------
INPUT_DIR="/home/tristan/Documents/misc/epstein/all_images"
# Backups of originals (subfolder of INPUT_DIR).
OUT_DIR="${INPUT_DIR}/images_downsized"
# --------------------------------------

if [[ ! -d "${INPUT_DIR}" ]]; then
  echo "Not a directory: ${INPUT_DIR}" >&2
  exit 1
fi

if ! command -v jpegoptim >/dev/null 2>&1; then
  echo "Missing dependency: jpegoptim (install it first)" >&2
  exit 1
fi

if ! command -v file >/dev/null 2>&1; then
  echo "Missing dependency: file (install it first)" >&2
  exit 1
fi

IMAGEMAGICK_BIN=""
if command -v magick >/dev/null 2>&1; then
  IMAGEMAGICK_BIN="magick"
elif command -v convert >/dev/null 2>&1; then
  IMAGEMAGICK_BIN="convert"
fi

INPUT_DIR="$(cd "${INPUT_DIR}" && pwd)"
OUT_DIR="$(mkdir -p "${OUT_DIR}" && cd "${OUT_DIR}" && pwd)"

# Rekognition Bytes limit is 5 MiB = 5 * 1024 * 1024 bytes = 5120 KiB.
MAX_BYTES=$((5 * 1024 * 1024))
TARGET_KB=5000

mkdir -p "${OUT_DIR}"

echo "Scanning: ${INPUT_DIR}"
echo "Output dir: ${OUT_DIR}"
echo "Max bytes: ${MAX_BYTES} (target jpegoptim --size=${TARGET_KB} --force)"
echo

count_found=0
count_copied=0
count_optimized=0
count_skipped_nonjpeg=0
count_converted_png=0
count_failed=0

# Find files larger than 5 MiB, excluding the backup folder itself.
while IFS= read -r -d '' src; do
  ((count_found++)) || true

  rel="${src#${INPUT_DIR}/}"
  backup="${OUT_DIR}/${rel}"
  mkdir -p "$(dirname "${backup}")"

  # 1) Backup original to images_downsized/ (unchanged)
  cp -f -- "${src}" "${backup}"
  ((count_copied++)) || true

  mime="$(file -b --mime-type -- "${src}" || true)"

  if [[ "${mime}" == "image/png" ]]; then
    if [[ -z "${IMAGEMAGICK_BIN}" ]]; then
      echo "Skip (PNG, but missing ImageMagick magick/convert): ${rel}" >&2
      ((count_failed++)) || true
      continue
    fi
    echo "Converting PNG -> JPEG (in place): ${rel}"
    tmp="${src}.tmp.jpg"
    if [[ "${IMAGEMAGICK_BIN}" == "magick" ]]; then
      magick "${src}" -strip -interlace Plane -quality 92 "${tmp}"
    else
      convert "${src}" -strip -interlace Plane -quality 92 "${tmp}"
    fi
    mv -f -- "${tmp}" "${src}"
    ((count_converted_png++)) || true
  fi

  mime2="$(file -b --mime-type -- "${src}" || true)"
  if [[ "${mime2}" != "image/jpeg" ]]; then
    echo "Skip (not JPEG): ${rel} (${mime2})"
    ((count_skipped_nonjpeg++)) || true
    continue
  fi

  echo "Optimizing (in place): ${rel}"
  if jpegoptim --quiet --force --strip-all --all-progressive --size="${TARGET_KB}" -- "${src}"; then
    ((count_optimized++)) || true
  else
    echo "  jpegoptim failed: ${rel}" >&2
    ((count_failed++)) || true
    continue
  fi

  # Verify size
  size_bytes="$(stat -c '%s' -- "${src}")"
  if (( size_bytes > MAX_BYTES )); then
    echo "  Warning: still > 5 MiB after jpegoptim: ${rel} ($(awk "BEGIN{printf \"%.2f\", ${size_bytes}/1024/1024}") MiB)" >&2
  fi

done < <(find "${INPUT_DIR}" -type f -size +5M ! -path "${OUT_DIR}/*" -print0)

echo
echo "Done."
echo "Found >5MiB: ${count_found}"
echo "Copied:      ${count_copied}"
echo "PNG->JPEG:   ${count_converted_png}"
echo "Optimized:   ${count_optimized}"
echo "Skipped:     ${count_skipped_nonjpeg} (non-JPEG)"
echo "Failed:      ${count_failed}"

#!/bin/bash
# Extract images from PDFs using GNU parallel for fast processing.
# Exits on first failure: set -e and parallel --halt now,fail=1.

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
INPUT_DIR="/home/tristan/Documents/misc/epstein/doj_release/"
OUTPUT_DIR="/home/tristan/Documents/misc/epstein/all_images"

INPUT_ROOT="$(cd "$INPUT_DIR" && pwd)"
export PDF_INPUT_ROOT="$INPUT_ROOT"

python3 "$SCRIPT_DIR/extract_pdf_images_parallel.py" "$INPUT_DIR" "$OUTPUT_DIR" --list-pdfs | \
    parallel -j $(nproc) --halt now,fail=1 env PDF_INPUT_ROOT="$INPUT_ROOT" python3 "$SCRIPT_DIR/extract_pdf_images_parallel.py" {} "$OUTPUT_DIR"
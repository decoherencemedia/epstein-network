#!/bin/bash
# Extract images from PDFs using GNU parallel for fast processing

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
INPUT_DIR="/home/tristan/Documents/misc/epstein/doj_release/"
OUTPUT_DIR="/home/tristan/Documents/misc/epstein/all_images"

# Use GNU parallel to process PDFs in parallel
# -j $(nproc) uses all available CPU cores
# You can adjust -j to use fewer cores if needed (e.g., -j 4)
INPUT_ROOT="$(cd "$INPUT_DIR" && pwd)"
export PDF_INPUT_ROOT="$INPUT_ROOT"

python3 "$SCRIPT_DIR/extract_pdf_images_parallel.py" "$INPUT_DIR" "$OUTPUT_DIR" --list-pdfs | \
    parallel -j $(nproc) env PDF_INPUT_ROOT="$INPUT_ROOT" python3 "$SCRIPT_DIR/extract_pdf_images_parallel.py" {} "$OUTPUT_DIR"
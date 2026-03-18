#!/usr/bin/env python3
"""
Recursively extract images from PDF files using pdfimages with GNU parallel support.

This script:
- Recursively searches through a directory for PDF files
- Runs pdfimages on each PDF
- Collects all extracted images into a single output directory
- Uses a well-defined naming structure: {pdf_basename}-{image_index:04d}.{ext}
- Designed to work with GNU parallel for fast parallel processing
"""

import argparse
import os
import re
import sqlite3
import time
import subprocess
import sys
import signal
from pathlib import Path
from tempfile import TemporaryDirectory
import shutil

# Single SQLite DB in output_dir to track extracted PDFs (one file for any number of PDFs).
EXTRACTED_DB = "_extracted.sqlite"

# ---------------- CONFIG (defaults used by 00__extract_pdf_images.sh) ----------------

DEFAULT_INDEX_WIDTH = 5
PDFIMAGES_CMD = "pdfimages"
DRY_RUN = False
QUIET = False
FORCE = False
PROGRESS_EVERY = 100
PROGRESS_INTERVAL = 10.0


def sanitize_filename(filename):
    """
    Sanitize a filename to be safe for use in filesystem.

    Args:
        filename: Original filename string

    Returns:
        Sanitized filename with problematic characters replaced
    """
    # Replace spaces and other problematic characters with underscores
    # Keep only alphanumeric, dashes, underscores, and dots
    sanitized = "".join(c if c.isalnum() or c in ('-', '_', '.') else '_' for c in filename)
    # Remove consecutive underscores
    while '__' in sanitized:
        sanitized = sanitized.replace('__', '_')
    return sanitized


def _extracted_db_path(output_dir):
    return Path(output_dir) / EXTRACTED_DB


def _ensure_extracted_table(conn):
    conn.execute("CREATE TABLE IF NOT EXISTS extracted (pdf_basename TEXT PRIMARY KEY)")
    conn.commit()


def _is_already_extracted(output_dir, pdf_basename):
    """Return True if this PDF basename is recorded as already extracted (O(1) via SQLite)."""
    db_path = _extracted_db_path(output_dir)
    if not db_path.exists():
        return False
    conn = sqlite3.connect(str(db_path), timeout=10.0)
    try:
        _ensure_extracted_table(conn)
        cur = conn.execute("SELECT 1 FROM extracted WHERE pdf_basename = ? LIMIT 1", (pdf_basename,))
        return cur.fetchone() is not None
    finally:
        conn.close()


def _mark_extracted(output_dir, pdf_basename):
    """Record that this PDF basename has been extracted (safe for concurrent workers)."""
    db_path = _extracted_db_path(output_dir)
    conn = sqlite3.connect(str(db_path), timeout=10.0)
    try:
        _ensure_extracted_table(conn)
        conn.execute("INSERT OR IGNORE INTO extracted (pdf_basename) VALUES (?)", (pdf_basename,))
        conn.commit()
    finally:
        conn.close()


def _remove_extracted(output_dir, pdf_basename):
    """Remove this PDF basename from the extracted DB (for --force re-extract)."""
    db_path = _extracted_db_path(output_dir)
    if not db_path.exists():
        return
    conn = sqlite3.connect(str(db_path), timeout=10.0)
    try:
        conn.execute("DELETE FROM extracted WHERE pdf_basename = ?", (pdf_basename,))
        conn.commit()
    finally:
        conn.close()


def _load_extracted_basenames(output_dir):
    """Load the set of pdf_basename that are already in the DB (one read, for filtering lists)."""
    db_path = _extracted_db_path(output_dir)
    if not db_path.exists():
        return set()
    conn = sqlite3.connect(str(db_path), timeout=10.0)
    try:
        cur = conn.execute("SELECT pdf_basename FROM extracted")
        return {row[0] for row in cur.fetchall()}
    finally:
        conn.close()


def seed_db_from_output(output_dir, verbose=True):
    """
    One-time migration helper: if you already extracted images in the past, populate the DB
    by scanning for files like '{pdf_basename}-0000.*' in output_dir.
    """
    output_dir = Path(output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)

    db_path = _extracted_db_path(output_dir)
    conn = sqlite3.connect(str(db_path), timeout=30.0)
    try:
        _ensure_extracted_table(conn)
        conn.execute("PRAGMA journal_mode=WAL;")

        basenames = set()
        scanned = 0
        matched = 0

        with os.scandir(output_dir) as it:
            for entry in it:
                if not entry.is_file():
                    continue
                scanned += 1
                name = entry.name
                # Match first-image output to infer completed PDFs (e.g. basename-00000.ext).
                marker = "-00000."
                i = name.find(marker)
                if i <= 0 or i + len(marker) >= len(name):
                    continue
                pdf_basename = name[:i]
                if pdf_basename:
                    basenames.add(pdf_basename)
                    matched += 1

                if verbose and scanned % 1_000_000 == 0:
                    print(f"Scanned {scanned:,} files, found {len(basenames):,} unique PDF basenames...", flush=True)

        if not basenames:
            if verbose:
                print(f"No '*-00000.*' files found in {output_dir} (nothing to seed).")
                return 0

        # Bulk insert
        conn.execute("BEGIN")
        conn.executemany(
            "INSERT OR IGNORE INTO extracted (pdf_basename) VALUES (?)",
            [(b,) for b in basenames],
        )
        conn.commit()

        if verbose:
            print(f"Seeded {len(basenames):,} PDF basenames into {db_path} (scanned {scanned:,} files).")
        return len(basenames)
    finally:
        conn.close()


def _numeric_sort_key(path: Path) -> tuple[int, str]:
    """Sort key: (numeric index from stem, full path) so temp-076 sorts before temp-100."""
    stem = path.stem
    # pdfimages: prefix-NNN or prefix-NNNN (e.g. temp-076, temp-1000)
    m = re.search(r"-(\d+)$", stem)
    if m:
        return (int(m.group(1)), str(path))
    return (0, str(path))


def extract_images_from_pdf(
    pdf_path,
    output_dir,
    pdfimages_cmd="pdfimages",
    verbose=True,
    input_dir_root=None,
    *,
    index_width: int = DEFAULT_INDEX_WIDTH,
    force: bool = False,
):
    """
    Extract images from a PDF file using pdfimages.

    Args:
        pdf_path: Path to the PDF file
        output_dir: Directory to save extracted images (flat, no subfolders)
        pdfimages_cmd: Command to run pdfimages (default: 'pdfimages')
        verbose: Whether to print progress messages
        input_dir_root: Root directory of input (used to determine subfolder name)

    Returns:
        Tuple of (pdf_path, count, success) where:
        - pdf_path is the input PDF path
        - count is the number of images extracted
        - success is True if extraction succeeded
    """
    pdf_path = Path(pdf_path)
    output_dir = Path(output_dir)
    # Ensure flat output directory exists
    output_dir.mkdir(parents=True, exist_ok=True)

    # Sanitize the PDF basename for use in output filenames
    pdf_basename = sanitize_filename(pdf_path.stem)

    # Skip if already extracted (unless --force). With --force, clear DB so we re-extract.
    if force:
        _remove_extracted(output_dir, pdf_basename)
    elif _is_already_extracted(output_dir, pdf_basename):
        if verbose:
            print(f"  Skipping (already done): {pdf_path.name}")
        return (str(pdf_path), 0, True)

    # Use a temporary directory for pdfimages output
    # pdfimages creates files with a prefix, so we'll extract there first
    with TemporaryDirectory() as temp_dir:
        temp_path = Path(temp_dir)
        # Use a simple prefix - pdfimages will append -000, -001, etc.
        temp_prefix = temp_path / "temp"

        try:
            # Run pdfimages command
            # -all: extract all images (both raster and vector)
            cmd = [
                pdfimages_cmd,
                '-all',  # Extract all images
                str(pdf_path),
                str(temp_prefix)
            ]

            subprocess.run(
                cmd,
                capture_output=True,
                text=True,
                check=True
            )

            # Find all extracted image files
            # pdfimages typically creates files like prefix-000.ppm, prefix-001.ppm, etc.
            # It might also create .jpg, .tiff, or other formats depending on the PDF content
            extracted_files = []
            for ext in ['ppm', 'pbm', 'jpg', 'jpeg', 'png', 'tiff', 'tif']:
                # Check for numbered files (pdfimages uses -000, -001, etc.)
                pattern_files = list(temp_path.glob(f"temp-*.{ext}"))
                pattern_files.extend(temp_path.glob(f"temp-*.{ext.upper()}"))

                # Also check for unnumbered files (some versions of pdfimages)
                pattern_files.extend(temp_path.glob(f"temp.{ext}"))
                pattern_files.extend(temp_path.glob(f"temp.{ext.upper()}"))

                extracted_files.extend(pattern_files)

            # Sort by numeric index from pdfimages output (temp-000, temp-001, ... temp-100, ...)
            extracted_files.sort(key=_numeric_sort_key)

            if not extracted_files:
                if verbose:
                    print(f"  No images in {pdf_path.name} (skipping, marking done)")
                _mark_extracted(output_dir, pdf_basename)
                return (str(pdf_path), 0, True)

            # Move and rename files to output directory
            output_paths = []
            for idx, extracted_file in enumerate(extracted_files):
                # Determine the original extension
                original_ext = extracted_file.suffix.lower()

                # For consistency, convert to .jpg for most image types
                # You can modify this if you want to preserve original formats
                if original_ext in ['.ppm', '.pbm', '.tiff', '.tif', '.png']:
                    output_ext = '.jpg'
                else:
                    output_ext = original_ext if original_ext else '.jpg'

                # Create output filename: {pdf_basename}-{image_index:0Nd}.{ext}
                output_filename = f"{pdf_basename}-{idx:0{index_width}d}{output_ext}"
                output_path = output_dir / output_filename

                # Handle potential filename collisions (e.g. from a previous partial run)
                collision_idx = 1
                while output_path.exists():
                    output_filename = f"{pdf_basename}-{idx:0{index_width}d}_{collision_idx}{output_ext}"
                    output_path = output_dir / output_filename
                    collision_idx += 1

                # Copy the file to output directory
                shutil.copy2(extracted_file, output_path)
                output_paths.append(output_path)

            if verbose:
                print(f"  Extracted {len(output_paths)} image(s) from {pdf_path.name}")
            _mark_extracted(output_dir, pdf_basename)
            return (str(pdf_path), len(output_paths), True)

        except subprocess.CalledProcessError as e:
            err = (e.stderr or str(e)).strip()
            # Only skip on these exact pdfimages corruption messages; any other failure we raise.
            skip_errors = (
                "Syntax Error: Couldn't find trailer dictionary",
                "Syntax Error: Couldn't read xref table",
            )
            if any(msg in err for msg in skip_errors):
                if verbose:
                    print(f"  Skipping (corrupted PDF): {pdf_path.name} — {err}", file=sys.stderr)
                _mark_extracted(output_dir, pdf_basename)
                return (str(pdf_path), 0, True)
            msg = f"pdfimages failed on {pdf_path.name}: {e.stderr or e}"
            if verbose:
                print(f"  Error: {msg}", file=sys.stderr)
            raise RuntimeError(msg) from e
        except Exception:
            if verbose:
                print(f"  Unexpected error processing {pdf_path.name}", file=sys.stderr)
            raise


def find_pdf_files(directory):
    """
    Recursively find all PDF files in a directory.

    Args:
        directory: Root directory to search

    Returns:
        List of Path objects for PDF files
    """
    directory = Path(directory)
    if not directory.exists():
        raise ValueError(f"Directory does not exist: {directory}")

    pdf_files = list(directory.rglob("*.pdf"))
    pdf_files.extend(directory.rglob("*.PDF"))

    return sorted(pdf_files)


def main():
    parser = argparse.ArgumentParser(
        description="Extract images from PDFs using pdfimages. Use with 00__extract_pdf_images.sh (GNU parallel).",
    )
    parser.add_argument("input_path", type=str, help="Input directory (recursive) or single PDF file")
    parser.add_argument("output_dir", type=str, help="Output directory for extracted images")
    parser.add_argument(
        "--list-pdfs",
        action="store_true",
        help="List PDFs (filtered by extracted DB if output_dir exists) and exit; for piping to parallel",
    )
    args = parser.parse_args()

    if args.list_pdfs:
        if not args.input_path:
            raise RuntimeError("input_path required when using --list-pdfs")
        pdf_files = find_pdf_files(args.input_path)
        if args.output_dir:
            output_dir = Path(args.output_dir)
            done_basenames = _load_extracted_basenames(output_dir)
            pdf_files = [p for p in pdf_files if sanitize_filename(Path(p).stem) not in done_basenames]
        for pdf_file in pdf_files:
            print(pdf_file, flush=True)
        return

    if not args.input_path or not args.output_dir:
        raise RuntimeError("input_path and output_dir required (or use --list-pdfs with input_path)")

    input_path = Path(args.input_path)
    if not input_path.exists():
        raise FileNotFoundError(f"Path does not exist: {input_path}")

    output_dir = Path(args.output_dir)

    # Single PDF (e.g. worker invoked by parallel)
    if input_path.is_file() and input_path.suffix.lower() == ".pdf":
        input_dir_root = os.environ.get("PDF_INPUT_ROOT")
        if input_dir_root:
            input_dir_root = Path(input_dir_root).resolve()
        else:
            input_dir_root = input_path.parent.resolve()
        _, _, success = extract_images_from_pdf(
            input_path,
            output_dir,
            PDFIMAGES_CMD,
            verbose=False,
            input_dir_root=input_dir_root,
            index_width=DEFAULT_INDEX_WIDTH,
            force=FORCE,
        )
        if not success:
            raise RuntimeError(f"Extraction failed for {input_path}")
        return

    # Directory mode
    input_dir_root = input_path.resolve()
    if not DRY_RUN:
        try:
            subprocess.run(
                [PDFIMAGES_CMD, "-v"],
                capture_output=True,
                check=True,
                timeout=5,
            )
        except (subprocess.CalledProcessError, FileNotFoundError, subprocess.TimeoutExpired) as e:
            raise RuntimeError(
                f"'{PDFIMAGES_CMD}' not found or not working. Install poppler-utils (apt install poppler-utils; macOS: brew install poppler)."
            ) from e

    print(f"Searching for PDF files in: {input_path}")
    pdf_files = find_pdf_files(input_path)
    if not pdf_files:
        print(f"No PDF files found in {input_path}")
        return
    print(f"Found {len(pdf_files)} PDF file(s)")
    print()

    if DRY_RUN:
        print("Dry run - would process:")
        for p in pdf_files:
            print(f"  {p}")
        return

    output_dir.mkdir(parents=True, exist_ok=True)
    done_basenames = _load_extracted_basenames(output_dir)
    todo = [p for p in pdf_files if sanitize_filename(p.stem) not in done_basenames]
    skipped_count = len(pdf_files) - len(todo)

    total_images = 0
    successful_pdfs = 0
    failed_pdfs = 0
    total_todo = len(todo)
    start_time = time.monotonic()
    last_progress_time = 0.0

    for pdf_file in todo:
        done_so_far = successful_pdfs + failed_pdfs
        current = done_so_far + 1
        left = total_todo - current
        now = time.monotonic()

        show_progress = False
        if not QUIET:
            if current == 1 or left == 0:
                show_progress = True
            elif PROGRESS_EVERY and (current - 1) % PROGRESS_EVERY == 0:
                show_progress = True
            elif PROGRESS_INTERVAL and (now - last_progress_time) >= PROGRESS_INTERVAL:
                show_progress = True

        if show_progress:
            last_progress_time = now
            eta_str = ""
            if done_so_far > 0 and left > 0:
                elapsed = now - start_time
                rate = done_so_far / elapsed
                eta_seconds = left / rate
                if eta_seconds >= 3600:
                    eta_str = f" [~{eta_seconds / 3600:.1f}h left]"
                elif eta_seconds >= 60:
                    eta_str = f" [~{eta_seconds / 60:.0f}m left]"
                else:
                    eta_str = f" [~{eta_seconds:.0f}s left]"
            print(f"Processing {current}/{total_todo} ({left} left){eta_str}")

        _, count, success = extract_images_from_pdf(
            pdf_file,
            output_dir,
            PDFIMAGES_CMD,
            verbose=show_progress,
            input_dir_root=input_dir_root,
            index_width=DEFAULT_INDEX_WIDTH,
            force=FORCE,
        )

        if not success:
            raise RuntimeError(f"Extraction failed for {pdf_file} (halting on first failure)")

        successful_pdfs += 1
        total_images += count
        if show_progress:
            print()

    print("=" * 60)
    print("Extraction Summary")
    print("=" * 60)
    print(f"Total PDF files in input: {len(pdf_files)}")
    if skipped_count:
        print(f"Skipped (already in DB): {skipped_count}")
    print(f"Processed this run: {len(todo)}")
    print(f"Successful: {successful_pdfs}")
    print(f"Failed: {failed_pdfs}")
    print(f"Total images extracted: {total_images}")
    print(f"Output directory: {output_dir.resolve()}")


if __name__ == "__main__":
    signal.signal(signal.SIGPIPE, signal.SIG_DFL)
    main()

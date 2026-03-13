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
import sqlite3
import subprocess
import sys
import signal
from pathlib import Path
from tempfile import TemporaryDirectory
import shutil

# Single SQLite DB in output_dir to track extracted PDFs (one file for any number of PDFs).
EXTRACTED_DB = "_extracted.sqlite"


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
                # Match only first-image outputs to infer completed PDFs.
                marker = "-0000."
                idx = name.find(marker)
                if idx <= 0:
                    continue
                # Ensure there's an extension after "-0000."
                if idx + len(marker) >= len(name):
                    continue
                pdf_basename = name[:idx]
                if pdf_basename:
                    basenames.add(pdf_basename)
                    matched += 1

                if verbose and scanned % 1_000_000 == 0:
                    print(f"Scanned {scanned:,} files, found {len(basenames):,} unique PDF basenames...", flush=True)

        if not basenames:
            if verbose:
                print(f"No '*-0000.*' files found in {output_dir} (nothing to seed).")
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


def extract_images_from_pdf(pdf_path, output_dir, pdfimages_cmd='pdfimages', verbose=True, input_dir_root=None):
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

    # Skip if already extracted (one SQLite DB in output_dir, no per-PDF files).
    if _is_already_extracted(output_dir, pdf_basename):
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

            # Sort files to maintain order
            extracted_files.sort()

            if not extracted_files:
                if verbose:
                    print(f"  Warning: No images extracted from {pdf_path.name}")
                return (str(pdf_path), 0, False)

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

                # Create output filename: {pdf_basename}-{image_index:04d}.{ext}
                output_filename = f"{pdf_basename}-{idx:04d}{output_ext}"
                output_path = output_dir / output_filename

                # Handle potential filename collisions (though unlikely with 4-digit index)
                collision_idx = 1
                while output_path.exists():
                    output_filename = f"{pdf_basename}-{idx:04d}_{collision_idx}{output_ext}"
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
            if verbose:
                print(f"  Error running pdfimages on {pdf_path.name}: {e.stderr}")
            return (str(pdf_path), 0, False)
        except Exception as e:
            if verbose:
                print(f"  Unexpected error processing {pdf_path.name}: {e}")
            return (str(pdf_path), 0, False)


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
        description="Recursively extract images from PDF files using pdfimages with GNU parallel support",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Sequential processing
  %(prog)s /path/to/pdfs /path/to/output

  # Process a single PDF file
  %(prog)s /path/to/file.pdf /path/to/output

  # Use with GNU parallel (recommended for many PDFs)
  %(prog)s /path/to/pdfs /path/to/output --list-pdfs | \\
    parallel -j $(nproc) %(prog)s {{}} /path/to/output

  # Or generate GNU parallel command
  %(prog)s /path/to/pdfs /path/to/output --gnu-parallel

Output naming format:
  {pdf_basename}-{image_index:04d}.{ext}

  Where:
    - pdf_basename is the PDF filename (without extension, sanitized)
    - image_index is a zero-padded 4-digit number (0000, 0001, etc.)
    - ext is the image extension (.jpg, .png, etc.)

  Example: "document-0000.jpg", "document-0001.jpg", "report-0000.jpg"
        """
    )

    parser.add_argument(
        'input_path',
        type=str,
        nargs='?',
        help='Input directory containing PDF files (searched recursively) or a single PDF file path. Required unless --list-pdfs is used.'
    )

    parser.add_argument(
        'output_dir',
        type=str,
        nargs='?',
        help='Output directory for extracted images. Required unless --list-pdfs is used.'
    )

    parser.add_argument(
        '--pdfimages-cmd',
        type=str,
        default='pdfimages',
        help='Command to run pdfimages (default: pdfimages)'
    )

    parser.add_argument(
        '--dry-run',
        action='store_true',
        help='Show what would be processed without actually extracting'
    )

    parser.add_argument(
        '--single-pdf',
        type=str,
        metavar='PDF_FILE',
        help='[DEPRECATED] Process a single PDF file. Just pass the PDF file as the first argument instead.'
    )

    parser.add_argument(
        '--list-pdfs',
        action='store_true',
        help='List all PDF files found and exit (for use with GNU parallel)'
    )

    parser.add_argument(
        '--gnu-parallel',
        action='store_true',
        help='Generate GNU parallel command and exit'
    )

    parser.add_argument(
        '--seed-db-from-output',
        action='store_true',
        help=f"Populate {EXTRACTED_DB} by scanning output_dir for '*-0000.*' files, then exit."
    )

    parser.add_argument(
        '--quiet', '-q',
        action='store_true',
        help='Suppress per-PDF messages (skips and extraction counts). Summary still printed.'
    )

    args = parser.parse_args()

    # Handle deprecated --single-pdf flag (for backward compatibility)
    if args.single_pdf:
        if not args.output_dir:
            print("Error: output_dir is required when using --single-pdf", file=sys.stderr)
            sys.exit(1)
        args.input_path = args.single_pdf

    # Handle list PDFs mode
    if args.list_pdfs:
        if not args.input_path:
            print("Error: input_path is required when using --list-pdfs", file=sys.stderr)
            sys.exit(1)

        try:
            pdf_files = find_pdf_files(args.input_path)
            # If output_dir given, only list PDFs not yet in the skip DB (so parallel runs fewer jobs).
            if args.output_dir:
                output_dir = Path(args.output_dir)
                done_basenames = _load_extracted_basenames(output_dir)
                pdf_files = [p for p in pdf_files if sanitize_filename(Path(p).stem) not in done_basenames]
            for pdf_file in pdf_files:
                try:
                    print(pdf_file, flush=True)
                except BrokenPipeError:
                    sys.exit(0)
        except ValueError as e:
            print(f"Error: {e}", file=sys.stderr)
            sys.exit(1)
        sys.exit(0)

    # Handle GNU parallel command generation
    if args.gnu_parallel:
        if not args.input_path or not args.output_dir:
            print("Error: input_path and output_dir are required when using --gnu-parallel", file=sys.stderr)
            sys.exit(1)

        script_path = Path(__file__).absolute()
        input_root = Path(args.input_path).resolve()
        print(f"# Run this command to process PDFs in parallel with GNU parallel:")
        print(f"# (Only PDFs not already in {EXTRACTED_DB} are listed, so reruns skip done work.)")
        print(f"export PDF_INPUT_ROOT='{input_root}'")
        print(f"{script_path} {args.input_path} {args.output_dir} --list-pdfs | \\")
        print(f"  parallel -j $(nproc) env PDF_INPUT_ROOT='{input_root}' {script_path} {{}} {args.output_dir} --quiet")
        sys.exit(0)

    # Normal mode requires both arguments
    if not args.input_path or not args.output_dir:
        parser.print_help()
        sys.exit(1)

    # Seed mode: scan output directory to populate skip DB, then exit.
    if args.seed_db_from_output:
        seed_db_from_output(args.output_dir, verbose=True)
        sys.exit(0)

    # Check if input_path is a single PDF file or a directory
    input_path = Path(args.input_path)
    if not input_path.exists():
        print(f"Error: Path does not exist: {input_path}", file=sys.stderr)
        sys.exit(1)

    # If it's a file (and looks like a PDF), process it as a single PDF
    if input_path.is_file() and input_path.suffix.lower() == '.pdf':
        output_dir = Path(args.output_dir)
        # For single file mode, try to get input root from environment variable (set by GNU parallel)
        # or try to detect it by looking for a parent directory with "DataSet" in subfolders
        input_dir_root = os.environ.get('PDF_INPUT_ROOT')
        if input_dir_root:
            input_dir_root = Path(input_dir_root).resolve()
        else:
            # Try to detect: walk up the directory tree looking for a folder that contains
            # subfolders matching "DataSet*" pattern
            current = input_path.parent.resolve()
            found_root = None
            for _ in range(10):  # Limit search depth
                # Check if current directory has subfolders matching "DataSet*"
                if current.exists() and current.is_dir():
                    subdirs = [d.name for d in current.iterdir() if d.is_dir()]
                    if any('dataset' in d.lower() for d in subdirs):
                        found_root = current
                        break
                parent = current.parent
                if parent == current:  # Reached filesystem root
                    break
                current = parent

            input_dir_root = found_root if found_root else input_path.parent

        result = extract_images_from_pdf(input_path, output_dir, args.pdfimages_cmd, verbose=not args.quiet, input_dir_root=input_dir_root)
        pdf_path_str, count, success = result
        sys.exit(0 if success else 1)

    # Otherwise, treat it as a directory
    input_dir = args.input_path
    input_dir_root = Path(input_dir).resolve()

    # Check if pdfimages is available
    if not args.dry_run:
        try:
            subprocess.run(
                [args.pdfimages_cmd, '-v'],
                capture_output=True,
                check=True,
                timeout=5
            )
        except (subprocess.CalledProcessError, FileNotFoundError, subprocess.TimeoutExpired):
            print(f"Error: '{args.pdfimages_cmd}' command not found or not working.", file=sys.stderr)
            print("Please install poppler-utils (which includes pdfimages):", file=sys.stderr)
            print("  Ubuntu/Debian: sudo apt-get install poppler-utils", file=sys.stderr)
            print("  macOS: brew install poppler", file=sys.stderr)
            print("  Or specify the correct command with --pdfimages-cmd", file=sys.stderr)
            sys.exit(1)

    # Find all PDF files
    print(f"Searching for PDF files in: {input_dir}")
    try:
        pdf_files = find_pdf_files(input_dir)
    except ValueError as e:
        print(f"Error: {e}", file=sys.stderr)
        sys.exit(1)

    if not pdf_files:
        print(f"No PDF files found in {input_dir}")
        sys.exit(0)

    print(f"Found {len(pdf_files)} PDF file(s)")
    print()

    if args.dry_run:
        print("Dry run mode - would process:")
        for pdf_file in pdf_files:
            print(f"  {pdf_file}")
        sys.exit(0)

    # Process each PDF file sequentially
    output_dir = Path(args.output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)

    # Only process PDFs not already in the skip DB (one DB read, then no per-PDF skip checks).
    done_basenames = _load_extracted_basenames(output_dir)
    todo = [p for p in pdf_files if sanitize_filename(p.stem) not in done_basenames]
    skipped_count = len(pdf_files) - len(todo)

    total_images = 0
    successful_pdfs = 0
    failed_pdfs = 0

    for pdf_file in todo:
        if not args.quiet:
            print(f"Processing: {pdf_file}")
        pdf_path_str, count, success = extract_images_from_pdf(
            pdf_file,
            output_dir,
            args.pdfimages_cmd,
            verbose=not args.quiet,
            input_dir_root=input_dir_root
        )

        if success and count > 0:
            successful_pdfs += 1
            total_images += count
        else:
            failed_pdfs += 1
        if not args.quiet:
            print()

    # Print summary
    print("=" * 60)
    print("Extraction Summary")
    print("=" * 60)
    print(f"Total PDF files in input: {len(pdf_files)}")
    if skipped_count:
        print(f"Skipped (already in DB): {skipped_count}")
    print(f"Processed this run: {len(todo)}")
    print(f"Successful: {successful_pdfs}")
    print(f"Failed/No images: {failed_pdfs}")
    print(f"Total images extracted: {total_images}")
    print(f"Output directory: {output_dir.absolute()}")


if __name__ == "__main__":
    # Handle broken pipe gracefully (common when piping to commands like head, parallel, etc.)
    # Set SIGPIPE to SIG_DFL to avoid traceback when pipe closes
    signal.signal(signal.SIGPIPE, signal.SIG_DFL)

    try:
        main()
    except BrokenPipeError:
        # Pipe was closed early (e.g., by GNU parallel)
        # Exit cleanly without error
        sys.exit(0)

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

# Default width for image index in output filenames (e.g. 5 -> -00000 to -99999).
DEFAULT_INDEX_WIDTH = 5

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
  {pdf_basename}-{image_index:0Nd}.{ext}  (N = --index-width, default 5)

  Where:
    - pdf_basename is the PDF filename (without extension, sanitized)
    - image_index is zero-padded (e.g. 00000..99999 for width 5)
    - ext is the image extension (.jpg, .png, etc.)

  Example: "document-00000.jpg", "document-00001.jpg"
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
        help=f"Populate {EXTRACTED_DB} by scanning output_dir for '*-00000.*' files, then exit."
    )

    parser.add_argument(
        '--quiet', '-q',
        action='store_true',
        help='Suppress per-PDF messages (skips and extraction counts). Summary still printed.'
    )

    parser.add_argument(
        '--index-width',
        type=int,
        default=DEFAULT_INDEX_WIDTH,
        metavar='N',
        help=f'Width of zero-padded image index in output filenames (default: {DEFAULT_INDEX_WIDTH}). E.g. 5 -> -00000 to -99999.'
    )

    parser.add_argument(
        '--force',
        action='store_true',
        help='Re-extract even if PDF is already in the extracted DB (clears DB entry for that PDF first).'
    )

    parser.add_argument(
        '--progress-every',
        type=int,
        default=100,
        metavar='N',
        help='In directory mode: print progress at most every N PDFs (default: 100). Use 1 for every PDF, 0 to disable count-based throttle.',
    )
    parser.add_argument(
        '--progress-interval',
        type=float,
        default=10.0,
        metavar='SECS',
        help='In directory mode: print progress at most every SECS seconds (default: 10). Use 0 to disable time-based throttle.',
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

        result = extract_images_from_pdf(
            input_path,
            output_dir,
            args.pdfimages_cmd,
            verbose=False,  # no per-PDF output when run by parallel (one PDF per process)
            input_dir_root=input_dir_root,
            index_width=args.index_width,
            force=args.force,
        )
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
    total_todo = len(todo)
    start_time = time.monotonic()
    last_progress_time = 0.0

    for pdf_file in todo:
        done_so_far = successful_pdfs + failed_pdfs
        current = done_so_far + 1
        left = total_todo - current
        now = time.monotonic()

        # Throttle progress: first, last, every N PDFs, or every N seconds
        progress_every = args.progress_every
        progress_interval = args.progress_interval
        if progress_every is None:
            progress_every = 0
        if progress_interval is None:
            progress_interval = 0.0
        show_progress = False
        if args.quiet:
            show_progress = False
        elif current == 1 or left == 0:
            show_progress = True
        elif progress_every and (current - 1) % progress_every == 0:
            show_progress = True
        elif progress_interval and (now - last_progress_time) >= progress_interval:
            show_progress = True

        if show_progress and not args.quiet:
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

        pdf_path_str, count, success = extract_images_from_pdf(
            pdf_file,
            output_dir,
            args.pdfimages_cmd,
            verbose=show_progress,
            input_dir_root=input_dir_root,
            index_width=args.index_width,
            force=args.force,
        )

        if not success:
            failed_pdfs += 1
            print(f"Error: extraction failed for {pdf_file}", file=sys.stderr)
            print("Halting on first failure (no further PDFs will be processed).", file=sys.stderr)
            sys.exit(1)
        successful_pdfs += 1
        total_images += count
        if show_progress:
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

    if failed_pdfs > 0:
        sys.exit(1)


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

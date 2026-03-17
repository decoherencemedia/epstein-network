# PDF extraction: missing early images (0000–0075) and padding

## Why you might only have -0076 through -0081

The script names outputs by **enumerate()** over the sorted list of files pdfimages wrote: first file → `-0000`, second → `-0001`, etc. So if you truly have `EFTA00251483-0076.jpg` … `-0081.jpg` and nothing below 0076, then either:

1. **Deletion after a full run**  
   A full extraction wrote `-0000` … `-0081`. Something later (manual, another script, or cleanup) removed `-0000` … `-0075`. The DB still has that PDF marked extracted, so re-runs skip it. **Most plausible.**

2. **Partial run then marked done**  
   An older version of the script (or a one-off run) might have marked the PDF as extracted before all files were written, or a crash left the DB in a bad state. Then only the last batch (76–81) was ever written, or a later run overwrote with a partial set. Less likely with the current “mark only at end” logic.

3. **GNU parallel / concurrency**  
   If the same PDF was processed by two workers (e.g. duplicate paths or list-pdfs bug), both run pdfimages and write to the same output dir. You’d usually get one full set (possibly with races); getting exactly 76–81 and nothing else would require a very specific overwrite pattern. Unlikely.

4. **pdfimages only emitted 76–81 for that PDF**  
   Then the script would name them `-0000` … `-0005`, not `-0076` … `-0081`. So this only fits if you’re not using the current naming (e.g. an old script used pdfimages’ own number in the filename).

**Practical fix:** Clear that PDF from the “extracted” DB and re-run extraction for it (or use a `--force` re-extract). The script now supports `--force` so you can re-extract without editing the DB by hand.

## Sort order

The code used `extracted_files.sort()` (lexicographic). For pdfimages’ usual 3-digit names (`temp-000` … `temp-999`) that’s correct. For 4-digit or 10k+ images, lexicographic order can be wrong (e.g. `temp-10000` before `temp-09999`). The script now sorts by the **numeric** index parsed from the stem so order is correct for any count.

## Padding and the “_1” suffix

- **Padding:** The script used a fixed `:04d`, so indices 0–9999 are 4-digit; 10000+ become 5-digit (`-10000`, `-10001`, …). That’s valid but inconsistent length. The script now supports `--index-width` (default 5) so you can use 5 or 6 digits and avoid mixed 4/5-digit names.

- **`_1` suffix:** That’s only for **collisions**: when the chosen output path already exists (e.g. a previous partial run left `-0003.jpg` and we write again). It’s not used for indices above 9999. So “messy” is either (a) collision re-runs producing `-0003_1.jpg`, or (b) mixed 4- vs 5-digit indices. Using a single, larger `--index-width` avoids (b).

---

## Error handling (fail loudly and halt)

- **Python script**
  - **pdfimages** non-zero exit: script raises `RuntimeError` with stderr, process exits 1.
  - **No images** from a PDF: script raises `RuntimeError`, process exits 1.
  - **Any other exception** (e.g. disk full, permission, `shutil.copy2` failure): exception is re-raised, process exits 1 with traceback.
  - **Directory mode**: on first failure (returned `success=False` or exception) the run stops; summary is only printed if the loop completes without error. Final exit code is 1 if any PDF failed.

- **Shell script (`00__extract_pdf_images.sh`)**
  - **`set -euo pipefail`**: any failed command (including the list-pdfs Python call) exits the script immediately.
  - **`parallel --halt now,fail=1`**: as soon as one worker exits non-zero, parallel kills remaining jobs and exits with that non-zero status. So the first failing PDF stops the whole run.

- **Resuming after a halt**: The SQLite “extracted” DB is only updated after a PDF is fully written. If the run stops (error or Ctrl+C), that PDF is not marked done. Re-run the same pipeline and it will skip already-extracted PDFs and continue from the next one. Fix the cause of the failure (e.g. disk space, bad PDF) before re-running.

## Other risks for long, large runs

- **Disk space**: Extraction runs in a temp dir then copies to `output_dir`. If the destination filesystem fills during `shutil.copy2`, you get `OSError` and the process exits. Ensure enough free space (hundreds of GB as you said) and monitor (e.g. `df`) or run with a quota.

- **Duplicate basenames**: Output names are `{pdf_basename}-{index}.ext` with no path. If two different PDFs in the tree have the same stem (e.g. `report.pdf` in two folders), they’ll both use the same basename and **overwrite** each other’s images. Use a single PDF tree with unique filenames, or change the script to include a path component in the basename.

- **SQLite under parallel**: The script uses one DB in `output_dir` and `INSERT OR IGNORE` / `DELETE` with short timeouts. Concurrent workers are safe for mark/remove; if you see lock errors, reduce `-j` or add a short retry.

- **Broken pipe**: If the list-pdfs pipe to parallel closes early, the Python producer gets `SIGPIPE`; the script catches `BrokenPipeError` and exits 0 so you don’t get a noisy traceback. That’s expected when parallel stops early (e.g. `--halt now,fail=1`).

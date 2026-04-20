#!/usr/bin/env python3
"""
Generate static HTML pages for people search URLs::

    /search/people/<sorted-person_id-1>-<sorted-person_id-2>-.../

One ``index.html`` per non-empty combination (same semantics as API ``/photos``:
``person_eligible_images`` intersection).

Edit ``DIST_DIR``, ``MAX_K``, ``SITE_ORIGIN``, ``DRY_RUN``, etc., then run::

    ~/.venv/epstein/bin/python epstein-web/scripts/generate_static_search_pages.py

Database: same env convention as ``epstein-api/run.sh`` — ``EPSTEIN_SQLITE_PATH`` (file path) and
``DB_URL`` (optional; if set, ``curl`` downloads into that path first). If ``EPSTEIN_SQLITE_PATH``
is unset, uses the local default ``network/faces_prod.db`` (API slice next to the pipeline DB).

Open Graph images use ``EPSTEIN_SPACES_CDN_BASE`` (default matches ``site/js/shared.js``): for a single
person, the same best-face WebP as ``GET /faces``; for multiple people, the first intersecting
document image (``ORDER BY image_name``), matching the first /photos result.

Optional: ``EPSTEIN_OG_SITE_NAME`` (default ``Epstein Network``), ``EPSTEIN_OG_LOCALE`` (default ``en_US``).

When not ``DRY_RUN``, run ``site/build.sh`` first so ``DIST_DIR`` contains ``js/``, ``styles.css``, …
Re-running ``build.sh`` preserves ``search/people/``, ``sitemap.xml``, and ``search-people-pages.json``
from a prior generator run.

Also writes a single root ``sitemap.xml`` in ``DIST_DIR`` (core pages from ``site/build.sh`` plus
every generated ``/search/people/…/`` URL) and ``search-people-pages.json`` (manifest of generated
pages only). Submit ``/sitemap.xml`` in Search Console.

Not wired into ``build.sh`` yet.

Faster variant (loads PEI once, in-memory set intersections): ``generate_static_search_pages_mem.py``.
"""

from __future__ import annotations

import html
import itertools
import json
import os
import sqlite3
import subprocess
import urllib.parse
from datetime import datetime, timezone
from pathlib import Path
from xml.sax.saxutils import escape as xml_escape

# --- run configuration (edit as needed) ---

EPSTEIN_WEB_ROOT = Path(__file__).resolve().parent.parent
NETWORK_ROOT = EPSTEIN_WEB_ROOT.parent

# Default when ``EPSTEIN_SQLITE_PATH`` is unset: local API slice (``update_materialized_content`` output).
_DEFAULT_LOCAL_SQLITE_PATH = NETWORK_ROOT / "faces_prod.db"


def resolve_sqlite_path() -> Path:
    """``EPSTEIN_SQLITE_PATH`` if set, else ``network/faces_prod.db`` (same name as ``epstein-api``)."""
    raw = os.environ.get("EPSTEIN_SQLITE_PATH", "").strip()
    if raw:
        return Path(raw).expanduser().resolve()
    return _DEFAULT_LOCAL_SQLITE_PATH.resolve()


def resolve_db_url() -> str | None:
    """``DB_URL`` — if set, ``main`` downloads into ``EPSTEIN_SQLITE_PATH`` first (cf. ``epstein-api/run.sh``)."""
    url = os.environ.get("DB_URL", "").strip()
    return url or None


def fetch_sqlite_db(db_path: Path, url: str) -> None:
    db_path.parent.mkdir(parents=True, exist_ok=True)
    subprocess.run(
        ["curl", "-fsSL", "-o", str(db_path), url],
        check=True,
    )
    if not db_path.is_file() or db_path.stat().st_size == 0:
        raise RuntimeError(
            f"Downloaded database is missing or empty: {db_path}"
        )

# Static site root after ``site/build.sh`` (must exist when not ``DRY_RUN``).
DIST_DIR = EPSTEIN_WEB_ROOT / "dist"

MAX_K = 3
SITE_ORIGIN = "https://epstein.photos"
DRY_RUN = False

OG_SITE_NAME = (os.environ.get("EPSTEIN_OG_SITE_NAME", "Epstein Network") or "Epstein Network").strip()
OG_LOCALE = (os.environ.get("EPSTEIN_OG_LOCALE", "en_US") or "en_US").strip()

# Same origin as ``site/js/shared.js`` ``SPACES_CDN_BASE`` (full image / faces assets).
SPACES_CDN_BASE = os.environ.get(
    "EPSTEIN_SPACES_CDN_BASE", "https://epstein.sfo3.cdn.digitaloceanspaces.com"
).rstrip("/")

PEI_TABLE = "person_eligible_images"

# Written to ``DIST_DIR`` when pages are generated (not in ``DRY_RUN``).
SITEMAP_FILENAME = "sitemap.xml"
MANIFEST_FILENAME = "search-people-pages.json"

# Top-level HTML routes produced by ``site/build.sh`` (must stay in sync with that script).
_CORE_PATHS = ("/", "/search/", "/people/", "/about/")


def canonical_urls_for_core_site(site_origin: str) -> list[str]:
    base = site_origin.rstrip("/")
    return [base + p for p in _CORE_PATHS]


def _require_table(conn: sqlite3.Connection, name: str) -> None:
    row = conn.execute(
        "SELECT 1 FROM sqlite_master WHERE type='table' AND name=? LIMIT 1",
        (name,),
    ).fetchone()
    if row is None:
        raise RuntimeError(
            f"Missing table {name!r}. Use the same DB the API uses (materialized "
            f"{PEI_TABLE} must exist)."
        )


def in_network_person_ids(conn: sqlite3.Connection) -> list[str]:
    rows = conn.execute(
        """
        SELECT person_id FROM people
        WHERE include_in_network = 1
        ORDER BY person_id
        """
    ).fetchall()
    return [str(r["person_id"]) for r in rows]


def count_qualifying_images(conn: sqlite3.Connection, person_ids: tuple[str, ...]) -> int:
    """Match ``photos_for_all_person_ids`` total (distinct image_name in PEI)."""
    n = len(person_ids)
    if n == 0:
        return 0
    branch = f"SELECT image_name FROM {PEI_TABLE} WHERE person_id = ?"
    if n == 1:
        row = conn.execute(
            f"SELECT COUNT(*) AS c FROM {PEI_TABLE} WHERE person_id = ?",
            (person_ids[0],),
        ).fetchone()
        return int(row["c"])
    intersect_body = " INTERSECT ".join([branch] * n)
    count_sql = f"WITH q AS ({intersect_body}) SELECT COUNT(*) AS c FROM q"
    row = conn.execute(count_sql, person_ids).fetchone()
    return int(row["c"])


def names_for_people(conn: sqlite3.Connection, person_ids: list[str]) -> dict[str, str | None]:
    if not person_ids:
        return {}
    ph = ",".join("?" * len(person_ids))
    rows = conn.execute(
        f"SELECT person_id, name FROM people WHERE person_id IN ({ph})",
        person_ids,
    ).fetchall()
    return {str(r["person_id"]): r["name"] for r in rows}


def chip_label(person_id: str, name: str | None) -> str:
    if name is not None and str(name).strip():
        return str(name).strip()
    return person_id


def format_photos_heading(labels: list[str]) -> str:
    if not labels:
        return "Photos"
    if len(labels) == 1:
        return "Photos of " + labels[0]
    if len(labels) == 2:
        return "Photos of " + labels[0] + " and " + labels[1]
    last = labels[-1]
    rest = labels[:-1]
    return "Photos of " + ", ".join(rest) + ", and " + last


def meta_description(heading: str, image_total: int) -> str:
    if image_total <= 0:
        return f"{heading} on Epstein Network."
    return f"{heading} — {image_total} images on Epstein Network."


def slug_for_person_ids(sorted_ids: list[str]) -> str:
    return "-".join(sorted_ids)


def _sanitize_label_for_filename(label: str) -> str:
    """Match ``epstein-api`` ``_sanitize_label_for_filename`` / ``12__export_node_faces`` stems."""
    s = "".join(c if c.isalnum() or c in "._- " else "" for c in label)
    return s.strip().replace(" ", "_").replace("/", "_") or "node"


def best_face_cdn_url(conn: sqlite3.Connection, person_id: str) -> str | None:
    """
    Same asset as ``GET /faces`` ``image`` for this person: ``…/faces/<label>_<best_face_id>.webp``.
    """
    row = conn.execute(
        "SELECT name, best_face_id FROM people WHERE person_id = ?",
        (person_id,),
    ).fetchone()
    if row is None:
        return None
    bf = row["best_face_id"]
    if bf is None or not str(bf).strip():
        return None
    name = row["name"]
    label = (name or "").strip() or person_id
    safe = _sanitize_label_for_filename(label)
    rel = f"faces/{safe}_{str(bf).strip()}.webp"
    return f"{SPACES_CDN_BASE}/{rel}"


def first_pei_image_name(
    conn: sqlite3.Connection, sorted_ids: tuple[str, ...]
) -> str | None:
    """First ``image_name`` for the people search (same ordering as ``/photos``: ``ORDER BY image_name``)."""
    n = len(sorted_ids)
    if n == 0:
        return None
    pei = PEI_TABLE
    branch = f"SELECT image_name FROM {pei} WHERE person_id = ?"
    if n == 1:
        row = conn.execute(
            f"SELECT image_name FROM {pei} WHERE person_id = ? ORDER BY image_name LIMIT 1",
            (sorted_ids[0],),
        ).fetchone()
    else:
        intersect_body = " INTERSECT ".join([branch] * n)
        row = conn.execute(
            f"WITH q AS ({intersect_body}) SELECT image_name FROM q ORDER BY image_name LIMIT 1",
            sorted_ids,
        ).fetchone()
    if row is None:
        return None
    raw = row["image_name"]
    return str(raw).strip() if raw is not None else None


def cdn_images_url_absolute(image_name: str) -> str:
    """
    Full CDN URL for a document image (``site/js/shared.js`` ``cdnImagesUrl``).

    SQLite stores ``image_name`` as ``*.jpg``; Spaces serves ``images/*.webp`` — same rule as
    ``epstein-api`` ``_aggregate_faces_ordered`` (``.jpg`` → ``.webp``).
    """
    base = image_name.split("/")[-1].strip()
    base = base.replace(".jpg", ".webp")
    enc = urllib.parse.quote(base, safe="")
    return f"{SPACES_CDN_BASE}/images/{enc}"


def og_image_url_for_page(
    conn: sqlite3.Connection, sorted_ids: list[str]
) -> str | None:
    """
    Open Graph image: for one person, the /faces best-face crop; if unset, first search image.
    For several people, the first intersecting search result image (document photo).
    """
    if len(sorted_ids) == 1:
        pid = sorted_ids[0]
        u = best_face_cdn_url(conn, pid)
        if u:
            return u
        first = first_pei_image_name(conn, (pid,))
        return cdn_images_url_absolute(first) if first else None
    first = first_pei_image_name(conn, tuple(sorted_ids))
    return cdn_images_url_absolute(first) if first else None


def render_head(
    *,
    title: str,
    meta_description_text: str,
    canonical_url: str,
    og_image_url: str | None = None,
) -> str:
    """Root-relative assets (same as ``site/partials/head-search.html`` + search-inner scripts)."""
    t = html.escape(title, quote=True)
    d = html.escape(meta_description_text, quote=True)
    c = html.escape(canonical_url, quote=True)
    sn = html.escape(OG_SITE_NAME, quote=True)
    loc = html.escape(OG_LOCALE, quote=True)
    og_block = ""
    if og_image_url:
        ogi = html.escape(og_image_url, quote=True)
        og_block = (
            f'  <meta property="og:image" content="{ogi}" />\n'
            f'  <meta property="og:image:secure_url" content="{ogi}" />\n'
            '  <meta name="twitter:card" content="summary_large_image" />\n'
            f'  <meta name="twitter:image" content="{ogi}" />\n'
        )
    else:
        og_block = '  <meta name="twitter:card" content="summary" />\n'
    return f"""<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1, viewport-fit=cover">
  <title>{t}</title>
  <meta name="description" content="{d}" />
  <link rel="canonical" href="{c}" />
  <meta property="og:title" content="{t}" />
  <meta property="og:description" content="{d}" />
  <meta property="og:url" content="{c}" />
  <meta property="og:site_name" content="{sn}" />
  <meta property="og:locale" content="{loc}" />
  <meta property="og:type" content="website" />
  <meta name="twitter:title" content="{t}" />
  <meta name="twitter:description" content="{d}" />
{og_block}  <link rel="icon" type="image/svg+xml" href="/favicon.svg">
  <link rel="stylesheet" href="/styles.css">
  <script src="/js/nav.js" defer></script>
</head>
"""


def render_nav_search_static() -> str:
    """Root-relative nav (matches ``site/partials/nav-search.html``); works at any URL depth."""
    return """<header class="site-header">
  <nav class="site-nav" aria-label="Main">
    <a class="site-nav-brand" href="/">Epstein Network</a>
    <button type="button" class="site-nav-toggle" aria-expanded="false" aria-controls="site-nav-menu" aria-label="Open menu">
      <span class="site-nav-toggle-bar" aria-hidden="true"></span>
      <span class="site-nav-toggle-bar" aria-hidden="true"></span>
      <span class="site-nav-toggle-bar" aria-hidden="true"></span>
    </button>
    <div class="site-nav-backdrop" tabindex="-1" aria-hidden="true"></div>
    <div class="site-nav-links" id="site-nav-menu" role="navigation" aria-label="Site pages">
      <a class="site-nav-link" href="/">Graph</a>
      <a class="site-nav-link" href="/people/">People</a>
      <a class="site-nav-link site-nav-active" href="/search/" aria-current="page">Search</a>
      <a class="site-nav-link" href="/about/">About</a>
    </div>
  </nav>
</header>
"""


def write_root_sitemap(*, dist: Path, loc_urls: list[str], lastmod: str) -> Path:
    """Single sitemap 0.9 ``urlset``: core site URLs plus generated ``/search/people/…/`` pages."""
    path = dist / SITEMAP_FILENAME
    lines = [
        '<?xml version="1.0" encoding="UTF-8"?>',
        '<urlset xmlns="http://www.sitemaps.org/schemas/sitemap/0.9">',
    ]
    for loc in sorted(loc_urls):
        lines.append("  <url>")
        lines.append(f"    <loc>{xml_escape(loc)}</loc>")
        lines.append(f"    <lastmod>{xml_escape(lastmod)}</lastmod>")
        lines.append("  </url>")
    lines.append("</urlset>")
    path.write_text("\n".join(lines) + "\n", encoding="utf8")
    return path


def write_search_people_manifest(
    *,
    dist: Path,
    site_origin: str,
    pages: list[dict[str, object]],
) -> Path:
    """Machine-readable list of generated pages (paths, ids, image counts)."""
    path = dist / MANIFEST_FILENAME
    payload = {
        "site_origin": site_origin,
        "page_count": len(pages),
        "pages": pages,
    }
    path.write_text(json.dumps(payload, indent=2, ensure_ascii=True) + "\n", encoding="utf8")
    return path


def build_page_html(
    *,
    search_inner_body: str,
    person_ids: list[str],
    image_total: int,
    page_title: str,
    meta_desc: str,
    canonical_url: str,
    og_image_url: str | None,
) -> str:
    bootstrap = {
        "person_ids": person_ids,
        "image_total": image_total,
        "canonical_path": f"/search/people/{slug_for_person_ids(person_ids)}/",
    }
    bootstrap_json = json.dumps(bootstrap, separators=(",", ":"), ensure_ascii=True)
    head = render_head(
        title=page_title,
        meta_description_text=meta_desc,
        canonical_url=canonical_url,
        og_image_url=og_image_url,
    )
    nav = render_nav_search_static()
    bootstrap_script = (
        f'<script>window.__EPSTEIN_STATIC_SEARCH__={bootstrap_json};</script>\n'
    )
    inner = search_inner_body
    footer = (
        '<footer class="site-footer">\n'
        '  <p class="site-footer-note">© 2026 '
        '<a href="https://decoherence.media/">Decoherence Media</a> · All rights reserved</p>\n'
        "</footer>\n"
    )
    close = "</body>\n</html>\n"
    return (
        head
        + '<body class="page-people">\n'
        + nav
        + bootstrap_script
        + inner
        + footer
        + close
    )


def main() -> None:
    db_path = resolve_sqlite_path()
    db_url = resolve_db_url()
    if db_url:
        fetch_sqlite_db(db_path, db_url)
    elif not db_path.is_file():
        raise FileNotFoundError(
            f"Database not found: {db_path}. "
            "Set DB_URL to fetch it (and EPSTEIN_SQLITE_PATH for the download target), "
            "or place faces_prod.db at the default path, or set EPSTEIN_SQLITE_PATH."
        )

    dist = DIST_DIR

    search_inner_path = EPSTEIN_WEB_ROOT / "site" / "pages" / "search-inner.html"
    if not search_inner_path.is_file():
        raise FileNotFoundError(f"Missing search-inner template: {search_inner_path}")

    if MAX_K < 1:
        raise ValueError("MAX_K must be at least 1")

    site_origin = SITE_ORIGIN.rstrip("/")

    search_inner_body = search_inner_path.read_text(encoding="utf8")
    out_root = dist / "search" / "people"

    if not DRY_RUN and not dist.is_dir():
        raise FileNotFoundError(
            f"DIST_DIR is not a directory: {dist} — run site/build.sh first."
        )

    with sqlite3.connect(db_path) as conn:
        conn.row_factory = sqlite3.Row
        _require_table(conn, PEI_TABLE)
        ids = in_network_person_ids(conn)
        if not ids:
            raise RuntimeError("No in-network people (include_in_network = 1) in people table.")

        generated = 0
        skipped_empty = 0
        canonical_urls: list[str] = []
        manifest_pages: list[dict[str, object]] = []
        lastmod = datetime.now(timezone.utc).date().isoformat()

        for k in range(1, MAX_K + 1):
            for combo in itertools.combinations(ids, k):
                total = count_qualifying_images(conn, combo)
                if total == 0:
                    skipped_empty += 1
                    continue
                sorted_ids = list(combo)
                slug = slug_for_person_ids(sorted_ids)
                names = names_for_people(conn, sorted_ids)
                labels = [chip_label(pid, names.get(pid)) for pid in sorted_ids]
                heading = format_photos_heading(labels)
                page_title = heading + " · Epstein Network"
                meta = meta_description(heading, total)
                canonical_url = f"{site_origin}/search/people/{slug}/"
                canonical_urls.append(canonical_url)
                og_img = og_image_url_for_page(conn, sorted_ids)
                manifest_pages.append(
                    {
                        "path": f"/search/people/{slug}/",
                        "canonical_url": canonical_url,
                        "person_ids": sorted_ids,
                        "image_total": total,
                        "og_image_url": og_img,
                    }
                )
                html_out = build_page_html(
                    search_inner_body=search_inner_body,
                    person_ids=sorted_ids,
                    image_total=total,
                    page_title=page_title,
                    meta_desc=meta,
                    canonical_url=canonical_url,
                    og_image_url=og_img,
                )
                out_dir = out_root / slug
                out_file = out_dir / "index.html"
                if not DRY_RUN:
                    out_dir.mkdir(parents=True, exist_ok=True)
                    out_file.write_text(html_out, encoding="utf8")
                generated += 1

    print(
        f"In-network people: {len(ids)}; max k={MAX_K}; "
        f"generated={generated} skipped_empty={skipped_empty} dry_run={DRY_RUN}"
    )
    if not DRY_RUN:
        sitemap_urls = canonical_urls_for_core_site(site_origin) + canonical_urls
        sm = write_root_sitemap(dist=dist, loc_urls=sitemap_urls, lastmod=lastmod)
        mf = write_search_people_manifest(
            dist=dist, site_origin=site_origin, pages=manifest_pages
        )
        print(f"Wrote pages under {out_root}")
        print(f"Wrote sitemap {sm} ({len(sitemap_urls)} URLs)")
        print(f"Wrote manifest {mf}")


if __name__ == "__main__":
    main()

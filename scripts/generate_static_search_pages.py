#!/usr/bin/env python3
"""
Generate static HTML for people-search URLs::

    /search/people/<sorted-person_id-1>-<sorted-person_id-2>-.../

One ``index.html`` per non-empty combination (same semantics as API ``/photos``:
``person_eligible_images`` intersection).

Loads ``person_eligible_images`` once into memory (``person_id`` → ``set`` of ``image_name``) and
uses set intersections / counts in Python — much faster than per-combo SQL when the DB dominates.

Run (after ``site/build.sh`` when not ``DRY_RUN``)::

    ~/.venv/epstein/bin/python epstein-web/scripts/generate_static_search_pages.py

Database: same env as ``epstein-api/run.sh`` — ``EPSTEIN_SQLITE_PATH`` and optional ``DB_URL``.

Open Graph: ``EPSTEIN_SPACES_CDN_BASE``, ``EPSTEIN_OG_SITE_NAME``, ``EPSTEIN_OG_LOCALE``.

Also writes ``dist/sitemap.xml``, ``dist/search-people-pages.json``, and
``epstein-web/sitemap-lastmod-state.json`` (compact keys: site path for core pages, slug for
``/search/people/…``; people fingerprints are image counts only; ``lastmod`` bumps when a fingerprint
changes). Submit ``/sitemap.xml`` in Search Console.

When not ``DRY_RUN``, **DigitalOcean Spaces** is required for sitemap lastmod state (same
``EPSTEIN_SPACES_*`` names as ``epstein-pipeline/scripts/pipeline/16__upload_to_spaces.py`` /
``epstein_photos.spaces.get_spaces_client``): ``EPSTEIN_SPACES_REGION``, ``EPSTEIN_SPACES_ENDPOINT``,
``EPSTEIN_SPACES_BUCKET``, ``EPSTEIN_SPACES_KEY``, ``EPSTEIN_SPACES_SECRET``. Each must be set and
non-empty (``os.environ[...]``). The state object key in the bucket is
``SITEMAP_LASTMOD_STATE_SPACES_OBJECT_KEY`` (``build/`` + local basename). Requires ``boto3``. The
object must already exist in the bucket; if it is missing, the run fails (seed it once, e.g.
``{"version": 2, "entries": {}}``, then re-run). After a successful run the object is overwritten.
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
from functools import cache
from pathlib import Path
from typing import NamedTuple
from xml.sax.saxutils import escape as xml_escape

import boto3
from botocore.exceptions import ClientError
# --- run configuration (edit as needed) ---

EPSTEIN_WEB_ROOT = Path(__file__).resolve().parent.parent
NETWORK_ROOT = EPSTEIN_WEB_ROOT.parent

_DEFAULT_LOCAL_SQLITE_PATH = NETWORK_ROOT / "faces_prod.db"


def resolve_sqlite_path() -> Path:
    """``EPSTEIN_SQLITE_PATH`` if set, else ``network/faces_prod.db`` (same name as ``epstein-api``)."""
    raw = os.environ.get("EPSTEIN_SQLITE_PATH", "").strip()
    if raw:
        return Path(raw).expanduser().resolve()
    return _DEFAULT_LOCAL_SQLITE_PATH.resolve()


def resolve_db_url() -> str | None:
    """``DB_URL`` — if set, ``main`` downloads into ``EPSTEIN_SQLITE_PATH`` first."""
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


DIST_DIR = EPSTEIN_WEB_ROOT / "dist"
SITE_PARTIALS_DIR = EPSTEIN_WEB_ROOT / "site" / "partials"
SITE_METADATA_PATH = EPSTEIN_WEB_ROOT / "site" / "site_metadata.json"

MAX_K = 3
DRY_RUN = False

# Shared OG/site defaults live in site/site_metadata.json (also consumed by
# scripts/render_head_partials.py to generate the per-page <head> partials).
# EPSTEIN_* env vars still override at deploy time.
_SITE_METADATA_SHARED = json.loads(SITE_METADATA_PATH.read_text(encoding="utf8"))["shared"]

SITE_ORIGIN = _SITE_METADATA_SHARED["site_origin"]
OG_SITE_NAME = (
    os.environ.get("EPSTEIN_OG_SITE_NAME", _SITE_METADATA_SHARED["og_site_name"])
    or _SITE_METADATA_SHARED["og_site_name"]
).strip()
OG_LOCALE = (
    os.environ.get("EPSTEIN_OG_LOCALE", _SITE_METADATA_SHARED["og_locale"])
    or _SITE_METADATA_SHARED["og_locale"]
).strip()
SPACES_CDN_BASE = os.environ.get(
    "EPSTEIN_SPACES_CDN_BASE", _SITE_METADATA_SHARED["spaces_cdn_base"]
).rstrip("/")

# Fallback social-card image used when a search result has no per-result image to advertise
# (matches the bare /search/ og:image set in site/partials/head-search.html). Per-result
# overrides from `og_image_url(...)` still take priority — see `build_page_html`.
DEFAULT_SEARCH_OG_IMAGE_URL = f"{SPACES_CDN_BASE}/og/search.webp"

PEI_TABLE = "person_eligible_images"

SITEMAP_FILENAME = "sitemap.xml"
MANIFEST_FILENAME = "search-people-pages.json"

SITEMAP_LASTMOD_STATE_FILENAME = "sitemap-lastmod-state.json"
SITEMAP_LASTMOD_STATE_PATH = EPSTEIN_WEB_ROOT / SITEMAP_LASTMOD_STATE_FILENAME
SITEMAP_LASTMOD_STATE_SPACES_OBJECT_KEY = f"build/{SITEMAP_LASTMOD_STATE_FILENAME}"
SITEMAP_STATE_VERSION = 2

_CORE_PATHS = ("/", "/search/", "/people/", "/about/")


def canonical_urls_for_core_site(site_origin: str) -> list[str]:
    base = site_origin.rstrip("/")
    return [base + p for p in _CORE_PATHS]


def fingerprint_for_people_page(image_total: int) -> str:
    """Stable when the static people-search page’s qualifying image count is unchanged."""
    return str(int(image_total))


def _dist_file_for_core_path(dist: Path, pathname: str) -> Path:
    if pathname == "/":
        return dist / "index.html"
    if pathname == "/search/":
        return dist / "search" / "index.html"
    if pathname == "/people/":
        return dist / "people" / "index.html"
    if pathname == "/about/":
        return dist / "about" / "index.html"
    raise ValueError(f"Unknown core sitemap path: {pathname!r}")


def fingerprint_for_core_path(dist: Path, pathname: str) -> str:
    f = _dist_file_for_core_path(dist, pathname)
    if not f.is_file():
        raise FileNotFoundError(
            f"Missing built core page for sitemap fingerprint (run site/build.sh first): {f}"
        )
    return "mtime_ns:" + str(f.stat().st_mtime_ns)


def build_sitemap_state_fingerprints(
    *,
    dist: Path,
    people_fingerprints_by_slug: dict[str, str],
) -> dict[str, str]:
    out = dict(people_fingerprints_by_slug)
    for pathname in _CORE_PATHS:
        out[pathname] = fingerprint_for_core_path(dist, pathname)
    return out


def _normalize_url_path(url: str) -> str:
    path = urllib.parse.urlparse(url).path or "/"
    if path != "/" and not path.endswith("/"):
        path = path + "/"
    return path


def _migrate_v1_urls_to_entries(urls: dict[str, object]) -> dict[str, dict[str, str]]:
    """Convert version-1 ``urls`` (full canonical ``<loc>`` keys) to v2 state keys."""
    out: dict[str, dict[str, str]] = {}
    people_prefix = "/search/people/"
    for url, v in urls.items():
        if not isinstance(v, dict):
            raise ValueError(f"Invalid sitemap state entry for {url!r} in v1 urls")
        fp = v.get("fingerprint")
        lm = v.get("lastmod")
        if fp is None or lm is None:
            raise ValueError(f"Invalid sitemap state entry for {url!r} (need fingerprint, lastmod)")
        path = _normalize_url_path(str(url))
        if path.startswith(people_prefix) and len(path) > len(people_prefix):
            slug = path[len(people_prefix) : -1]
            fp_s = str(fp)
            if "|" in fp_s:
                fp_s = fp_s.rsplit("|", 1)[-1]
            out[slug] = {"fingerprint": fp_s, "lastmod": str(lm)}
        elif path in _CORE_PATHS:
            out[path] = {"fingerprint": str(fp), "lastmod": str(lm)}
        else:
            raise ValueError(
                f"Cannot migrate v1 sitemap state key (unknown path {path!r}): {url!r}"
            )
    return out


def load_sitemap_lastmod_state(path: Path) -> dict[str, dict[str, str]]:
    if not path.is_file():
        return {}
    raw = json.loads(path.read_text(encoding="utf8"))
    if not isinstance(raw, dict):
        raise ValueError(f"Invalid sitemap state (not an object): {path}")
    ver = raw.get("version")
    entries = raw.get("entries")
    urls_v1 = raw.get("urls")

    if ver == 1 or (ver is None and isinstance(urls_v1, dict) and entries is None):
        if not isinstance(urls_v1, dict):
            raise ValueError(f"Invalid sitemap state: expected v1 'urls' object in {path}")
        return _migrate_v1_urls_to_entries(urls_v1)

    if ver is not None and ver != SITEMAP_STATE_VERSION:
        raise ValueError(
            f"Unsupported {path.name} version {ver!r} (expected {SITEMAP_STATE_VERSION}). "
            "Remove the file or add migration support."
        )
    if ver == SITEMAP_STATE_VERSION and not isinstance(entries, dict):
        raise ValueError(
            f"Invalid sitemap state: version {SITEMAP_STATE_VERSION} requires object 'entries' in {path}"
        )
    if entries is None:
        return {}
    if not isinstance(entries, dict):
        raise ValueError(f"Invalid sitemap state: 'entries' must be an object in {path}")
    out: dict[str, dict[str, str]] = {}
    for k, v in entries.items():
        if not isinstance(v, dict):
            raise ValueError(f"Invalid sitemap state entry for {k!r} in {path}")
        fp = v.get("fingerprint")
        lm = v.get("lastmod")
        if fp is None or lm is None:
            raise ValueError(f"Invalid sitemap state entry for {k!r} (need fingerprint, lastmod)")
        out[str(k)] = {"fingerprint": str(fp), "lastmod": str(lm)}
    return out


def save_sitemap_lastmod_state(path: Path, entries: dict[str, dict[str, str]]) -> None:
    payload = {
        "version": SITEMAP_STATE_VERSION,
        "entries": dict(sorted(entries.items(), key=lambda kv: kv[0])),
    }
    path.write_text(json.dumps(payload, ensure_ascii=True) + "\n", encoding="utf8")


class SpacesSitemapStateTarget(NamedTuple):
    bucket: str
    object_key: str


def _spaces_env(name: str) -> str:
    """``os.environ[name]`` (raises ``KeyError`` if missing); non-empty after strip."""
    v = os.environ[name].strip()
    if not v:
        raise ValueError(f"{name} must be non-empty")
    return v


def resolve_spaces_sitemap_state_target() -> SpacesSitemapStateTarget:
    """Bucket and object key for S3; validates all Spaces env vars (same names as ``get_spaces_client``)."""
    _spaces_env("EPSTEIN_SPACES_REGION")
    _spaces_env("EPSTEIN_SPACES_ENDPOINT")
    bucket = _spaces_env("EPSTEIN_SPACES_BUCKET")
    _spaces_env("EPSTEIN_SPACES_KEY")
    _spaces_env("EPSTEIN_SPACES_SECRET")
    return SpacesSitemapStateTarget(
        bucket=bucket,
        object_key=SITEMAP_LASTMOD_STATE_SPACES_OBJECT_KEY,
    )


def _spaces_s3_client():
    """Boto3 S3 client; env contract matches ``epstein_photos.spaces.get_spaces_client``."""
    region = _spaces_env("EPSTEIN_SPACES_REGION")
    endpoint = _spaces_env("EPSTEIN_SPACES_ENDPOINT")
    access_key = _spaces_env("EPSTEIN_SPACES_KEY")
    secret = _spaces_env("EPSTEIN_SPACES_SECRET")
    return boto3.client(
        "s3",
        region_name=region,
        endpoint_url=endpoint,
        aws_access_key_id=access_key,
        aws_secret_access_key=secret,
    )


def fetch_sitemap_lastmod_state_from_spaces(
    target: SpacesSitemapStateTarget, dest: Path
) -> None:
    """Download state JSON into ``dest``. The object must exist; missing object is a hard error."""

    client = _spaces_s3_client()
    try:
        resp = client.get_object(Bucket=target.bucket, Key=target.object_key)
    except ClientError as e:
        code = e.response.get("Error", {}).get("Code", "")
        if code in ("NoSuchKey", "404"):
            raise RuntimeError(
                f"Sitemap lastmod state is missing in Spaces "
                f"(s3://{target.bucket}/{target.object_key}). "
                'Seed that object once (e.g. {"version": 2, "entries": {}}) then re-run.'
            ) from e
        raise RuntimeError(
            f"Failed to download sitemap lastmod state from Spaces "
            f"s3://{target.bucket}/{target.object_key}: {e}"
        ) from e
    dest.write_bytes(resp["Body"].read())


def upload_sitemap_lastmod_state_to_spaces(target: SpacesSitemapStateTarget, src: Path) -> None:
    if not src.is_file():
        raise FileNotFoundError(f"Cannot upload missing sitemap state file: {src}")
    client = _spaces_s3_client()
    client.put_object(
        Bucket=target.bucket,
        Key=target.object_key,
        Body=src.read_bytes(),
        ContentType="application/json; charset=utf-8",
    )


def merge_lastmod_with_fingerprints(
    *,
    previous: dict[str, dict[str, str]],
    fingerprints: dict[str, str],
    today: str,
) -> tuple[dict[str, str], dict[str, dict[str, str]]]:
    new_urls: dict[str, dict[str, str]] = {}
    lastmod_by_url: dict[str, str] = {}
    for url in sorted(fingerprints.keys()):
        fp = fingerprints[url]
        prev = previous.get(url)
        if prev is None or prev.get("fingerprint") != fp:
            lm = today
        else:
            lm = prev.get("lastmod") or today
        new_urls[url] = {"fingerprint": fp, "lastmod": lm}
        lastmod_by_url[url] = lm
    return lastmod_by_url, new_urls


def lastmod_for_canonical_urls(
    *, site_origin: str, lastmod_by_state_key: dict[str, str]
) -> dict[str, str]:
    """Map full sitemap ``<loc>`` strings from compact state keys (paths + people slugs)."""
    base = site_origin.rstrip("/")
    out: dict[str, str] = {}
    for k, lm in lastmod_by_state_key.items():
        if k in _CORE_PATHS:
            out[base + k] = lm
        else:
            out[f"{base}/search/people/{k}/"] = lm
    return out


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
    s = "".join(c if c.isalnum() or c in "._- " else "" for c in label)
    return s.strip().replace(" ", "_").replace("/", "_") or "node"


def cdn_images_url_absolute(image_name: str) -> str:
    base = image_name.split("/")[-1].strip()
    base = base.replace(".jpg", ".webp")
    enc = urllib.parse.quote(base, safe="")
    return f"{SPACES_CDN_BASE}/images/{enc}"


def render_head(
    *,
    title: str,
    meta_description_text: str,
    canonical_url: str,
    og_image_url: str | None = None,
) -> str:
    t = html.escape(title, quote=True)
    d = html.escape(meta_description_text, quote=True)
    c = html.escape(canonical_url, quote=True)
    sn = html.escape(OG_SITE_NAME, quote=True)
    loc = html.escape(OG_LOCALE, quote=True)
    # Per-result override (face crop / first photo) wins; otherwise share the same default
    # image as the bare /search/ page so social cards always have a 1.91:1 image to render.
    ogi = html.escape(og_image_url or DEFAULT_SEARCH_OG_IMAGE_URL, quote=True)
    og_block = (
        f'  <meta property="og:image" content="{ogi}" />\n'
        f'  <meta property="og:image:secure_url" content="{ogi}" />\n'
        '  <meta name="twitter:card" content="summary_large_image" />\n'
        f'  <meta name="twitter:image" content="{ogi}" />\n'
    )
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
  <script src="/js/disclaimer.js" defer></script>
</head>
"""


@cache
def read_site_partial(name: str) -> str:
    """Read ``site/partials/<name>.html`` so the static generator shares a single source
    of truth with site/build.sh (instead of duplicating markup inline). Cached because
    partials are static for the lifetime of the build."""
    path = SITE_PARTIALS_DIR / f"{name}.html"
    if not path.is_file():
        raise FileNotFoundError(f"Missing site partial: {path}")
    return path.read_text(encoding="utf8")


def write_root_sitemap(*, dist: Path, loc_urls: list[str], lastmod_by_url: dict[str, str]) -> Path:
    path = dist / SITEMAP_FILENAME
    lines = [
        '<?xml version="1.0" encoding="UTF-8"?>',
        '<urlset xmlns="http://www.sitemaps.org/schemas/sitemap/0.9">',
    ]
    for loc in sorted(loc_urls):
        lm = lastmod_by_url.get(loc)
        if lm is None:
            raise RuntimeError(f"Missing lastmod for sitemap URL: {loc!r}")
        lines.append("  <url>")
        lines.append(f"    <loc>{xml_escape(loc)}</loc>")
        lines.append(f"    <lastmod>{xml_escape(lm)}</lastmod>")
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
    nav = read_site_partial("nav")
    disclaimer = read_site_partial("disclaimer")
    footer = read_site_partial("footer")
    bootstrap_script = (
        f'<script>window.__EPSTEIN_STATIC_SEARCH__={bootstrap_json};</script>\n'
    )
    close = "</body>\n</html>\n"
    return (
        head
        + '<body class="page-search">\n'
        + nav
        + bootstrap_script
        + search_inner_body
        + footer
        + disclaimer
        + close
    )


def load_pei_map(conn: sqlite3.Connection) -> dict[str, set[str]]:
    pei_map: dict[str, set[str]] = {}
    cur = conn.execute(f"SELECT person_id, image_name FROM {PEI_TABLE}")
    for row in cur:
        pid = str(row["person_id"])
        raw = row["image_name"]
        if raw is None:
            continue
        img = str(raw).strip()
        if not img:
            continue
        pei_map.setdefault(pid, set()).add(img)
    return pei_map


def load_in_network_people_rows(
    conn: sqlite3.Connection,
) -> tuple[dict[str, str | None], dict[str, tuple[str | None, str | None]]]:
    names: dict[str, str | None] = {}
    faces: dict[str, tuple[str | None, str | None]] = {}
    rows = conn.execute(
        """
        SELECT person_id, name, best_face_id
        FROM people
        WHERE include_in_network = 1
        ORDER BY person_id
        """
    ).fetchall()
    for r in rows:
        pid = str(r["person_id"])
        names[pid] = r["name"]
        faces[pid] = (r["name"], r["best_face_id"])
    return names, faces


def count_qualifying_images(
    pei_map: dict[str, set[str]], person_ids: tuple[str, ...]
) -> int:
    if not person_ids:
        return 0
    sets = [pei_map.get(pid, set()) for pid in person_ids]
    return len(set.intersection(*sets))


def first_pei_image_name(
    pei_map: dict[str, set[str]], sorted_ids: tuple[str, ...]
) -> str | None:
    if not sorted_ids:
        return None
    sets = [pei_map.get(pid, set()) for pid in sorted_ids]
    inter = set.intersection(*sets)
    return min(inter) if inter else None


def best_face_cdn_url(
    person_id: str,
    name_bf: tuple[str | None, str | None] | None,
) -> str | None:
    if name_bf is None:
        return None
    name, bf = name_bf
    if bf is None or not str(bf).strip():
        return None
    label = (name or "").strip() or person_id
    safe = _sanitize_label_for_filename(label)
    rel = f"faces/{safe}_{str(bf).strip()}.webp"
    return f"{SPACES_CDN_BASE}/{rel}"


def og_image_url(
    pei_map: dict[str, set[str]],
    faces_by_id: dict[str, tuple[str | None, str | None]],
    sorted_ids: list[str],
) -> str | None:
    if len(sorted_ids) == 1:
        pid = sorted_ids[0]
        u = best_face_cdn_url(pid, faces_by_id.get(pid))
        if u:
            return u
        first = first_pei_image_name(pei_map, (pid,))
        return cdn_images_url_absolute(first) if first else None
    first = first_pei_image_name(pei_map, tuple(sorted_ids))
    return cdn_images_url_absolute(first) if first else None


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

    spaces_state_target: SpacesSitemapStateTarget | None = None
    if not DRY_RUN:
        spaces_state_target = resolve_spaces_sitemap_state_target()
        fetch_sitemap_lastmod_state_from_spaces(
            spaces_state_target, SITEMAP_LASTMOD_STATE_PATH
        )

    with sqlite3.connect(db_path) as conn:
        conn.row_factory = sqlite3.Row
        _require_table(conn, PEI_TABLE)
        ids = in_network_person_ids(conn)
        if not ids:
            raise RuntimeError("No in-network people (include_in_network = 1) in people table.")

        pei_map = load_pei_map(conn)
        names_map, faces_by_id = load_in_network_people_rows(conn)

    generated = 0
    skipped_empty = 0
    canonical_urls: list[str] = []
    manifest_pages: list[dict[str, object]] = []
    people_fingerprints: dict[str, str] = {}

    for k in range(1, MAX_K + 1):
        for combo in itertools.combinations(ids, k):
            total = count_qualifying_images(pei_map, combo)
            if total == 0:
                skipped_empty += 1
                continue
            sorted_ids = list(combo)
            slug = slug_for_person_ids(sorted_ids)
            labels = [chip_label(pid, names_map.get(pid)) for pid in sorted_ids]
            heading = format_photos_heading(labels)
            page_title = heading + " · Epstein Network"
            meta = meta_description(heading, total)
            canonical_url = f"{site_origin}/search/people/{slug}/"
            canonical_urls.append(canonical_url)
            people_fingerprints[slug] = fingerprint_for_people_page(total)
            og_img = og_image_url(pei_map, faces_by_id, sorted_ids)
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
        assert spaces_state_target is not None
        sitemap_urls = canonical_urls_for_core_site(site_origin) + canonical_urls
        today = datetime.now(timezone.utc).date().isoformat()
        fingerprints = build_sitemap_state_fingerprints(
            dist=dist,
            people_fingerprints_by_slug=people_fingerprints,
        )
        prev_state = load_sitemap_lastmod_state(SITEMAP_LASTMOD_STATE_PATH)
        lastmod_by_key, new_state = merge_lastmod_with_fingerprints(
            previous=prev_state,
            fingerprints=fingerprints,
            today=today,
        )
        lastmod_by_url = lastmod_for_canonical_urls(
            site_origin=site_origin, lastmod_by_state_key=lastmod_by_key
        )
        save_sitemap_lastmod_state(SITEMAP_LASTMOD_STATE_PATH, new_state)
        upload_sitemap_lastmod_state_to_spaces(
            spaces_state_target, SITEMAP_LASTMOD_STATE_PATH
        )
        sm = write_root_sitemap(
            dist=dist, loc_urls=sitemap_urls, lastmod_by_url=lastmod_by_url
        )
        mf = write_search_people_manifest(
            dist=dist, site_origin=site_origin, pages=manifest_pages
        )
        print(f"Wrote pages under {out_root}")
        print(f"Wrote sitemap {sm} ({len(sitemap_urls)} URLs)")
        print(f"Wrote manifest {mf}")
        print(f"Updated {SITEMAP_LASTMOD_STATE_PATH.name} ({len(new_state)} entries)")
        print(
            f"Synced sitemap lastmod state to Spaces s3://{spaces_state_target.bucket}/"
            f"{spaces_state_target.object_key}"
        )


if __name__ == "__main__":
    main()

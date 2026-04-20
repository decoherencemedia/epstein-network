#!/usr/bin/env python3
"""
Generate static people-search HTML — same output as ``generate_static_search_pages.py``, but loads
``person_eligible_images`` once into memory (``person_id`` → ``set`` of ``image_name``) and uses set
intersections / counts in Python. Much faster when the DB round-trips dominate (typical).

Same env vars and CLI usage::

    ~/.venv/epstein/bin/python epstein-web/scripts/generate_static_search_pages_mem.py

Run ``site/build.sh`` first when not ``DRY_RUN``. Requires the same SQLite schema and tables.
"""

from __future__ import annotations

import itertools
import sqlite3
from datetime import datetime, timezone

from generate_static_search_pages import (
    DIST_DIR,
    DRY_RUN,
    EPSTEIN_WEB_ROOT,
    MAX_K,
    PEI_TABLE,
    SITE_ORIGIN,
    SPACES_CDN_BASE,
    _require_table,
    _sanitize_label_for_filename,
    build_page_html,
    canonical_urls_for_core_site,
    chip_label,
    cdn_images_url_absolute,
    fetch_sqlite_db,
    format_photos_heading,
    in_network_person_ids,
    meta_description,
    resolve_db_url,
    resolve_sqlite_path,
    slug_for_person_ids,
    write_root_sitemap,
    write_search_people_manifest,
)


def load_pei_map(conn: sqlite3.Connection) -> dict[str, set[str]]:
    """``person_id`` → distinct ``image_name`` set (same rows as ``person_eligible_images``)."""
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
    """
    All in-network people: ``names[person_id]`` and ``faces[person_id]`` → ``(name, best_face_id)``
    for OG /faces URL logic.
    """
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


def count_qualifying_images_mem(
    pei_map: dict[str, set[str]], person_ids: tuple[str, ...]
) -> int:
    """Match SQL ``count_qualifying_images`` / API intersection semantics."""
    if not person_ids:
        return 0
    sets = [pei_map.get(pid, set()) for pid in person_ids]
    return len(set.intersection(*sets))


def first_pei_image_name_mem(
    pei_map: dict[str, set[str]], sorted_ids: tuple[str, ...]
) -> str | None:
    """Lexicographically first ``image_name`` in the qualifying set (``ORDER BY image_name``)."""
    if not sorted_ids:
        return None
    sets = [pei_map.get(pid, set()) for pid in sorted_ids]
    inter = set.intersection(*sets)
    return min(inter) if inter else None


def best_face_cdn_url_mem(
    person_id: str,
    name_bf: tuple[str | None, str | None] | None,
) -> str | None:
    """Same URL as ``GET /faces`` for this person (see ``generate_static_search_pages.best_face_cdn_url``)."""
    if name_bf is None:
        return None
    name, bf = name_bf
    if bf is None or not str(bf).strip():
        return None
    label = (name or "").strip() or person_id
    safe = _sanitize_label_for_filename(label)
    rel = f"faces/{safe}_{str(bf).strip()}.webp"
    return f"{SPACES_CDN_BASE}/{rel}"


def og_image_url_mem(
    pei_map: dict[str, set[str]],
    faces_by_id: dict[str, tuple[str | None, str | None]],
    sorted_ids: list[str],
) -> str | None:
    if len(sorted_ids) == 1:
        pid = sorted_ids[0]
        u = best_face_cdn_url_mem(pid, faces_by_id.get(pid))
        if u:
            return u
        first = first_pei_image_name_mem(pei_map, (pid,))
        return cdn_images_url_absolute(first) if first else None
    first = first_pei_image_name_mem(pei_map, tuple(sorted_ids))
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
    lastmod = datetime.now(timezone.utc).date().isoformat()

    for k in range(1, MAX_K + 1):
        for combo in itertools.combinations(ids, k):
            total = count_qualifying_images_mem(pei_map, combo)
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
            og_img = og_image_url_mem(pei_map, faces_by_id, sorted_ids)
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
        f"[mem] In-network people: {len(ids)}; max k={MAX_K}; "
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

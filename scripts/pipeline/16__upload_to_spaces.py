#!/usr/bin/env python3
"""
Upload graph assets to DigitalOcean Spaces.

- Uploads atlas files used by the graph UI:
    atlas/atlas.webp
    atlas/atlas_manifest.json

- Uploads manually selected node face crops (flattened) from:
    images/node_faces_selected_optimized/**/*.webp
  (produced by ``13__optimize_node_faces.py`` from ``node_faces_selected/``) to:
    faces/<basename>.webp
  (files in subfolders such as ``old/`` share the same flat ``faces/`` key by basename.)

When no ``*.webp`` files exist under ``node_faces_selected_optimized/``, the ``faces/`` prefix is not
synced (existing remote ``faces/*`` objects are left unchanged).

Note: document originals/thumbnails for the photo viewer are hosted separately.

DigitalOcean Spaces is S3-compatible. Configure via environment variables:

    EPSTEIN_SPACES_REGION   (e.g. "sfo3")
    EPSTEIN_SPACES_ENDPOINT (e.g. "https://sfo3.digitaloceanspaces.com")
    EPSTEIN_SPACES_BUCKET   (bucket name)
    EPSTEIN_SPACES_KEY      (access key)
    EPSTEIN_SPACES_SECRET   (secret key)
"""


from pathlib import Path

from epstein_photos.config import NETWORK_ROOT
from epstein_photos.spaces import delete_keys, get_spaces_client, list_remote_objects, upload_file

REPO_DIR = NETWORK_ROOT

VIZ_DATA_DIR = REPO_DIR / "viz_data"

ATLAS_WEBP_PATH = REPO_DIR / "images" / "atlas.webp"
ATLAS_MANIFEST_PATH = VIZ_DATA_DIR / "atlas_manifest.json"
NODE_FACES_OPTIMIZED_DIR = REPO_DIR / "images" / "node_faces_selected_optimized"
SYNC_PREFIXES_ATLAS = ("atlas/",)
FORCE_UPLOAD_KEYS = {"atlas/atlas.webp", "atlas/atlas_manifest.json"}
ATLAS_IMAGE_CACHE_CONTROL = "public, max-age=3600, stale-while-revalidate=86400"
ATLAS_MANIFEST_CACHE_CONTROL = "no-cache, max-age=0, must-revalidate"
FACES_CACHE_CONTROL = "public, max-age=86400, stale-while-revalidate=604800"


def collect_flat_face_webp_uploads() -> dict[str, Path]:
    """
    Map ``faces/<basename>`` -> local path for every ``*.webp`` under node_faces_selected_optimized.
    Errors if two files share the same basename in different folders.
    """
    if not NODE_FACES_OPTIMIZED_DIR.is_dir():
        return {}
    out: dict[str, Path] = {}
    for p in sorted(NODE_FACES_OPTIMIZED_DIR.rglob("*.webp")):
        key = f"faces/{p.name}"
        if key in out and out[key] != p:
            raise RuntimeError(
                f"Duplicate basename after flattening: {p.name!r}\n"
                f"  First: {out[key]}\n  Second: {p}"
            )
        out[key] = p
    return out


def main():
    s3, bucket = get_spaces_client()

    desired: dict[str, Path] = {}
    # Atlas: used by index.html to render per-node faces without many HTTP requests.
    desired["atlas/atlas.webp"] = ATLAS_WEBP_PATH
    desired["atlas/atlas_manifest.json"] = ATLAS_MANIFEST_PATH

    face_uploads = collect_flat_face_webp_uploads()
    desired.update(face_uploads)

    # Validate all desired local files before mutating remote state.
    for key, local_path in desired.items():
        if not local_path.is_file():
            raise FileNotFoundError(f"Required file missing for upload key={key}: {local_path}")

    list_prefixes: tuple[str, ...] = SYNC_PREFIXES_ATLAS
    if face_uploads:
        list_prefixes = ("atlas/", "faces/")

    remote = list_remote_objects(s3, bucket, list_prefixes)

    # Upload only missing/changed files.
    uploaded = 0
    skipped = 0
    for key, local_path in desired.items():
        if key in FORCE_UPLOAD_KEYS:
            # Atlas files are small and frequently replaced. Force upload to
            # avoid false "unchanged" skips when file size happens to match.
            cache_control = (
                ATLAS_IMAGE_CACHE_CONTROL
                if key == "atlas/atlas.webp"
                else ATLAS_MANIFEST_CACHE_CONTROL
            )
            upload_file(
                s3,
                bucket,
                local_path,
                key,
                cache_control=cache_control,
            )
            uploaded += 1
            continue

        if key.startswith("faces/"):
            local_size = local_path.stat().st_size
            remote_size = remote.get(key)
            if remote_size is not None and remote_size == local_size:
                skipped += 1
                continue
            upload_file(
                s3,
                bucket,
                local_path,
                key,
                cache_control=FACES_CACHE_CONTROL,
            )
            uploaded += 1
            continue

        local_size = local_path.stat().st_size
        remote_size = remote.get(key)
        if remote_size is not None and remote_size == local_size:
            skipped += 1
            continue
        upload_file(s3, bucket, local_path, key)
        uploaded += 1

    # Delete stale remote files in managed prefixes (faces/ only when we synced faces).
    desired_keys = set(desired.keys())
    remote_keys = set(remote.keys())
    stale_keys = sorted(remote_keys - desired_keys)
    delete_keys(s3, bucket, stale_keys)

    print(
        "Spaces sync complete. "
        f"desired={len(desired_keys)} uploaded={uploaded} skipped={skipped} deleted={len(stale_keys)}"
        + (f" (faces local files: {len(face_uploads)})" if face_uploads else "")
    )


if __name__ == "__main__":
    main()

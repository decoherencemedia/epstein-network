"""DigitalOcean Spaces (S3-compatible) helpers shared by ingest and upload scripts."""

import io
import mimetypes
import os
from pathlib import Path
from typing import Any

import boto3


def get_spaces_client() -> tuple[Any, str]:
    region = os.environ.get("EPSTEIN_SPACES_REGION")
    endpoint = os.environ.get("EPSTEIN_SPACES_ENDPOINT")
    bucket = os.environ.get("EPSTEIN_SPACES_BUCKET")
    key = os.environ.get("EPSTEIN_SPACES_KEY")
    secret = os.environ.get("EPSTEIN_SPACES_SECRET")

    missing = [
        name
        for name, val in [
            ("EPSTEIN_SPACES_REGION", region),
            ("EPSTEIN_SPACES_ENDPOINT", endpoint),
            ("EPSTEIN_SPACES_BUCKET", bucket),
            ("EPSTEIN_SPACES_KEY", key),
            ("EPSTEIN_SPACES_SECRET", secret),
        ]
        if not val
    ]
    if missing:
        raise RuntimeError(f"Missing required environment variables for Spaces: {', '.join(missing)}")

    s3 = boto3.client(
        "s3",
        region_name=region,
        endpoint_url=endpoint,
        aws_access_key_id=key,
        aws_secret_access_key=secret,
    )
    return s3, bucket


def list_remote_basenames(s3: Any, bucket: str, prefix: str) -> set[str]:
    """Basenames (e.g. ``foo.webp``) under ``prefix`` (e.g. ``images/``)."""
    out: set[str] = set()
    paginator = s3.get_paginator("list_objects_v2")
    for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
        for obj in page.get("Contents", []):
            key = obj["Key"]
            if key.endswith("/"):
                continue
            out.add(Path(key).name)
    return out


def list_remote_objects(s3: Any, bucket: str, prefixes: tuple[str, ...]) -> dict[str, int]:
    """Return ``{key: size_bytes}`` for all objects under the provided prefixes."""
    out: dict[str, int] = {}
    paginator = s3.get_paginator("list_objects_v2")
    for prefix in prefixes:
        for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
            for obj in page.get("Contents", []):
                out[obj["Key"]] = int(obj["Size"])
    return out


def delete_keys(s3: Any, bucket: str, keys: list[str]) -> None:
    """Delete keys in batches (max 1000 per request)."""
    if not keys:
        return
    chunk_size = 1000
    for i in range(0, len(keys), chunk_size):
        chunk = keys[i : i + chunk_size]
        print(f"Deleting {len(chunk)} stale object(s) from s3://{bucket}/...")
        s3.delete_objects(
            Bucket=bucket,
            Delete={"Objects": [{"Key": k} for k in chunk], "Quiet": True},
        )


def upload_file(
    s3: Any,
    bucket: str,
    local_path: Path,
    key: str,
    cache_control: str | None = None,
) -> None:
    if not local_path.is_file():
        raise FileNotFoundError(f"Required file missing for upload: {local_path}")
    print(f"Uploading {local_path} -> s3://{bucket}/{key}")
    extra_args: dict[str, str] = {"ACL": "public-read"}
    content_type, _ = mimetypes.guess_type(str(local_path))
    if content_type is None:
        suf = local_path.suffix.lower()
        if suf == ".webp":
            content_type = "image/webp"
        elif suf in (".jpg", ".jpeg"):
            content_type = "image/jpeg"
        elif suf == ".json":
            content_type = "application/json"
    if content_type is not None:
        extra_args["ContentType"] = content_type
    if cache_control is not None:
        extra_args["CacheControl"] = cache_control
    s3.upload_file(
        Filename=str(local_path),
        Bucket=bucket,
        Key=key,
        ExtraArgs=extra_args,
    )


def upload_bytes(
    s3: Any,
    bucket: str,
    key: str,
    data: bytes,
    *,
    content_type: str,
    cache_control: str,
    dry_run: bool = False,
) -> None:
    """Upload in-memory bytes (e.g. WebP from ``cwebp``) with public-read ACL."""
    extra: dict[str, str] = {
        "ACL": "public-read",
        "ContentType": content_type,
        "CacheControl": cache_control,
    }
    if dry_run:
        print(f"  [dry-run] would upload {len(data)} bytes -> s3://{bucket}/{key}")
        return
    print(f"  uploading {len(data)} bytes -> s3://{bucket}/{key}")
    s3.upload_fileobj(io.BytesIO(data), Bucket=bucket, Key=key, ExtraArgs=extra)

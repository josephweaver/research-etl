from __future__ import annotations

import os
from pathlib import Path
from typing import Any, Dict, Tuple
from urllib.parse import urlparse

from botocore.config import Config


def _parse_gcs_uri(uri: str) -> Tuple[str, str]:
    text = str(uri or "").strip()
    parsed = urlparse(text)
    if parsed.scheme.lower() != "gcs":
        raise ValueError(f"gcs transport requires gcs://bucket/path URI, got: {uri}")
    bucket = str(parsed.netloc or "").strip()
    key = str(parsed.path or "").lstrip("/")
    if not bucket:
        raise ValueError(f"gcs transport requires bucket name in URI, got: {uri}")
    return (bucket, key)


def _normalize_upload_key(key: str, src_name: str) -> str:
    text = str(key or "").strip().strip("/")
    if not text:
        return src_name
    leaf = text.rsplit("/", 1)[-1]
    if "." in leaf:
        return text
    return f"{text}/{src_name}"


def _build_client(options: Dict[str, Any] | None = None):
    try:
        import boto3
    except Exception as exc:  # noqa: BLE001
        raise RuntimeError("gcs transport requires the `boto3` package") from exc

    opts = dict(options or {})
    access_key = (
        str(opts.get("access_key") or "").strip()
        or str(os.environ.get("GCS_HMAC_KEY") or "").strip()
        or str(os.environ.get("AWS_ACCESS_KEY_ID") or "").strip()
    )
    secret_key = (
        str(opts.get("secret_key") or "").strip()
        or str(os.environ.get("GCS_HMAC_SECRET") or "").strip()
        or str(os.environ.get("AWS_SECRET_ACCESS_KEY") or "").strip()
    )
    endpoint_url = str(opts.get("endpoint_url") or os.environ.get("GCS_S3_ENDPOINT") or "").strip()
    region_name = str(opts.get("region_name") or os.environ.get("AWS_DEFAULT_REGION") or "auto").strip() or "auto"
    if not access_key or not secret_key:
        raise RuntimeError("gcs transport requires GCS_HMAC_KEY and GCS_HMAC_SECRET (or explicit access_key/secret_key)")
    if not endpoint_url:
        endpoint_url = "https://storage.googleapis.com"
    session = boto3.session.Session()
    return session.client(
        "s3",
        aws_access_key_id=access_key,
        aws_secret_access_key=secret_key,
        endpoint_url=endpoint_url,
        region_name=region_name,
        config=Config(
            signature_version="s3v4",
            s3={"addressing_style": "path"},
            request_checksum_calculation="when_required",
            response_checksum_validation="when_required",
        ),
    )


def transfer_gcs(
    *,
    source_path: str,
    target_uri: str,
    dry_run: bool = False,
    options: Dict[str, Any] | None = None,
) -> Dict[str, Any]:
    src = Path(source_path).expanduser().resolve()
    if not src.exists():
        raise FileNotFoundError(f"source_path not found: {src}")
    bucket, key = _parse_gcs_uri(target_uri)
    if dry_run:
        return {
            "transport": "gcs",
            "target_uri": str(target_uri),
            "dry_run": True,
        }

    client = _build_client(options)
    if src.is_file():
        final_key = _normalize_upload_key(key, src.name)
        with src.open("rb") as f:
            client.put_object(Bucket=bucket, Key=final_key, Body=f)
        return {
            "transport": "gcs",
            "target_uri": f"gcs://{bucket}/{final_key}",
            "dry_run": False,
        }

    prefix = key.rstrip("/")
    for child in src.rglob("*"):
        if not child.is_file():
            continue
        rel = child.relative_to(src).as_posix()
        child_key = f"{prefix}/{rel}" if prefix else rel
        with child.open("rb") as f:
            client.put_object(Bucket=bucket, Key=child_key, Body=f)
    final_target = f"gcs://{bucket}/{prefix}" if prefix else f"gcs://{bucket}"
    return {
        "transport": "gcs",
        "target_uri": final_target,
        "dry_run": False,
    }


def fetch_gcs(
    *,
    source_uri: str,
    target_path: str,
    dry_run: bool = False,
    options: Dict[str, Any] | None = None,
) -> Dict[str, Any]:
    bucket, key = _parse_gcs_uri(source_uri)
    out = Path(target_path).expanduser().resolve()
    if dry_run:
        return {
            "transport": "gcs",
            "target_path": str(out),
            "dry_run": True,
        }

    client = _build_client(options)
    try:
        if key:
            client.head_object(Bucket=bucket, Key=key)
            out.parent.mkdir(parents=True, exist_ok=True)
            client.download_file(bucket, key, str(out))
            return {
                "transport": "gcs",
                "target_path": str(out),
                "dry_run": False,
            }
    except Exception:
        pass

    prefix = key.rstrip("/")
    list_prefix = f"{prefix}/" if prefix else ""
    paginator = client.get_paginator("list_objects_v2")
    found = False
    for page in paginator.paginate(Bucket=bucket, Prefix=list_prefix):
        for item in list(page.get("Contents") or []):
            obj_key = str(item.get("Key") or "").strip()
            if not obj_key or obj_key.endswith("/"):
                continue
            found = True
            rel = obj_key[len(list_prefix) :] if list_prefix and obj_key.startswith(list_prefix) else obj_key
            dst = out / rel if prefix else out / obj_key
            dst.parent.mkdir(parents=True, exist_ok=True)
            client.download_file(bucket, obj_key, str(dst))
    if not found:
        raise FileNotFoundError(f"gcs source not found: {source_uri}")
    return {
        "transport": "gcs",
        "target_path": str(out),
        "dry_run": False,
    }


__all__ = ["transfer_gcs", "fetch_gcs"]

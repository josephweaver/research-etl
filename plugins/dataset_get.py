from __future__ import annotations

from etl.datasets import get_data


meta = {
    "name": "dataset_get",
    "version": "0.1.0",
    "description": "Resolve and retrieve a dataset version to a local path.",
    "inputs": [],
    "outputs": [
        "dataset_id",
        "version_label",
        "environment",
        "location_type",
        "source_uri",
        "local_path",
        "transport",
        "fetched",
        "checksum",
        "size_bytes",
        "dry_run",
    ],
    "params": {
        "dataset_id": {"type": "str", "default": ""},
        "version": {"type": "str", "default": "latest"},
        "environment": {"type": "str", "default": ""},
        "runtime_context": {"type": "str", "default": "local"},
        "location_type": {"type": "str", "default": ""},
        "cache_dir": {"type": "str", "default": ".runs/datasets_cache"},
        "transport": {"type": "str", "default": ""},
        "dry_run": {"type": "bool", "default": False},
        "no_direct_local": {"type": "bool", "default": False},
        "rclone_bin": {"type": "str", "default": "rclone"},
        "shared_drive_id": {"type": "str", "default": ""},
    },
    "idempotent": True,
}


def run(args, ctx):
    dataset_id = str(args.get("dataset_id") or "").strip()
    if not dataset_id:
        raise ValueError("dataset_id is required")

    version = str(args.get("version") or "latest").strip() or "latest"
    runtime_context = str(args.get("runtime_context") or "local").strip() or "local"
    cache_dir = str(args.get("cache_dir") or ".runs/datasets_cache").strip() or ".runs/datasets_cache"
    ctx.log(
        f"[dataset_get] dataset_id={dataset_id} version={version} runtime={runtime_context} cache_dir={cache_dir}"
    )

    return get_data(
        dataset_id=dataset_id,
        version=version,
        environment=str(args.get("environment") or "").strip() or None,
        runtime_context=runtime_context,
        location_type=str(args.get("location_type") or "").strip() or None,
        cache_dir=cache_dir,
        transport=str(args.get("transport") or "").strip() or None,
        dry_run=bool(args.get("dry_run", False)),
        prefer_direct_local=not bool(args.get("no_direct_local", False)),
        transport_options={
            "rclone_bin": str(args.get("rclone_bin") or "rclone").strip() or "rclone",
            "shared_drive_id": str(args.get("shared_drive_id") or "").strip(),
        },
    )

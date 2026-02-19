from __future__ import annotations

from etl.datasets import DatasetServiceError, store_data


meta = {
    "name": "dataset_store",
    "version": "0.1.0",
    "description": "Store a local file/directory as a dataset version using dataset registry routing.",
    "inputs": [],
    "outputs": [
        "dataset_id",
        "version_label",
        "stage",
        "environment",
        "location_type",
        "target_uri",
        "transport",
        "checksum",
        "size_bytes",
        "dry_run",
    ],
    "params": {
        "dataset_id": {"type": "str", "default": ""},
        "path": {"type": "str", "default": ""},
        "stage": {"type": "str", "default": "staging"},
        "version": {"type": "str", "default": ""},
        "environment": {"type": "str", "default": ""},
        "runtime_context": {"type": "str", "default": "local"},
        "location_type": {"type": "str", "default": ""},
        "location_alias": {"type": "str", "default": ""},
        "locations_config": {"type": "str", "default": ""},
        "target_uri": {"type": "str", "default": ""},
        "transport": {"type": "str", "default": ""},
        "owner": {"type": "str", "default": ""},
        "data_class": {"type": "str", "default": ""},
        "dry_run": {"type": "bool", "default": False},
        "rclone_bin": {"type": "str", "default": "rclone"},
        "shared_drive_id": {"type": "str", "default": ""},
    },
    "idempotent": False,
}


def run(args, ctx):
    dataset_id = str(args.get("dataset_id") or "").strip()
    source_path = str(args.get("path") or "").strip()
    if not dataset_id:
        raise ValueError("dataset_id is required")
    if not source_path:
        raise ValueError("path is required")

    ctx.log(
        f"[dataset_store] dataset_id={dataset_id} path={source_path} "
        f"stage={str(args.get('stage') or 'staging').strip()} runtime={str(args.get('runtime_context') or 'local').strip()}"
    )

    try:
        receipt = store_data(
            dataset_id=dataset_id,
            source_path=source_path,
            stage=str(args.get("stage") or "staging").strip(),
            version_label=str(args.get("version") or "").strip() or None,
            environment=str(args.get("environment") or "").strip() or None,
            runtime_context=str(args.get("runtime_context") or "local").strip() or "local",
            location_type=str(args.get("location_type") or "").strip() or None,
            location_alias=str(args.get("location_alias") or "").strip() or None,
            locations_config_path=str(args.get("locations_config") or "").strip() or None,
            target_uri=str(args.get("target_uri") or "").strip() or None,
            transport=str(args.get("transport") or "").strip() or None,
            owner_user=str(args.get("owner") or "").strip() or None,
            data_class=str(args.get("data_class") or "").strip() or None,
            dry_run=bool(args.get("dry_run", False)),
            transport_options={
                "rclone_bin": str(args.get("rclone_bin") or "rclone").strip() or "rclone",
                "shared_drive_id": str(args.get("shared_drive_id") or "").strip(),
            },
        )
    except DatasetServiceError as exc:
        for line in list(getattr(exc, "details", {}).get("operation_log") or []):
            ctx.error(f"[dataset_store] {line}")
        raise

    for line in list(receipt.get("operation_log") or []):
        ctx.log(f"[dataset_store] {line}")
    return receipt

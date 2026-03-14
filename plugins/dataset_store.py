# research-etl
# Copyright (c) 2026 Joseph Weaver
# This file is part of the research-etl project and is licensed under the MIT License.
# You may not use this file except in compliance with the License.
# See https://github.com/josephweaver/research-etl for details.

from __future__ import annotations

import os
from pathlib import Path

from etl.datasets import DatasetServiceError, store_data
from etl.source_control import checkin_single_file


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
        "schema_hash",
        "profile",
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
        "project_id": {"type": "str", "default": ""},
        "projects_config": {"type": "str", "default": ""},
        "workspace_auto_register": {"type": "bool", "default": True},
        "workspace_config_path": {"type": "str", "default": ""},
        "workspace_table_name": {"type": "str", "default": ""},
        "workspace_use_partials": {"type": "bool", "default": True},
        "workspace_partial_path": {"type": "str", "default": ""},
        "workspace_git_commit": {"type": "bool", "default": False},
        "workspace_git_push": {"type": "bool", "default": False},
        "workspace_git_remote": {"type": "str", "default": "origin"},
        "workspace_git_branch": {"type": "str", "default": ""},
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

def _maybe_commit_workspace(receipt: dict, args: dict, ctx) -> dict:
    info = (((receipt or {}).get("profile") or {}).get("workspace_auto_register") or {})
    if not bool(info.get("updated")):
        return {"committed": False, "reason": "workspace_not_updated"}
    if not bool(args.get("workspace_git_commit", False)):
        return {"committed": False, "reason": "workspace_git_commit_disabled"}
    ws_path = Path(str(info.get("workspace_path") or "").strip())
    if not ws_path.exists():
        return {"committed": False, "reason": "workspace_path_missing"}
    env = dict(os.environ)
    dataset_id = str((receipt or {}).get("dataset_id") or "").strip()
    version = str((receipt or {}).get("version_label") or "").strip()
    msg = f"chore(workspace): update {dataset_id} {version}".strip()
    return checkin_single_file(
        file_path=ws_path,
        message=msg,
        push=bool(args.get("workspace_git_push", False)),
        remote=str(args.get("workspace_git_remote") or "origin").strip() or "origin",
        branch=str(args.get("workspace_git_branch") or "").strip(),
        env=env,
    )


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
            project_id=str(args.get("project_id") or "").strip() or None,
            projects_config_path=str(args.get("projects_config") or "").strip() or None,
            workspace_auto_register=bool(args.get("workspace_auto_register", True)),
            workspace_config_path=str(args.get("workspace_config_path") or "").strip() or None,
            workspace_table_name=str(args.get("workspace_table_name") or "").strip() or None,
            workspace_use_partials=bool(args.get("workspace_use_partials", True)),
            workspace_partial_path=str(args.get("workspace_partial_path") or "").strip() or None,
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

    git_result = _maybe_commit_workspace(receipt, args, ctx)
    receipt["workspace_git"] = git_result
    if git_result.get("committed"):
        ctx.log(f"[dataset_store] workspace committed: {git_result.get('workspace_path')}")
    else:
        ctx.log(f"[dataset_store] workspace git: {git_result.get('reason')}")

    for line in list(receipt.get("operation_log") or []):
        ctx.log(f"[dataset_store] {line}")
    return receipt

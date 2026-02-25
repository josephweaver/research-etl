# research-etl
# Copyright (c) 2026 Joseph Weaver
# This file is part of the research-etl project and is licensed under the MIT License.
# You may not use this file except in compliance with the License.
# See https://github.com/josephweaver/research-etl for details.

from __future__ import annotations
"""Datasets CLI command group.

High-level idea:
- Offer dataset registry and transfer operations from the command line.
- Includes read commands (`list`, `show`), data movement (`store`, `get`),
  and dictionary PR automation (`dictionary-pr`).
"""

import argparse
import sys

from etl.cli_cmd.common import CommandHandler
from etl.datasets import DatasetServiceError, get_data, get_dataset, list_datasets, store_data
from etl.dictionary_pr import DictionaryPRError, create_dictionary_pr


def register_datasets_args(
    datasets_sub: argparse._SubParsersAction,
    *,
    cmd_datasets_list: CommandHandler,
    cmd_datasets_show: CommandHandler,
    cmd_datasets_store: CommandHandler,
    cmd_datasets_get: CommandHandler,
    cmd_datasets_dictionary_pr: CommandHandler,
) -> None:
    # CLI inputs: datasets list/show/store/get/dictionary-pr argument sets.
    # CLI output: configures subparsers that map each subcommand to a handler.
    p_datasets_list = datasets_sub.add_parser("list", help="List registered datasets")
    p_datasets_list.add_argument("--limit", type=int, default=50, help="Max datasets to show")
    p_datasets_list.add_argument("--q", default=None, help="Filter by dataset id or owner")
    p_datasets_list.set_defaults(func=cmd_datasets_list)

    p_datasets_show = datasets_sub.add_parser("show", help="Show one dataset with versions/locations")
    p_datasets_show.add_argument("dataset_id", help="Dataset id to inspect")
    p_datasets_show.set_defaults(func=cmd_datasets_show)

    p_datasets_store = datasets_sub.add_parser("store", help="Store local data into dataset registry/location")
    p_datasets_store.add_argument("dataset_id", help="Dataset id to store under")
    p_datasets_store.add_argument("--path", required=True, help="Local source file/directory path")
    p_datasets_store.add_argument("--stage", default="staging", choices=["staging", "published"], help="Storage stage")
    p_datasets_store.add_argument("--version", default=None, help="Version label (default generated UTC label)")
    p_datasets_store.add_argument("--environment", default=None, help="Target environment name")
    p_datasets_store.add_argument("--runtime-context", default="local", help="Current runtime context (local|slurm)")
    p_datasets_store.add_argument("--location-alias", default=None, help="Named data location alias (for example LC_GDrive)")
    p_datasets_store.add_argument("--locations-config", default=None, help="Path to data locations config YAML")
    p_datasets_store.add_argument("--location-type", default=None, help="Target location type (policy key)")
    p_datasets_store.add_argument("--target-uri", default=None, help="Explicit target URI/path")
    p_datasets_store.add_argument("--transport", default=None, help="Transport override (local_fs|rclone|rsync)")
    p_datasets_store.add_argument("--owner", default=None, help="Optional owner_user value for dataset record")
    p_datasets_store.add_argument("--data-class", default=None, help="Optional data_class value for dataset record")
    p_datasets_store.add_argument("--dry-run", action="store_true", help="Plan transfer without copying bytes")
    p_datasets_store.add_argument("--rclone-bin", default="rclone", help="rclone binary for rclone transport")
    p_datasets_store.add_argument("--shared-drive-id", default="", help="Optional team drive id for rclone transport")
    p_datasets_store.set_defaults(func=cmd_datasets_store)

    p_datasets_get = datasets_sub.add_parser("get", help="Retrieve dataset version to local cache path")
    p_datasets_get.add_argument("dataset_id", help="Dataset id to retrieve")
    p_datasets_get.add_argument("--version", default="latest", help="Version label or 'latest'")
    p_datasets_get.add_argument("--environment", default=None, help="Preferred source environment")
    p_datasets_get.add_argument("--runtime-context", default="local", help="Current runtime context (local|slurm)")
    p_datasets_get.add_argument("--location-alias", default=None, help="Named data location alias (for example LC_GDrive)")
    p_datasets_get.add_argument("--locations-config", default=None, help="Path to data locations config YAML")
    p_datasets_get.add_argument("--location-type", default=None, help="Optional location type filter")
    p_datasets_get.add_argument("--cache-dir", default=".runs/datasets_cache", help="Local cache root for fetched data")
    p_datasets_get.add_argument("--transport", default=None, help="Transport override (local_fs|rclone|rsync)")
    p_datasets_get.add_argument("--dry-run", action="store_true", help="Plan retrieval without copying bytes")
    p_datasets_get.add_argument("--no-direct-local", action="store_true", help="Force copy even if source local path exists")
    p_datasets_get.add_argument("--rclone-bin", default="rclone", help="rclone binary for rclone transport")
    p_datasets_get.add_argument("--shared-drive-id", default="", help="Optional team drive id for rclone transport")
    p_datasets_get.set_defaults(func=cmd_datasets_get)

    p_datasets_dict_pr = datasets_sub.add_parser(
        "dictionary-pr",
        help="Create/update dictionary entry in mapped repo and open PR",
    )
    p_datasets_dict_pr.add_argument("dataset_id", help="Dataset id to update in dictionary")
    p_datasets_dict_pr.add_argument("--repo-key", required=True, help="Dictionary repo key from etl_dictionary_repos")
    p_datasets_dict_pr.add_argument("--source-file", required=True, help="Local YAML source file to copy into repo")
    p_datasets_dict_pr.add_argument("--file-path", default=None, help="Target file path in repo (default from mapping)")
    p_datasets_dict_pr.add_argument("--branch-name", default=None, help="Branch name override")
    p_datasets_dict_pr.add_argument("--base-branch", default=None, help="Base branch override")
    p_datasets_dict_pr.add_argument("--pr-title", default=None, help="Pull request title override")
    p_datasets_dict_pr.add_argument("--pr-body", default=None, help="Pull request body override")
    p_datasets_dict_pr.add_argument("--commit-message", default=None, help="Git commit message override")
    p_datasets_dict_pr.add_argument("--no-pr", action="store_true", help="Do not create a pull request")
    p_datasets_dict_pr.add_argument(
        "--no-github-api",
        action="store_true",
        help="Skip GitHub API and use `gh pr create` directly.",
    )
    p_datasets_dict_pr.add_argument("--dry-run", action="store_true", help="Show commands without writing/pushing")
    p_datasets_dict_pr.set_defaults(func=cmd_datasets_dictionary_pr)


def cmd_datasets_list(args: argparse.Namespace) -> int:
    """List dataset records.

    Inputs:
    - `args.limit`: maximum records returned.
    - `args.q`: optional free-text filter.

    Outputs:
    - stdout: one summary line per dataset.
    - stderr: query error details.
    - return code: `0` on success, `1` on service error.
    """
    try:
        datasets = list_datasets(limit=args.limit, q=args.q)
    except DatasetServiceError as exc:
        print(f"Dataset query error: {exc}", file=sys.stderr)
        return 1
    if not datasets:
        print("No datasets found.")
        return 0
    for item in datasets:
        print(
            f"{item['dataset_id']} | status={item['status']} | class={item['data_class'] or '-'} "
            f"| owner={item['owner_user'] or '-'} | versions={item['version_count']} "
            f"| latest={item['latest_version'] or '-'}"
        )
    return 0


def cmd_datasets_show(args: argparse.Namespace) -> int:
    """Show one dataset with versions and locations.

    Inputs:
    - `args.dataset_id`: dataset key to inspect.

    Outputs:
    - stdout: dataset metadata, versions, and location entries.
    - stderr: not-found or query error details.
    - return code: `0` on success, `1` on failure.
    """
    try:
        dataset = get_dataset(args.dataset_id)
    except DatasetServiceError as exc:
        print(f"Dataset query error: {exc}", file=sys.stderr)
        return 1
    if not dataset:
        print(f"Dataset not found: {args.dataset_id}", file=sys.stderr)
        return 1

    print(f"Dataset: {dataset['dataset_id']}")
    print(f"Status: {dataset['status']}")
    print(f"Class: {dataset['data_class'] or '-'}")
    print(f"Owner: {dataset['owner_user'] or '-'}")
    print(f"Created: {dataset['created_at'] or '-'}")
    print(f"Updated: {dataset['updated_at'] or '-'}")
    print("Versions:")
    if not dataset["versions"]:
        print("  (none)")
    else:
        for version in dataset["versions"]:
            print(
                f"  - {version['version_label']} "
                f"(id={version['dataset_version_id']}, immutable={version['is_immutable']}, "
                f"run={version['created_by_run_id'] or '-'}, created={version['created_at'] or '-'})"
            )
    print("Locations:")
    if not dataset["locations"]:
        print("  (none)")
    else:
        for location in dataset["locations"]:
            print(
                f"  - version={location['version_label']} env={location['environment'] or '-'} "
                f"type={location['location_type']} canonical={location['is_canonical']} "
                f"uri={location['uri']}"
            )
    return 0


def cmd_datasets_store(args: argparse.Namespace) -> int:
    """Store local data into a dataset/version/location target.

    Inputs:
    - dataset id + transfer/config arguments from `datasets store`.

    Outputs:
    - stdout: receipt summary and operation trace lines.
    - stderr: service error and trace lines.
    - return code: `0` on success, `1` on failure.
    """
    try:
        receipt = store_data(
            dataset_id=args.dataset_id,
            source_path=args.path,
            stage=args.stage,
            version_label=args.version,
            environment=args.environment,
            runtime_context=args.runtime_context,
            location_alias=args.location_alias,
            locations_config_path=args.locations_config,
            location_type=args.location_type,
            target_uri=args.target_uri,
            transport=args.transport,
            owner_user=args.owner,
            data_class=args.data_class,
            dry_run=bool(args.dry_run),
            transport_options={
                "rclone_bin": args.rclone_bin,
                "shared_drive_id": args.shared_drive_id,
            },
        )
    except DatasetServiceError as exc:
        print(f"Dataset store error: {exc}", file=sys.stderr)
        for line in list(getattr(exc, "details", {}).get("operation_log") or []):
            print(f"  [trace] {line}", file=sys.stderr)
        return 1

    print(f"Stored dataset: {receipt['dataset_id']}")
    print(f"Version: {receipt['version_label']}")
    print(f"Stage: {receipt['stage']}")
    print(f"Location: {receipt['location_type']} -> {receipt['target_uri']}")
    print(f"Transport: {receipt['transport']} (dry_run={receipt['dry_run']})")
    print(f"Checksum: {receipt['checksum'] or '-'}")
    print(f"Size bytes: {receipt['size_bytes'] if receipt['size_bytes'] is not None else '-'}")
    for line in list(receipt.get("operation_log") or []):
        print(f"  [trace] {line}")
    return 0


def cmd_datasets_get(args: argparse.Namespace) -> int:
    """Fetch dataset data into local cache/workspace.

    Inputs:
    - dataset id + retrieval options from `datasets get`.

    Outputs:
    - stdout: retrieval receipt and operation trace lines.
    - stderr: service error and trace lines.
    - return code: `0` on success, `1` on failure.
    """
    try:
        receipt = get_data(
            dataset_id=args.dataset_id,
            version=args.version,
            environment=args.environment,
            runtime_context=args.runtime_context,
            location_alias=args.location_alias,
            locations_config_path=args.locations_config,
            location_type=args.location_type,
            cache_dir=args.cache_dir,
            transport=args.transport,
            dry_run=bool(args.dry_run),
            prefer_direct_local=not bool(args.no_direct_local),
            transport_options={
                "rclone_bin": args.rclone_bin,
                "shared_drive_id": args.shared_drive_id,
            },
        )
    except DatasetServiceError as exc:
        print(f"Dataset get error: {exc}", file=sys.stderr)
        for line in list(getattr(exc, "details", {}).get("operation_log") or []):
            print(f"  [trace] {line}", file=sys.stderr)
        return 1

    print(f"Retrieved dataset: {receipt['dataset_id']}")
    print(f"Version: {receipt['version_label']}")
    print(f"Source: {receipt['location_type']} -> {receipt['source_uri']}")
    print(f"Local path: {receipt['local_path']}")
    print(f"Transport: {receipt['transport']} (fetched={receipt['fetched']}, dry_run={receipt['dry_run']})")
    print(f"Checksum: {receipt['checksum'] or '-'}")
    print(f"Size bytes: {receipt['size_bytes'] if receipt['size_bytes'] is not None else '-'}")
    for line in list(receipt.get("operation_log") or []):
        print(f"  [trace] {line}")
    return 0


def cmd_datasets_dictionary_pr(args: argparse.Namespace) -> int:
    """Create or update a dictionary file and optionally open a PR.

    Inputs:
    - dataset id, repo selection, source file, and PR/branch options.

    Outputs:
    - stdout: workflow receipt and operation trace lines.
    - stderr: dictionary workflow errors and trace lines.
    - return code: `0` on success, `1` on failure.
    """
    try:
        receipt = create_dictionary_pr(
            dataset_id=args.dataset_id,
            repo_key=args.repo_key,
            source_file=args.source_file,
            file_path=args.file_path,
            branch_name=args.branch_name,
            base_branch=args.base_branch,
            pr_title=args.pr_title,
            pr_body=args.pr_body,
            commit_message=args.commit_message,
            create_pr=not bool(args.no_pr),
            use_github_api=not bool(args.no_github_api),
            dry_run=bool(args.dry_run),
        )
    except DictionaryPRError as exc:
        print(f"Dictionary PR error: {exc}", file=sys.stderr)
        for line in list(getattr(exc, "details", {}).get("operation_log") or []):
            print(f"  [trace] {line}", file=sys.stderr)
        return 1

    print(f"Dictionary workflow dataset={receipt['dataset_id']} repo={receipt['repo_key']}")
    print(f"Target file: {receipt['file_path']}")
    print(f"Branch: {receipt['branch_name']} (base={receipt['base_branch']})")
    print(f"Has changes: {receipt['has_changes']}")
    print(f"Commit: {receipt['commit_sha'] or '-'}")
    print(f"PR: {receipt['pr_url'] or '-'}")
    for line in list(receipt.get("operation_log") or []):
        print(f"  [trace] {line}")
    return 0

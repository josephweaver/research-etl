# research-etl
# Copyright (c) 2026 Joseph Weaver
# This file is part of the research-etl project and is licensed under the MIT License.
# You may not use this file except in compliance with the License.
# See https://github.com/josephweaver/research-etl for details.

from __future__ import annotations
"""Artifacts CLI command group.

High-level idea:
- Execute artifact policy enforcement tasks (retention, canonical location checks).
- Intended for operations/audit workflows around artifact lifecycle management.
"""

import argparse
import sys
from pathlib import Path

from etl.artifacts import (
    ArtifactPolicyError,
    enforce_artifact_policies,
    load_artifact_policy,
    resolve_artifact_policy_path,
)
from etl.cli_cmd.common import CommandHandler, emit_error_with_report


def register_artifacts_args(
    artifacts_sub: argparse._SubParsersAction,
    *,
    cmd_artifacts_enforce: CommandHandler,
) -> None:
    # CLI inputs: artifacts enforce [--config] [--dry-run] [--limit].
    # CLI output: registers the artifact enforcement subcommand.
    p_artifacts_enforce = artifacts_sub.add_parser("enforce", help="Enforce artifact retention/canonical policies")
    p_artifacts_enforce.add_argument(
        "--config",
        default=None,
        help="Path to artifact policy YAML (default: config/artifacts.yml)",
    )
    p_artifacts_enforce.add_argument(
        "--dry-run",
        action="store_true",
        help="Evaluate policy actions without deleting files or updating location state.",
    )
    p_artifacts_enforce.add_argument(
        "--limit",
        type=int,
        default=2000,
        help="Max number of artifact records to inspect per run.",
    )
    p_artifacts_enforce.set_defaults(func=cmd_artifacts_enforce)

def cmd_artifacts_enforce(args: argparse.Namespace) -> int:
    """Run artifact policy enforcement and print a summary report.

    Inputs:
    - `args.config`: optional policy config path.
    - `args.dry_run`: evaluate actions without mutating state.
    - `args.limit`: max number of artifacts to inspect.

    Outputs:
    - stdout: aggregate summary and sample violations.
    - stderr: config and runtime failure details.
    - diagnostics: writes report on unexpected enforcement exceptions.
    - return code: `0` on success, `1` on failure.
    """
    try:
        policy_path = resolve_artifact_policy_path(Path(args.config) if args.config else None)
    except ArtifactPolicyError as exc:
        print(f"Artifact policy config error: {exc}", file=sys.stderr)
        return 1
    if not policy_path:
        print(
            "Artifact policy config error: no policy file found. "
            "Create config/artifacts.yml (see config/artifacts.example.yml).",
            file=sys.stderr,
        )
        return 1
    try:
        policy = load_artifact_policy(policy_path)
    except ArtifactPolicyError as exc:
        print(f"Artifact policy config error: {exc}", file=sys.stderr)
        return 1

    try:
        summary = enforce_artifact_policies(
            config=policy,
            dry_run=bool(args.dry_run),
            limit=int(args.limit),
        )
    except Exception as exc:  # noqa: BLE001
        emit_error_with_report(f"Artifact enforcement failed: {exc}", exc, args)
        return 1

    print(
        "Artifact policy summary: "
        f"inspected_artifacts={summary.get('inspected_artifacts', 0)} "
        f"inspected_locations={summary.get('inspected_locations', 0)} "
        f"missing_canonical={summary.get('violations_missing_canonical', 0)} "
        f"policy_violations={summary.get('violations_policy', 0)} "
        f"deleted={summary.get('deleted_files', 0)} "
        f"would_delete={summary.get('would_delete_files', 0)} "
        f"missing_marked={summary.get('missing_files_marked', 0)} "
        f"errors={summary.get('delete_errors', 0)}"
    )

    violations = summary.get("violations") or []
    if violations:
        print("Sample violations:")
        for item in violations[:10]:
            print(
                f"- {item.get('artifact_key')} ({item.get('artifact_class')}): "
                f"{item.get('issue')} expected={item.get('expected_location_type')}"
            )

    return 0

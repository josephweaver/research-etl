# research-etl
# Copyright (c) 2026 Joseph Weaver
# This file is part of the research-etl project and is licensed under the MIT License.
# You may not use this file except in compliance with the License.
# See https://github.com/josephweaver/research-etl for details.

from __future__ import annotations
"""Validate CLI command group.

High-level idea:
- Perform static pipeline validation using resolved config/project context.
- Confirms parser/resolution integrity before execution.
"""

import argparse
import sys
from pathlib import Path

from etl.cli_cmd.common import CommandHandler
from etl.config import ConfigError, load_global_config, resolve_global_config_path
from etl.pipeline import PipelineError, parse_pipeline
from etl.pipeline_assets import PipelineAssetError, resolve_pipeline_path_from_project_sources
from etl.projects import (
    ProjectConfigError,
    load_project_vars,
    resolve_project_id,
    resolve_projects_config_path,
)


def register_validate_args(
    subparsers: argparse._SubParsersAction,
    *,
    cmd_validate: CommandHandler,
) -> None:
    # CLI inputs: validate <pipeline> with optional global/projects/project-id context.
    # CLI output: registers parser for pipeline validation command.
    p_validate = subparsers.add_parser("validate", help="Validate a pipeline file")
    p_validate.add_argument("pipeline", help="Path to pipeline YAML")
    p_validate.add_argument("--global-config", help="Path to global config YAML", default=None)
    p_validate.add_argument(
        "--projects-config",
        help="Path to project vars config YAML (default: config/projects.yml when present)",
        default=None,
    )
    p_validate.add_argument("--project-id", default=None, help="Project id override for validation context.")
    p_validate.set_defaults(func=cmd_validate)


def cmd_validate(args: argparse.Namespace) -> int:
    """Validate a pipeline file and print step-count summary.

    Inputs:
    - pipeline path plus optional config/project selection arguments.

    Outputs:
    - stdout: success message with resolved step count.
    - stderr: config, asset-resolution, or parser validation errors.
    - return code: `0` on valid pipeline, `1` on failure.
    """
    pipeline_path = Path(args.pipeline)
    global_vars = {}
    selected_global_config: Path | None = None
    try:
        selected_global_config = resolve_global_config_path(
            Path(args.global_config) if args.global_config else None
        )
    except ConfigError as exc:
        print(f"Global config error: {exc}", file=sys.stderr)
        return 1
    if selected_global_config:
        try:
            global_vars = load_global_config(selected_global_config)
        except ConfigError as exc:
            print(f"Global config error: {exc}", file=sys.stderr)
            return 1
        args.global_config = str(selected_global_config)
    selected_projects_config: Path | None = None
    try:
        selected_projects_config = resolve_projects_config_path(
            Path(args.projects_config) if getattr(args, "projects_config", None) else None
        )
    except ProjectConfigError as exc:
        print(f"Projects config error: {exc}", file=sys.stderr)
        return 1
    try:
        tentative_project_id = resolve_project_id(
            explicit_project_id=getattr(args, "project_id", None),
            pipeline_project_id=None,
            pipeline_path=pipeline_path,
        )
        tentative_project_vars = load_project_vars(
            project_id=tentative_project_id,
            projects_config_path=selected_projects_config,
        )
        pipeline_path = resolve_pipeline_path_from_project_sources(
            pipeline_path,
            project_vars=tentative_project_vars,
            repo_root=Path(".").resolve(),
        )
        pre = parse_pipeline(pipeline_path, global_vars=global_vars, env_vars={})
        project_id = resolve_project_id(
            explicit_project_id=getattr(args, "project_id", None),
            pipeline_project_id=getattr(pre, "project_id", None),
            pipeline_path=pipeline_path,
        )
        project_vars = load_project_vars(project_id=project_id, projects_config_path=selected_projects_config)
        parse_kwargs = {"global_vars": global_vars, "env_vars": {}}
        if project_vars:
            parse_kwargs["project_vars"] = project_vars
        pipeline = parse_pipeline(pipeline_path, **parse_kwargs)
    except (PipelineError, FileNotFoundError) as exc:
        print(f"Invalid pipeline: {exc}", file=sys.stderr)
        return 1
    except ProjectConfigError as exc:
        print(f"Projects config error: {exc}", file=sys.stderr)
        return 1
    except PipelineAssetError as exc:
        print(f"Pipeline asset resolution error: {exc}", file=sys.stderr)
        return 1
    print(f"Pipeline is valid. Steps: {len(pipeline.steps)}")
    return 0

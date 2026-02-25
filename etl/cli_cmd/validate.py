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
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict

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
from etl.variable_solver import VariableSolver


@dataclass(frozen=True)
class ValidateVariableContext:
    """Solver-backed variable context used during validation parsing."""

    solver: VariableSolver
    parse_context_vars: Dict[str, Any]

    def global_vars(self) -> Dict[str, Any]:
        return dict(self.solver.get("global", {}, resolve=False) or {})

    def env_vars(self) -> Dict[str, Any]:
        return dict(self.solver.get("env", {}, resolve=False) or {})


def _resolve_solver_max_passes(*, global_vars: Dict[str, Any], env_vars: Dict[str, Any], default: int = 20) -> int:
    raw = env_vars.get("resolve_max_passes", global_vars.get("resolve_max_passes", default))
    try:
        value = int(raw)
    except Exception:
        value = default
    return max(1, min(100, value))


def _build_validate_variable_context(
    *,
    base_solver: VariableSolver | None,
    global_vars: Dict[str, Any],
    env_vars: Dict[str, Any],
    parse_context_vars: Dict[str, Any],
) -> ValidateVariableContext:
    max_passes = _resolve_solver_max_passes(global_vars=global_vars, env_vars=env_vars)
    if base_solver is not None:
        solver = VariableSolver(max_passes=max_passes, initial=base_solver.context())
    else:
        solver = VariableSolver(max_passes=max_passes)
    solver.overlay("global", global_vars or {}, add_namespace=True, add_flat=True)
    solver.overlay("globals", global_vars or {}, add_namespace=True, add_flat=False)
    solver.overlay("env", env_vars or {}, add_namespace=True, add_flat=True)
    solver.overlay("context", parse_context_vars or {}, add_namespace=True, add_flat=False)
    return ValidateVariableContext(
        solver=solver,
        parse_context_vars=dict(parse_context_vars or {}),
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
    base_ctx = getattr(args, "runtime_context", None)
    base_target_solver: VariableSolver | None = None
    if base_ctx is not None:
        try:
            base_target_solver = base_ctx.solver("target")
        except Exception:
            base_target_solver = None
    pipeline_path = Path(args.pipeline)
    global_vars = dict(getattr(base_ctx, "global_vars", {}) or {})
    env_vars = dict(getattr(base_ctx, "exec_env", {}) or {})
    parse_context_vars = dict(getattr(base_ctx, "parse_context_vars", {}) or {})
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
    vars_ctx = _build_validate_variable_context(
        base_solver=base_target_solver,
        global_vars=global_vars,
        env_vars=env_vars,
        parse_context_vars=parse_context_vars,
    )
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
        pre = parse_pipeline(
            pipeline_path,
            global_vars=vars_ctx.global_vars(),
            env_vars=vars_ctx.env_vars(),
            context_vars=vars_ctx.parse_context_vars,
        )
        project_id = resolve_project_id(
            explicit_project_id=getattr(args, "project_id", None),
            pipeline_project_id=getattr(pre, "project_id", None),
            pipeline_path=pipeline_path,
        )
        project_vars = load_project_vars(project_id=project_id, projects_config_path=selected_projects_config)
        parse_kwargs = {
            "global_vars": vars_ctx.global_vars(),
            "env_vars": vars_ctx.env_vars(),
            "context_vars": vars_ctx.parse_context_vars,
        }
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

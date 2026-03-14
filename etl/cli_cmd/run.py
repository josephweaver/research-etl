# research-etl
# Copyright (c) 2026 Joseph Weaver
# This file is part of the research-etl project and is licensed under the MIT License.
# You may not use this file except in compliance with the License.
# See https://github.com/josephweaver/research-etl for details.

from __future__ import annotations
"""Run CLI command group.

High-level idea:
- Orchestrate end-to-end pipeline execution from CLI input through executor submit.
- Handles config resolution, dependency pre-runs, execution-source policy, and submit/status reporting.
"""

import argparse
import subprocess
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, List, Set

from etl.cli_cmd.common import CommandHandler, emit_error_with_report, resolve_workdir_from_solver
from etl.executors import HpccDirectExecutor, LocalExecutor, SlurmExecutor
from etl.executors.slurm import SlurmSubmitError
from etl.pipeline import PipelineError, parse_pipeline
from etl.projects import (
    ProjectConfigError,
    load_project_vars,
    resolve_project_id,
)
from etl.source_control import merge_source_commandline_vars
from etl.provenance import collect_run_provenance
from etl.runtime_context import (
    apply_db_mode_from_exec_env,
    collect_secret_vars,
    merge_context_with_secrets,
    parse_cli_var_overrides,
)
from etl.tracking import load_runs, load_latest_run_context_snapshot
from etl.variable_solver import VariableSolver


@dataclass(frozen=True)
class RunVariableContext:
    """Solver-backed variable context used during run/dependency submission."""

    solver: VariableSolver
    parse_context_vars: Dict[str, Any]

    def global_vars(self) -> Dict[str, Any]:
        return dict(self.solver.get("global", {}, resolve=False) or {})

    def env_vars(self) -> Dict[str, Any]:
        return dict(self.solver.get("env", {}, resolve=False) or {})

    def commandline_vars(self) -> Dict[str, Any]:
        return dict(self.solver.get("commandline", {}, resolve=False) or {})


@dataclass(frozen=True)
class PipelineResolveContext:
    """Shared parse context for dependency graph resolution."""

    vars_ctx: RunVariableContext


def register_run_args(
    subparsers: argparse._SubParsersAction,
    *,
    cmd_run: CommandHandler,
) -> None:
    # CLI inputs: full runtime/executor/config/retry/source options for `etl run`.
    # CLI output: registers and binds the run command parser.
    p_run = subparsers.add_parser("run", help="Run a pipeline")
    p_run.add_argument("pipeline", help="Path to pipeline YAML")
    p_run.add_argument("--global-config", help="Path to global config YAML", default=None)
    p_run.add_argument(
        "--projects-config",
        help="Path to project vars config YAML (default: config/projects.yml when present)",
        default=None,
    )
    p_run.add_argument(
        "--environments-config",
        help="Path to environments config YAML (default: config/environments.yml)",
        default=None,
    )
    p_run.add_argument("--env", help="Execution environment name (from environments config)", default=None)
    p_run.add_argument("--project-id", default=None, help="Project partition id override (default inferred from pipeline metadata/path).")
    p_run.add_argument("--plugins-dir", default="plugins", help="Directory containing plugins")
    p_run.add_argument("--workdir", default=None, help="Directory to store run artifacts (overrides pipeline/env/global workdir).")
    p_run.add_argument(
        "--var",
        action="append",
        default=[],
        help="Runtime variable override in KEY=VALUE form (repeatable; highest precedence).",
    )
    p_run.add_argument("--dry-run", action="store_true", help="Parse and plan without executing plugins")
    p_run.add_argument("--max-retries", type=int, default=None, help="Max retries per step after first failure")
    p_run.add_argument("--retry-delay-seconds", type=float, default=None, help="Delay between retries in seconds")
    p_run.add_argument("--resume-run-id", default=None, help="Resume by skipping steps that succeeded in a prior run_id")
    p_run.add_argument(
        "--state-run-id",
        default=None,
        help="Load latest persisted context snapshot from a prior run_id and seed current execution state.",
    )
    p_run.add_argument(
        "--step-indices",
        default=None,
        help="Comma-separated step indices to execute (0-based).",
    )
    p_run.add_argument(
        "--ignore-dependencies",
        action="store_true",
        help="Skip requires_pipelines resolution/execution (useful for faster local iteration).",
    )
    p_run.add_argument(
        "--executor",
        choices=["local", "slurm", "hpcc_direct"],
        default=None,
        help="DEPRECATED: execution backend override. Preferred: use --env and set executor in environments.yml.",
    )
    p_run.add_argument("--allow-dirty-git", action="store_true", help="Allow strict git checkout runs from a dirty local worktree")
    p_run.add_argument(
        "--execution-source",
        choices=["auto", "git_remote", "git_bundle", "snapshot", "workspace"],
        default=None,
        help="Source mode for run code checkout (default from environments config or auto).",
    )
    p_run.add_argument("--source-bundle", default=None, help="Path to git bundle file for offline checkout fallback.")
    p_run.add_argument("--source-snapshot", default=None, help="Path to tar/zip snapshot for offline checkout fallback.")
    p_run.add_argument(
        "--allow-workspace-source",
        action="store_true",
        help="Allow execution_source=workspace (runs directly from existing repo path).",
    )
    p_run.add_argument("--verbose", action="store_true", help="Verbose executor output (e.g., show SSH commands)")
    p_run.set_defaults(func=cmd_run)


def _run_store_path(workdir: str) -> Path:
    return Path(workdir) / "runs.jsonl"


def _resolve_solver_max_passes(*, global_vars: Dict[str, Any], env_vars: Dict[str, Any], default: int = 20) -> int:
    raw = env_vars.get("resolve_max_passes", global_vars.get("resolve_max_passes", default))
    try:
        value = int(raw)
    except Exception:
        value = default
    return max(1, min(100, value))


def _build_run_variable_context(
    *,
    base_solver: VariableSolver | None,
    global_vars: Dict[str, Any],
    env_vars: Dict[str, Any],
    commandline_vars: Dict[str, Any],
    parse_context_vars: Dict[str, Any],
) -> RunVariableContext:
    max_passes = _resolve_solver_max_passes(global_vars=global_vars, env_vars=env_vars)
    if base_solver is not None:
        solver = VariableSolver(max_passes=max_passes, initial=base_solver.context())
    else:
        solver = VariableSolver(max_passes=max_passes)
    solver.overlay("global", global_vars or {}, add_namespace=True, add_flat=True)
    solver.overlay("globals", global_vars or {}, add_namespace=True, add_flat=False)
    solver.overlay("env", env_vars or {}, add_namespace=True, add_flat=True)
    solver.overlay("commandline", commandline_vars or {}, add_namespace=True, add_flat=True)
    solver.overlay("context", parse_context_vars or {}, add_namespace=True, add_flat=False)
    return RunVariableContext(
        solver=solver,
        parse_context_vars=dict(parse_context_vars or {}),
    )


def _resolve_workdir_with_solver(
    *,
    base_solver: VariableSolver | None,
    pipeline,
    project_vars: Dict[str, Any],
) -> str:
    """Resolve workdir precedence through VariableSolver overlays."""
    max_passes = max(1, int(getattr(pipeline, "resolve_max_passes", 20) or 20))
    if base_solver is not None:
        # Start from entrypoint scope state and layer pipeline/run specifics.
        solver = VariableSolver(max_passes=max_passes, initial=base_solver.context())
    else:
        solver = VariableSolver(max_passes=max_passes)
    solver.overlay("project", project_vars or {}, add_namespace=True, add_flat=True)
    solver.overlay("pipe", dict(getattr(pipeline, "vars", {}) or {}), add_namespace=True, add_flat=True)
    solver.overlay("dirs", dict(getattr(pipeline, "dirs", {}) or {}), add_namespace=True, add_flat=True)
    return resolve_workdir_from_solver(
        solver=solver,
        pipeline_workdir=str(getattr(pipeline, "workdir", "") or ""),
        fallback=".runs",
    )


def _has_successful_run_for_pipeline(pipeline_path: Path, *, workdir: str) -> bool:
    records = load_runs(_run_store_path(workdir))
    target = pipeline_path.resolve().as_posix()
    for rec in reversed(records):
        try:
            rec_path = Path(rec.pipeline).resolve().as_posix()
        except Exception:
            rec_path = Path(rec.pipeline).as_posix()
        if rec.success and rec_path == target:
            return True
    return False


def _resolve_required_pipelines(
    pipeline_path: Path,
    *,
    ctx: PipelineResolveContext,
    stack: List[Path],
    visiting: Set[str],
    ordered: List[Path],
) -> None:
    canonical = pipeline_path.resolve()
    key = canonical.as_posix()
    if key in visiting:
        cycle = " -> ".join([p.as_posix() for p in stack] + [canonical.as_posix()])
        raise PipelineError(f"Pipeline dependency cycle detected: {cycle}")
    if any(p.resolve() == canonical for p in stack):
        return

    visiting.add(key)
    stack.append(canonical)
    pipeline = parse_pipeline(
        canonical,
        global_vars=ctx.vars_ctx.global_vars(),
        env_vars=ctx.vars_ctx.env_vars(),
        context_vars=ctx.vars_ctx.parse_context_vars,
    )
    base_dir = canonical.parent
    for req in pipeline.requires_pipelines:
        req_path = Path(req)
        if not req_path.is_absolute():
            from_cwd = req_path.resolve()
            if from_cwd.exists():
                req_path = from_cwd
            else:
                req_path = (base_dir / req_path).resolve()
        _resolve_required_pipelines(
            req_path,
            ctx=ctx,
            stack=stack,
            visiting=visiting,
            ordered=ordered,
        )
        if all(p.resolve() != req_path.resolve() for p in ordered):
            ordered.append(req_path.resolve())
    stack.pop()
    visiting.remove(key)


def _submit_pipeline_run(
    args: argparse.Namespace,
    *,
    pipeline_path: Path,
    vars_ctx: RunVariableContext,
    resume_run_id: str | None,
) -> int:
    plugins_dir_path = Path(args.plugins_dir)
    repo_root = Path(".").resolve()
    try:
        pipeline_path.resolve().relative_to(repo_root)
        pipeline_inside_repo = True
    except ValueError:
        pipeline_inside_repo = False
    global_vars = vars_ctx.global_vars()
    exec_env = vars_ctx.env_vars()
    commandline_vars = vars_ctx.commandline_vars()
    parse_context_vars = vars_ctx.parse_context_vars
    try:
        pre_pipeline = parse_pipeline(
            pipeline_path,
            global_vars=global_vars,
            env_vars=exec_env,
            context_vars=parse_context_vars,
        )
    except (PipelineError, FileNotFoundError) as exc:
        print(f"Invalid pipeline: {exc}", file=sys.stderr)
        return 1
    project_id = resolve_project_id(
        explicit_project_id=args.project_id,
        pipeline_project_id=getattr(pre_pipeline, "project_id", None),
        pipeline_path=pipeline_path,
    )
    project_vars = {}
    if getattr(args, "projects_config", None):
        try:
            project_vars = load_project_vars(
                project_id=project_id,
                projects_config_path=Path(args.projects_config),
            )
        except ProjectConfigError as exc:
            print(f"Projects config error: {exc}", file=sys.stderr)
            return 1
    try:
        parse_kwargs = {
            "global_vars": global_vars,
            "env_vars": exec_env,
            "context_vars": parse_context_vars,
        }
        if project_vars:
            parse_kwargs["project_vars"] = project_vars
        pipeline = parse_pipeline(pipeline_path, **parse_kwargs)
    except (PipelineError, FileNotFoundError) as exc:
        print(f"Invalid pipeline: {exc}", file=sys.stderr)
        return 1
    resolved_workdir = _resolve_workdir_with_solver(
        base_solver=vars_ctx.solver,
        pipeline=pipeline,
        project_vars=project_vars,
    )
    cli_argv = list(getattr(args, "_raw_argv", []))
    if cli_argv:
        try:
            idx = cli_argv.index(args.pipeline)
            cli_argv[idx] = str(pipeline_path)
        except ValueError:
            pass
    cli_command = "python cli.py " + " ".join(cli_argv)
    provenance = collect_run_provenance(
        repo_root=repo_root,
        pipeline_path=pipeline_path,
        global_config_path=Path(args.global_config) if args.global_config else None,
        environments_config_path=Path(args.environments_config) if args.environments_config else None,
        plugin_dir=plugins_dir_path,
        pipeline=pipeline,
        cli_command=cli_command,
    )
    commandline_vars = merge_source_commandline_vars(
        commandline_vars,
        repo_root=repo_root,
        project_vars=project_vars,
        provenance=provenance,
    )
    execution_source = args.execution_source or exec_env.get("execution_source", "auto")
    source_bundle = args.source_bundle or exec_env.get("source_bundle")
    source_snapshot = args.source_snapshot or exec_env.get("source_snapshot")
    allow_workspace_source = bool(args.allow_workspace_source or exec_env.get("allow_workspace_source", False))
    if args.executor in {"slurm", "hpcc_direct"} and not pipeline_inside_repo:
        print(
            "External pipeline asset paths are currently supported for local executor only. "
            "Use --executor local for external repos, or place pipeline inside this repo for remote executors.",
            file=sys.stderr,
        )
        return 1
    if args.executor == "slurm":
        executor = SlurmExecutor(
            env_config=exec_env,
            repo_root=repo_root,
            plugins_dir=plugins_dir_path,
            workdir=Path(resolved_workdir),
            global_config=Path(args.global_config) if args.global_config else None,
            projects_config=Path(args.projects_config) if getattr(args, "projects_config", None) else None,
            environments_config=Path(args.environments_config) if args.environments_config else None,
            env_name=args.env,
            dry_run=args.dry_run,
            verbose=args.verbose,
            enforce_git_checkout=True,
            require_clean_git=not bool(args.allow_dirty_git),
            execution_source=execution_source,
            source_bundle=source_bundle,
            source_snapshot=source_snapshot,
            allow_workspace_source=allow_workspace_source,
        )
    elif args.executor == "hpcc_direct":
        executor = HpccDirectExecutor(
            env_config=exec_env,
            repo_root=repo_root,
            plugins_dir=plugins_dir_path,
            workdir=Path(resolved_workdir),
            global_config=Path(args.global_config) if args.global_config else None,
            projects_config=Path(args.projects_config) if getattr(args, "projects_config", None) else None,
            environments_config=Path(args.environments_config) if args.environments_config else None,
            env_name=args.env,
            dry_run=args.dry_run,
            verbose=args.verbose,
        )
    else:
        executor = LocalExecutor(
            plugin_dir=plugins_dir_path,
            workdir=Path(resolved_workdir),
            dry_run=args.dry_run,
            max_retries=int(exec_env.get("step_max_retries", 0)),
            retry_delay_seconds=float(exec_env.get("step_retry_delay_seconds", 0.0)),
            enforce_git_checkout=True,
            require_clean_git=not bool(args.allow_dirty_git),
            execution_source=execution_source,
            source_bundle=source_bundle,
            source_snapshot=source_snapshot,
            allow_workspace_source=allow_workspace_source,
        )

    try:
        result = executor.submit(
            str(pipeline_path),
            context={
                "pipeline": pipeline,
                "execution_env": exec_env,
                "resume_run_id": resume_run_id,
                "provenance": provenance,
                "repo_root": repo_root,
                "global_vars": global_vars,
                "project_vars": project_vars,
                "commandline_vars": commandline_vars,
                "execution_source": execution_source,
                "source_bundle": source_bundle,
                "source_snapshot": source_snapshot,
                "allow_workspace_source": allow_workspace_source,
                "allow_dirty_git": bool(args.allow_dirty_git),
                "project_id": project_id,
                "step_indices": str(args.step_indices or "").strip() or None,
                "seed_context": (
                    load_latest_run_context_snapshot(str(args.state_run_id))
                    if str(args.state_run_id or "").strip()
                    else {}
                ),
            },
        )
    except Exception as exc:  # noqa: BLE001
        emit_error_with_report(_format_run_submission_error(exc, args), exc, args)
        return 1
    status = executor.status(result.run_id)
    job_info = f" (jobs: {result.job_ids})" if getattr(result, "job_ids", None) else ""
    print(f"Run {result.run_id} -> {status.state}{job_info} [{pipeline_path}]")
    status_msg = str(getattr(status, "message", "") or "").strip()
    if status_msg and status.state not in ("succeeded", "queued", "running"):
        print(status_msg, file=sys.stderr)
    if status.state in ("succeeded", "queued", "running"):
        return 0
    return 1


def _format_run_submission_error(exc: Exception, args: argparse.Namespace) -> str:
    text = str(exc).strip()
    if "ETL_DATABASE_URL is required for --resume-run-id" in text:
        return (
            "Resume failed: --resume-run-id requires ETL_DATABASE_URL so prior step state can be loaded from DB. "
            "Set ETL_DATABASE_URL (and reopen your shell on Windows if you used setx), or run without --resume-run-id."
        )
    if isinstance(exc, SlurmSubmitError):
        if "Could not initialize remote DB secret" in text:
            return (
                "SLURM submission failed while preparing remote DB secret. "
                "Set local ETL_DATABASE_URL or ensure remote ~/.secrets/etl contains "
                "'export ETL_DATABASE_URL=...'."
            )
        return (
            f"SLURM submission failed: {text} "
            "Check SSH connectivity, remote paths, and that sbatch is available on the login host."
        )
    if "hpcc_direct executor requires" in text:
        return (
            f"HPCC direct submission failed: {text} "
            "Set execution env ssh_host (and optionally ssh_user/ssh_jump/remote_repo)."
        )
    if isinstance(exc, FileNotFoundError):
        missing = getattr(exc, "filename", None) or text
        if "ssh" in str(missing).lower():
            return "Required command 'ssh' was not found. Install OpenSSH client and ensure it is on PATH."
        if "scp" in str(missing).lower():
            return "Required command 'scp' was not found. Install OpenSSH client and ensure it is on PATH."
        if "sbatch" in str(missing).lower():
            return (
                "Required command 'sbatch' was not found. For local SLURM mode, run on a host with SLURM client tools; "
                "for remote SLURM mode, verify ssh_host and remote sbatch availability."
            )
        return f"Required command not found: {missing}"
    if isinstance(exc, subprocess.TimeoutExpired):
        return (
            f"Operation timed out while executing '{exc.cmd}'. "
            "Increase ssh_timeout/scp_timeout in environments config or check cluster/login-node responsiveness."
        )
    return f"Run failed before submission/execution: {text}"


def cmd_run(args: argparse.Namespace) -> int:
    """Resolve runtime context and submit a pipeline through the selected executor.

    Inputs:
    - pipeline path and optional global/projects/environments config.
    - executor selection/options, retry controls, source controls, variable overrides.
    - dependency behavior (`--ignore-dependencies`) and resume run id.

    Outputs:
    - stdout: dependency progress and final submit/status line.
    - stderr: validation/config/submission failures with actionable messages.
    - side effects: optional dependency runs + executor job submission.
    - return code: `0` when submitted/running/succeeded, `1` on failure.
    """
    base_ctx = getattr(args, "runtime_context", None)
    if base_ctx is None:
        print("Runtime context error: missing runtime_context for `run` command.", file=sys.stderr)
        return 1
    try:
        base_target_solver = base_ctx.solver("target")
    except Exception:
        print("Runtime context error: missing target variable solver.", file=sys.stderr)
        return 1
    global_vars = dict(getattr(base_ctx, "global_vars", {}) or {})
    exec_env = dict(getattr(base_ctx, "exec_env", {}) or {})
    if not exec_env:
        print(
            "Runtime context error: execution environment was not resolved. "
            "Provide --env (or ensure default local env exists).",
            file=sys.stderr,
        )
        return 1
    selected_env_name = str(getattr(base_ctx, "env_name", "") or "").strip() or None
    if not selected_env_name:
        print("Runtime context error: environment name is not set.", file=sys.stderr)
        return 1
    env_executor = str(exec_env.get("executor") or "").strip().lower()
    if not env_executor:
        print("Runtime context error: selected environment does not define `executor`.", file=sys.stderr)
        return 1
    selected_executor = env_executor
    if getattr(base_ctx, "global_config_path", None):
        args.global_config = str(base_ctx.global_config_path)
    if getattr(base_ctx, "projects_config_path", None):
        args.projects_config = str(base_ctx.projects_config_path)
    if getattr(base_ctx, "environments_config_path", None):
        args.environments_config = str(base_ctx.environments_config_path)
    args.env = selected_env_name
    if args.executor:
        print(
            "[etl][WARN] `--executor` is deprecated and ignored; "
            "set executor in the selected `--env` configuration.",
            file=sys.stderr,
        )
    args.executor = selected_executor
    max_retries = args.max_retries if args.max_retries is not None else int(exec_env.get("step_max_retries", 0) or 0)
    retry_delay_seconds = (
        args.retry_delay_seconds
        if args.retry_delay_seconds is not None
        else float(exec_env.get("step_retry_delay_seconds", 0.0) or 0.0)
    )
    if max_retries < 0:
        print("Invalid --max-retries: must be >= 0.", file=sys.stderr)
        return 1
    if retry_delay_seconds < 0:
        print("Invalid --retry-delay-seconds: must be >= 0.", file=sys.stderr)
        return 1
    exec_env["step_max_retries"] = max_retries
    exec_env["step_retry_delay_seconds"] = retry_delay_seconds
    apply_db_mode_from_exec_env(exec_env)
    commandline_vars = dict(getattr(base_ctx, "commandline_vars", {}) or {})
    try:
        parsed_cli_vars = parse_cli_var_overrides(getattr(args, "var", None))
        commandline_vars.update(parsed_cli_vars)
    except Exception as exc:
        print(str(exc), file=sys.stderr)
        return 1
    if args.workdir:
        commandline_vars["workdir"] = str(args.workdir)
    parse_context_vars = merge_context_with_secrets(commandline_vars, collect_secret_vars(exec_env))
    run_vars_ctx = _build_run_variable_context(
        base_solver=base_target_solver,
        global_vars=global_vars,
        env_vars=exec_env,
        commandline_vars=commandline_vars,
        parse_context_vars=parse_context_vars,
    )
    pipeline_path = Path(getattr(base_ctx, "pipeline_path", None) or args.pipeline)
    if not pipeline_path.exists():
        print(f"Pipeline path not found: {pipeline_path}", file=sys.stderr)
        return 1
    if bool(getattr(args, "ignore_dependencies", False)):
        print("Ignoring pipeline dependencies (--ignore-dependencies).")
        return _submit_pipeline_run(
            args,
            pipeline_path=pipeline_path.resolve(),
            vars_ctx=run_vars_ctx,
            resume_run_id=args.resume_run_id,
        )
    try:
        dep_ctx = PipelineResolveContext(
            vars_ctx=run_vars_ctx,
        )
        ordered_reqs: List[Path] = []
        _resolve_required_pipelines(
            pipeline_path.resolve(),
            ctx=dep_ctx,
            stack=[],
            visiting=set(),
            ordered=ordered_reqs,
        )
    except (PipelineError, FileNotFoundError) as exc:
        print(f"Invalid pipeline: {exc}", file=sys.stderr)
        return 1

    target = pipeline_path.resolve()
    for dep in ordered_reqs:
        if dep.resolve() == target:
            continue
        try:
            dep_pipeline = parse_pipeline(
                dep,
                global_vars=run_vars_ctx.global_vars(),
                env_vars=run_vars_ctx.env_vars(),
                context_vars=run_vars_ctx.parse_context_vars,
            )
        except (PipelineError, FileNotFoundError) as exc:
            print(f"Invalid pipeline: {exc}", file=sys.stderr)
            return 1
        dep_workdir = _resolve_workdir_with_solver(
            base_solver=run_vars_ctx.solver,
            pipeline=dep_pipeline,
            project_vars={},
        )
        if _has_successful_run_for_pipeline(dep, workdir=dep_workdir):
            print(f"Dependency already satisfied: {dep}")
            continue
        print(f"Running missing dependency pipeline: {dep}")
        dep_rc = _submit_pipeline_run(
            args,
            pipeline_path=dep,
            vars_ctx=run_vars_ctx,
            resume_run_id=None,
        )
        if dep_rc != 0:
            print(f"Dependency pipeline failed: {dep}", file=sys.stderr)
            return dep_rc

    return _submit_pipeline_run(
        args,
        pipeline_path=target,
        vars_ctx=run_vars_ctx,
        resume_run_id=args.resume_run_id,
    )

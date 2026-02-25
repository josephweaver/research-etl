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
import os
import subprocess
import sys
from pathlib import Path
from typing import Any, Dict, List, Set

from etl.cli_cmd.common import CommandHandler, emit_error_with_report
from etl.config import ConfigError, load_global_config, resolve_global_config_path
from etl.execution_config import (
    ExecutionConfigError,
    apply_execution_env_overrides,
    load_execution_config,
    resolve_execution_config_path,
    resolve_execution_env_templates,
    validate_environment_executor,
)
from etl.executors import HpccDirectExecutor, LocalExecutor, SlurmExecutor
from etl.executors.slurm import SlurmSubmitError
from etl.pipeline import PipelineError, parse_pipeline
from etl.pipeline_assets import PipelineAssetError, resolve_pipeline_path_from_project_sources
from etl.projects import (
    ProjectConfigError,
    load_project_vars,
    resolve_project_id,
    resolve_projects_config_path,
)
from etl.provenance import collect_run_provenance
from etl.tracking import load_runs

DEFAULT_SECRET_ENV_KEYS = ("ETL_DATABASE_URL", "OPENAI_API_KEY", "GITHUB_TOKEN")


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


def _first_non_empty_text(*values: Any) -> str | None:
    for value in values:
        text = str(value or "").strip()
        if text:
            return text
    return None


def _resolve_workdir(
    *,
    cli_workdir: str | None,
    commandline_vars: Dict[str, Any] | None = None,
    pipeline,
    env_vars: Dict[str, Any],
    global_vars: Dict[str, Any],
) -> str:
    resolved = _first_non_empty_text(
        cli_workdir,
        (commandline_vars or {}).get("workdir"),
        getattr(pipeline, "workdir", None),
        env_vars.get("workdir"),
        global_vars.get("workdir"),
    )
    return resolved or ".runs"


def _assign_dotted_path(target: Dict[str, Any], dotted_key: str, value: Any) -> None:
    parts = [p.strip() for p in str(dotted_key or "").split(".")]
    if not parts or any(not p for p in parts):
        raise ValueError(f"Invalid --var key: '{dotted_key}'")
    cur: Dict[str, Any] = target
    for part in parts[:-1]:
        nxt = cur.get(part)
        if not isinstance(nxt, dict):
            nxt = {}
            cur[part] = nxt
        cur = nxt
    cur[parts[-1]] = value


def _parse_cli_var_overrides(entries: List[str] | None) -> Dict[str, Any]:
    out: Dict[str, Any] = {}
    for raw in list(entries or []):
        text = str(raw or "").strip()
        if not text:
            continue
        if "=" not in text:
            raise ValueError(f"Invalid --var '{text}': expected KEY=VALUE")
        key, value = text.split("=", 1)
        key_text = key.strip()
        if not key_text:
            raise ValueError(f"Invalid --var '{text}': key may not be empty")
        _assign_dotted_path(out, key_text, value)
    return out


def _parse_secret_env_keys(exec_env: Dict[str, Any]) -> List[str]:
    raw = exec_env.get("secret_env_keys")
    items: List[str]
    if isinstance(raw, (list, tuple, set)):
        items = [str(x).strip() for x in raw]
    elif raw is None:
        env_raw = str(os.environ.get("ETL_SECRET_ENV_KEYS") or "").strip()
        if env_raw:
            items = [x.strip() for x in env_raw.replace(";", ",").split(",")]
        else:
            items = list(DEFAULT_SECRET_ENV_KEYS)
    else:
        items = [x.strip() for x in str(raw).replace(";", ",").split(",")]
    out: List[str] = []
    seen: Set[str] = set()
    for key in items:
        if not key:
            continue
        if key in seen:
            continue
        seen.add(key)
        out.append(key)
    return out


def _collect_secret_vars(exec_env: Dict[str, Any]) -> Dict[str, str]:
    out: Dict[str, str] = {}
    for key in _parse_secret_env_keys(exec_env):
        val = os.environ.get(key)
        if val is None:
            continue
        text = str(val).strip()
        if not text:
            continue
        out[key] = text
    return out


def _merge_context_with_secrets(context_vars: Dict[str, Any], secret_vars: Dict[str, str]) -> Dict[str, Any]:
    merged = dict(context_vars or {})
    if not secret_vars:
        return merged
    secret_ns: Dict[str, Any] = dict(secret_vars)
    existing = merged.get("secret")
    if isinstance(existing, dict):
        secret_ns.update({str(k): v for k, v in existing.items()})
    merged["secret"] = secret_ns
    return merged


def _apply_db_mode_from_exec_env(exec_env: Dict[str, Any]) -> None:
    mode = str(exec_env.get("db_mode") or "").strip()
    if mode:
        os.environ["ETL_DB_MODE"] = mode
    verbose = exec_env.get("db_verbose")
    if verbose is not None:
        if bool(verbose):
            os.environ["ETL_DB_VERBOSE"] = "1"
        else:
            os.environ["ETL_DB_VERBOSE"] = "0"


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
    global_vars: Dict[str, Any],
    env_vars: Dict[str, Any],
    context_vars: Dict[str, Any] | None,
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
    pipeline = parse_pipeline(canonical, global_vars=global_vars, env_vars=env_vars, context_vars=context_vars)
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
            global_vars=global_vars,
            env_vars=env_vars,
            context_vars=context_vars,
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
    global_vars: Dict[str, Any],
    exec_env: Dict[str, Any],
    commandline_vars: Dict[str, Any],
    resume_run_id: str | None,
) -> int:
    plugins_dir_path = Path(args.plugins_dir)
    repo_root = Path(".").resolve()
    try:
        pipeline_path.resolve().relative_to(repo_root)
        pipeline_inside_repo = True
    except ValueError:
        pipeline_inside_repo = False
    parse_context_vars = _merge_context_with_secrets(commandline_vars, _collect_secret_vars(exec_env))
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
    resolved_workdir = _resolve_workdir(
        cli_workdir=None,
        commandline_vars=commandline_vars,
        pipeline=pipeline,
        env_vars=exec_env,
        global_vars=global_vars,
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
    pipeline_path = Path(args.pipeline)
    global_vars = {}
    exec_env = {}
    selected_global_config: Path | None = None
    selected_environments_config: Path | None = None
    selected_env_name: str | None = None
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
    if selected_projects_config:
        args.projects_config = str(selected_projects_config)
    try:
        selected_environments_config = resolve_execution_config_path(
            Path(args.environments_config) if args.environments_config else None
        )
    except ExecutionConfigError as exc:
        print(f"Environments config error: {exc}", file=sys.stderr)
        return 1
    if args.env:
        selected_env_name = args.env
    elif not args.executor:
        selected_env_name = "local"
    if selected_environments_config and selected_env_name:
        try:
            envs = load_execution_config(selected_environments_config)
            exec_env = envs.get(selected_env_name, {})
            if not exec_env:
                print(f"Execution env '{selected_env_name}' not found in config", file=sys.stderr)
                return 1
            validate_environment_executor(selected_env_name, exec_env, executor=args.executor)
            exec_env = apply_execution_env_overrides(exec_env)
            exec_env = resolve_execution_env_templates(exec_env, global_vars=global_vars)
        except ExecutionConfigError as exc:
            print(f"Environments config error: {exc}", file=sys.stderr)
            return 1
    if args.env and not selected_environments_config:
        print("Environments config error: `--env` was provided but no environments config was found.", file=sys.stderr)
        return 1
    if selected_environments_config:
        args.environments_config = str(selected_environments_config)
    if selected_env_name:
        args.env = selected_env_name
    env_executor = str(exec_env.get("executor") or "").strip().lower()
    selected_executor = env_executor or str(args.executor or "").strip().lower() or "local"
    if args.executor:
        if env_executor:
            print(
                "[etl][WARN] `--executor` is deprecated; using executor from `--env` configuration.",
                file=sys.stderr,
            )
        else:
            print(
                "[etl][WARN] `--executor` is deprecated; prefer setting/using `--env` with an executor.",
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
    _apply_db_mode_from_exec_env(exec_env)
    try:
        commandline_vars = _parse_cli_var_overrides(getattr(args, "var", None))
    except ValueError as exc:
        print(str(exc), file=sys.stderr)
        return 1
    if args.workdir:
        commandline_vars["workdir"] = str(args.workdir)
    parse_context_vars = _merge_context_with_secrets(commandline_vars, _collect_secret_vars(exec_env))
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
    except (ProjectConfigError, PipelineAssetError) as exc:
        print(f"Pipeline asset resolution error: {exc}", file=sys.stderr)
        return 1
    if not pipeline_path.exists():
        print(f"Pipeline path not found: {pipeline_path}", file=sys.stderr)
        return 1
    if bool(getattr(args, "ignore_dependencies", False)):
        print("Ignoring pipeline dependencies (--ignore-dependencies).")
        return _submit_pipeline_run(
            args,
            pipeline_path=pipeline_path.resolve(),
            global_vars=global_vars,
            exec_env=exec_env,
            commandline_vars=commandline_vars,
            resume_run_id=args.resume_run_id,
        )
    try:
        ordered_reqs: List[Path] = []
        _resolve_required_pipelines(
            pipeline_path.resolve(),
            global_vars=global_vars,
            env_vars=exec_env,
            context_vars=parse_context_vars,
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
                global_vars=global_vars,
                env_vars=exec_env,
                context_vars=parse_context_vars,
            )
        except (PipelineError, FileNotFoundError) as exc:
            print(f"Invalid pipeline: {exc}", file=sys.stderr)
            return 1
        dep_workdir = _resolve_workdir(
            cli_workdir=None,
            commandline_vars=commandline_vars,
            pipeline=dep_pipeline,
            env_vars=exec_env,
            global_vars=global_vars,
        )
        if _has_successful_run_for_pipeline(dep, workdir=dep_workdir):
            print(f"Dependency already satisfied: {dep}")
            continue
        print(f"Running missing dependency pipeline: {dep}")
        dep_rc = _submit_pipeline_run(
            args,
            pipeline_path=dep,
            global_vars=global_vars,
            exec_env=exec_env,
            commandline_vars=commandline_vars,
            resume_run_id=None,
        )
        if dep_rc != 0:
            print(f"Dependency pipeline failed: {dep}", file=sys.stderr)
            return dep_rc

    return _submit_pipeline_run(
        args,
        pipeline_path=target,
        global_vars=global_vars,
        exec_env=exec_env,
        commandline_vars=commandline_vars,
        resume_run_id=args.resume_run_id,
    )

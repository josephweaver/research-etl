"""
Lightweight CLI skeleton for the ETL tool.

Commands:
  - plugins list
  - run <pipeline.yml>
  - validate <pipeline.yml> (stub)

The implementation is intentionally minimal; real pipeline parsing and
execution wiring will be layered in later.
"""

from __future__ import annotations

import argparse
import json
import subprocess
import sys
import shlex
from pathlib import Path
from typing import Any, Dict, List, Set

from etl import __version__
from etl.ai_research import AIResearchError, generate_dataset_research
from etl.artifacts import (
    ArtifactPolicyError,
    enforce_artifact_policies,
    load_artifact_policy,
    resolve_artifact_policy_path,
)
from etl.config import load_global_config, resolve_global_config_path, ConfigError
from etl.db import ensure_database_schema, DatabaseError, get_database_url
from etl.db_sync_queue import apply_tracking_queue
from etl.dictionary_pr import DictionaryPRError, create_dictionary_pr
from etl.datasets import DatasetServiceError, get_data, get_dataset, list_datasets, store_data
from etl.diagnostics import write_error_report, find_latest_error_report
from etl.executors import LocalExecutor, SlurmExecutor
from etl.executors.slurm import SlurmSubmitError
from etl.pipeline import PipelineError, parse_pipeline
from etl.provenance import collect_run_provenance
from etl.projects import resolve_project_id
from etl.plugins.base import describe_plugin, discover_plugins, PluginLoadError
from etl.tracking import load_runs, find_run
from etl.execution_config import (
    load_execution_config,
    apply_execution_env_overrides,
    resolve_execution_env_templates,
    resolve_execution_config_path,
    validate_environment_executor,
    ExecutionConfigError,
)


DEFAULT_PLUGIN_DIR = Path("plugins")


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
        pipeline = parse_pipeline(
            pipeline_path,
            global_vars=global_vars,
            env_vars=exec_env,
            context_vars=commandline_vars,
        )
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
    project_id = resolve_project_id(
        explicit_project_id=args.project_id,
        pipeline_project_id=getattr(pipeline, "project_id", None),
        pipeline_path=pipeline_path,
    )

    if args.executor == "slurm":
        executor = SlurmExecutor(
            env_config=exec_env,
            repo_root=repo_root,
            plugins_dir=plugins_dir_path,
            workdir=Path(resolved_workdir),
            global_config=Path(args.global_config) if args.global_config else None,
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
                "commandline_vars": commandline_vars,
                "execution_source": execution_source,
                "source_bundle": source_bundle,
                "source_snapshot": source_snapshot,
                "allow_workspace_source": allow_workspace_source,
                "project_id": project_id,
            },
        )
    except Exception as exc:  # noqa: BLE001
        _emit_error_with_report(_format_run_submission_error(exc, args), exc, args)
        return 1
    status = executor.status(result.run_id)
    job_info = f" (jobs: {result.job_ids})" if getattr(result, "job_ids", None) else ""
    print(f"Run {result.run_id} -> {status.state}{job_info} [{pipeline_path}]")
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


def _format_database_init_error(exc: DatabaseError) -> str:
    text = str(exc).strip()
    if "not a valid URL" in text:
        return (
            "Database initialization error: ETL_DATABASE_URL is set but invalid. "
            "Use a full DSN like postgresql://user:pass@host:5432/dbname."
        )
    if "DDL directory not found" in text or "No .sql DDL scripts found" in text:
        return f"Database initialization error: {text} Check db/ddl migration files."
    return f"Database initialization error: {text}"


def _emit_error_with_report(message: str, exc: Exception, args: argparse.Namespace) -> None:
    print(message, file=sys.stderr)
    try:
        report_path = write_error_report(
            exc=exc,
            command="etl",
            argv=getattr(args, "_raw_argv", []) or [],
            workdir=Path(".runs"),
            repo_root=Path(".").resolve(),
        )
        print(f"Diagnostic report saved: {report_path}", file=sys.stderr)
    except Exception:
        # Keep user-facing failure path robust even if diagnostics fail.
        pass


def _auto_apply_local_db_queue() -> None:
    queue_dir = Path(".runs") / "db_sync_inbox" / "pending"
    if not queue_dir.exists():
        return
    if not get_database_url():
        return
    processed_dir = queue_dir.parent / "processed"
    summary = apply_tracking_queue(queue_dir, processed_dir=processed_dir)
    if summary.applied or summary.failed:
        print(
            f"DB queue sync: queued={summary.queued} applied={summary.applied} failed={summary.failed}",
            file=sys.stderr,
        )


def cmd_plugins_list(args: argparse.Namespace) -> int:
    directory = Path(args.directory or DEFAULT_PLUGIN_DIR)
    if not directory.exists():
        print(f"No plugin directory found at {directory}", file=sys.stderr)
        return 1
    try:
        plugins = discover_plugins(directory)
    except PluginLoadError as exc:
        print(f"Error loading plugins: {exc}", file=sys.stderr)
        return 1
    if not plugins:
        print("No plugins discovered.")
        return 0
    for p in plugins:
        info = describe_plugin(p)
        print(f"- {info['name']} ({info['version']}): {info['description']}")
    return 0


def cmd_run(args: argparse.Namespace) -> int:
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
    try:
        selected_environments_config = resolve_execution_config_path(
            Path(args.environments_config) if args.environments_config else None
        )
    except ExecutionConfigError as exc:
        print(f"Environments config error: {exc}", file=sys.stderr)
        return 1
    if args.env:
        selected_env_name = args.env
    elif args.executor == "local":
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
    # Ensure SLURM path inherits explicit CLI overrides even when environments config is used.
    exec_env["step_max_retries"] = max_retries
    exec_env["step_retry_delay_seconds"] = retry_delay_seconds
    try:
        commandline_vars = _parse_cli_var_overrides(getattr(args, "var", None))
    except ValueError as exc:
        print(str(exc), file=sys.stderr)
        return 1
    if args.workdir:
        # Keep explicit --workdir as the strongest CLI override, unified under one overlay.
        commandline_vars["workdir"] = str(args.workdir)
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
            context_vars=commandline_vars,
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
                context_vars=commandline_vars,
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


def cmd_validate(args: argparse.Namespace) -> int:
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
    try:
        pipeline = parse_pipeline(pipeline_path, global_vars=global_vars, env_vars={})
    except (PipelineError, FileNotFoundError) as exc:
        print(f"Invalid pipeline: {exc}", file=sys.stderr)
        return 1
    print(f"Pipeline is valid. Steps: {len(pipeline.steps)}")
    return 0


def cmd_runs_list(args: argparse.Namespace) -> int:
    records = load_runs(Path(args.store))
    if not records:
        print("No run records found.")
        return 0
    for rec in records[-args.num :]:
        print(f"{rec.run_id} | {rec.status} | {rec.pipeline} | {rec.started_at}")
    return 0


def cmd_runs_show(args: argparse.Namespace) -> int:
    rec = find_run(Path(args.store), args.run_id)
    if not rec:
        print(f"Run not found: {args.run_id}", file=sys.stderr)
        return 1
    print(f"Run: {rec.run_id}")
    print(f"Status: {rec.status} (success={rec.success})")
    print(f"Pipeline: {rec.pipeline}")
    print(f"Started: {rec.started_at}")
    print(f"Ended: {rec.ended_at}")
    print("Steps:")
    for step in rec.steps:
        status = "ok" if step["success"] else f"error: {step['error']}"
        print(f"  - {step['name']}: {status}")
    return 0


def cmd_datasets_list(args: argparse.Namespace) -> int:
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
    try:
        receipt = store_data(
            dataset_id=args.dataset_id,
            source_path=args.path,
            stage=args.stage,
            version_label=args.version,
            environment=args.environment,
            runtime_context=args.runtime_context,
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
    try:
        receipt = get_data(
            dataset_id=args.dataset_id,
            version=args.version,
            environment=args.environment,
            runtime_context=args.runtime_context,
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


def cmd_diagnostics_latest(args: argparse.Namespace) -> int:
    latest = find_latest_error_report(Path(args.workdir))
    if not latest:
        print(f"No diagnostic reports found under {Path(args.workdir) / 'error_reports'}.", file=sys.stderr)
        return 1
    print(latest)
    if args.show:
        try:
            print(latest.read_text(encoding="utf-8"))
        except Exception as exc:  # noqa: BLE001
            print(f"Failed to read diagnostic report: {exc}", file=sys.stderr)
            return 1
    return 0


def cmd_web(args: argparse.Namespace) -> int:
    try:
        import uvicorn  # type: ignore
    except ImportError:
        print(
            "Web UI requires uvicorn/fastapi. Install with: pip install \"research-etl[web]\"",
            file=sys.stderr,
        )
        return 1
    uvicorn.run("etl.web_api:app", host=args.host, port=args.port, reload=bool(args.reload))
    return 0


def cmd_artifacts_enforce(args: argparse.Namespace) -> int:
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
        _emit_error_with_report(f"Artifact enforcement failed: {exc}", exc, args)
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


def cmd_ai_research(args: argparse.Namespace) -> int:
    dataset_id = str(args.dataset_id or "").strip()
    if not dataset_id:
        print("AI research error: --dataset-id is required.", file=sys.stderr)
        return 1

    sample_text = None
    if args.sample_file:
        sample_path = Path(args.sample_file)
        if not sample_path.exists():
            print(f"AI research error: sample file not found: {sample_path}", file=sys.stderr)
            return 1
        sample_text = sample_path.read_text(encoding="utf-8", errors="replace")

    schema_text = None
    if args.schema_file:
        schema_path = Path(args.schema_file)
        if not schema_path.exists():
            print(f"AI research error: schema file not found: {schema_path}", file=sys.stderr)
            return 1
        schema_text = schema_path.read_text(encoding="utf-8", errors="replace")

    supplemental_urls: list[str] = []
    for url in list(args.supplemental_url or []):
        text = str(url or "").strip()
        if text:
            supplemental_urls.append(text)
    if args.supplemental_urls_file:
        urls_path = Path(args.supplemental_urls_file)
        if not urls_path.exists():
            print(f"AI research error: supplemental URLs file not found: {urls_path}", file=sys.stderr)
            return 1
        for line in urls_path.read_text(encoding="utf-8", errors="replace").splitlines():
            text = line.strip()
            if not text or text.startswith("#"):
                continue
            supplemental_urls.append(text)

    try:
        payload = generate_dataset_research(
            dataset_id=dataset_id,
            data_class=args.data_class,
            title=args.title,
            artifact_uri=args.artifact_uri,
            sample_text=sample_text,
            schema_text=schema_text,
            notes=args.notes,
            supplemental_urls=supplemental_urls or None,
            model=args.model,
        )
    except AIResearchError as exc:
        print(f"AI research error: {exc}", file=sys.stderr)
        return 1

    out = json.dumps(payload, indent=2, ensure_ascii=True)
    if args.output:
        out_path = Path(args.output)
        out_path.parent.mkdir(parents=True, exist_ok=True)
        out_path.write_text(out + "\n", encoding="utf-8")
        print(f"Wrote AI research output: {out_path}")
        return 0

    print(out)
    return 0


def cmd_sync_db(args: argparse.Namespace) -> int:
    local_queue = Path(args.local_queue_dir)
    processed_dir = Path(args.processed_dir)
    local_queue.mkdir(parents=True, exist_ok=True)
    processed_dir.mkdir(parents=True, exist_ok=True)

    global_vars: Dict[str, Any] = {}
    try:
        selected_global = resolve_global_config_path(Path(args.global_config) if args.global_config else None)
    except ConfigError as exc:
        print(f"Global config error: {exc}", file=sys.stderr)
        return 1
    if selected_global:
        try:
            global_vars = load_global_config(selected_global)
        except ConfigError as exc:
            print(f"Global config error: {exc}", file=sys.stderr)
            return 1

    try:
        cfg_path = resolve_execution_config_path(Path(args.environments_config) if args.environments_config else None)
    except ExecutionConfigError as exc:
        print(f"Environments config error: {exc}", file=sys.stderr)
        return 1
    if not cfg_path:
        print("Environments config error: no environments config found.", file=sys.stderr)
        return 1
    if not args.env:
        print("`--env` is required for sync-db.", file=sys.stderr)
        return 1
    try:
        envs = load_execution_config(cfg_path)
        exec_env = envs.get(args.env, {})
        if not exec_env:
            print(f"Execution env '{args.env}' not found in config", file=sys.stderr)
            return 1
        exec_env = apply_execution_env_overrides(exec_env)
        exec_env = resolve_execution_env_templates(exec_env, global_vars=global_vars)
    except ExecutionConfigError as exc:
        print(f"Environments config error: {exc}", file=sys.stderr)
        return 1

    ssh_host = str(exec_env.get("ssh_host") or "").strip()
    if not ssh_host:
        print(f"Execution env '{args.env}' has no ssh_host; cannot pull remote queue.", file=sys.stderr)
        return 1
    ssh_user = str(exec_env.get("ssh_user") or "").strip()
    ssh_jump = str(exec_env.get("ssh_jump") or "").strip()
    target = f"{ssh_user + '@' if ssh_user else ''}{ssh_host}"
    remote_dir = str(args.remote_dir or exec_env.get("db_sync_queue_dir") or "").strip()
    if not remote_dir:
        workdir = str(exec_env.get("workdir") or "").strip()
        remote_dir = f"{workdir}/db_sync_queue/pending" if workdir else "~/.etl/db_sync_queue/pending"

    ssh_cmd = ["ssh"] + (["-J", ssh_jump] if ssh_jump else []) + [target, f"mkdir -p {shlex.quote(remote_dir)}"]
    proc = subprocess.run(ssh_cmd, capture_output=True, text=True)
    if proc.returncode != 0:
        print(f"Failed to ensure remote queue dir: {proc.stderr or proc.stdout}", file=sys.stderr)
        return 1

    if not args.apply_only:
        scp_cmd = ["scp"] + (["-J", ssh_jump] if ssh_jump else []) + ["-r", f"{target}:{remote_dir}/.", str(local_queue)]
        proc_pull = subprocess.run(scp_cmd, capture_output=True, text=True)
        if proc_pull.returncode != 0:
            stderr_text = (proc_pull.stderr or "").strip()
            if "No such file or directory" not in stderr_text and "not a regular file" not in stderr_text:
                print(f"Failed to pull remote queue: {stderr_text or proc_pull.stdout}", file=sys.stderr)
                return 1

    if not get_database_url():
        print("ETL_DATABASE_URL is not set locally; cannot apply pulled queue.", file=sys.stderr)
        return 1

    before = {p.name for p in processed_dir.glob("*.json")}
    summary = apply_tracking_queue(local_queue, processed_dir=processed_dir)
    after = {p.name for p in processed_dir.glob("*.json")}
    new_processed = sorted(after - before)

    if new_processed and not args.keep_remote:
        quoted = " ".join(shlex.quote(str((Path(remote_dir) / name).as_posix())) for name in new_processed)
        rm_cmd = ["ssh"] + (["-J", ssh_jump] if ssh_jump else []) + [target, f"rm -f {quoted}"]
        subprocess.run(rm_cmd, capture_output=True, text=True)

    print(
        f"DB queue sync complete: queued={summary.queued} applied={summary.applied} failed={summary.failed} local_queue={local_queue}"
    )
    return 0 if summary.failed == 0 else 1


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="etl", description="ETL pipeline runner")
    parser.add_argument("--version", action="version", version=f"%(prog)s {__version__}")

    subparsers = parser.add_subparsers(dest="command", required=True)

    p_plugins = subparsers.add_parser("plugins", help="Plugin related commands")
    plugins_sub = p_plugins.add_subparsers(dest="plugins_command", required=True)

    p_plugins_list = plugins_sub.add_parser("list", help="List available plugins")
    p_plugins_list.add_argument(
        "-d", "--directory", help="Directory to search for plugins", default=None
    )
    p_plugins_list.set_defaults(func=cmd_plugins_list)

    p_run = subparsers.add_parser("run", help="Run a pipeline")
    p_run.add_argument("pipeline", help="Path to pipeline YAML")
    p_run.add_argument("--global-config", help="Path to global config YAML", default=None)
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
    p_run.add_argument("--executor", choices=["local", "slurm"], default="local", help="Execution backend")
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

    p_runs = subparsers.add_parser("runs", help="Inspect previous runs")
    runs_sub = p_runs.add_subparsers(dest="runs_command", required=True)

    p_runs_list = runs_sub.add_parser("list", help="List recorded runs")
    p_runs_list.add_argument("--store", default=".runs/runs.jsonl", help="Run record store path")
    p_runs_list.add_argument("-n", "--num", type=int, default=10, help="Number of runs to show")
    p_runs_list.set_defaults(func=cmd_runs_list)

    p_runs_show = runs_sub.add_parser("show", help="Show details for a run")
    p_runs_show.add_argument("run_id", help="Run ID to show")
    p_runs_show.add_argument("--store", default=".runs/runs.jsonl", help="Run record store path")
    p_runs_show.set_defaults(func=cmd_runs_show)

    p_datasets = subparsers.add_parser("datasets", help="Inspect registered datasets")
    datasets_sub = p_datasets.add_subparsers(dest="datasets_command", required=True)

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

    p_diag = subparsers.add_parser("diagnostics", help="Inspect generated diagnostic reports")
    diag_sub = p_diag.add_subparsers(dest="diagnostics_command", required=True)

    p_diag_latest = diag_sub.add_parser("latest", help="Print path to most recent diagnostic report")
    p_diag_latest.add_argument("--workdir", default=".runs", help="Base run artifact directory")
    p_diag_latest.add_argument("--show", action="store_true", help="Also print report JSON contents")
    p_diag_latest.set_defaults(func=cmd_diagnostics_latest)

    p_validate = subparsers.add_parser("validate", help="Validate a pipeline file")
    p_validate.add_argument("pipeline", help="Path to pipeline YAML")
    p_validate.add_argument("--global-config", help="Path to global config YAML", default=None)
    p_validate.set_defaults(func=cmd_validate)

    p_web = subparsers.add_parser("web", help="Run the minimal web UI/API server")
    p_web.add_argument("--host", default="127.0.0.1", help="Bind host")
    p_web.add_argument("--port", type=int, default=8000, help="Bind port")
    p_web.add_argument("--reload", action="store_true", help="Auto-reload on code changes")
    p_web.set_defaults(func=cmd_web)

    p_artifacts = subparsers.add_parser("artifacts", help="Artifact policy commands")
    artifacts_sub = p_artifacts.add_subparsers(dest="artifacts_command", required=True)

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

    p_ai = subparsers.add_parser("ai", help="AI-assisted research/documentation helpers")
    ai_sub = p_ai.add_subparsers(dest="ai_command", required=True)

    p_ai_research = ai_sub.add_parser("research", help="Generate dataset explanation draft JSON")
    p_ai_research.add_argument("--dataset-id", required=True, help="Catalog dataset_id (e.g., serve.example_v1)")
    p_ai_research.add_argument("--data-class", default=None, help="Catalog data_class hint (RAW|EXT|STAGE|MODEL_IN|MODEL_OUT|SERVE|REF)")
    p_ai_research.add_argument("--title", default=None, help="Optional dataset title hint")
    p_ai_research.add_argument("--artifact-uri", default=None, help="Optional storage URI/path hint")
    p_ai_research.add_argument("--sample-file", default=None, help="Optional sample data excerpt file")
    p_ai_research.add_argument("--schema-file", default=None, help="Optional schema/column description file")
    p_ai_research.add_argument(
        "--supplemental-url",
        action="append",
        default=[],
        help="Optional URL to fetch and include as supplemental research context (repeatable).",
    )
    p_ai_research.add_argument(
        "--supplemental-urls-file",
        default=None,
        help="Optional text file with one supplemental URL per line (# comments allowed).",
    )
    p_ai_research.add_argument("--notes", default=None, help="Optional analyst notes/instructions")
    p_ai_research.add_argument("--model", default=None, help="Optional OpenAI model override")
    p_ai_research.add_argument("--output", default=None, help="Write JSON output to file path instead of stdout")
    p_ai_research.set_defaults(func=cmd_ai_research)

    p_sync_db = subparsers.add_parser("sync-db", help="Pull/apply queued DB updates from remote environment")
    p_sync_db.add_argument("--global-config", help="Path to global config YAML", default=None)
    p_sync_db.add_argument(
        "--environments-config",
        help="Path to environments config YAML (default: config/environments.yml)",
        default=None,
    )
    p_sync_db.add_argument("--env", help="Execution environment name (from environments config)", required=True)
    p_sync_db.add_argument("--remote-dir", default=None, help="Remote queue directory override")
    p_sync_db.add_argument("--local-queue-dir", default=".runs/db_sync_inbox/pending", help="Local inbox queue directory")
    p_sync_db.add_argument("--processed-dir", default=".runs/db_sync_inbox/processed", help="Local processed queue directory")
    p_sync_db.add_argument("--apply-only", action="store_true", help="Apply local queue only; do not pull via SSH/SCP")
    p_sync_db.add_argument("--keep-remote", action="store_true", help="Do not delete remote files after successful apply")
    p_sync_db.set_defaults(func=cmd_sync_db)

    return parser


def main(argv: list[str] | None = None) -> int:
    parser = build_parser()
    args, unknown_args = parser.parse_known_args(argv)
    if unknown_args:
        print(f"[etl][WARN] ignoring unknown arguments: {' '.join(str(x) for x in unknown_args)}")
    args._raw_argv = argv if argv is not None else sys.argv[1:]
    try:
        ensure_database_schema(Path("db/ddl"))
    except DatabaseError as exc:
        _emit_error_with_report(_format_database_init_error(exc), exc, args)
        return 1
    try:
        _auto_apply_local_db_queue()
    except Exception:
        # Startup queue apply should not block CLI commands.
        pass
    try:
        return args.func(args)
    except Exception as exc:  # noqa: BLE001
        _emit_error_with_report(f"Unhandled error: {exc}", exc, args)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())

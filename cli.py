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
import subprocess
import sys
from pathlib import Path

from etl import __version__
from etl.config import load_global_config, ConfigError
from etl.db import ensure_database_schema, DatabaseError
from etl.diagnostics import write_error_report, find_latest_error_report
from etl.executors import LocalExecutor, SlurmExecutor
from etl.executors.slurm import SlurmSubmitError
from etl.pipeline import PipelineError, parse_pipeline
from etl.provenance import collect_run_provenance
from etl.plugins.base import describe_plugin, discover_plugins, PluginLoadError
from etl.tracking import load_runs, find_run
from etl.execution_config import (
    load_execution_config,
    apply_execution_env_overrides,
    ExecutionConfigError,
)


DEFAULT_PLUGIN_DIR = Path("plugins")


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
            "Increase ssh_timeout/scp_timeout in execution config or check cluster/login-node responsiveness."
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
    plugins_dir_path = Path(args.plugins_dir)
    repo_root = Path(".").resolve()
    global_vars = {}
    exec_env = {}
    if args.global_config:
        try:
            global_vars = load_global_config(Path(args.global_config))
        except ConfigError as exc:
            print(f"Global config error: {exc}", file=sys.stderr)
            return 1
    if args.execution_config and args.env:
        try:
            envs = load_execution_config(Path(args.execution_config))
            exec_env = envs.get(args.env, {})
            if not exec_env:
                print(f"Execution env '{args.env}' not found in config", file=sys.stderr)
                return 1
            exec_env = apply_execution_env_overrides(exec_env)
        except ExecutionConfigError as exc:
            print(f"Execution config error: {exc}", file=sys.stderr)
            return 1
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
    # Ensure SLURM path inherits explicit CLI overrides even when execution config is used.
    exec_env["step_max_retries"] = max_retries
    exec_env["step_retry_delay_seconds"] = retry_delay_seconds
    try:
        pipeline = parse_pipeline(pipeline_path, global_vars=global_vars)
    except (PipelineError, FileNotFoundError) as exc:
        print(f"Invalid pipeline: {exc}", file=sys.stderr)
        return 1
    cli_command = "python cli.py " + " ".join(getattr(args, "_raw_argv", []))
    provenance = collect_run_provenance(
        repo_root=repo_root,
        pipeline_path=pipeline_path,
        global_config_path=Path(args.global_config) if args.global_config else None,
        execution_config_path=Path(args.execution_config) if args.execution_config else None,
        plugin_dir=plugins_dir_path,
        pipeline=pipeline,
        cli_command=cli_command,
    )

    if args.executor == "slurm":
        executor = SlurmExecutor(
            env_config=exec_env,
            repo_root=repo_root,
            plugins_dir=plugins_dir_path,
            workdir=Path(args.workdir),
            global_config=Path(args.global_config) if args.global_config else None,
            execution_config=Path(args.execution_config) if args.execution_config else None,
            env_name=args.env,
            dry_run=args.dry_run,
            verbose=args.verbose,
        )
    else:
        executor = LocalExecutor(
            plugin_dir=plugins_dir_path,
            workdir=Path(args.workdir),
            dry_run=args.dry_run,
            max_retries=max_retries,
            retry_delay_seconds=retry_delay_seconds,
        )
    try:
        result = executor.submit(
            str(pipeline_path),
            context={
                "pipeline": pipeline,
                "execution_env": exec_env,
                "resume_run_id": args.resume_run_id,
                "provenance": provenance,
            },
        )
    except Exception as exc:  # noqa: BLE001
        _emit_error_with_report(_format_run_submission_error(exc, args), exc, args)
        return 1
    status = executor.status(result.run_id)
    job_info = f" (jobs: {result.job_ids})" if getattr(result, "job_ids", None) else ""
    print(f"Run {result.run_id} -> {status.state}{job_info}")
    if status.state in ("succeeded", "queued", "running"):
        return 0
    return 1


def cmd_validate(args: argparse.Namespace) -> int:
    pipeline_path = Path(args.pipeline)
    global_vars = {}
    if args.global_config:
        try:
            global_vars = load_global_config(Path(args.global_config))
        except ConfigError as exc:
            print(f"Global config error: {exc}", file=sys.stderr)
            return 1
    try:
        pipeline = parse_pipeline(pipeline_path, global_vars=global_vars)
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
    p_run.add_argument("--execution-config", help="Path to execution env config YAML", default=None)
    p_run.add_argument("--env", help="Execution environment name (from execution config)", default=None)
    p_run.add_argument("--plugins-dir", default="plugins", help="Directory containing plugins")
    p_run.add_argument("--workdir", default=".runs", help="Directory to store run artifacts")
    p_run.add_argument("--dry-run", action="store_true", help="Parse and plan without executing plugins")
    p_run.add_argument("--max-retries", type=int, default=None, help="Max retries per step after first failure")
    p_run.add_argument("--retry-delay-seconds", type=float, default=None, help="Delay between retries in seconds")
    p_run.add_argument("--resume-run-id", default=None, help="Resume by skipping steps that succeeded in a prior run_id")
    p_run.add_argument("--executor", choices=["local", "slurm"], default="local", help="Execution backend")
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

    return parser


def main(argv: list[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    args._raw_argv = argv if argv is not None else sys.argv[1:]
    try:
        ensure_database_schema(Path("db/ddl"))
    except DatabaseError as exc:
        _emit_error_with_report(_format_database_init_error(exc), exc, args)
        return 1
    try:
        return args.func(args)
    except Exception as exc:  # noqa: BLE001
        _emit_error_with_report(f"Unhandled error: {exc}", exc, args)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())

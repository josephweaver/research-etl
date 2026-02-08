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
import sys
from pathlib import Path

from etl import __version__
from etl.config import load_global_config, ConfigError
from etl.db import ensure_database_schema, DatabaseError
from etl.executors import LocalExecutor, SlurmExecutor
from etl.pipeline import PipelineError, parse_pipeline
from etl.plugins.base import describe_plugin, discover_plugins, PluginLoadError
from etl.tracking import load_runs, find_run
from etl.execution_config import (
    load_execution_config,
    apply_execution_env_overrides,
    ExecutionConfigError,
)


DEFAULT_PLUGIN_DIR = Path("plugins")


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
    # Ensure SLURM path inherits explicit CLI overrides even when execution config is used.
    exec_env["step_max_retries"] = max_retries
    exec_env["step_retry_delay_seconds"] = retry_delay_seconds
    try:
        pipeline = parse_pipeline(pipeline_path, global_vars=global_vars)
    except (PipelineError, FileNotFoundError) as exc:
        print(f"Invalid pipeline: {exc}", file=sys.stderr)
        return 1

    if args.executor == "slurm":
        executor = SlurmExecutor(
            env_config=exec_env,
            repo_root=Path(".").resolve(),
            plugins_dir=Path(args.plugins_dir),
            workdir=Path(args.workdir),
            global_config=Path(args.global_config) if args.global_config else None,
            execution_config=Path(args.execution_config) if args.execution_config else None,
            env_name=args.env,
            dry_run=args.dry_run,
            verbose=args.verbose,
        )
    else:
        executor = LocalExecutor(
            plugin_dir=Path(args.plugins_dir),
            workdir=Path(args.workdir),
            dry_run=args.dry_run,
            max_retries=max_retries,
            retry_delay_seconds=retry_delay_seconds,
        )
    result = executor.submit(str(pipeline_path), context={"pipeline": pipeline, "execution_env": exec_env})
    status = executor.status(result.run_id)
    job_info = f" (jobs: {result.job_ids})" if getattr(result, "job_ids", None) else ""
    print(f"Run {result.run_id} -> {status.state}{job_info}")
    if status.state in ("succeeded", "queued", "running"):
        return 0
    return 1


    pipeline_path = Path(args.pipeline)
    try:
        pipeline = parse_pipeline(pipeline_path)
    except (PipelineError, FileNotFoundError) as exc:
        print(f"Invalid pipeline: {exc}", file=sys.stderr)
        return 1

    executor = LocalExecutor(
        plugin_dir=Path(args.plugins_dir),
        workdir=Path(args.workdir),
        dry_run=args.dry_run,
    )
    result = executor.submit(str(pipeline_path), context={"pipeline": pipeline})
    status = executor.status(result.run_id)
    print(f"Run {result.run_id} -> {status.state}")
    return 0 if status.state == "succeeded" else 1


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

    p_validate = subparsers.add_parser("validate", help="Validate a pipeline file")
    p_validate.add_argument("pipeline", help="Path to pipeline YAML")
    p_validate.add_argument("--global-config", help="Path to global config YAML", default=None)
    p_validate.set_defaults(func=cmd_validate)

    return parser


def main(argv: list[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    try:
        ensure_database_schema(Path("db/ddl"))
    except DatabaseError as exc:
        print(f"Database initialization error: {exc}", file=sys.stderr)
        return 1
    return args.func(args)


if __name__ == "__main__":
    raise SystemExit(main())

"""
Batch runner: execute a subset of pipeline steps.

Intended for use by the SLURM executor to chain batches/arrays.
It:
  - loads the pipeline YAML
  - loads shared context JSON (vars/dirs/outputs) if provided
  - executes the specified step indices
  - saves updated context back to disk
"""

from __future__ import annotations

import argparse
import json
import os
from datetime import datetime
from pathlib import Path

from etl.db_sync_queue import queue_tracking_update
from etl.config import load_global_config, resolve_global_config_path, ConfigError
from etl.execution_config import (
    load_execution_config,
    apply_execution_env_overrides,
    resolve_execution_env_templates,
    resolve_execution_config_path,
    validate_environment_executor,
    ExecutionConfigError,
)
from etl.pipeline import parse_pipeline, PipelineError
from etl.provenance import collect_run_provenance
from etl.projects import resolve_project_id
from etl.runner import run_pipeline, RunResult
from etl.tracking import upsert_run_status, upsert_step_attempt, load_run_step_states


def _vprint(enabled: bool, message: str) -> None:
    if enabled:
        print(f"[run_batch] {message}")


def load_context(path: Path) -> dict:
    if not path or not path.exists():
        return {}
    with path.open("r", encoding="utf-8") as f:
        return json.load(f)


def save_context(path: Path, ctx: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as f:
        json.dump(ctx, f)


def _build_step_attempt_summary(result: RunResult) -> list[dict]:
    summary: list[dict] = []
    for step_res in result.steps:
        attempts = step_res.attempts or []
        final_error = step_res.error
        if attempts:
            final_error = attempts[-1].get("error", final_error)
        summary.append(
            {
                "step_name": step_res.step.name,
                "step_id": step_res.step_id,
                "attempts": int(step_res.attempt_no),
                "success": bool(step_res.success),
                "skipped": bool(step_res.skipped),
                "final_error": final_error,
            }
        )
    return summary


def _safe_tracking_write(action_name: str, fn, *, queue_dir: Path, **kwargs) -> None:
    try:
        fn(**kwargs)
    except Exception as exc:  # noqa: BLE001
        # Do not block batch execution when DB tracking is unavailable.
        op = "upsert_step_attempt" if action_name == "step_attempt" else "upsert_run_status"
        try:
            queued_path = queue_tracking_update(queue_dir, operation=op, payload=kwargs)
            print(f"Tracking warning ({action_name}): {exc} | queued={queued_path}")
        except Exception as queue_exc:  # noqa: BLE001
            print(f"Tracking warning ({action_name}): {exc} | queue_failed={queue_exc}")


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Run a batch of pipeline steps")
    parser.add_argument("pipeline", help="Pipeline YAML path")
    parser.add_argument("--steps", required=True, help="Comma-separated step indices to run (0-based)")
    parser.add_argument("--plugins-dir", default="plugins", help="Plugins directory")
    parser.add_argument("--workdir", default=".runs", help="Work directory base")
    parser.add_argument("--context-file", help="JSON file to load/save shared context", default=None)
    parser.add_argument("--run-id", help="Existing run_id (for chained batches)", default=None)
    parser.add_argument("--project-id", help="Project partition id override", default=None)
    parser.add_argument("--resume-run-id", default=None, help="Prior run_id to resume from (skip successful steps)")
    parser.add_argument("--max-retries", type=int, default=0, help="Max step retries after first failure")
    parser.add_argument("--retry-delay-seconds", type=float, default=0.0, help="Delay between retries")
    parser.add_argument("--global-config", help="Path to global config YAML", default=None)
    parser.add_argument(
        "--environments-config",
        help="Path to environments config YAML (default: config/environments.yml)",
        default=None,
    )
    parser.add_argument("--env", help="Execution environment name (from environments config)", default=None)
    parser.add_argument("--verbose", action="store_true", help="Print batch progress details")
    args = parser.parse_args(argv)
    run_id = args.run_id
    if not run_id:
        print("run_batch.py requires --run-id for remote tracking consistency")
        return 1
    _vprint(args.verbose, f"starting run_id={run_id} pipeline={args.pipeline} steps={args.steps}")

    global_vars = {}
    exec_env = {}
    resolved_global_cfg: Path | None = None
    try:
        resolved_global_cfg = resolve_global_config_path(
            Path(args.global_config) if args.global_config else None
        )
    except ConfigError as exc:
        print(f"Global config error: {exc}")
        return 1
    if resolved_global_cfg:
        try:
            global_vars = load_global_config(resolved_global_cfg)
        except ConfigError as exc:
            print(f"Global config error: {exc}")
            return 1
        args.global_config = str(resolved_global_cfg)
        _vprint(args.verbose, f"loaded global config: {args.global_config}")
    try:
        resolved_exec_cfg = resolve_execution_config_path(
            Path(args.environments_config) if args.environments_config else None
        )
    except ExecutionConfigError as exc:
        print(f"Environments config error: {exc}")
        return 1
    selected_env_name = args.env
    if resolved_exec_cfg and selected_env_name:
        try:
            envs = load_execution_config(resolved_exec_cfg)
            exec_env = envs.get(selected_env_name, {})
            if not exec_env:
                print(f"Execution env '{selected_env_name}' not found in config")
                return 1
            validate_environment_executor(selected_env_name, exec_env, executor="slurm")
            exec_env = apply_execution_env_overrides(exec_env)
            exec_env = resolve_execution_env_templates(exec_env, global_vars=global_vars)
            _vprint(args.verbose, f"loaded execution env: {selected_env_name}")
        except ExecutionConfigError as exc:
            print(f"Environments config error: {exc}")
            return 1
    if args.env and not resolved_exec_cfg:
        print("Environments config error: `--env` was provided but no environments config was found.")
        return 1
    if resolved_exec_cfg:
        args.environments_config = str(resolved_exec_cfg)

    try:
        full_pipeline = parse_pipeline(Path(args.pipeline), global_vars=global_vars, env_vars=exec_env)
    except PipelineError as exc:
        print(f"Pipeline error: {exc}")
        return 1
    _vprint(args.verbose, f"parsed pipeline with {len(full_pipeline.steps)} step(s)")
    provenance = collect_run_provenance(
        repo_root=Path(".").resolve(),
        pipeline_path=Path(args.pipeline),
        global_config_path=Path(args.global_config) if args.global_config else None,
        environments_config_path=Path(args.environments_config) if args.environments_config else None,
        plugin_dir=Path(args.plugins_dir),
        pipeline=full_pipeline,
        cli_command="python etl/run_batch.py " + " ".join(argv or []),
    )

    ctx = load_context(Path(args.context_file)) if args.context_file else {}
    if args.context_file:
        _vprint(args.verbose, f"loaded context file: {args.context_file}")
    project_id = resolve_project_id(
        explicit_project_id=args.project_id or ctx.get("project_id"),
        pipeline_project_id=getattr(full_pipeline, "project_id", None),
        pipeline_path=args.pipeline,
    )

    # slice pipeline steps
    indices = [int(x) for x in args.steps.split(",") if x.strip() != ""]
    if not indices:
        print("No step indices provided")
        return 1
    _vprint(args.verbose, f"resolved step indices: {indices}")
    total_steps = len(full_pipeline.steps)
    pipeline = full_pipeline
    pipeline.steps = [full_pipeline.steps[i] for i in indices]
    _vprint(args.verbose, f"batch will execute steps: {[s.name for s in pipeline.steps]}")
    resume_succeeded_steps: set[str] = set()
    prior_step_outputs: dict[str, dict] = {}
    if args.resume_run_id:
        states = load_run_step_states(str(args.resume_run_id))
        resume_succeeded_steps = {name for name, st in states.items() if st.success}
        prior_step_outputs = {name: st.outputs for name, st in states.items()}
        _vprint(
            args.verbose,
            f"resume enabled from run_id={args.resume_run_id}; prior successful steps={len(resume_succeeded_steps)}",
        )
    queue_dir = Path(exec_env.get("db_sync_queue_dir") or (Path(args.workdir) / "db_sync_queue" / "pending"))
    scheduler_meta = {
        "slurm_job_id": str(os.environ.get("SLURM_JOB_ID", "") or ""),
        "slurm_array_task_id": str(os.environ.get("SLURM_ARRAY_TASK_ID", "") or ""),
    }
    for step in pipeline.steps:
        if step.output_var and step.name in prior_step_outputs:
            ctx[step.output_var] = prior_step_outputs.get(step.name, {})
    batch_started_at = datetime.utcnow().isoformat() + "Z"
    _safe_tracking_write(
        "batch_started",
        upsert_run_status,
        queue_dir=queue_dir,
        run_id=run_id,
        pipeline=args.pipeline,
        project_id=project_id,
        status="running",
        success=False,
        started_at=batch_started_at,
        ended_at=batch_started_at,
        message=f"running steps {indices}",
        executor="slurm",
        artifact_dir=str(args.workdir),
        provenance=provenance,
        event_type="batch_started",
        event_details={"step_indices": indices, "scheduler": scheduler_meta},
    )
    _vprint(args.verbose, "recorded batch_started tracking event")

    _vprint(
        args.verbose,
        f"executing batch in workdir={args.workdir} with max_retries={args.max_retries}, retry_delay_seconds={args.retry_delay_seconds}",
    )
    result: RunResult = run_pipeline(
        pipeline,
        plugin_dir=Path(args.plugins_dir),
        workdir=Path(args.workdir),
        run_id=run_id,
        dry_run=False,
        max_retries=args.max_retries,
        retry_delay_seconds=args.retry_delay_seconds,
        resume_succeeded_steps=resume_succeeded_steps,
        prior_step_outputs=prior_step_outputs,
        log_func=None,
    )
    _vprint(args.verbose, f"batch execution finished: success={result.success}")
    batch_ended_at = datetime.utcnow().isoformat() + "Z"
    for step_res in result.steps:
        step_artifact_dir = str(Path(args.workdir) / step_res.step.name / str(step_res.step_id or ""))
        attempts = step_res.attempts or []
        if attempts:
            for att in attempts:
                _safe_tracking_write(
                    "step_attempt",
                    upsert_step_attempt,
                    queue_dir=queue_dir,
                    run_id=run_id,
                    step_name=step_res.step.name,
                    attempt_no=int(att.get("attempt_no", step_res.attempt_no)),
                    script=step_res.step.script,
                    success=bool(att.get("success", step_res.success)),
                    skipped=bool(att.get("skipped", step_res.skipped)),
                    error=att.get("error", step_res.error),
                    outputs=att.get("outputs", step_res.outputs),
                    plugin_name=att.get("plugin_name"),
                    plugin_version=att.get("plugin_version"),
                    failure_category=att.get("failure_category"),
                    runtime_seconds=att.get("runtime_seconds"),
                    memory_gb=att.get("memory_gb"),
                    cpu_cores=att.get("cpu_cores"),
                    started_at=att.get("started_at", batch_started_at),
                    ended_at=att.get("ended_at", batch_ended_at),
                    pipeline=args.pipeline,
                    project_id=project_id,
                    artifact_dir=step_artifact_dir,
                    executor="slurm",
                )
        elif step_res.attempt_no > 0:
            _safe_tracking_write(
                "step_attempt",
                upsert_step_attempt,
                queue_dir=queue_dir,
                run_id=run_id,
                step_name=step_res.step.name,
                attempt_no=step_res.attempt_no,
                script=step_res.step.script,
                success=step_res.success,
                skipped=step_res.skipped,
                error=step_res.error,
                outputs=step_res.outputs,
                started_at=batch_started_at,
                ended_at=batch_ended_at,
                pipeline=args.pipeline,
                project_id=project_id,
                artifact_dir=step_artifact_dir,
                executor="slurm",
            )

    # merge outputs into ctx (simple overwrite)
    ctx["project_id"] = project_id
    for step_res in result.steps:
        if step_res.success and step_res.step.output_var:
            ctx[step_res.step.output_var] = step_res.outputs

    if args.context_file:
        save_context(Path(args.context_file), ctx)
        _vprint(args.verbose, f"saved context file: {args.context_file}")

    # If a batch fails, mark the full run failed immediately.
    if not result.success:
        step_attempts = _build_step_attempt_summary(result)
        _safe_tracking_write(
            "batch_failed",
            upsert_run_status,
            queue_dir=queue_dir,
            run_id=run_id,
            pipeline=args.pipeline,
            project_id=project_id,
            status="failed",
            success=False,
            started_at=batch_started_at,
            ended_at=batch_ended_at,
            message=f"batch failed for steps {indices}",
            executor="slurm",
            artifact_dir=str(args.workdir),
            provenance=provenance,
            event_type="batch_failed",
            event_details={"step_indices": indices, "step_attempts": step_attempts, "scheduler": scheduler_meta},
        )
        _vprint(args.verbose, "recorded batch_failed tracking event")
        return 1

    # Batch succeeded event.
    step_attempts = _build_step_attempt_summary(result)
    all_skipped = bool(step_attempts) and all(bool(s.get("skipped")) for s in step_attempts)
    _safe_tracking_write(
        "batch_completed",
        upsert_run_status,
        queue_dir=queue_dir,
        run_id=run_id,
        pipeline=args.pipeline,
        project_id=project_id,
        status="running",
        success=False,
        started_at=batch_started_at,
        ended_at=batch_ended_at,
        message=f"batch completed for steps {indices}",
        executor="slurm",
        artifact_dir=str(args.workdir),
        provenance=provenance,
        event_type="batch_skipped" if all_skipped else "batch_completed",
        event_details={"step_indices": indices, "step_attempts": step_attempts, "scheduler": scheduler_meta},
    )
    _vprint(args.verbose, "recorded batch_completed tracking event")

    # No polling: mark run complete when the last planned step index completes.
    if max(indices) == total_steps - 1:
        _safe_tracking_write(
            "run_completed",
            upsert_run_status,
            queue_dir=queue_dir,
            run_id=run_id,
            pipeline=args.pipeline,
            project_id=project_id,
            status="succeeded",
            success=True,
            started_at=batch_started_at,
            ended_at=batch_ended_at,
            message="all batches completed",
            executor="slurm",
            artifact_dir=str(args.workdir),
            provenance=provenance,
            event_type="run_completed",
            event_details={"step_indices": indices, "step_attempts": step_attempts, "scheduler": scheduler_meta},
        )
        _vprint(args.verbose, "recorded run_completed tracking event")

    _vprint(args.verbose, "done")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

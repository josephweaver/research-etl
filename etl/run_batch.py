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
from datetime import datetime
from pathlib import Path

from etl.config import load_global_config, ConfigError
from etl.execution_config import (
    load_execution_config,
    apply_execution_env_overrides,
    ExecutionConfigError,
)
from etl.pipeline import parse_pipeline, PipelineError
from etl.provenance import collect_run_provenance
from etl.runner import run_pipeline, RunResult
from etl.tracking import upsert_run_status, upsert_step_attempt, load_run_step_states


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
                "attempts": int(step_res.attempt_no),
                "success": bool(step_res.success),
                "skipped": bool(step_res.skipped),
                "final_error": final_error,
            }
        )
    return summary


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Run a batch of pipeline steps")
    parser.add_argument("pipeline", help="Pipeline YAML path")
    parser.add_argument("--steps", required=True, help="Comma-separated step indices to run (0-based)")
    parser.add_argument("--plugins-dir", default="plugins", help="Plugins directory")
    parser.add_argument("--workdir", default=".runs", help="Work directory base")
    parser.add_argument("--context-file", help="JSON file to load/save shared context", default=None)
    parser.add_argument("--run-id", help="Existing run_id (for chained batches)", default=None)
    parser.add_argument("--resume-run-id", default=None, help="Prior run_id to resume from (skip successful steps)")
    parser.add_argument("--max-retries", type=int, default=0, help="Max step retries after first failure")
    parser.add_argument("--retry-delay-seconds", type=float, default=0.0, help="Delay between retries")
    parser.add_argument("--global-config", help="Path to global config YAML", default=None)
    parser.add_argument("--execution-config", help="Path to execution env config YAML", default=None)
    parser.add_argument("--env", help="Execution environment name (from execution config)", default=None)
    args = parser.parse_args(argv)
    run_id = args.run_id
    if not run_id:
        print("run_batch.py requires --run-id for remote tracking consistency")
        return 1

    global_vars = {}
    exec_env = {}
    if args.global_config:
        try:
            global_vars = load_global_config(Path(args.global_config))
        except ConfigError as exc:
            print(f"Global config error: {exc}")
            return 1
    if args.execution_config and args.env:
        try:
            envs = load_execution_config(Path(args.execution_config))
            exec_env = envs.get(args.env, {})
            if not exec_env:
                print(f"Execution env '{args.env}' not found in config")
                return 1
            exec_env = apply_execution_env_overrides(exec_env)
        except ExecutionConfigError as exc:
            print(f"Execution config error: {exc}")
            return 1

    try:
        full_pipeline = parse_pipeline(Path(args.pipeline), global_vars=global_vars, env_vars=exec_env)
    except PipelineError as exc:
        print(f"Pipeline error: {exc}")
        return 1
    provenance = collect_run_provenance(
        repo_root=Path(".").resolve(),
        pipeline_path=Path(args.pipeline),
        global_config_path=Path(args.global_config) if args.global_config else None,
        execution_config_path=Path(args.execution_config) if args.execution_config else None,
        plugin_dir=Path(args.plugins_dir),
        pipeline=full_pipeline,
        cli_command="python etl/run_batch.py " + " ".join(argv or []),
    )

    ctx = load_context(Path(args.context_file)) if args.context_file else {}

    # slice pipeline steps
    indices = [int(x) for x in args.steps.split(",") if x.strip() != ""]
    if not indices:
        print("No step indices provided")
        return 1
    total_steps = len(full_pipeline.steps)
    pipeline = full_pipeline
    pipeline.steps = [full_pipeline.steps[i] for i in indices]
    resume_succeeded_steps: set[str] = set()
    prior_step_outputs: dict[str, dict] = {}
    if args.resume_run_id:
        states = load_run_step_states(str(args.resume_run_id))
        resume_succeeded_steps = {name for name, st in states.items() if st.success}
        prior_step_outputs = {name: st.outputs for name, st in states.items()}
    for step in pipeline.steps:
        if step.output_var and step.name in prior_step_outputs:
            ctx[step.output_var] = prior_step_outputs.get(step.name, {})
    batch_started_at = datetime.utcnow().isoformat() + "Z"
    upsert_run_status(
        run_id=run_id,
        pipeline=args.pipeline,
        status="running",
        success=False,
        started_at=batch_started_at,
        ended_at=batch_started_at,
        message=f"running steps {indices}",
        executor="slurm",
        artifact_dir=str(args.workdir),
        provenance=provenance,
        event_type="batch_started",
        event_details={"step_indices": indices},
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
    batch_ended_at = datetime.utcnow().isoformat() + "Z"
    for step_res in result.steps:
        attempts = step_res.attempts or []
        if attempts:
            for att in attempts:
                upsert_step_attempt(
                    run_id=run_id,
                    step_name=step_res.step.name,
                    attempt_no=int(att.get("attempt_no", step_res.attempt_no)),
                    script=step_res.step.script,
                    success=bool(att.get("success", step_res.success)),
                    skipped=bool(att.get("skipped", step_res.skipped)),
                    error=att.get("error", step_res.error),
                    outputs=att.get("outputs", step_res.outputs),
                    started_at=att.get("started_at", batch_started_at),
                    ended_at=att.get("ended_at", batch_ended_at),
                )
        elif step_res.attempt_no > 0:
            upsert_step_attempt(
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
            )

    # merge outputs into ctx (simple overwrite)
    for step_res in result.steps:
        if step_res.success and step_res.step.output_var:
            ctx[step_res.step.output_var] = step_res.outputs

    if args.context_file:
        save_context(Path(args.context_file), ctx)

    # If a batch fails, mark the full run failed immediately.
    if not result.success:
        step_attempts = _build_step_attempt_summary(result)
        upsert_run_status(
            run_id=run_id,
            pipeline=args.pipeline,
            status="failed",
            success=False,
            started_at=batch_started_at,
            ended_at=batch_ended_at,
            message=f"batch failed for steps {indices}",
            executor="slurm",
            artifact_dir=str(args.workdir),
            provenance=provenance,
            event_type="batch_failed",
            event_details={"step_indices": indices, "step_attempts": step_attempts},
        )
        return 1

    # Batch succeeded event.
    step_attempts = _build_step_attempt_summary(result)
    all_skipped = bool(step_attempts) and all(bool(s.get("skipped")) for s in step_attempts)
    upsert_run_status(
        run_id=run_id,
        pipeline=args.pipeline,
        status="running",
        success=False,
        started_at=batch_started_at,
        ended_at=batch_ended_at,
        message=f"batch completed for steps {indices}",
        executor="slurm",
        artifact_dir=str(args.workdir),
        provenance=provenance,
        event_type="batch_skipped" if all_skipped else "batch_completed",
        event_details={"step_indices": indices, "step_attempts": step_attempts},
    )

    # No polling: mark run complete when the last planned step index completes.
    if max(indices) == total_steps - 1:
        upsert_run_status(
            run_id=run_id,
            pipeline=args.pipeline,
            status="succeeded",
            success=True,
            started_at=batch_started_at,
            ended_at=batch_ended_at,
            message="all batches completed",
            executor="slurm",
            artifact_dir=str(args.workdir),
            provenance=provenance,
            event_type="run_completed",
            event_details={"step_indices": indices, "step_attempts": step_attempts},
        )

    return 0


if __name__ == "__main__":
    raise SystemExit(main())

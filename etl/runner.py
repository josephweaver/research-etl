"""
Pipeline runner for local execution.

This is a lightweight synchronous runner that:
1) Parses a pipeline object.
2) Resolves each step's script to a plugin file.
3) Invokes the plugin's `run` (and optional `validate`) with minimal context.

Argument handling:
- The `script` field is split with `shlex.split`.
- First token is the plugin path/name; remaining tokens are parsed as either
  key=value pairs or positional arguments.
- Plugins receive a dict with parsed key/value pairs plus `args` (list of
  remaining positional tokens) and `env` (step env mapping).
"""

from __future__ import annotations

import os
import shlex
import time
import uuid
from datetime import datetime
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple
from concurrent.futures import ThreadPoolExecutor, as_completed

from .pipeline import Pipeline, Step
from .plugins.base import (
    PluginContext,
    PluginDefinition,
    PluginLoadError,
    load_plugin,
)


class RunError(RuntimeError):
    """Raised when a pipeline run fails."""


@dataclass
class StepResult:
    step: Step
    success: bool
    outputs: Dict[str, Any] = field(default_factory=dict)
    error: Optional[str] = None
    skipped: bool = False
    attempt_no: int = 1
    attempts: List[Dict[str, Any]] = field(default_factory=list)


@dataclass
class RunResult:
    run_id: str
    steps: List[StepResult]
    artifact_dir: Optional[str] = None

    @property
    def success(self) -> bool:
        return all(r.success for r in self.steps) if self.steps else False


def _parse_script(script: str) -> (str, List[str]):
    tokens = shlex.split(script)
    if not tokens:
        raise RunError("Empty script value")
    return tokens[0], tokens[1:]


def _parse_args(tokens: List[str]) -> Dict[str, Any]:
    params: Dict[str, Any] = {}
    positional: List[str] = []
    for tok in tokens:
        if "=" in tok:
            key, val = tok.split("=", 1)
            params[key] = val
        else:
            positional.append(tok)
    params["args"] = positional
    return params


def _coerce(value: str, typ: Any) -> Any:
    if typ in ("int", int):
        try:
            return int(value)
        except ValueError:
            return value
    if typ in ("float", float):
        try:
            return float(value)
        except ValueError:
            return value
    if typ in ("bool", bool):
        lowered = value.lower()
        if lowered in ("true", "1", "yes", "y", "on"):
            return True
        if lowered in ("false", "0", "no", "n", "off"):
            return False
        return value
    return value


def _apply_param_types(args: Dict[str, Any], meta_params: Dict[str, Any]) -> Dict[str, Any]:
    result = {}
    for key, spec in meta_params.items():
        default = spec.get("default") if isinstance(spec, dict) else None
        typ = spec.get("type") if isinstance(spec, dict) else None
        if key in args:
            result[key] = _coerce(str(args[key]), typ)
        elif default is not None:
            result[key] = default
    # preserve extras and positional args
    for k, v in args.items():
        if k not in result:
            result[k] = v
    return result


def _resolve_plugin_path(plugin_dir: Path, ref: str) -> Path:
    candidate = Path(ref)
    if not candidate.suffix:
        candidate = candidate.with_suffix(".py")
    if not candidate.is_absolute():
        candidate = plugin_dir / candidate
    if not candidate.exists():
        raise PluginLoadError(f"Plugin file not found: {candidate}")
    return candidate


def _eval_when(expr: Optional[str], ctx: Dict[str, Any]) -> bool:
    if expr is None:
        return True
    try:
        return bool(eval(expr, {"__builtins__": {}}, ctx))
    except Exception:
        return False


def _batch_steps(steps: List[Step]) -> List[List[Step]]:
    """
    Group steps into batches. Consecutive steps sharing the same
    `parallel_with` value are run in parallel within a batch.
    """
    batches: List[List[Step]] = []
    i = 0
    while i < len(steps):
        current = steps[i]
        group = [current]
        if current.parallel_with:
            key = current.parallel_with
            j = i + 1
            while j < len(steps) and steps[j].parallel_with == key:
                group.append(steps[j])
                j += 1
            i = j
        else:
            i += 1
        batches.append(group)
    return batches


class _SafeDict(dict):
    def __missing__(self, key):
        return "{" + key + "}"


def _format_value(value: Any, ctx: Dict[str, Any]) -> Any:
    if isinstance(value, str):
        return value.format_map(_SafeDict(ctx))
    if isinstance(value, dict):
        return {k: _format_value(v, ctx) for k, v in value.items()}
    return value


def run_pipeline(
    pipeline: Pipeline,
    *,
    plugin_dir: Path,
    workdir: Path,
    run_id: Optional[str] = None,
    dry_run: bool = False,
    max_retries: int = 0,
    retry_delay_seconds: float = 0.0,
    resume_succeeded_steps: Optional[set[str]] = None,
    prior_step_outputs: Optional[Dict[str, Dict[str, Any]]] = None,
    log_func=None,
) -> RunResult:
    run_id = run_id or uuid.uuid4().hex
    ts = datetime.utcnow()
    date_dir = ts.strftime("%y%m%d")
    run_dir = f"{ts.strftime('%H%M%S')}-{run_id[:8]}"
    base_workdir = workdir / date_dir / run_dir
    base_workdir.mkdir(parents=True, exist_ok=True)

    def log(msg: str) -> None:
        if log_func:
            log_func(msg)
        else:
            print(msg)

    step_results: List[StepResult] = []
    ctx_vars: Dict[str, Any] = dict(pipeline.vars)
    ctx_vars.update(pipeline.dirs)
    prior_step_outputs = prior_step_outputs or {}

    expanded_steps: List[Step] = []
    for step in pipeline.steps:
        if step.foreach:
            items = ctx_vars.get(step.foreach, [])
            if not isinstance(items, (list, tuple)):
                raise RunError(f"`foreach` expects a list; got {type(items)} for {step.foreach}")
            for idx, item in enumerate(items):
                local_ctx = dict(ctx_vars)
                local_ctx["item"] = item
                new_step = Step(
                    name=f"{step.name}_{idx}",
                    script=_format_value(step.script, local_ctx),
                    output_var=f"{step.output_var}_{idx}" if step.output_var else None,
                    env=_format_value(step.env, local_ctx),
                    when=step.when,
                    parallel_with=step.parallel_with,
                    foreach=None,
                )
                expanded_steps.append(new_step)
        else:
            expanded_steps.append(step)

    batches = _batch_steps(expanded_steps)
    if resume_succeeded_steps:
        filtered_batches: List[List[Step]] = []
        for batch in batches:
            kept: List[Step] = []
            for step in batch:
                if step.name in resume_succeeded_steps:
                    log(f"[{run_id}] step {step.name} skipped (resume from prior success)")
                    if step.output_var and step.name in prior_step_outputs:
                        ctx_vars[step.output_var] = prior_step_outputs.get(step.name, {})
                    step_results.append(
                        StepResult(
                            step=step,
                            success=True,
                            skipped=True,
                            attempt_no=0,
                            attempts=[],
                        )
                    )
                else:
                    kept.append(step)
            if kept:
                filtered_batches.append(kept)
        batches = filtered_batches

    for batch_idx, batch in enumerate(batches):
        if len(batch) == 1:
            step = batch[0]
            res = _execute_step(
                step,
                run_id,
                plugin_dir,
                base_workdir,
                dry_run,
                max_retries,
                retry_delay_seconds,
                log,
                ctx_vars,
            )
            step_results.append(res)
            if res.success and step.output_var:
                ctx_vars[step.output_var] = res.outputs
            if not res.success and not res.skipped:
                break
        else:
            # parallel batch
            futures = []
            with ThreadPoolExecutor(max_workers=len(batch)) as pool:
                for step in batch:
                    futures.append(
                        pool.submit(
                            _execute_step,
                            step,
                            run_id,
                            plugin_dir,
                            base_workdir,
                            dry_run,
                            max_retries,
                            retry_delay_seconds,
                            log,
                            dict(ctx_vars),  # snapshot
                        )
                    )
                for fut in as_completed(futures):
                    res = fut.result()
                    step_results.append(res)
                    if res.success and res.step.output_var:
                        ctx_vars[res.step.output_var] = res.outputs
                    if not res.success and not res.skipped:
                        # stop remaining? let pool finish but don't break pipeline context
                        pass

    return RunResult(run_id=run_id, steps=step_results, artifact_dir=str(base_workdir))


def _execute_step(
    step: Step,
    run_id: str,
    plugin_dir: Path,
    base_workdir: Path,
    dry_run: bool,
    max_retries: int,
    retry_delay_seconds: float,
    log,
    ctx_vars: Dict[str, Any],
) -> StepResult:
    log(f"[{run_id}] step {step.name}")

    if not _eval_when(step.when, ctx_vars):
        log(f"[{run_id}] step {step.name} skipped (when={step.when})")
        return StepResult(step=step, success=True, skipped=True, attempt_no=0, attempts=[])

    plugin_ref, arg_tokens = _parse_script(step.script)
    try:
        plugin_path = _resolve_plugin_path(plugin_dir, plugin_ref)
        plugin = load_plugin(plugin_path)
    except Exception as exc:  # noqa: BLE001
        return StepResult(step=step, success=False, error=f"Plugin load failed: {exc}")

    args = _parse_args(arg_tokens)
    args = _apply_param_types(args, plugin.meta.params)
    args["env"] = step.env

    step_workdir = base_workdir / step.name
    step_workdir.mkdir(parents=True, exist_ok=True)

    ctx = PluginContext(run_id=run_id, workdir=step_workdir, log=log)

    if dry_run:
        log(f"[{run_id}] dry-run -> {plugin_path} args={args}")
        started = datetime.utcnow().isoformat() + "Z"
        ended = datetime.utcnow().isoformat() + "Z"
        return StepResult(
            step=step,
            success=True,
            outputs={},
            attempt_no=1,
            attempts=[
                {
                    "attempt_no": 1,
                    "success": True,
                    "skipped": False,
                    "error": None,
                    "outputs": {},
                    "started_at": started,
                    "ended_at": ended,
                }
            ],
        )

    attempt_history: List[Dict[str, Any]] = []
    max_attempts = max(1, int(max_retries) + 1)
    for attempt_no in range(1, max_attempts + 1):
        original_env = os.environ.copy()
        started = datetime.utcnow().isoformat() + "Z"
        try:
            os.environ.update({str(k): str(v) for k, v in step.env.items()})
            outputs = plugin.run(args, ctx)
            if plugin.validate:
                plugin.validate(args, outputs, ctx)
            ended = datetime.utcnow().isoformat() + "Z"
            attempt_history.append(
                {
                    "attempt_no": attempt_no,
                    "success": True,
                    "skipped": False,
                    "error": None,
                    "outputs": outputs,
                    "started_at": started,
                    "ended_at": ended,
                }
            )
            if attempt_no > 1:
                log(f"[{run_id}] step {step.name} succeeded on attempt {attempt_no}")
            return StepResult(
                step=step,
                success=True,
                outputs=outputs,
                attempt_no=attempt_no,
                attempts=attempt_history,
            )
        except Exception as exc:  # noqa: BLE001
            ended = datetime.utcnow().isoformat() + "Z"
            err = str(exc)
            attempt_history.append(
                {
                    "attempt_no": attempt_no,
                    "success": False,
                    "skipped": False,
                    "error": err,
                    "outputs": {},
                    "started_at": started,
                    "ended_at": ended,
                }
            )
            if attempt_no < max_attempts:
                log(
                    f"[{run_id}] step {step.name} attempt {attempt_no}/{max_attempts} failed: {err}; retrying"
                )
                if retry_delay_seconds > 0:
                    time.sleep(retry_delay_seconds)
            else:
                return StepResult(
                    step=step,
                    success=False,
                    error=err,
                    attempt_no=attempt_no,
                    attempts=attempt_history,
                )
        finally:
            os.environ.clear()
            os.environ.update(original_env)


__all__ = ["run_pipeline", "RunResult", "StepResult", "RunError"]

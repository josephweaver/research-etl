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
import sys
import time
import uuid
from datetime import datetime
import glob
import re
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple
from concurrent.futures import ThreadPoolExecutor, as_completed

from .pipeline import Pipeline, Step
from .logging import CallbackLogSink, CompositeLogSink, ConsoleLogSink, FileLogSink, StepLogger
from .plugins.base import (
    PluginContext,
    PluginDefinition,
    PluginLoadError,
    load_plugin,
)


class RunError(RuntimeError):
    """Raised when a pipeline run fails."""


def _current_rss_bytes() -> Optional[int]:
    # Linux: use /proc for current RSS.
    proc_statm = Path("/proc/self/statm")
    if proc_statm.exists():
        try:
            text = proc_statm.read_text(encoding="utf-8").strip()
            parts = text.split()
            if len(parts) >= 2:
                rss_pages = int(parts[1])
                return int(rss_pages * os.sysconf("SC_PAGE_SIZE"))
        except Exception:
            pass

    if os.name == "nt":
        # Windows: current working set (RSS) via Win32 API.
        try:
            import ctypes
            from ctypes import wintypes

            class PROCESS_MEMORY_COUNTERS(ctypes.Structure):
                _fields_ = [
                    ("cb", wintypes.DWORD),
                    ("PageFaultCount", wintypes.DWORD),
                    ("PeakWorkingSetSize", ctypes.c_size_t),
                    ("WorkingSetSize", ctypes.c_size_t),
                    ("QuotaPeakPagedPoolUsage", ctypes.c_size_t),
                    ("QuotaPagedPoolUsage", ctypes.c_size_t),
                    ("QuotaPeakNonPagedPoolUsage", ctypes.c_size_t),
                    ("QuotaNonPagedPoolUsage", ctypes.c_size_t),
                    ("PagefileUsage", ctypes.c_size_t),
                    ("PeakPagefileUsage", ctypes.c_size_t),
                ]

            get_current_process = ctypes.windll.kernel32.GetCurrentProcess
            get_process_memory_info = ctypes.windll.psapi.GetProcessMemoryInfo
            counters = PROCESS_MEMORY_COUNTERS()
            counters.cb = ctypes.sizeof(PROCESS_MEMORY_COUNTERS)
            proc = get_current_process()
            ok = get_process_memory_info(proc, ctypes.byref(counters), counters.cb)
            if ok:
                return int(counters.WorkingSetSize)
        except Exception:
            pass
        # Fallback: parse tasklist current memory usage for this PID.
        try:
            import subprocess

            pid = os.getpid()
            proc = subprocess.run(
                ["tasklist", "/FI", f"PID eq {pid}", "/FO", "CSV", "/NH"],
                capture_output=True,
                text=True,
                check=False,
            )
            line = str(proc.stdout or "").strip()
            if line and "," in line:
                cols = [c.strip().strip('"') for c in line.split(",")]
                if len(cols) >= 5:
                    mem_text = cols[4].replace(",", "").replace("K", "").replace("k", "").strip()
                    kb = int(mem_text)
                    if kb > 0:
                        return kb * 1024
        except Exception:
            pass

    # macOS/Unix fallback: ru_maxrss (peak RSS for process lifetime).
    try:
        import resource

        usage = resource.getrusage(resource.RUSAGE_SELF)
        rss = int(getattr(usage, "ru_maxrss", 0) or 0)
        if rss <= 0:
            return None
        if sys.platform == "darwin":
            return rss
        return rss * 1024
    except Exception:
        return None


def _bytes_to_gb(value: Optional[int]) -> Optional[float]:
    if value in (None, 0):
        return None
    try:
        return float(value) / (1024.0 ** 3)
    except Exception:
        return None


def _estimate_cpu_cores_used(cpu_seconds: float, wall_seconds: float) -> Optional[float]:
    if wall_seconds <= 0:
        return None
    try:
        v = float(cpu_seconds) / float(wall_seconds)
        return max(0.0, v)
    except Exception:
        return None


@dataclass
class StepResult:
    step: Step
    success: bool
    step_id: Optional[str] = None
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


def _classify_attempt_failure(*, success: bool, skipped: bool, error: Optional[str]) -> str:
    if skipped:
        return "skipped"
    if success:
        return "success"
    text = str(error or "").strip().lower()
    if not text:
        return "failed"
    if any(tok in text for tok in ("cancel", "cancelled", "keyboardinterrupt", "sigint")):
        return "cancelled"
    if any(tok in text for tok in ("out of memory", "oom", "killed")):
        return "oom"
    if any(tok in text for tok in ("timeout", "time limit", "timed out")):
        return "timeout"
    if any(tok in text for tok in ("node fail", "slurmstepd", "connection reset", "temporary failure", "i/o error")):
        return "infra"
    return "failed"


def _extract_numeric_metric(outputs: Dict[str, Any], keys: List[str]) -> Optional[float]:
    if not isinstance(outputs, dict):
        return None
    for key in keys:
        if key not in outputs:
            continue
        raw = outputs.get(key)
        if raw in (None, ""):
            continue
        try:
            return float(raw)
        except (TypeError, ValueError):
            text = str(raw).strip().lower()
            m = re.match(r"^\s*([0-9]+(?:\.[0-9]+)?)\s*([gm]b?)?\s*$", text)
            if not m:
                continue
            num = float(m.group(1))
            unit = str(m.group(2) or "").strip().lower()
            if unit.startswith("m"):
                return num / 1024.0
            return num
    return None


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


_TPL_RE = re.compile(r"\{([^{}]+)\}")


def _lookup_ctx_path(ctx: Dict[str, Any], dotted: str) -> tuple[Any, bool]:
    cur: Any = ctx
    for part in str(dotted or "").split("."):
        if isinstance(cur, dict) and part in cur:
            cur = cur[part]
            continue
        return None, False
    return cur, True


def _resolve_text_with_ctx(value: str, ctx: Dict[str, Any]) -> str:
    text = str(value or "")

    def _repl(match: re.Match[str]) -> str:
        key = str(match.group(1) or "")
        found, ok = _lookup_ctx_path(ctx, key)
        if not ok or isinstance(found, (dict, list)):
            return match.group(0)
        return str(found)

    return _TPL_RE.sub(_repl, text)


def _resolve_with_ctx(value: Any, ctx: Dict[str, Any], *, max_passes: int = 20) -> Any:
    def _walk(v: Any) -> Any:
        if isinstance(v, str):
            return _resolve_text_with_ctx(v, ctx)
        if isinstance(v, list):
            return [_walk(x) for x in v]
        if isinstance(v, dict):
            return {k: _walk(x) for k, x in v.items()}
        return v

    cur = value
    for _ in range(max_passes):
        nxt = _walk(cur)
        if nxt == cur:
            return cur
        cur = nxt
    return cur


def _foreach_items_from_var(foreach: str, ctx: Dict[str, Any]) -> List[Any]:
    items, ok = _lookup_ctx_path(ctx, str(foreach or ""))
    if not ok:
        return []
    if not isinstance(items, (list, tuple)):
        raise RunError(f"`foreach` expects a list; got {type(items)} for {foreach}")
    return list(items)


def _foreach_items_from_glob(
    foreach_glob: str,
    *,
    foreach_kind: Optional[str],
    ctx: Dict[str, Any],
    max_passes: int,
) -> List[str]:
    pattern = str(_resolve_with_ctx(foreach_glob, ctx, max_passes=max_passes) or "").strip()
    if not pattern:
        return []
    matches = sorted(glob.glob(pattern, recursive=True))
    kind = str(foreach_kind or "any").strip().lower()
    if kind == "dirs":
        matches = [m for m in matches if Path(m).is_dir()]
    elif kind == "files":
        matches = [m for m in matches if Path(m).is_file()]
    deduped: List[str] = []
    seen: set[str] = set()
    for raw in matches:
        resolved = Path(raw).resolve().as_posix()
        if resolved in seen:
            continue
        seen.add(resolved)
        deduped.append(resolved)
    return deduped


def _expand_step(
    step: Step,
    *,
    ctx_vars: Dict[str, Any],
    run_id: str,
    run_started: datetime,
    job_name: str,
    resolve_max_passes: int,
) -> List[Step]:
    if not step.foreach and not step.foreach_glob:
        return [step]

    if step.foreach:
        items: List[Any] = _foreach_items_from_var(step.foreach, ctx_vars)
    else:
        items = _foreach_items_from_glob(
            str(step.foreach_glob or ""),
            foreach_kind=step.foreach_kind,
            ctx=ctx_vars,
            max_passes=resolve_max_passes,
        )
    expanded: List[Step] = []
    for idx, item in enumerate(items):
        item_text = str(item)
        item_path = Path(item_text)
        local_ctx = _with_runtime_sys(
            dict(ctx_vars),
            run_id=run_id,
            run_started=run_started,
            job_name=job_name,
            step_name=f"{step.name}_{idx}",
            step_index=idx,
        )
        local_ctx["item"] = item
        local_ctx["item_index"] = idx
        local_ctx["item_name"] = item_path.name
        local_ctx["item_stem"] = item_path.stem
        new_step = Step(
            name=f"{step.name}_{idx}",
            script=str(_resolve_with_ctx(step.script, local_ctx, max_passes=resolve_max_passes)),
            output_var=f"{step.output_var}_{idx}" if step.output_var else None,
            env=_resolve_with_ctx(step.env, local_ctx, max_passes=resolve_max_passes),
            resources=dict(step.resources or {}),
            when=step.when,
            parallel_with=step.parallel_with,
            foreach=None,
            foreach_glob=None,
            foreach_kind=None,
        )
        expanded.append(new_step)
    return expanded


def _with_runtime_sys(
    base_ctx: Dict[str, Any],
    *,
    run_id: str,
    run_started: datetime,
    job_name: str = "",
    step_name: str = "",
    step_id: str = "",
    step_index: Optional[int] = None,
) -> Dict[str, Any]:
    out = dict(base_ctx or {})
    sys_ns = {
        "run": {
            "id": str(run_id),
            "short_id": str(run_id)[:8],
        },
        "job": {
            "id": str(run_id),
            "name": str(job_name or ""),
        },
        "step": {
            "id": str(step_id or step_name or ""),
            "name": str(step_name or ""),
            "index": "" if step_index is None else str(step_index),
        },
        "now": {
            "iso_utc": run_started.isoformat() + "Z",
            "yymmdd": run_started.strftime("%y%m%d"),
            "hhmmss": run_started.strftime("%H%M%S"),
            "yymmdd_hhmmss": run_started.strftime("%y%m%d-%H%M%S"),
        },
    }
    out["sys"] = sys_ns
    # Convenience flat aliases used in historical templates.
    out["run_id"] = sys_ns["run"]["id"]
    out["job_id"] = sys_ns["job"]["id"]
    out["job_name"] = sys_ns["job"]["name"]
    out["step_id"] = sys_ns["step"]["id"]
    out["step_name"] = sys_ns["step"]["name"]
    out["step_index"] = sys_ns["step"]["index"]
    out["date"] = sys_ns["now"]["yymmdd"]
    out["time"] = sys_ns["now"]["hhmmss"]
    return out


def run_pipeline(
    pipeline: Pipeline,
    *,
    plugin_dir: Path,
    workdir: Path,
    logdir: Optional[Path] = None,
    run_id: Optional[str] = None,
    run_started: Optional[datetime] = None,
    dry_run: bool = False,
    max_retries: int = 0,
    retry_delay_seconds: float = 0.0,
    resume_succeeded_steps: Optional[set[str]] = None,
    prior_step_outputs: Optional[Dict[str, Dict[str, Any]]] = None,
    log_func=None,
    step_log_func=None,
) -> RunResult:
    run_id = run_id or uuid.uuid4().hex
    ts = run_started or datetime.utcnow()
    date_dir = ts.strftime("%y%m%d")
    run_dir = f"{ts.strftime('%H%M%S')}-{run_id[:8]}"
    # Idempotent run-directory stamping:
    # - if caller already provides current run stamp, reuse it
    # - if caller provides any stamped run dir for this run_id short suffix,
    #   reuse it as well (prevents nested date/time paths when timestamps drift).
    is_same_stamp = workdir.name == run_dir and workdir.parent.name == date_dir
    is_stamped_for_run = bool(re.fullmatch(r"\d{6}", workdir.parent.name)) and bool(
        re.fullmatch(rf"\d{{6}}-{re.escape(run_id[:8])}", workdir.name)
    )
    if is_same_stamp or is_stamped_for_run:
        base_workdir = workdir
    else:
        base_workdir = workdir / date_dir / run_dir
    base_workdir.mkdir(parents=True, exist_ok=True)

    def log(msg: str, level: str = "INFO") -> None:
        if log_func:
            try:
                log_func(msg, level)
            except TypeError:
                log_func(f"[{level}] {msg}")
        else:
            print(f"[{run_id}] [{level}] {msg}")

    step_results: List[StepResult] = []
    ctx_vars: Dict[str, Any] = dict(pipeline.vars)
    ctx_vars.update(pipeline.dirs)
    ctx_vars = _with_runtime_sys(
        ctx_vars,
        run_id=run_id,
        run_started=ts,
        job_name=str((pipeline.vars or {}).get("jobname") or ""),
    )
    prior_step_outputs = prior_step_outputs or {}
    resolve_max_passes = max(1, int(getattr(pipeline, "resolve_max_passes", 20) or 20))
    base_logdir: Path = base_workdir
    if logdir is not None:
        base_logdir = logdir
    else:
        for key in ("logdir", "log", "log_dir"):
            raw = str((pipeline.dirs or {}).get(key) or "").strip()
            if not raw:
                continue
            resolved = _resolve_with_ctx(raw, ctx_vars, max_passes=resolve_max_passes)
            resolved_text = str(resolved or "").strip()
            if resolved_text:
                base_logdir = Path(resolved_text)
                break
    base_logdir.mkdir(parents=True, exist_ok=True)

    batches = _batch_steps(pipeline.steps)
    failed = False
    for orig_batch in batches:
        expansion_ctx = dict(ctx_vars)
        expanded_for_batch: List[Step] = []
        for step in orig_batch:
            expanded_for_batch.extend(
                _expand_step(
                    step,
                    ctx_vars=expansion_ctx,
                    run_id=run_id,
                    run_started=ts,
                    job_name=str((pipeline.vars or {}).get("jobname") or ""),
                    resolve_max_passes=resolve_max_passes,
                )
            )
        run_batches = _batch_steps(expanded_for_batch)
        if resume_succeeded_steps:
            filtered_batches: List[List[Step]] = []
            for batch in run_batches:
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
            run_batches = filtered_batches

        for batch in run_batches:
            if len(batch) == 1:
                step = batch[0]
                res = _execute_step(
                    step,
                    run_id,
                    ts,
                    plugin_dir,
                    base_workdir,
                    base_logdir,
                    dry_run,
                    max_retries,
                    retry_delay_seconds,
                    log,
                    ctx_vars,
                    resolve_max_passes=resolve_max_passes,
                    step_index=len(step_results),
                    step_log_func=step_log_func,
                )
                step_results.append(res)
                if res.success and step.output_var:
                    ctx_vars[step.output_var] = res.outputs
                if not res.success and not res.skipped:
                    failed = True
                    break
            else:
                futures = []
                with ThreadPoolExecutor(max_workers=len(batch)) as pool:
                    for step in batch:
                        futures.append(
                            pool.submit(
                                _execute_step,
                                step,
                                run_id,
                                ts,
                                plugin_dir,
                                base_workdir,
                                base_logdir,
                                dry_run,
                                max_retries,
                                retry_delay_seconds,
                                log,
                                dict(ctx_vars),
                                resolve_max_passes,
                                None,
                                step_log_func,
                            )
                        )
                    for fut in as_completed(futures):
                        res = fut.result()
                        step_results.append(res)
                        if res.success and res.step.output_var:
                            ctx_vars[res.step.output_var] = res.outputs
                        if not res.success and not res.skipped:
                            pass
        if failed:
            break

    return RunResult(run_id=run_id, steps=step_results, artifact_dir=str(base_workdir))


def _execute_step(
    step: Step,
    run_id: str,
    run_started: datetime,
    plugin_dir: Path,
    base_workdir: Path,
    base_logdir: Path,
    dry_run: bool,
    max_retries: int,
    retry_delay_seconds: float,
    log,
    ctx_vars: Dict[str, Any],
    resolve_max_passes: int = 20,
    step_index: Optional[int] = None,
    step_log_func=None,
) -> StepResult:
    log(f"step {step.name}")
    step_id = uuid.uuid4().hex
    runtime_ctx = _with_runtime_sys(
        dict(ctx_vars),
        run_id=run_id,
        run_started=run_started,
        job_name=str(ctx_vars.get("job_name") or ""),
        step_name=step.name,
        step_id=step_id,
        step_index=step_index,
    )

    if not _eval_when(step.when, runtime_ctx):
        log(f"step {step.name} skipped (when={step.when})")
        return StepResult(step=step, success=True, step_id=step_id, skipped=True, attempt_no=0, attempts=[])
    script_runtime = str(_resolve_with_ctx(step.script, runtime_ctx, max_passes=resolve_max_passes))
    env_runtime = _resolve_with_ctx(step.env, runtime_ctx, max_passes=resolve_max_passes)
    plugin_ref, arg_tokens = _parse_script(script_runtime)
    try:
        plugin_path = _resolve_plugin_path(plugin_dir, plugin_ref)
        plugin = load_plugin(plugin_path)
    except Exception as exc:  # noqa: BLE001
        return StepResult(step=step, success=False, step_id=step_id, error=f"Plugin load failed: {exc}")

    args = _parse_args(arg_tokens)
    args = _apply_param_types(args, plugin.meta.params)
    args["env"] = env_runtime

    step_workdir = base_workdir / step.name / step_id
    step_logdir = base_logdir / step.name / step_id / "logs"

    sinks: List[Any] = [ConsoleLogSink(run_id, step.name), FileLogSink(step_logdir / "step.log")]
    if step_log_func is not None:
        sinks.append(CallbackLogSink(lambda level, message: step_log_func(step.name, message, level)))
    step_logger = StepLogger(CompositeLogSink(sinks))
    ctx = PluginContext(run_id=run_id, workdir=step_workdir, log=step_logger)

    if dry_run:
        log(f"dry-run -> {plugin_path} args={args}")
        started = datetime.utcnow().isoformat() + "Z"
        ended = datetime.utcnow().isoformat() + "Z"
        plugin_name = str(getattr(plugin.meta, "name", "") or "")
        plugin_version = str(getattr(plugin.meta, "version", "") or "")
        return StepResult(
            step=step,
            success=True,
            step_id=step_id,
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
                    "plugin_name": plugin_name,
                    "plugin_version": plugin_version,
                    "failure_category": "success",
                }
            ],
        )

    attempt_history: List[Dict[str, Any]] = []
    max_attempts = max(1, int(max_retries) + 1)
    plugin_name = str(getattr(plugin.meta, "name", "") or "")
    plugin_version = str(getattr(plugin.meta, "version", "") or "")
    for attempt_no in range(1, max_attempts + 1):
        original_env = os.environ.copy()
        started_dt = datetime.utcnow()
        started = started_dt.isoformat() + "Z"
        cpu_started = time.process_time()
        try:
            os.environ.update({str(k): str(v) for k, v in dict(env_runtime or {}).items()})
            outputs = plugin.run(args, ctx)
            if plugin.validate:
                plugin.validate(args, outputs, ctx)
            ended_dt = datetime.utcnow()
            ended = ended_dt.isoformat() + "Z"
            runtime_seconds = max(0.0, (ended_dt - started_dt).total_seconds())
            cpu_seconds = max(0.0, time.process_time() - cpu_started)
            engine_cpu_cores = _estimate_cpu_cores_used(cpu_seconds, runtime_seconds)
            engine_memory_gb = _bytes_to_gb(_current_rss_bytes())
            engine_cpu_count = os.cpu_count()
            memory_gb = _extract_numeric_metric(
                dict(outputs or {}),
                ["peak_memory_gb", "memory_gb", "max_rss_gb", "rss_gb", "peak_mem_gb"],
            )
            cpu_cores = _extract_numeric_metric(
                dict(outputs or {}),
                ["cpu_cores", "peak_cpu_cores", "used_cpu_cores", "cpu_count"],
            )
            if memory_gb is None:
                memory_gb = engine_memory_gb
            if cpu_cores is None:
                cpu_cores = engine_cpu_cores
            attempt_history.append(
                {
                    "attempt_no": attempt_no,
                    "success": True,
                    "skipped": False,
                    "error": None,
                    "outputs": outputs,
                    "started_at": started,
                    "ended_at": ended,
                    "plugin_name": plugin_name,
                    "plugin_version": plugin_version,
                    "failure_category": "success",
                    "runtime_seconds": runtime_seconds,
                    "memory_gb": memory_gb,
                    "cpu_cores": cpu_cores,
                    "cpu_seconds": cpu_seconds,
                    "cpu_count": engine_cpu_count,
                    "engine_memory_gb": engine_memory_gb,
                    "engine_cpu_cores": engine_cpu_cores,
                }
            )
            if attempt_no > 1:
                log(f"step {step.name} succeeded on attempt {attempt_no}")
            return StepResult(
                step=step,
                success=True,
                step_id=step_id,
                outputs=outputs,
                attempt_no=attempt_no,
                attempts=attempt_history,
            )
        except Exception as exc:  # noqa: BLE001
            ended_dt = datetime.utcnow()
            ended = ended_dt.isoformat() + "Z"
            err = str(exc)
            runtime_seconds = max(0.0, (ended_dt - started_dt).total_seconds())
            cpu_seconds = max(0.0, time.process_time() - cpu_started)
            engine_cpu_cores = _estimate_cpu_cores_used(cpu_seconds, runtime_seconds)
            engine_memory_gb = _bytes_to_gb(_current_rss_bytes())
            engine_cpu_count = os.cpu_count()
            attempt_history.append(
                {
                    "attempt_no": attempt_no,
                    "success": False,
                    "skipped": False,
                    "error": err,
                    "outputs": {},
                    "started_at": started,
                    "ended_at": ended,
                    "plugin_name": plugin_name,
                    "plugin_version": plugin_version,
                    "failure_category": _classify_attempt_failure(success=False, skipped=False, error=err),
                    "runtime_seconds": runtime_seconds,
                    "memory_gb": engine_memory_gb,
                    "cpu_cores": engine_cpu_cores,
                    "cpu_seconds": cpu_seconds,
                    "cpu_count": engine_cpu_count,
                    "engine_memory_gb": engine_memory_gb,
                    "engine_cpu_cores": engine_cpu_cores,
                }
            )
            if attempt_no < max_attempts:
                log(f"step {step.name} attempt {attempt_no}/{max_attempts} failed: {err}; retrying", "WARN")
                if retry_delay_seconds > 0:
                    time.sleep(retry_delay_seconds)
            else:
                return StepResult(
                    step=step,
                    success=False,
                    step_id=step_id,
                    error=err,
                    attempt_no=attempt_no,
                    attempts=attempt_history,
                )
        finally:
            os.environ.clear()
            os.environ.update(original_env)


__all__ = ["run_pipeline", "RunResult", "StepResult", "RunError"]

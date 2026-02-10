"""
Pipeline parsing and validation.

The goal is to keep the YAML flexible while enforcing enough structure to
catch authoring errors early. A pipeline file is expected to look like:

```yaml
vars:
  jobname: prism
dirs:
  datadir: {global.data}/{jobname}
steps:
  - name: fetch
    script: import/download_http.py {bindir}/urls.txt
    output_var: urls
```

We intentionally allow simple string templating via `{var}` using the
combined context of provided `global_vars` and pipeline `vars`.
"""

from __future__ import annotations

import copy
from dataclasses import dataclass, field
from pathlib import Path
import re
from typing import Any, Dict, List, Optional

import yaml


class PipelineError(ValueError):
    """Raised when pipeline parsing or validation fails."""


@dataclass
class Step:
    name: str
    script: str
    output_var: Optional[str] = None
    env: Dict[str, str] = field(default_factory=dict)
    when: Optional[str] = None  # simple expression string, evaluated later
    parallel_with: Optional[str] = None  # group key for parallel batches
    foreach: Optional[str] = None  # name of list variable to fan out over


@dataclass
class Pipeline:
    vars: Dict[str, Any] = field(default_factory=dict)
    dirs: Dict[str, Any] = field(default_factory=dict)
    requires_pipelines: List[str] = field(default_factory=list)
    steps: List[Step] = field(default_factory=list)


# ----------------------------
# Parsing helpers
# ----------------------------


_PLACEHOLDER_RE = re.compile(r"\{([^{}]+)\}")


def _lookup_path(ctx: Dict[str, Any], path: str) -> tuple[Any, bool]:
    current: Any = ctx
    for part in path.split("."):
        if isinstance(current, dict) and part in current:
            current = current[part]
            continue
        return None, False
    return current, True


def _resolve_string(value: str, ctx: Dict[str, Any]) -> Any:
    exact = _PLACEHOLDER_RE.fullmatch(value)
    if exact:
        resolved, ok = _lookup_path(ctx, exact.group(1))
        if ok:
            if isinstance(resolved, (dict, list)):
                return copy.deepcopy(resolved)
            return str(resolved)
        return value

    def _repl(match: re.Match[str]) -> str:
        resolved, ok = _lookup_path(ctx, match.group(1))
        if not ok or isinstance(resolved, (dict, list)):
            return match.group(0)
        return str(resolved)

    return _PLACEHOLDER_RE.sub(_repl, value)


def _interpolate(value: Any, ctx: Dict[str, Any]) -> Any:
    """Recursively interpolate strings using ctx; leave unresolved placeholders as-is."""
    if isinstance(value, str):
        return _resolve_string(value, ctx)
    if isinstance(value, list):
        return [_interpolate(v, ctx) for v in value]
    if isinstance(value, dict):
        return {k: _interpolate(v, ctx) for k, v in value.items()}
    return value


def _resolve_iterative(value: Any, ctx: Dict[str, Any], max_passes: int = 20) -> Any:
    current = copy.deepcopy(value)
    for _ in range(max_passes):
        nxt = _interpolate(current, ctx)
        if nxt == current:
            return current
        current = nxt
    return current


def _resolve_context_iterative(ctx: Dict[str, Any], max_passes: int = 20) -> Dict[str, Any]:
    current = copy.deepcopy(ctx)
    for _ in range(max_passes):
        nxt = _interpolate(current, current)
        if nxt == current:
            return current
        current = nxt
    return current


def _merge_with_namespace(ctx: Dict[str, Any], namespace: str, values: Dict[str, Any]) -> Dict[str, Any]:
    out = copy.deepcopy(ctx)
    ns_values = copy.deepcopy(values or {})
    out[namespace] = ns_values
    # Flat keys are intentionally overwritten by later merges.
    for k, v in ns_values.items():
        out[k] = copy.deepcopy(v)
    return out


def _normalize_step(raw: Any, index: int) -> Dict[str, Any]:
    if not isinstance(raw, dict):
        raise PipelineError(f"Step {index} must be a mapping")
    # allow {"step": {...}} or direct mapping
    if "step" in raw and isinstance(raw["step"], dict):
        raw = raw["step"]
    return raw


def parse_pipeline(
    path: Path,
    global_vars: Optional[Dict[str, Any]] = None,
    env_vars: Optional[Dict[str, Any]] = None,
    context_vars: Optional[Dict[str, Any]] = None,
) -> Pipeline:
    """Parse a pipeline YAML file into a Pipeline object."""
    if not path.exists():
        raise PipelineError(f"Pipeline file not found: {path}")
    with path.open("r", encoding="utf-8") as f:
        try:
            data = yaml.safe_load(f) or {}
        except yaml.YAMLError as exc:
            raise PipelineError(f"Invalid YAML: {exc}") from exc

    if not isinstance(data, dict):
        raise PipelineError("Pipeline file must contain a mapping at top level")

    vars_section = data.get("vars", {}) or {}
    dirs_section = data.get("dirs", {}) or {}
    requires_section = data.get("requires_pipelines", []) or []
    steps_section = data.get("steps", []) or []

    if not isinstance(vars_section, dict):
        raise PipelineError("`vars` must be a mapping")
    if not isinstance(dirs_section, dict):
        raise PipelineError("`dirs` must be a mapping")
    if not isinstance(requires_section, list):
        raise PipelineError("`requires_pipelines` must be a list")
    for idx, req in enumerate(requires_section):
        if not isinstance(req, str) or not req.strip():
            raise PipelineError(f"`requires_pipelines[{idx}]` must be a non-empty string")
    if not isinstance(steps_section, list):
        raise PipelineError("`steps` must be a list")

    ctx_raw: Dict[str, Any] = {}
    ctx_raw = _merge_with_namespace(ctx_raw, "global", global_vars or {})
    ctx_raw = _merge_with_namespace(ctx_raw, "env", env_vars or {})
    ctx_raw = _merge_with_namespace(ctx_raw, "pipe", vars_section)
    if context_vars:
        # Context vars are extra overlays (already-resolved call-site values).
        ctx_raw.update(copy.deepcopy(context_vars))
    ctx = _resolve_context_iterative(ctx_raw)

    vars_interp = _resolve_iterative(vars_section, ctx)

    ctx_dirs = copy.deepcopy(ctx)
    ctx_dirs.update(copy.deepcopy(vars_interp))
    ctx_dirs["vars"] = copy.deepcopy(vars_interp)
    ctx_dirs["dirs"] = copy.deepcopy(dirs_section)
    ctx_dirs = _resolve_context_iterative(ctx_dirs)
    dirs_interp = _resolve_iterative(dirs_section, ctx_dirs)

    step_ctx = copy.deepcopy(ctx_dirs)
    step_ctx["dirs"] = copy.deepcopy(dirs_interp)
    step_ctx.update(copy.deepcopy(dirs_interp))

    steps: List[Step] = []
    for idx, raw in enumerate(steps_section):
        step_map = _normalize_step(raw, idx)
        name = step_map.get("name") or f"step_{idx}"
        script = step_map.get("script")
        if not isinstance(script, str):
            raise PipelineError(f"Step {idx} missing required string `script`")
        output_var = step_map.get("output_var")
        if output_var is not None and not isinstance(output_var, str):
            raise PipelineError(f"Step {idx} `output_var` must be a string if provided")
        env = step_map.get("env", {}) or {}
        if not isinstance(env, dict):
            raise PipelineError(f"Step {idx} `env` must be a mapping")
        when = step_map.get("when")
        parallel_with = step_map.get("parallel_with")
        if parallel_with is not None and not isinstance(parallel_with, str):
            raise PipelineError(f"Step {idx} `parallel_with` must be a string if provided")
        foreach = step_map.get("foreach")
        if foreach is not None and not isinstance(foreach, str):
            raise PipelineError(f"Step {idx} `foreach` must be a string if provided")
        script_interp = _resolve_iterative(script, step_ctx)
        env_interp = _resolve_iterative(env, step_ctx)
        steps.append(
            Step(
                name=name,
                script=script_interp,
                output_var=output_var,
                env=env_interp,
                when=when,
                parallel_with=parallel_with,
                foreach=foreach,
            )
        )

    pipeline = Pipeline(
        vars=vars_interp,
        dirs=dirs_interp,
        requires_pipelines=[str(x).strip() for x in requires_section if str(x).strip()],
        steps=steps,
    )
    validate_pipeline(pipeline)  # raise if invalid
    return pipeline


# ----------------------------
# Validation
# ----------------------------


def validate_pipeline(p: Pipeline) -> None:
    """Raise PipelineError on validation problems."""
    errors: List[str] = []

    if not p.steps:
        errors.append("Pipeline must contain at least one step")

    for idx, step in enumerate(p.steps):
        if not step.script:
            errors.append(f"Step {idx} missing script")
        if step.output_var and not step.output_var.isidentifier():
            errors.append(f"Step {idx} output_var must be a valid identifier: {step.output_var}")

    if errors:
        raise PipelineError("; ".join(errors))


__all__ = [
    "Pipeline",
    "Step",
    "PipelineError",
    "parse_pipeline",
    "validate_pipeline",
]

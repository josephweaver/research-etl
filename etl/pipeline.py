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

import dataclasses
from dataclasses import dataclass, field
from pathlib import Path
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
    steps: List[Step] = field(default_factory=list)


# ----------------------------
# Parsing helpers
# ----------------------------


class _SafeDict(dict):
    """Missing keys remain untouched during format_map."""

    def __missing__(self, key):
        return "{" + key + "}"


def _interpolate(value: Any, ctx: Dict[str, Any]) -> Any:
    """Recursively interpolate strings using ctx; leave missing placeholders as-is."""
    if isinstance(value, str):
        return value.format_map(_SafeDict(ctx))
    if isinstance(value, list):
        return [_interpolate(v, ctx) for v in value]
    if isinstance(value, dict):
        return {k: _interpolate(v, ctx) for k, v in value.items()}
    return value


def _normalize_step(raw: Any, index: int) -> Dict[str, Any]:
    if not isinstance(raw, dict):
        raise PipelineError(f"Step {index} must be a mapping")
    # allow {"step": {...}} or direct mapping
    if "step" in raw and isinstance(raw["step"], dict):
        raw = raw["step"]
    return raw


def parse_pipeline(path: Path, global_vars: Optional[Dict[str, Any]] = None) -> Pipeline:
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
    steps_section = data.get("steps", []) or []

    if not isinstance(vars_section, dict):
        raise PipelineError("`vars` must be a mapping")
    if not isinstance(dirs_section, dict):
        raise PipelineError("`dirs` must be a mapping")
    if not isinstance(steps_section, list):
        raise PipelineError("`steps` must be a list")

    ctx: Dict[str, Any] = {}
    if global_vars:
        ctx.update(global_vars)
    ctx.update(vars_section)
    ctx.update({"global": global_vars or {}})

    vars_interp = _interpolate(vars_section, ctx)
    ctx.update(vars_interp)
    dirs_interp = _interpolate(dirs_section, ctx)
    ctx.update(dirs_interp)

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
        script_interp = _interpolate(script, ctx)
        env_interp = _interpolate(env, ctx)
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

    pipeline = Pipeline(vars=vars_interp, dirs=dirs_interp, steps=steps)
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

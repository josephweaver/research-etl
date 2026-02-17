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
import json
from dataclasses import dataclass, field
from pathlib import Path
import re
import shlex
from typing import Any, Dict, List, Optional, Set

import yaml
from .variable_solver import VariableSolver


class PipelineError(ValueError):
    """Raised when pipeline parsing or validation fails."""


@dataclass
class Step:
    name: str
    script: str
    output_var: Optional[str] = None
    env: Dict[str, str] = field(default_factory=dict)
    resources: Dict[str, Any] = field(default_factory=dict)
    when: Optional[str] = None  # simple expression string, evaluated later
    parallel_with: Optional[str] = None  # group key for parallel batches
    foreach: Optional[str] = None  # name of list variable to fan out over
    foreach_glob: Optional[str] = None  # glob pattern to fan out over filesystem paths
    foreach_kind: Optional[str] = None  # any|files|dirs (applies to foreach_glob)
    disabled: bool = False


@dataclass
class Pipeline:
    vars: Dict[str, Any] = field(default_factory=dict)
    dirs: Dict[str, Any] = field(default_factory=dict)
    requires_pipelines: List[str] = field(default_factory=list)
    workdir: Optional[str] = None
    project_id: Optional[str] = None
    shared_with_projects: List[str] = field(default_factory=list)
    resolve_max_passes: int = 20
    steps: List[Step] = field(default_factory=list)


# ----------------------------
# Parsing helpers
# ----------------------------


_PLACEHOLDER_RE = re.compile(r"\{([^{}]+)\}")
DEFAULT_RESOLVE_MAX_PASSES = 20
MIN_RESOLVE_MAX_PASSES = 1
MAX_RESOLVE_MAX_PASSES = 100


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


def _resolve_iterative(value: Any, ctx: Dict[str, Any], max_passes: int = DEFAULT_RESOLVE_MAX_PASSES) -> Any:
    current = copy.deepcopy(value)
    for _ in range(max_passes):
        nxt = _interpolate(current, ctx)
        if nxt == current:
            return current
        current = nxt
    return current


def _resolve_context_iterative(ctx: Dict[str, Any], max_passes: int = DEFAULT_RESOLVE_MAX_PASSES) -> Dict[str, Any]:
    current = copy.deepcopy(ctx)
    for _ in range(max_passes):
        nxt = _interpolate(current, current)
        if nxt == current:
            return current
        current = nxt
    return current


def resolve_max_passes_setting(
    *,
    global_vars: Optional[Dict[str, Any]] = None,
    env_vars: Optional[Dict[str, Any]] = None,
    default: int = DEFAULT_RESOLVE_MAX_PASSES,
) -> int:
    def _from_map(source: Optional[Dict[str, Any]]) -> Optional[int]:
        if not isinstance(source, dict):
            return None
        candidates = (
            source.get("resolve_max_passes"),
            source.get("resolver_max_passes"),
            source.get("var_resolve_max_passes"),
            (source.get("resolver") or {}).get("max_passes")
            if isinstance(source.get("resolver"), dict)
            else None,
        )
        for raw in candidates:
            if raw in (None, ""):
                continue
            try:
                value = int(raw)
            except (TypeError, ValueError):
                continue
            return max(MIN_RESOLVE_MAX_PASSES, min(MAX_RESOLVE_MAX_PASSES, value))
        return None

    env_value = _from_map(env_vars)
    if env_value is not None:
        return env_value
    global_value = _from_map(global_vars)
    if global_value is not None:
        return global_value
    return max(MIN_RESOLVE_MAX_PASSES, min(MAX_RESOLVE_MAX_PASSES, int(default)))


def _resolve_dirs_iterative(
    dirs_section: Dict[str, Any],
    base_ctx: Dict[str, Any],
    max_passes: int = DEFAULT_RESOLVE_MAX_PASSES,
) -> Dict[str, Any]:
    """
    Resolve dirs with sibling feedback so dir keys can reference each other.

    Example: cachedir: "{workdir}/raw" should bind to dirs.workdir (if present),
    not an older flat workdir injected by env/global context.
    """
    current = copy.deepcopy(dirs_section or {})
    for _ in range(max_passes):
        pass_ctx_base = copy.deepcopy(base_ctx)
        pass_ctx_base["dirs"] = copy.deepcopy(current)
        # Dirs are also available as flat keys; these must reflect current pass.
        pass_ctx_base.update(copy.deepcopy(current))

        nxt: Dict[str, Any] = {}
        pass_ctx_live = copy.deepcopy(pass_ctx_base)
        for key, value in current.items():
            key_text = str(key)
            entry_ctx = copy.deepcopy(pass_ctx_live)
            # Prevent direct self-recursion (`workdir: "{workdir}/..."`).
            entry_ctx.pop(key_text, None)
            dirs_ns = dict(entry_ctx.get("dirs") or {})
            dirs_ns.pop(key_text, None)
            entry_ctx["dirs"] = dirs_ns
            # Preserve fallback to pre-dir context when available.
            if key_text in base_ctx and not isinstance(base_ctx.get(key_text), (dict, list)):
                entry_ctx[key_text] = copy.deepcopy(base_ctx.get(key_text))
            resolved_value = _interpolate(value, entry_ctx)
            nxt[key_text] = resolved_value
            live_dirs = dict(pass_ctx_live.get("dirs") or {})
            live_dirs[key_text] = copy.deepcopy(resolved_value)
            pass_ctx_live["dirs"] = live_dirs
            pass_ctx_live[key_text] = copy.deepcopy(resolved_value)
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


def _arg_scalar_to_text(value: Any) -> str:
    if isinstance(value, bool):
        return "true" if value else "false"
    if isinstance(value, (int, float)):
        return str(value)
    if value is None:
        return ""
    if isinstance(value, (dict, list)):
        return json.dumps(value, ensure_ascii=True, separators=(",", ":"))
    return str(value)


def _compose_script_from_parts(
    *,
    plugin: str,
    args_map: Optional[Dict[str, Any]] = None,
    arg_list: Optional[List[Any]] = None,
) -> str:
    tokens: List[str] = [str(plugin).strip()]
    for key, raw_val in (args_map or {}).items():
        key_text = str(key).strip()
        if not key_text:
            continue
        value_text = _arg_scalar_to_text(raw_val)
        if value_text == "":
            continue
        tokens.append(f"{key_text}={value_text}")
    for item in (arg_list or []):
        value_text = _arg_scalar_to_text(item)
        if value_text == "":
            continue
        tokens.append(value_text)
    return shlex.join(tokens)


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
    metadata_section = data.get("metadata", {}) or {}
    project_id_raw = data.get("project_id")
    workdir_raw = data.get("workdir")
    shared_with_raw = data.get("shared_with_projects")
    if isinstance(metadata_section, dict):
        if project_id_raw is None:
            project_id_raw = metadata_section.get("project_id")
        if workdir_raw is None:
            workdir_raw = metadata_section.get("workdir")
        if shared_with_raw is None:
            shared_with_raw = metadata_section.get("shared_with_projects")
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
    if workdir_raw is not None and not isinstance(workdir_raw, str):
        raise PipelineError("`workdir` must be a string when provided")
    if project_id_raw is not None and not isinstance(project_id_raw, str):
        raise PipelineError("`project_id` must be a string when provided")
    if shared_with_raw is None:
        shared_with_raw = []
    if not isinstance(shared_with_raw, list):
        raise PipelineError("`shared_with_projects` must be a list when provided")
    for idx, owner in enumerate(shared_with_raw):
        if not isinstance(owner, str) or not owner.strip():
            raise PipelineError(f"`shared_with_projects[{idx}]` must be a non-empty string")
    if not isinstance(steps_section, list):
        raise PipelineError("`steps` must be a list")

    resolve_max_passes = resolve_max_passes_setting(global_vars=global_vars, env_vars=env_vars)
    global_ns = global_vars or {}
    solver = VariableSolver(max_passes=resolve_max_passes)
    solver.overlay("global", global_ns, add_namespace=True, add_flat=True)
    solver.overlay("globals", global_ns, add_namespace=True, add_flat=False)
    solver.overlay("env", env_vars or {}, add_namespace=True, add_flat=True)
    solver.overlay("pipe", vars_section, add_namespace=True, add_flat=True)
    if context_vars:
        # Context vars are extra overlays (already-resolved call-site values).
        solver.update(copy.deepcopy(context_vars))
    ctx = solver.resolved_context()

    vars_interp = _resolve_iterative(vars_section, ctx, max_passes=resolve_max_passes)

    ctx_dirs = copy.deepcopy(ctx)
    ctx_dirs.update(copy.deepcopy(vars_interp))
    ctx_dirs["vars"] = copy.deepcopy(vars_interp)
    ctx_dirs["dirs"] = copy.deepcopy(dirs_section)
    ctx_dirs = _resolve_context_iterative(ctx_dirs, max_passes=resolve_max_passes)
    dirs_interp = _resolve_dirs_iterative(dirs_section, ctx_dirs, max_passes=resolve_max_passes)

    step_ctx = copy.deepcopy(ctx_dirs)
    step_ctx["dirs"] = copy.deepcopy(dirs_interp)
    step_ctx.update(copy.deepcopy(dirs_interp))

    steps: List[Step] = []
    for idx, raw in enumerate(steps_section):
        step_map = _normalize_step(raw, idx)
        name = step_map.get("name") or f"step_{idx}"
        script = step_map.get("script")
        plugin = step_map.get("plugin")
        args_map = step_map.get("args")
        arg_list = step_map.get("arg_list")
        if script is not None and plugin is not None:
            raise PipelineError(f"Step {idx} may not define both `script` and `plugin`.")
        if script is None and plugin is None:
            raise PipelineError(f"Step {idx} missing required `script` or `plugin`.")
        if plugin is not None and not isinstance(plugin, str):
            raise PipelineError(f"Step {idx} `plugin` must be a string when provided")
        if args_map is None:
            args_map = {}
        if args_map is not None and not isinstance(args_map, dict):
            raise PipelineError(f"Step {idx} `args` must be a mapping when provided")
        if arg_list is None:
            arg_list = []
        if arg_list is not None and not isinstance(arg_list, list):
            raise PipelineError(f"Step {idx} `arg_list` must be a list when provided")
        if isinstance(script, list):
            if not all(not isinstance(tok, (dict, list)) for tok in script):
                raise PipelineError(f"Step {idx} `script` list entries must be scalar values")
            script = shlex.join([str(tok) for tok in script])
        if script is not None and not isinstance(script, str):
            raise PipelineError(f"Step {idx} `script` must be a string or list of scalar tokens")
        if plugin is not None:
            script = _compose_script_from_parts(plugin=plugin, args_map=args_map, arg_list=arg_list)
        assert isinstance(script, str)
        output_var = step_map.get("output_var")
        if output_var is not None and not isinstance(output_var, str):
            raise PipelineError(f"Step {idx} `output_var` must be a string if provided")
        env = step_map.get("env", {}) or {}
        if not isinstance(env, dict):
            raise PipelineError(f"Step {idx} `env` must be a mapping")
        resources = step_map.get("resources", {}) or {}
        if not isinstance(resources, dict):
            raise PipelineError(f"Step {idx} `resources` must be a mapping")
        when = step_map.get("when")
        parallel_with = step_map.get("parallel_with")
        if parallel_with is not None and not isinstance(parallel_with, str):
            raise PipelineError(f"Step {idx} `parallel_with` must be a string if provided")
        foreach = step_map.get("foreach")
        if foreach is not None and not isinstance(foreach, str):
            raise PipelineError(f"Step {idx} `foreach` must be a string if provided")
        foreach_glob = step_map.get("foreach_glob")
        if foreach_glob is not None and not isinstance(foreach_glob, str):
            raise PipelineError(f"Step {idx} `foreach_glob` must be a string if provided")
        if foreach and foreach_glob:
            raise PipelineError(f"Step {idx} may not define both `foreach` and `foreach_glob`.")
        foreach_kind = step_map.get("foreach_kind")
        if foreach_kind is not None and not isinstance(foreach_kind, str):
            raise PipelineError(f"Step {idx} `foreach_kind` must be a string if provided")
        foreach_kind_norm: Optional[str] = None
        if foreach_kind is not None:
            foreach_kind_norm = str(foreach_kind).strip().lower()
            if foreach_kind_norm not in {"any", "files", "dirs"}:
                raise PipelineError(f"Step {idx} `foreach_kind` must be one of: any, files, dirs")
            if foreach_glob is None:
                raise PipelineError(f"Step {idx} `foreach_kind` requires `foreach_glob`.")
        disabled = step_map.get("disabled")
        if disabled is None and "Disabled" in step_map:
            disabled = step_map.get("Disabled")
        if disabled is None:
            disabled = False
        if not isinstance(disabled, bool):
            raise PipelineError(f"Step {idx} `disabled` must be a boolean if provided")
        if disabled:
            continue
        script_interp = _resolve_iterative(script, step_ctx, max_passes=resolve_max_passes)
        env_interp = _resolve_iterative(env, step_ctx, max_passes=resolve_max_passes)
        foreach_glob_interp: Optional[str] = None
        if foreach_glob is not None:
            foreach_glob_interp = _resolve_iterative(foreach_glob, step_ctx, max_passes=resolve_max_passes)
        steps.append(
            Step(
                name=name,
                script=script_interp,
                output_var=output_var,
                env=env_interp,
                resources=copy.deepcopy(resources),
                when=when,
                parallel_with=parallel_with,
                foreach=foreach,
                foreach_glob=foreach_glob_interp,
                foreach_kind=foreach_kind_norm,
                disabled=False,
            )
        )

    pipeline = Pipeline(
        vars=vars_interp,
        dirs=dirs_interp,
        requires_pipelines=[str(x).strip() for x in requires_section if str(x).strip()],
        workdir=str(workdir_raw).strip() if isinstance(workdir_raw, str) and str(workdir_raw).strip() else None,
        project_id=str(project_id_raw).strip() if isinstance(project_id_raw, str) and project_id_raw.strip() else None,
        shared_with_projects=[str(x).strip() for x in shared_with_raw if str(x).strip()],
        resolve_max_passes=resolve_max_passes,
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
    closed_parallel_groups: Set[str] = set()
    active_parallel_group: Optional[str] = None

    if not p.steps:
        errors.append("Pipeline must contain at least one step")

    for idx, step in enumerate(p.steps):
        if not step.script:
            errors.append(f"Step {idx} missing script")
        if step.output_var and not step.output_var.isidentifier():
            errors.append(f"Step {idx} output_var must be a valid identifier: {step.output_var}")

        token = (step.parallel_with or "").strip()
        if token:
            if active_parallel_group is None:
                if token in closed_parallel_groups:
                    errors.append(
                        f"Step {idx} `parallel_with={token}` is out of order; "
                        "parallel groups must be contiguous."
                    )
                active_parallel_group = token
            elif token != active_parallel_group:
                closed_parallel_groups.add(active_parallel_group)
                if token in closed_parallel_groups:
                    errors.append(
                        f"Step {idx} `parallel_with={token}` is out of order; "
                        "parallel groups must be contiguous."
                    )
                active_parallel_group = token
        else:
            if active_parallel_group is not None:
                closed_parallel_groups.add(active_parallel_group)
                active_parallel_group = None

    if errors:
        raise PipelineError("; ".join(errors))


__all__ = [
    "Pipeline",
    "Step",
    "PipelineError",
    "DEFAULT_RESOLVE_MAX_PASSES",
    "MIN_RESOLVE_MAX_PASSES",
    "MAX_RESOLVE_MAX_PASSES",
    "resolve_max_passes_setting",
    "parse_pipeline",
    "validate_pipeline",
]

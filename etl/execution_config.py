"""
Execution environment configuration loader.

Used to define multiple execution targets (e.g., different HPCC clusters),
each with its own SLURM settings and paths.
"""

from __future__ import annotations

from pathlib import Path
from typing import Any, Dict, Optional

import yaml
import os
import copy
import re

from .pipeline import resolve_max_passes_setting


class ExecutionConfigError(ValueError):
    """Raised when environments configuration cannot be loaded."""


DEFAULT_ENV_CONFIG_PATH = Path("config/environments.yml")
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
    if isinstance(value, str):
        return _resolve_string(value, ctx)
    if isinstance(value, list):
        return [_interpolate(v, ctx) for v in value]
    if isinstance(value, dict):
        return {k: _interpolate(v, ctx) for k, v in value.items()}
    return value


def _resolve_context_iterative(ctx: Dict[str, Any], max_passes: int) -> Dict[str, Any]:
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
    for k, v in ns_values.items():
        out[k] = copy.deepcopy(v)
    return out


def resolve_execution_config_path(path: Optional[Path]) -> Optional[Path]:
    """Resolve environments config path."""
    if path:
        candidate = Path(path)
        if candidate.exists():
            return candidate
        raise ExecutionConfigError(f"Environments config not found: {candidate}")
    if DEFAULT_ENV_CONFIG_PATH.exists():
        return DEFAULT_ENV_CONFIG_PATH
    return None


def load_execution_config(path: Path) -> Dict[str, Dict[str, Any]]:
    if not path.exists():
        raise ExecutionConfigError(f"Environments config not found: {path}")
    try:
        with path.open("r", encoding="utf-8") as f:
            data = yaml.safe_load(f) or {}
    except yaml.YAMLError as exc:
        raise ExecutionConfigError(f"Invalid YAML in {path}: {exc}") from exc
    if not isinstance(data, dict):
        raise ExecutionConfigError("Environments config must be a mapping at the top level")
    envs = data.get("environments", {})
    if not isinstance(envs, dict):
        raise ExecutionConfigError("`environments` must be a mapping of name -> settings")
    return envs


def validate_environment_executor(env_name: str, env: Dict[str, Any], *, executor: Optional[str]) -> None:
    expected = str(executor or "").strip().lower()
    if not expected:
        return
    declared = str(env.get("executor") or "").strip().lower()
    if not declared:
        return
    if declared != expected:
        raise ExecutionConfigError(
            f"Environment '{env_name}' is for executor '{declared}', not '{expected}'."
        )


def apply_execution_env_overrides(env: Dict[str, Any]) -> Dict[str, Any]:
    """
    Apply environment-variable overrides for common SLURM limits/concurrency.

    Supported overrides:
      ETL_SLURM_JOB_LIMIT -> job_limit (int)
      ETL_SLURM_ARRAY_LIMIT -> array_task_limit (int)
      ETL_MAX_PARALLEL -> max_parallel (int)
    """
    out = dict(env)
    if "ETL_SLURM_JOB_LIMIT" in os.environ:
        try:
            out["job_limit"] = int(os.environ["ETL_SLURM_JOB_LIMIT"])
        except ValueError as exc:
            raise ExecutionConfigError("ETL_SLURM_JOB_LIMIT must be an integer.") from exc
    if "ETL_SLURM_ARRAY_LIMIT" in os.environ:
        try:
            out["array_task_limit"] = int(os.environ["ETL_SLURM_ARRAY_LIMIT"])
        except ValueError as exc:
            raise ExecutionConfigError("ETL_SLURM_ARRAY_LIMIT must be an integer.") from exc
    if "ETL_MAX_PARALLEL" in os.environ:
        try:
            out["max_parallel"] = int(os.environ["ETL_MAX_PARALLEL"])
        except ValueError as exc:
            raise ExecutionConfigError("ETL_MAX_PARALLEL must be an integer.") from exc
    return out


def resolve_execution_env_templates(
    env: Dict[str, Any],
    *,
    global_vars: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    """
    Resolve templated execution env values (for example `{basedir}/jobs`)
    using the same namespace precedence model as pipeline parsing:
    global -> env.
    """
    if not isinstance(env, dict):
        return {}
    max_passes = resolve_max_passes_setting(global_vars=global_vars, env_vars=env)
    ctx: Dict[str, Any] = {}
    ctx = _merge_with_namespace(ctx, "global", global_vars or {})
    ctx = _merge_with_namespace(ctx, "globals", global_vars or {})
    ctx = _merge_with_namespace(ctx, "env", env)
    resolved = _resolve_context_iterative(ctx, max_passes=max_passes)
    env_resolved = resolved.get("env")
    if isinstance(env_resolved, dict):
        return copy.deepcopy(env_resolved)
    return copy.deepcopy(env)


__all__ = [
    "load_execution_config",
    "apply_execution_env_overrides",
    "resolve_execution_config_path",
    "validate_environment_executor",
    "resolve_execution_env_templates",
    "ExecutionConfigError",
]

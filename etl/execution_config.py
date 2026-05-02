# research-etl
# Copyright (c) 2026 Joseph Weaver
# This file is part of the research-etl project and is licensed under the MIT License.
# You may not use this file except in compliance with the License.
# See https://github.com/josephweaver/research-etl for details.

"""
Execution environment configuration loader.

Used to define multiple execution targets (e.g., different HPCC clusters),
each with its own SLURM settings and paths.
"""

from __future__ import annotations

from pathlib import Path
import platform
import socket
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
_MODULE_REPO_ROOT = Path(__file__).resolve().parents[1]


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
        repo_root_env = str(os.environ.get("ETL_REPO_ROOT") or "").strip()
        if repo_root_env and not candidate.is_absolute():
            repo_candidate = (Path(repo_root_env).expanduser().resolve() / candidate).resolve()
            if repo_candidate.exists():
                return repo_candidate
        module_candidate = (_MODULE_REPO_ROOT / candidate).resolve() if not candidate.is_absolute() else None
        if module_candidate is not None and module_candidate.exists():
            return module_candidate
        raise ExecutionConfigError(f"Environments config not found: {candidate}")
    if DEFAULT_ENV_CONFIG_PATH.exists():
        return DEFAULT_ENV_CONFIG_PATH
    module_candidate = (_MODULE_REPO_ROOT / DEFAULT_ENV_CONFIG_PATH).resolve()
    if module_candidate.exists():
        return module_candidate
    repo_root_env = str(os.environ.get("ETL_REPO_ROOT") or "").strip()
    if repo_root_env:
        repo_candidate = (Path(repo_root_env).expanduser().resolve() / DEFAULT_ENV_CONFIG_PATH).resolve()
        if repo_candidate.exists():
            return repo_candidate
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


def _env_role(env: Dict[str, Any]) -> str:
    return str(env.get("role") or env.get("environment_role") or env.get("environment_type") or "").strip().lower()


def environment_is_local(env: Dict[str, Any]) -> bool:
    """Return true when an environment describes the process running `etl`."""
    role = _env_role(env)
    if role in {"local", "control", "both", "worker"}:
        return True
    if role in {"remote", "target", "scheduler"}:
        return False
    return str(env.get("executor") or "").strip().lower() == "local"


def local_environment_facts() -> Dict[str, Any]:
    system = platform.system().lower()
    os_name = "windows" if system == "windows" else system
    return {
        "os": os_name,
        "is_windows": os.name == "nt",
        "is_posix": os.name == "posix",
        "hostname": socket.gethostname(),
        "cwd": Path.cwd().resolve().as_posix(),
    }


def _detect_rule_matches(rule: Dict[str, Any], facts: Dict[str, Any]) -> bool:
    expected_os = str(rule.get("os") or "").strip().lower()
    if expected_os:
        aliases = {
            "unix": {"linux", "darwin", "posix"},
            "posix": {"linux", "darwin", "posix"},
            "mac": {"darwin"},
            "macos": {"darwin"},
            "windows": {"windows"},
        }
        allowed = aliases.get(expected_os, {expected_os})
        fact_os = str(facts.get("os") or "").strip().lower()
        if fact_os not in allowed and not (expected_os in {"unix", "posix"} and bool(facts.get("is_posix"))):
            return False

    host_re = str(rule.get("hostname_regex") or rule.get("host_regex") or "").strip()
    if host_re and not re.search(host_re, str(facts.get("hostname") or ""), flags=re.IGNORECASE):
        return False

    paths_raw = rule.get("path_exists") or rule.get("paths_exist") or []
    paths = paths_raw if isinstance(paths_raw, list) else [paths_raw]
    for raw in paths:
        text = str(raw or "").strip()
        if text and not Path(text).expanduser().exists():
            return False

    cwd_contains = str(rule.get("cwd_contains") or "").strip()
    if cwd_contains and cwd_contains not in str(facts.get("cwd") or ""):
        return False

    env_var = str(rule.get("env_var") or "").strip()
    if env_var and not str(os.environ.get(env_var) or "").strip():
        return False

    return True


def detect_control_environment(
    envs: Dict[str, Dict[str, Any]],
    *,
    selected_env_name: Optional[str] = None,
    explicit_control_env: Optional[str] = None,
) -> Optional[str]:
    """Select the local/control environment for the current process."""
    requested = str(explicit_control_env or os.environ.get("ETL_CONTROL_ENV") or "").strip()
    if requested and requested.lower() != "auto":
        if requested not in envs:
            raise ExecutionConfigError(f"Control env '{requested}' not found in config")
        if not environment_is_local(envs.get(requested, {}) or {}):
            raise ExecutionConfigError(f"Control env '{requested}' is not marked local/control")
        return requested

    if selected_env_name and selected_env_name in envs:
        selected = envs.get(selected_env_name, {}) or {}
        if environment_is_local(selected):
            return selected_env_name

    facts = local_environment_facts()
    for name, env in envs.items():
        if not environment_is_local(env or {}):
            continue
        detect = (env or {}).get("detect")
        rules = detect if isinstance(detect, list) else ([detect] if isinstance(detect, dict) else [])
        if rules and any(_detect_rule_matches(rule, facts) for rule in rules if isinstance(rule, dict)):
            return str(name)

    if bool(facts.get("is_windows")) and "local" in envs and environment_is_local(envs["local"]):
        return "local"
    if not bool(facts.get("is_windows")):
        for candidate in ("hpcc_local", "unix_local", "local"):
            env = envs.get(candidate)
            if env and environment_is_local(env):
                detect = env.get("detect")
                if candidate == "hpcc_local" and detect:
                    rules = detect if isinstance(detect, list) else ([detect] if isinstance(detect, dict) else [])
                    if not any(_detect_rule_matches(rule, facts) for rule in rules if isinstance(rule, dict)):
                        continue
                return candidate
    return None


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
    "detect_control_environment",
    "environment_is_local",
    "local_environment_facts",
    "resolve_execution_env_templates",
    "ExecutionConfigError",
]

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


class ExecutionConfigError(ValueError):
    """Raised when environments configuration cannot be loaded."""


DEFAULT_ENV_CONFIG_PATH = Path("config/environments.yml")


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


__all__ = [
    "load_execution_config",
    "apply_execution_env_overrides",
    "resolve_execution_config_path",
    "validate_environment_executor",
    "ExecutionConfigError",
]

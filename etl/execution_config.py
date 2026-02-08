"""
Execution environment configuration loader.

Used to define multiple execution targets (e.g., different HPCC clusters),
each with its own SLURM settings and paths.
"""

from __future__ import annotations

from pathlib import Path
from typing import Any, Dict

import yaml
import os


class ExecutionConfigError(ValueError):
    """Raised when execution configuration cannot be loaded."""


def load_execution_config(path: Path) -> Dict[str, Dict[str, Any]]:
    if not path.exists():
        raise ExecutionConfigError(f"Execution config not found: {path}")
    try:
        with path.open("r", encoding="utf-8") as f:
            data = yaml.safe_load(f) or {}
    except yaml.YAMLError as exc:
        raise ExecutionConfigError(f"Invalid YAML in {path}: {exc}") from exc
    if not isinstance(data, dict):
        raise ExecutionConfigError("Execution config must be a mapping at the top level")
    envs = data.get("environments", {})
    if not isinstance(envs, dict):
        raise ExecutionConfigError("`environments` must be a mapping of name -> settings")
    return envs


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


__all__ = ["load_execution_config", "apply_execution_env_overrides", "ExecutionConfigError"]

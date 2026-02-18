"""
Project/tenant helpers.

Project partitioning is path- and metadata-aware:
- preferred explicit value from caller or pipeline YAML
- fallback inferred from `pipelines/<project_id>/...`
"""

from __future__ import annotations

import re
from pathlib import Path
from typing import Any, Dict, Optional

import yaml


_PROJECT_TOKEN_RE = re.compile(r"[^a-z0-9_-]+")


class ProjectConfigError(RuntimeError):
    """Raised when project config is invalid."""


def normalize_project_id(value: Optional[str]) -> Optional[str]:
    text = str(value or "").strip().lower()
    if not text:
        return None
    text = text.replace(" ", "_")
    text = _PROJECT_TOKEN_RE.sub("_", text).strip("_")
    return text or None


def infer_project_id_from_pipeline_path(pipeline_path: str | Path) -> Optional[str]:
    try:
        path = Path(pipeline_path)
    except Exception:
        return None
    parts = [p for p in path.as_posix().split("/") if p]
    if not parts:
        return None
    for idx, part in enumerate(parts):
        if part.lower() == "pipelines":
            if idx + 1 >= len(parts):
                return None
            candidate = parts[idx + 1]
            # Flat pipelines/sample.yml -> no project segment.
            if "." in candidate and candidate.lower().endswith((".yml", ".yaml")):
                return None
            return normalize_project_id(candidate)
    return None


def resolve_project_id(
    *,
    explicit_project_id: Optional[str] = None,
    pipeline_project_id: Optional[str] = None,
    pipeline_path: Optional[str | Path] = None,
    default_project_id: str = "default",
) -> str:
    explicit = normalize_project_id(explicit_project_id)
    if explicit:
        return explicit
    pipeline_value = normalize_project_id(pipeline_project_id)
    if pipeline_value:
        return pipeline_value
    if pipeline_path is not None:
        inferred = infer_project_id_from_pipeline_path(pipeline_path)
        if inferred:
            return inferred
    fallback = normalize_project_id(default_project_id)
    return fallback or "default"


def resolve_projects_config_path(path: Optional[Path]) -> Optional[Path]:
    """Resolve project config path with sensible defaults."""
    if path is not None:
        p = Path(path)
        if not p.exists():
            raise ProjectConfigError(f"Projects config not found: {p}")
        return p.resolve()
    candidate = Path("config/projects.yml")
    return candidate.resolve() if candidate.exists() else None


def _coerce_project_vars_block(block: Any, *, label: str) -> Dict[str, Any]:
    if block is None:
        return {}
    if not isinstance(block, dict):
        raise ProjectConfigError(f"{label} must be a mapping")
    raw_vars = block.get("vars", block.get("project_vars", block))
    if raw_vars is None:
        return {}
    if not isinstance(raw_vars, dict):
        raise ProjectConfigError(f"{label}.vars must be a mapping")
    return dict(raw_vars)


def load_project_vars(*, project_id: Optional[str], projects_config_path: Optional[Path]) -> Dict[str, Any]:
    """Load project-scoped vars from config/projects.yml."""
    if projects_config_path is None:
        return {}
    cfg_path = Path(projects_config_path)
    if not cfg_path.exists():
        raise ProjectConfigError(f"Projects config not found: {cfg_path}")
    with cfg_path.open("r", encoding="utf-8") as f:
        try:
            data = yaml.safe_load(f) or {}
        except yaml.YAMLError as exc:
            raise ProjectConfigError(f"Invalid projects config YAML: {exc}") from exc
    if not isinstance(data, dict):
        raise ProjectConfigError("Projects config must be a mapping at top level")

    projects_section = data.get("projects")
    projects_map: Dict[str, Any]
    if projects_section is None:
        projects_map = data
    elif isinstance(projects_section, dict):
        projects_map = projects_section
    else:
        raise ProjectConfigError("`projects` must be a mapping when provided")

    default_vars = {}
    explicit_default = projects_map.get("default")
    if explicit_default is not None:
        default_vars = _coerce_project_vars_block(explicit_default, label="projects.default")

    pid = normalize_project_id(project_id)
    if not pid:
        return dict(default_vars)

    selected_block = None
    for key, value in projects_map.items():
        if normalize_project_id(str(key)) == pid:
            selected_block = value
            break
    if selected_block is None:
        return dict(default_vars)

    selected_vars = _coerce_project_vars_block(selected_block, label=f"projects.{pid}")
    merged = dict(default_vars)
    merged.update(selected_vars)
    return merged


__all__ = [
    "ProjectConfigError",
    "normalize_project_id",
    "infer_project_id_from_pipeline_path",
    "resolve_project_id",
    "resolve_projects_config_path",
    "load_project_vars",
]

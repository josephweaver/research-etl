"""
Project/tenant helpers.

Project partitioning is path- and metadata-aware:
- preferred explicit value from caller or pipeline YAML
- fallback inferred from `pipelines/<project_id>/...`
"""

from __future__ import annotations

import re
from pathlib import Path
from typing import Optional


_PROJECT_TOKEN_RE = re.compile(r"[^a-z0-9_-]+")


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


__all__ = [
    "normalize_project_id",
    "infer_project_id_from_pipeline_path",
    "resolve_project_id",
]

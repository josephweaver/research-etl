# research-etl
# Copyright (c) 2026 Joseph Weaver
# This file is part of the research-etl project and is licensed under the MIT License.
# You may not use this file except in compliance with the License.
# See https://github.com/josephweaver/research-etl for details.

from __future__ import annotations

from pathlib import Path
from typing import Any, Dict, Optional

import yaml


class SourceControlConfigError(ValueError):
    """Raised when source-control config cannot be loaded or resolved."""


DEFAULT_SOURCE_CONTROL_CONFIG_PATHS = (
    Path("config/source_control.yml"),
    Path("config/source_control.example.yml"),
)


def resolve_source_control_config_path(path: Optional[str | Path]) -> Optional[Path]:
    if path:
        candidate = Path(path)
        if candidate.exists():
            return candidate
        raise SourceControlConfigError(f"Source-control config not found: {candidate}")
    for candidate in DEFAULT_SOURCE_CONTROL_CONFIG_PATHS:
        if candidate.exists():
            return candidate
    return None


def load_source_control_config(path: Path) -> Dict[str, Any]:
    if not path.exists():
        raise SourceControlConfigError(f"Source-control config not found: {path}")
    try:
        with path.open("r", encoding="utf-8") as f:
            data = yaml.safe_load(f) or {}
    except yaml.YAMLError as exc:
        raise SourceControlConfigError(f"Invalid YAML in {path}: {exc}") from exc
    if not isinstance(data, dict):
        raise SourceControlConfigError("Source-control config must be a mapping at the top level")
    return data


def resolve_repo_config(
    *,
    repo_alias: str,
    config_path: Optional[str | Path] = None,
) -> Dict[str, Any]:
    alias = str(repo_alias or "").strip()
    if not alias:
        raise SourceControlConfigError("repo_alias is required")
    path = resolve_source_control_config_path(config_path)
    if path is None:
        raise SourceControlConfigError(
            "No source-control config found. Create config/source_control.yml or pass config_path."
        )
    data = load_source_control_config(path)
    repos = data.get("repositories")
    if not isinstance(repos, dict):
        raise SourceControlConfigError("Source-control config must contain a 'repositories' mapping")
    raw = repos.get(alias)
    if not isinstance(raw, dict):
        raise SourceControlConfigError(f"Repository alias not found: {alias}")
    out = dict(raw)
    out["repo_alias"] = alias
    out["config_path"] = str(path)
    return out


__all__ = [
    "DEFAULT_SOURCE_CONTROL_CONFIG_PATHS",
    "SourceControlConfigError",
    "load_source_control_config",
    "resolve_repo_config",
    "resolve_source_control_config_path",
]

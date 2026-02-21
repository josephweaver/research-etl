# research-etl
# Copyright (c) 2026 Joseph Weaver
# This file is part of the research-etl project and is licensed under the MIT License.
# You may not use this file except in compliance with the License.
# See https://github.com/josephweaver/research-etl for details.

"""
Global configuration loader.

Expected format (YAML):
```yaml
data: /data
bin: /opt/etl/bin
log: /data/logs
```
The loaded dict is passed into pipeline parsing as `global` for templating.
"""

from __future__ import annotations

from pathlib import Path
from typing import Any, Dict, Optional

import yaml


class ConfigError(ValueError):
    """Raised when global config cannot be loaded."""


DEFAULT_GLOBAL_CONFIG_PATHS = (
    Path("config/global.yml"),
    Path("config/globals.yml"),
)


def resolve_global_config_path(path: Optional[Path]) -> Optional[Path]:
    """Resolve global config path with sensible defaults."""
    if path:
        candidate = Path(path)
        if candidate.exists():
            return candidate
        raise ConfigError(f"Global config not found: {candidate}")
    for candidate in DEFAULT_GLOBAL_CONFIG_PATHS:
        if candidate.exists():
            return candidate
    return None


def load_global_config(path: Path) -> Dict[str, Any]:
    if not path.exists():
        raise ConfigError(f"Global config not found: {path}")
    try:
        with path.open("r", encoding="utf-8") as f:
            data = yaml.safe_load(f) or {}
    except yaml.YAMLError as exc:
        raise ConfigError(f"Invalid YAML in {path}: {exc}") from exc
    if not isinstance(data, dict):
        raise ConfigError("Global config must be a mapping at the top level")
    return data


__all__ = ["load_global_config", "resolve_global_config_path", "ConfigError"]

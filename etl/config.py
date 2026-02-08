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
from typing import Any, Dict

import yaml


class ConfigError(ValueError):
    """Raised when global config cannot be loaded."""


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


__all__ = ["load_global_config", "ConfigError"]

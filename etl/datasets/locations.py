from __future__ import annotations

from pathlib import Path
from typing import Any, Dict, Optional

import yaml


class DataLocationConfigError(ValueError):
    """Raised when data location configuration cannot be loaded/resolved."""


DEFAULT_DATA_LOCATIONS_PATHS = (
    Path("config/data_locations.yml"),
    Path("config/data_locations.yaml"),
)


def resolve_data_locations_config_path(path: Optional[Path]) -> Optional[Path]:
    if path:
        candidate = Path(path)
        if candidate.exists():
            return candidate
        raise DataLocationConfigError(f"Data locations config not found: {candidate}")
    for candidate in DEFAULT_DATA_LOCATIONS_PATHS:
        if candidate.exists():
            return candidate
    return None


def load_data_locations(path: Path) -> Dict[str, Any]:
    if not path.exists():
        raise DataLocationConfigError(f"Data locations config not found: {path}")
    try:
        with path.open("r", encoding="utf-8") as f:
            data = yaml.safe_load(f) or {}
    except yaml.YAMLError as exc:
        raise DataLocationConfigError(f"Invalid YAML in {path}: {exc}") from exc
    if not isinstance(data, dict):
        raise DataLocationConfigError("Data locations config must be a mapping at the top level")
    locations = data.get("locations")
    if locations is None:
        return {"locations": {}}
    if not isinstance(locations, dict):
        raise DataLocationConfigError("`locations` must be a mapping")
    out: Dict[str, Any] = {}
    for alias, spec in locations.items():
        key = str(alias or "").strip()
        if not key:
            continue
        if not isinstance(spec, dict):
            raise DataLocationConfigError(f"Location spec for '{key}' must be a mapping")
        out[key] = dict(spec)
    return {"locations": out}


def resolve_data_location_alias(
    alias: str,
    *,
    config_path: Optional[Path] = None,
    config_data: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    name = str(alias or "").strip()
    if not name:
        raise DataLocationConfigError("location alias is required")
    data = config_data
    if data is None:
        resolved = resolve_data_locations_config_path(config_path)
        if not resolved:
            raise DataLocationConfigError(
                "No data locations config found. Create config/data_locations.yml or pass locations_config."
            )
        data = load_data_locations(resolved)
    locations = data.get("locations") if isinstance(data, dict) else None
    if not isinstance(locations, dict):
        raise DataLocationConfigError("Data locations config must contain a `locations` mapping")
    spec = locations.get(name)
    if not isinstance(spec, dict):
        known = ", ".join(sorted(str(k) for k in locations.keys()))
        suffix = f" Known aliases: {known}" if known else ""
        raise DataLocationConfigError(f"Unknown data location alias '{name}'.{suffix}")
    out = dict(spec)
    out["alias"] = name
    return out


__all__ = [
    "DataLocationConfigError",
    "resolve_data_locations_config_path",
    "load_data_locations",
    "resolve_data_location_alias",
]

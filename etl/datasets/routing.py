from __future__ import annotations

from pathlib import Path
from typing import Any, Dict, Optional
from urllib.parse import urlparse, unquote


class DatasetRoutingError(ValueError):
    """Raised when dataset storage routing or policy validation fails."""


def location_kind(policy: Optional[Dict[str, Any]], location_type: str) -> str:
    locations = (policy or {}).get("locations") or {}
    spec = locations.get(location_type, {}) if isinstance(locations, dict) else {}
    if isinstance(spec, dict):
        kind = str(spec.get("kind", "filesystem")).strip().lower()
        return kind or "filesystem"
    return "filesystem"


def infer_transport(
    *,
    runtime_context: str,
    target_location_type: str,
    policy: Optional[Dict[str, Any]],
    explicit_transport: Optional[str] = None,
) -> str:
    if explicit_transport:
        return str(explicit_transport).strip().lower()

    runtime = str(runtime_context or "local").strip().lower() or "local"
    kind = location_kind(policy, target_location_type)

    if kind == "filesystem":
        return "local_fs"
    if target_location_type == "gdrive" or kind == "gdrive":
        return "rclone"
    if runtime == "local" and target_location_type.startswith("hpcc_"):
        return "rsync"
    raise DatasetRoutingError(
        f"No route for runtime_context='{runtime_context}' target_location_type='{target_location_type}'."
    )


def default_location_type(*, stage: str, runtime_context: str, policy: Optional[Dict[str, Any]]) -> str:
    stage_text = str(stage or "staging").strip().lower()
    runtime = str(runtime_context or "local").strip().lower() or "local"
    if stage_text == "published":
        default_publish = str((policy or {}).get("default_publish_location_type") or "").strip().lower()
        return default_publish or "gdrive"
    if runtime == "slurm":
        return "hpcc_cache"
    return "local_cache"


def _path_within_root(path_text: str, root_text: str) -> bool:
    try:
        path = Path(path_text).expanduser().resolve()
        root = Path(root_text).expanduser().resolve()
        return path == root or root in path.parents
    except Exception:
        return False


def _as_filesystem_path(uri_or_path: str) -> Optional[str]:
    text = str(uri_or_path or "").strip()
    if not text:
        return None
    if "://" not in text:
        return str(Path(text).expanduser().resolve())
    parsed = urlparse(text)
    if parsed.scheme != "file":
        return None
    path_text = unquote(parsed.path or "")
    if len(path_text) >= 3 and path_text[0] == "/" and path_text[2] == ":":
        path_text = path_text[1:]
    if not path_text:
        return None
    return str(Path(path_text).expanduser().resolve())


def validate_target(
    *,
    policy: Optional[Dict[str, Any]],
    artifact_class: str,
    location_type: str,
    target_uri: str,
) -> None:
    if not policy:
        return
    locations = policy.get("locations", {})
    if isinstance(locations, dict) and locations:
        if location_type not in locations:
            raise DatasetRoutingError(f"location_type '{location_type}' is not configured in artifacts policy")
    loc_spec = (locations or {}).get(location_type, {}) if isinstance(locations, dict) else {}

    cls_spec = (policy.get("classes") or {}).get(artifact_class, {})
    if isinstance(cls_spec, dict):
        allowed = cls_spec.get("allowed_location_types")
        if isinstance(allowed, list):
            allowed_set = {str(v).strip().lower() for v in allowed if str(v).strip()}
            if allowed_set and location_type not in allowed_set:
                raise DatasetRoutingError(
                    f"class '{artifact_class}' does not allow location_type '{location_type}' (allowed: {sorted(allowed_set)})"
                )

    kind = location_kind(policy, location_type)
    if kind == "filesystem":
        root_path = str(loc_spec.get("root_path") or "").strip() if isinstance(loc_spec, dict) else ""
        if root_path:
            target_path = _as_filesystem_path(target_uri)
            if not target_path or not _path_within_root(target_path, root_path):
                raise DatasetRoutingError(f"target_uri '{target_uri}' is outside root_path '{root_path}'")
    else:
        root_uri = str(loc_spec.get("root_uri") or "").strip() if isinstance(loc_spec, dict) else ""
        if root_uri:
            left = target_uri.strip().rstrip("/").lower()
            right = root_uri.strip().rstrip("/").lower()
            if not (left == right or left.startswith(right + "/")):
                raise DatasetRoutingError(f"target_uri '{target_uri}' is outside root_uri '{root_uri}'")


def build_default_target_uri(
    *,
    policy: Optional[Dict[str, Any]],
    location_type: str,
    dataset_id: str,
    version_label: str,
    source_name: str,
) -> str:
    locations = (policy or {}).get("locations") or {}
    loc_spec = locations.get(location_type, {}) if isinstance(locations, dict) else {}
    kind = location_kind(policy, location_type)
    ds = dataset_id.strip().replace(".", "/")
    if kind == "filesystem":
        root = str((loc_spec or {}).get("root_path") or "").strip()
        if not root:
            return str(Path(".runs") / "datasets" / ds / version_label / source_name)
        return str(Path(root) / ds / version_label / source_name)
    root_uri = str((loc_spec or {}).get("root_uri") or "").strip().rstrip("/")
    if not root_uri:
        root_uri = "gdrive://data/etl"
    return f"{root_uri}/{ds}/{version_label}/{source_name}"


__all__ = [
    "DatasetRoutingError",
    "location_kind",
    "infer_transport",
    "default_location_type",
    "validate_target",
    "build_default_target_uri",
]

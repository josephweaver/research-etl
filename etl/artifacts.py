# research-etl
# Copyright (c) 2026 Joseph Weaver
# This file is part of the research-etl project and is licensed under the MIT License.
# You may not use this file except in compliance with the License.
# See https://github.com/josephweaver/research-etl for details.

"""
Artifact policy config + enforcement helpers.

This module intentionally stores metadata only (no file blobs) in Postgres.
"""

from __future__ import annotations

import hashlib
import json
import os
import shutil
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Dict, Iterable, Optional
from urllib.parse import unquote, urlparse

import psycopg
import yaml

from .db import get_database_url


class ArtifactPolicyError(ValueError):
    """Raised when artifact policy config is invalid."""


DEFAULT_ARTIFACT_POLICY_PATH = Path("config/artifacts.yml")

_EXPLICIT_ARTIFACT_KEYS = ("_artifacts", "artifacts")


def _artifact_class_for(path_or_uri: str, key_hint: str = "") -> str:
    key = (key_hint or "").lower()
    value = (path_or_uri or "").lower()
    if value.startswith("gdrive://"):
        return "published"
    if "publish" in key or "published" in key or "canonical" in key:
        return "published"
    if "/tmp/" in value or "\\tmp\\" in value or "/temp/" in value or "\\temp\\" in value or "tmp" in key:
        return "tmp"
    if "log" in key or "/logs/" in value or "\\logs\\" in value:
        return "run_log"
    if "cache" in key or "/cache/" in value or "\\cache\\" in value:
        return "cache"
    return "cache"


def _location_type_for(artifact_class: str, path_or_uri: str) -> str:
    if (path_or_uri or "").lower().startswith("gdrive://"):
        return "gdrive"
    if artifact_class == "tmp":
        return "local_tmp"
    if artifact_class == "run_log":
        return "run_artifact"
    return "local_cache"


def _artifact_key(run_id: str, step_name: str, artifact_class: str, uri: str, key_hint: str = "") -> str:
    payload = f"{run_id}|{step_name}|{artifact_class}|{uri}|{key_hint}"
    digest = hashlib.sha256(payload.encode("utf-8")).hexdigest()[:24]
    return f"{run_id}:{step_name}:{artifact_class}:{digest}"


def _normalize_uri(value: str) -> str:
    text = (value or "").strip()
    if not text:
        return text
    if text.lower().startswith("gdrive://"):
        return text
    p = Path(text).expanduser()
    if p.is_absolute():
        return p.resolve().as_posix()
    # Preserve relative hints if not a clear path from CWD.
    resolved = (Path(".").resolve() / p).resolve()
    return resolved.as_posix()


def _normalize_prefix(value: str) -> str:
    return (value or "").strip().rstrip("/")


def _path_within_root(path_text: str, root_text: str) -> bool:
    try:
        path = Path(path_text).expanduser().resolve()
        root = Path(root_text).expanduser().resolve()
        return path == root or root in path.parents
    except Exception:
        return False


def _looks_absolute_path(text: str) -> bool:
    raw = (text or "").strip()
    if not raw:
        return False
    # Accept POSIX absolute paths even on Windows hosts (common for remote HPCC roots).
    if raw.startswith("/"):
        return True
    # Windows drive-root style.
    if len(raw) >= 3 and raw[1] == ":" and raw[2] in ("/", "\\"):
        return True
    return False


def _is_pathlike_string(value: str) -> bool:
    text = (value or "").strip()
    if not text:
        return False
    if text.lower().startswith("gdrive://"):
        return True
    if "://" in text:
        return text.lower().startswith("file://")
    if text.startswith(".") or text.startswith("/") or text.startswith("\\"):
        return True
    if len(text) >= 3 and text[1] == ":" and text[2] in ("/", "\\"):
        return True
    return "/" in text or "\\" in text


def _file_stats(path_or_uri: str) -> tuple[Optional[int], Optional[str]]:
    if (path_or_uri or "").lower().startswith("gdrive://"):
        return (None, None)
    path = _path_from_uri(path_or_uri)
    if not path or not path.exists():
        return (None, None)
    try:
        if path.is_dir():
            total = 0
            for child in path.rglob("*"):
                if child.is_file():
                    total += child.stat().st_size
            return (total, None)
        size = path.stat().st_size
        checksum = None
        # Keep hash cost bounded for very large artifacts.
        if size <= 64 * 1024 * 1024:
            h = hashlib.sha256()
            with path.open("rb") as f:
                while True:
                    chunk = f.read(1024 * 1024)
                    if not chunk:
                        break
                    h.update(chunk)
            checksum = h.hexdigest()
        return (size, checksum)
    except Exception:
        return (None, None)


def _iter_explicit_artifacts(outputs: Any) -> Iterable[Dict[str, Any]]:
    if not isinstance(outputs, dict):
        return []
    for key in _EXPLICIT_ARTIFACT_KEYS:
        raw = outputs.get(key)
        if raw is None:
            continue
        items = raw if isinstance(raw, list) else [raw]
        out: list[Dict[str, Any]] = []
        for item in items:
            if not isinstance(item, dict):
                continue
            uri = str(item.get("uri") or item.get("path") or item.get("location_uri") or "").strip()
            if not uri:
                continue
            out.append(
                {
                    "artifact_class": str(item.get("class") or item.get("artifact_class") or "").strip().lower(),
                    "location_type": str(item.get("location_type") or "").strip().lower(),
                    "location_uri": _normalize_uri(uri),
                    "is_canonical": bool(item.get("is_canonical", item.get("canonical", False))),
                    "key_hint": str(item.get("key") or "").strip(),
                    "metadata": item.get("metadata") if isinstance(item.get("metadata"), dict) else {},
                }
            )
        return out
    return []


def _walk_output_paths(value: Any, key_path: str = "") -> Iterable[tuple[str, str]]:
    if isinstance(value, dict):
        for k, v in value.items():
            if str(k) in _EXPLICIT_ARTIFACT_KEYS:
                continue
            next_key = f"{key_path}.{k}" if key_path else str(k)
            yield from _walk_output_paths(v, next_key)
        return
    if isinstance(value, (list, tuple)):
        for idx, item in enumerate(value):
            next_key = f"{key_path}[{idx}]"
            yield from _walk_output_paths(item, next_key)
        return
    if isinstance(value, str) and _is_pathlike_string(value):
        yield (key_path, _normalize_uri(value))


def _upsert_artifact_and_location(
    conn: psycopg.Connection,
    *,
    run_id: str,
    pipeline: str,
    project_id: Optional[str],
    step_name: str,
    artifact_class: str,
    location_type: str,
    location_uri: str,
    is_canonical: bool,
    metadata: Optional[Dict[str, Any]] = None,
    key_hint: str = "",
    state: str = "present",
    last_error: Optional[str] = None,
) -> int:
    key = _artifact_key(run_id, step_name, artifact_class, location_uri, key_hint=key_hint)
    size_bytes, checksum = _file_stats(location_uri)
    metadata_json = json.dumps(metadata or {}, default=str)
    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO etl_artifacts (
                artifact_key, artifact_class, project_id, pipeline, run_id, step_name, size_bytes, checksum, metadata_json, created_at, updated_at
            )
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s::jsonb, NOW(), NOW())
            ON CONFLICT (artifact_key)
            DO UPDATE SET
                artifact_class = EXCLUDED.artifact_class,
                project_id = COALESCE(EXCLUDED.project_id, etl_artifacts.project_id),
                pipeline = EXCLUDED.pipeline,
                run_id = EXCLUDED.run_id,
                step_name = EXCLUDED.step_name,
                size_bytes = COALESCE(EXCLUDED.size_bytes, etl_artifacts.size_bytes),
                checksum = COALESCE(EXCLUDED.checksum, etl_artifacts.checksum),
                metadata_json = COALESCE(EXCLUDED.metadata_json, etl_artifacts.metadata_json),
                updated_at = NOW()
            RETURNING artifact_id
            """,
            (key, artifact_class, project_id, pipeline, run_id, step_name, size_bytes, checksum, metadata_json),
        )
        row = cur.fetchone()
        artifact_id = int(row[0]) if row else 0
        cur.execute(
            """
            INSERT INTO etl_artifact_locations (
                artifact_id, location_type, location_uri, is_canonical, state, last_error, created_at, updated_at
            )
            VALUES (%s, %s, %s, %s, %s, %s, NOW(), NOW())
            ON CONFLICT (artifact_id, location_type, location_uri)
            DO UPDATE SET
                is_canonical = EXCLUDED.is_canonical,
                state = EXCLUDED.state,
                updated_at = NOW(),
                last_error = EXCLUDED.last_error
            """,
            (artifact_id, location_type, location_uri, is_canonical, state, last_error),
        )
    return artifact_id


def _registration_policy() -> Optional[Dict[str, Any]]:
    try:
        path = resolve_artifact_policy_path(None)
    except ArtifactPolicyError:
        return None
    if not path:
        return None
    try:
        return load_artifact_policy(path)
    except ArtifactPolicyError:
        return None


def _validate_registration_target(
    *,
    policy: Optional[Dict[str, Any]],
    artifact_class: str,
    location_type: str,
    location_uri: str,
) -> tuple[bool, Optional[str]]:
    if not policy:
        return (True, None)

    locations = policy.get("locations", {})
    if isinstance(locations, dict) and locations:
        if location_type not in locations:
            return (False, f"location_type '{location_type}' is not configured in artifacts policy")
    loc_spec = (locations or {}).get(location_type, {}) if isinstance(locations, dict) else {}

    cls_spec = (policy.get("classes") or {}).get(artifact_class, {})
    if isinstance(cls_spec, dict):
        allowed = cls_spec.get("allowed_location_types")
        if isinstance(allowed, list):
            allowed_set = {str(v).strip().lower() for v in allowed if str(v).strip()}
            if allowed_set and location_type not in allowed_set:
                return (
                    False,
                    f"class '{artifact_class}' does not allow location_type '{location_type}' "
                    f"(allowed: {sorted(allowed_set)})",
                )

    kind = "filesystem"
    if isinstance(loc_spec, dict):
        kind = str(loc_spec.get("kind", "filesystem")).strip().lower() or "filesystem"

    if kind == "filesystem":
        root_path = str(loc_spec.get("root_path") or "").strip() if isinstance(loc_spec, dict) else ""
        if root_path and not _path_within_root(location_uri, root_path):
            return (False, f"location_uri '{location_uri}' is outside root_path '{root_path}'")
    else:
        root_uri = str(loc_spec.get("root_uri") or "").strip() if isinstance(loc_spec, dict) else ""
        if root_uri:
            uri = _normalize_prefix(location_uri).lower()
            root = _normalize_prefix(root_uri).lower()
            if not (uri == root or uri.startswith(root + "/")):
                return (False, f"location_uri '{location_uri}' is outside root_uri '{root_uri}'")
    return (True, None)


def register_step_artifacts(
    *,
    run_id: str,
    pipeline: str,
    project_id: Optional[str] = None,
    step_name: str,
    outputs: Optional[Dict[str, Any]],
    executor: Optional[str] = None,
) -> int:
    db_url = get_database_url()
    if not db_url or not outputs:
        return 0

    policy = _registration_policy()
    inserted = 0
    seen: set[tuple[str, str, str]] = set()
    with psycopg.connect(db_url) as conn:
        explicit_items = list(_iter_explicit_artifacts(outputs))
        for item in explicit_items:
            location_uri = str(item.get("location_uri") or "").strip()
            if not location_uri:
                continue
            artifact_class = str(item.get("artifact_class") or _artifact_class_for(location_uri, item.get("key_hint", ""))).strip().lower()
            location_type = str(item.get("location_type") or _location_type_for(artifact_class, location_uri)).strip().lower()
            key = (artifact_class, location_type, location_uri)
            if key in seen:
                continue
            seen.add(key)
            ok, reason = _validate_registration_target(
                policy=policy,
                artifact_class=artifact_class,
                location_type=location_type,
                location_uri=location_uri,
            )
            metadata = {"source": "explicit", "executor": executor}
            if isinstance(item.get("metadata"), dict):
                metadata.update(item["metadata"])
            _upsert_artifact_and_location(
                conn,
                run_id=run_id,
                pipeline=pipeline,
                project_id=project_id,
                step_name=step_name,
                artifact_class=artifact_class,
                location_type=location_type,
                location_uri=location_uri,
                is_canonical=bool(item.get("is_canonical", artifact_class == "published")),
                metadata=metadata,
                key_hint=str(item.get("key_hint") or ""),
                state="present" if ok else "policy_violation",
                last_error=reason,
            )
            inserted += 1

        for key_hint, location_uri in _walk_output_paths(outputs):
            artifact_class = _artifact_class_for(location_uri, key_hint)
            location_type = _location_type_for(artifact_class, location_uri)
            key = (artifact_class, location_type, location_uri)
            if key in seen:
                continue
            seen.add(key)
            ok, reason = _validate_registration_target(
                policy=policy,
                artifact_class=artifact_class,
                location_type=location_type,
                location_uri=location_uri,
            )
            _upsert_artifact_and_location(
                conn,
                run_id=run_id,
                pipeline=pipeline,
                project_id=project_id,
                step_name=step_name,
                artifact_class=artifact_class,
                location_type=location_type,
                location_uri=location_uri,
                is_canonical=(artifact_class == "published" and location_type == "gdrive"),
                metadata={"source": "inferred", "output_key": key_hint, "executor": executor},
                key_hint=key_hint,
                state="present" if ok else "policy_violation",
                last_error=reason,
            )
            inserted += 1
        conn.commit()
    return inserted


def register_run_artifacts(
    *,
    run_id: str,
    pipeline: str,
    project_id: Optional[str] = None,
    artifact_dir: Optional[str],
    steps: Iterable[Dict[str, Any]],
    executor: Optional[str] = None,
) -> int:
    db_url = get_database_url()
    if not db_url:
        return 0
    policy = _registration_policy()
    inserted = 0
    with psycopg.connect(db_url) as conn:
        if artifact_dir:
            uri = _normalize_uri(artifact_dir)
            ok, reason = _validate_registration_target(
                policy=policy,
                artifact_class="run_log",
                location_type="run_artifact",
                location_uri=uri,
            )
            _upsert_artifact_and_location(
                conn,
                run_id=run_id,
                pipeline=pipeline,
                project_id=project_id,
                step_name="_run",
                artifact_class="run_log",
                location_type="run_artifact",
                location_uri=uri,
                is_canonical=False,
                metadata={"source": "run_artifact_dir", "executor": executor},
                key_hint="artifact_dir",
                state="present" if ok else "policy_violation",
                last_error=reason,
            )
            inserted += 1
        conn.commit()

    for step in steps:
        if not bool(step.get("success")) or bool(step.get("skipped")):
            continue
        step_name = str(step.get("name") or "")
        outputs = step.get("outputs")
        if not step_name or not isinstance(outputs, dict):
            continue
        inserted += register_step_artifacts(
            run_id=run_id,
            pipeline=pipeline,
            project_id=project_id,
            step_name=step_name,
            outputs=outputs,
            executor=executor,
        )
    return inserted


def resolve_artifact_policy_path(path: Optional[Path]) -> Optional[Path]:
    if path:
        candidate = Path(path)
        if candidate.exists():
            return candidate
        raise ArtifactPolicyError(f"Artifact policy config not found: {candidate}")
    if DEFAULT_ARTIFACT_POLICY_PATH.exists():
        return DEFAULT_ARTIFACT_POLICY_PATH
    return None


def load_artifact_policy(path: Path) -> Dict[str, Any]:
    if not path.exists():
        raise ArtifactPolicyError(f"Artifact policy config not found: {path}")
    try:
        with path.open("r", encoding="utf-8") as f:
            data = yaml.safe_load(f) or {}
    except yaml.YAMLError as exc:
        raise ArtifactPolicyError(f"Invalid YAML in {path}: {exc}") from exc
    if not isinstance(data, dict):
        raise ArtifactPolicyError("Artifact policy config must be a mapping at the top level")

    classes = data.get("classes", {})
    if not isinstance(classes, dict):
        raise ArtifactPolicyError("`classes` must be a mapping of class -> policy")
    for klass, policy in classes.items():
        if not isinstance(policy, dict):
            raise ArtifactPolicyError(f"Policy for class '{klass}' must be a mapping")
        if "retention_days" in policy:
            value = policy.get("retention_days")
            if value is not None:
                try:
                    retention_days = int(value)
                except Exception as exc:  # noqa: BLE001
                    raise ArtifactPolicyError(
                        f"`classes.{klass}.retention_days` must be an integer or null"
                    ) from exc
                if retention_days < 0:
                    raise ArtifactPolicyError(f"`classes.{klass}.retention_days` must be >= 0")
        if "allowed_location_types" in policy:
            allowed = policy.get("allowed_location_types")
            if not isinstance(allowed, list):
                raise ArtifactPolicyError(f"`classes.{klass}.allowed_location_types` must be a list")
            if not all(isinstance(v, str) and v.strip() for v in allowed):
                raise ArtifactPolicyError(
                    f"`classes.{klass}.allowed_location_types` values must be non-empty strings"
                )

    locations = data.get("locations", {})
    if not isinstance(locations, dict):
        raise ArtifactPolicyError("`locations` must be a mapping of location_type -> settings")
    for location_type, spec in locations.items():
        if not isinstance(spec, dict):
            raise ArtifactPolicyError(f"Location settings for '{location_type}' must be a mapping")
        kind = spec.get("kind", "filesystem")
        if not isinstance(kind, str) or not kind.strip():
            raise ArtifactPolicyError(f"`locations.{location_type}.kind` must be a non-empty string")
        kind_text = kind.strip().lower()
        root_path = spec.get("root_path")
        root_uri = spec.get("root_uri")
        if root_path is not None and not isinstance(root_path, str):
            raise ArtifactPolicyError(f"`locations.{location_type}.root_path` must be a string when set")
        if root_uri is not None and not isinstance(root_uri, str):
            raise ArtifactPolicyError(f"`locations.{location_type}.root_uri` must be a string when set")
        if kind_text == "filesystem" and isinstance(root_path, str) and root_path.strip():
            if not _looks_absolute_path(root_path.strip()):
                raise ArtifactPolicyError(f"`locations.{location_type}.root_path` must be an absolute path")

    default_publish_type = data.get("default_publish_location_type", "gdrive")
    if not isinstance(default_publish_type, str) or not default_publish_type.strip():
        raise ArtifactPolicyError("`default_publish_location_type` must be a non-empty string")

    return data


def _as_utc_naive(value: datetime) -> datetime:
    if value.tzinfo is None:
        return value
    return value.astimezone(timezone.utc).replace(tzinfo=None)


def _location_kind(config: Dict[str, Any], location_type: str) -> str:
    spec = (config.get("locations") or {}).get(location_type, {})
    if isinstance(spec, dict):
        kind = str(spec.get("kind", "filesystem")).strip().lower()
        return kind or "filesystem"
    return "filesystem"


def _path_from_uri(uri: str) -> Optional[Path]:
    text = (uri or "").strip()
    if not text:
        return None
    if "://" in text:
        parsed = urlparse(text)
        if parsed.scheme != "file":
            return None
        path_text = unquote(parsed.path or "")
        if os.name == "nt" and len(path_text) >= 3 and path_text[0] == "/" and path_text[2] == ":":
            path_text = path_text[1:]
        candidate = Path(path_text).expanduser()
    else:
        candidate = Path(text).expanduser()
    if not candidate.is_absolute():
        return None
    return candidate


def enforce_artifact_policies(
    *,
    config: Dict[str, Any],
    dry_run: bool = False,
    limit: int = 2000,
) -> Dict[str, Any]:
    db_url = get_database_url()
    if not db_url:
        raise RuntimeError("ETL_DATABASE_URL is not configured.")
    if limit <= 0:
        raise ValueError("limit must be > 0")

    classes: Dict[str, Dict[str, Any]] = dict(config.get("classes") or {})
    now = datetime.utcnow()
    summary: Dict[str, Any] = {
        "inspected_artifacts": 0,
        "inspected_locations": 0,
        "violations_missing_canonical": 0,
        "violations_policy": 0,
        "deleted_files": 0,
        "would_delete_files": 0,
        "missing_files_marked": 0,
        "delete_errors": 0,
        "dry_run": bool(dry_run),
        "violations": [],
    }

    with psycopg.connect(db_url) as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT artifact_id, artifact_key, artifact_class, created_at
                FROM etl_artifacts
                ORDER BY created_at ASC
                LIMIT %s
                """,
                (limit,),
            )
            artifact_rows = cur.fetchall() or []
        if not artifact_rows:
            return summary

        artifact_ids = [int(row[0]) for row in artifact_rows]
        by_artifact: Dict[int, Dict[str, Any]] = {}
        for row in artifact_rows:
            artifact_id = int(row[0])
            by_artifact[artifact_id] = {
                "artifact_id": artifact_id,
                "artifact_key": str(row[1]),
                "artifact_class": str(row[2] or "").strip().lower(),
                "created_at": row[3],
                "locations": [],
            }

        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT location_id, artifact_id, location_type, location_uri, is_canonical, state
                FROM etl_artifact_locations
                WHERE artifact_id = ANY(%s)
                """,
                (artifact_ids,),
            )
            location_rows = cur.fetchall() or []
        for row in location_rows:
            artifact_id = int(row[1])
            if artifact_id not in by_artifact:
                continue
            by_artifact[artifact_id]["locations"].append(
                {
                    "location_id": int(row[0]),
                    "location_type": str(row[2] or "").strip(),
                    "location_uri": str(row[3] or "").strip(),
                    "is_canonical": bool(row[4]),
                    "state": str(row[5] or "present").strip().lower(),
                }
            )

        summary["inspected_artifacts"] = len(by_artifact)
        summary["inspected_locations"] = len(location_rows)

        for artifact in by_artifact.values():
            artifact_class = artifact["artifact_class"] or "other"
            policy = dict(classes.get(artifact_class) or {})

            require_canonical = bool(policy.get("require_canonical", artifact_class == "published"))
            canonical_location_type = str(
                policy.get("canonical_location_type") or config.get("default_publish_location_type", "gdrive")
            ).strip()

            present_locations = [l for l in artifact["locations"] if l["state"] == "present"]
            policy_violations = [l for l in artifact["locations"] if l["state"] == "policy_violation"]
            if policy_violations:
                summary["violations_policy"] += len(policy_violations)
                violations = summary.get("violations")
                if isinstance(violations, list):
                    for loc in policy_violations:
                        if len(violations) >= 50:
                            break
                        violations.append(
                            {
                                "artifact_id": artifact["artifact_id"],
                                "artifact_key": artifact["artifact_key"],
                                "artifact_class": artifact_class,
                                "issue": "policy_violation",
                                "location_type": loc.get("location_type"),
                                "location_uri": loc.get("location_uri"),
                            }
                        )
            has_canonical = any(
                l["is_canonical"] or (canonical_location_type and l["location_type"] == canonical_location_type)
                for l in present_locations
            )
            if require_canonical and not has_canonical:
                summary["violations_missing_canonical"] += 1
                violations = summary.get("violations")
                if isinstance(violations, list) and len(violations) < 50:
                    violations.append(
                        {
                            "artifact_id": artifact["artifact_id"],
                            "artifact_key": artifact["artifact_key"],
                            "artifact_class": artifact_class,
                            "issue": "missing_canonical_location",
                            "expected_location_type": canonical_location_type,
                        }
                    )

            retention_days = policy.get("retention_days")
            enforce_delete = bool(policy.get("enforce_delete", False))
            if retention_days is None or not enforce_delete:
                continue
            cutoff = now - timedelta(days=int(retention_days))
            created_at = artifact.get("created_at")
            if not isinstance(created_at, datetime) or _as_utc_naive(created_at) > cutoff:
                continue

            for loc in present_locations:
                if _location_kind(config, loc["location_type"]) != "filesystem":
                    continue
                local_path = _path_from_uri(loc["location_uri"])
                if local_path is None:
                    continue

                if dry_run:
                    summary["would_delete_files"] += 1
                    continue

                try:
                    if local_path.exists():
                        if local_path.is_dir():
                            shutil.rmtree(local_path)
                        else:
                            local_path.unlink()
                        new_state = "deleted"
                        summary["deleted_files"] += 1
                    else:
                        new_state = "missing"
                        summary["missing_files_marked"] += 1
                    with conn.cursor() as cur:
                        cur.execute(
                            """
                            UPDATE etl_artifact_locations
                            SET state = %s,
                                last_verified_at = NOW(),
                                deleted_at = CASE WHEN %s = 'deleted' THEN NOW() ELSE deleted_at END,
                                last_error = NULL
                            WHERE location_id = %s
                            """,
                            (new_state, new_state, loc["location_id"]),
                        )
                except Exception as exc:  # noqa: BLE001
                    summary["delete_errors"] += 1
                    with conn.cursor() as cur:
                        cur.execute(
                            """
                            UPDATE etl_artifact_locations
                            SET last_verified_at = NOW(),
                                last_error = %s
                            WHERE location_id = %s
                            """,
                            (str(exc)[:4000], loc["location_id"]),
                        )
        conn.commit()

    return summary


__all__ = [
    "ArtifactPolicyError",
    "DEFAULT_ARTIFACT_POLICY_PATH",
    "resolve_artifact_policy_path",
    "load_artifact_policy",
    "enforce_artifact_policies",
    "register_step_artifacts",
    "register_run_artifacts",
]

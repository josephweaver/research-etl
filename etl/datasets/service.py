from __future__ import annotations

import hashlib
import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional

import psycopg

from etl.artifacts import load_artifact_policy, resolve_artifact_policy_path, ArtifactPolicyError
from etl.db import get_database_url
from etl.datasets.routing import (
    DatasetRoutingError,
    build_default_target_uri,
    default_location_type,
    infer_transport,
    validate_target,
)
from etl.datasets.transports import DatasetTransportError, transfer_via_transport
from etl.datasets.transports.base import fetch_via_transport


class DatasetServiceError(RuntimeError):
    """Raised when dataset queries fail."""


def _connect() -> psycopg.Connection:
    db_url = get_database_url()
    if not db_url:
        raise DatasetServiceError("ETL_DATABASE_URL is not configured.")
    try:
        return psycopg.connect(db_url)
    except Exception as exc:  # noqa: BLE001
        raise DatasetServiceError(f"Could not connect to database: {exc}") from exc


def _load_policy_or_none() -> Optional[Dict[str, Any]]:
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


def _file_stats(path_text: str) -> tuple[Optional[int], Optional[str]]:
    p = Path(path_text).expanduser().resolve()
    if not p.exists():
        return (None, None)
    try:
        if p.is_dir():
            total = 0
            for child in p.rglob("*"):
                if child.is_file():
                    total += child.stat().st_size
            return (total, None)
        size = p.stat().st_size
        checksum = None
        if size <= 64 * 1024 * 1024:
            digest = hashlib.sha256()
            with p.open("rb") as f:
                while True:
                    chunk = f.read(1024 * 1024)
                    if not chunk:
                        break
                    digest.update(chunk)
            checksum = digest.hexdigest()
        return (size, checksum)
    except Exception:
        return (None, None)


def _normalize_stage(stage: str) -> str:
    text = str(stage or "staging").strip().lower() or "staging"
    if text not in {"staging", "published"}:
        raise DatasetServiceError("stage must be one of: staging, published")
    return text


def _default_version_label() -> str:
    now = datetime.now(timezone.utc)
    return now.strftime("v%Y%m%dT%H%M%SZ")


def _infer_fetch_transport(*, location_type: str, policy: Optional[Dict[str, Any]], explicit: Optional[str] = None) -> str:
    if explicit:
        return str(explicit).strip().lower()
    locations = (policy or {}).get("locations") or {}
    spec = locations.get(location_type, {}) if isinstance(locations, dict) else {}
    kind = str((spec or {}).get("kind", "filesystem")).strip().lower() if isinstance(spec, dict) else "filesystem"
    if kind == "filesystem":
        return "local_fs"
    if location_type == "gdrive" or kind == "gdrive":
        return "rclone"
    raise DatasetServiceError(f"No fetch transport mapping for location_type='{location_type}'")


def _location_name_from_uri(uri: str) -> str:
    text = str(uri or "").strip().rstrip("/")
    if not text:
        return "dataset"
    leaf = text.split("/")[-1]
    return leaf or "dataset"


def list_datasets(limit: int = 50, *, q: Optional[str] = None) -> List[Dict[str, Any]]:
    limit = max(1, min(int(limit), 500))
    try:
        with _connect() as conn:
            with conn.cursor() as cur:
                where_sql = ""
                params: List[Any] = []
                if q:
                    where_sql = "WHERE (d.dataset_id ILIKE %s OR COALESCE(d.owner_user, '') ILIKE %s)"
                    like = f"%{q}%"
                    params.extend([like, like])
                cur.execute(
                    f"""
                    SELECT
                        d.dataset_id,
                        d.data_class,
                        d.owner_user,
                        d.status,
                        d.created_at,
                        d.updated_at,
                        COALESCE(v.version_count, 0) AS version_count,
                        lv.version_label AS latest_version
                    FROM etl_datasets d
                    LEFT JOIN (
                        SELECT dataset_id, COUNT(*) AS version_count
                        FROM etl_dataset_versions
                        GROUP BY dataset_id
                    ) v ON v.dataset_id = d.dataset_id
                    LEFT JOIN LATERAL (
                        SELECT version_label
                        FROM etl_dataset_versions vv
                        WHERE vv.dataset_id = d.dataset_id
                        ORDER BY vv.created_at DESC, vv.dataset_version_id DESC
                        LIMIT 1
                    ) lv ON TRUE
                    {where_sql}
                    ORDER BY d.dataset_id
                    LIMIT %s
                    """,
                    (*params, limit),
                )
                rows = cur.fetchall()
    except DatasetServiceError:
        raise
    except Exception as exc:  # noqa: BLE001
        raise DatasetServiceError(f"Failed to list datasets: {exc}") from exc

    out: List[Dict[str, Any]] = []
    for row in rows:
        out.append(
            {
                "dataset_id": row[0],
                "data_class": row[1],
                "owner_user": row[2],
                "status": row[3],
                "created_at": row[4].isoformat() if row[4] is not None else None,
                "updated_at": row[5].isoformat() if row[5] is not None else None,
                "version_count": int(row[6] or 0),
                "latest_version": row[7],
            }
        )
    return out


def get_dataset(dataset_id: str) -> Optional[Dict[str, Any]]:
    dataset_id = str(dataset_id or "").strip()
    if not dataset_id:
        return None
    try:
        with _connect() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT
                        dataset_id,
                        data_class,
                        owner_user,
                        status,
                        created_at,
                        updated_at
                    FROM etl_datasets
                    WHERE dataset_id = %s
                    """,
                    (dataset_id,),
                )
                row = cur.fetchone()
                if not row:
                    return None

                cur.execute(
                    """
                    SELECT
                        dataset_version_id,
                        version_label,
                        is_immutable,
                        schema_hash,
                        created_by_run_id,
                        created_at
                    FROM etl_dataset_versions
                    WHERE dataset_id = %s
                    ORDER BY created_at DESC, dataset_version_id DESC
                    """,
                    (dataset_id,),
                )
                version_rows = cur.fetchall()

                cur.execute(
                    """
                    SELECT
                        l.dataset_version_id,
                        v.version_label,
                        l.environment,
                        l.location_type,
                        l.uri,
                        l.is_canonical,
                        l.checksum,
                        l.size_bytes,
                        l.created_at
                    FROM etl_dataset_locations l
                    JOIN etl_dataset_versions v
                      ON v.dataset_version_id = l.dataset_version_id
                    WHERE v.dataset_id = %s
                    ORDER BY v.created_at DESC, l.created_at DESC, l.dataset_location_id DESC
                    """,
                    (dataset_id,),
                )
                location_rows = cur.fetchall()
    except DatasetServiceError:
        raise
    except Exception as exc:  # noqa: BLE001
        raise DatasetServiceError(f"Failed to load dataset '{dataset_id}': {exc}") from exc

    versions: List[Dict[str, Any]] = []
    for version in version_rows:
        versions.append(
            {
                "dataset_version_id": int(version[0]),
                "version_label": version[1],
                "is_immutable": bool(version[2]),
                "schema_hash": version[3],
                "created_by_run_id": version[4],
                "created_at": version[5].isoformat() if version[5] is not None else None,
            }
        )

    locations: List[Dict[str, Any]] = []
    for location in location_rows:
        locations.append(
            {
                "dataset_version_id": int(location[0]),
                "version_label": location[1],
                "environment": location[2],
                "location_type": location[3],
                "uri": location[4],
                "is_canonical": bool(location[5]),
                "checksum": location[6],
                "size_bytes": int(location[7]) if location[7] is not None else None,
                "created_at": location[8].isoformat() if location[8] is not None else None,
            }
        )

    return {
        "dataset_id": row[0],
        "data_class": row[1],
        "owner_user": row[2],
        "status": row[3],
        "created_at": row[4].isoformat() if row[4] is not None else None,
        "updated_at": row[5].isoformat() if row[5] is not None else None,
        "versions": versions,
        "locations": locations,
    }


def store_data(
    *,
    dataset_id: str,
    source_path: str,
    stage: str = "staging",
    version_label: Optional[str] = None,
    environment: Optional[str] = None,
    runtime_context: str = "local",
    location_type: Optional[str] = None,
    target_uri: Optional[str] = None,
    transport: Optional[str] = None,
    owner_user: Optional[str] = None,
    data_class: Optional[str] = None,
    dry_run: bool = False,
    transport_options: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    ds_id = str(dataset_id or "").strip()
    if not ds_id:
        raise DatasetServiceError("dataset_id is required")
    src = Path(str(source_path or "")).expanduser().resolve()
    if not src.exists():
        raise DatasetServiceError(f"source_path not found: {src}")

    stage_text = _normalize_stage(stage)
    ver = str(version_label or "").strip() or _default_version_label()
    policy = _load_policy_or_none()
    loc_type = str(location_type or "").strip().lower() or default_location_type(
        stage=stage_text,
        runtime_context=runtime_context,
        policy=policy,
    )
    tgt = str(target_uri or "").strip() or build_default_target_uri(
        policy=policy,
        location_type=loc_type,
        dataset_id=ds_id,
        version_label=ver,
        source_name=src.name,
    )
    artifact_class = "published" if stage_text == "published" else "cache"
    try:
        validate_target(
            policy=policy,
            artifact_class=artifact_class,
            location_type=loc_type,
            target_uri=tgt,
        )
        chosen_transport = infer_transport(
            runtime_context=runtime_context,
            target_location_type=loc_type,
            policy=policy,
            explicit_transport=transport,
        )
    except DatasetRoutingError as exc:
        raise DatasetServiceError(str(exc)) from exc

    try:
        transfer_receipt = transfer_via_transport(
            transport=chosen_transport,
            source_path=str(src),
            target_uri=tgt,
            dry_run=bool(dry_run),
            options=transport_options or {},
        )
    except (DatasetTransportError, FileNotFoundError, ValueError, RuntimeError) as exc:
        raise DatasetServiceError(f"store_data transfer failed: {exc}") from exc

    persisted_uri = str(transfer_receipt.get("target_uri") or tgt)
    size_bytes, checksum = _file_stats(str(src))
    env_name = str(environment or runtime_context or "").strip().lower() or None
    canonical = stage_text == "published"
    data_class_text = str(data_class or "").strip() or None
    owner_user_text = str(owner_user or "").strip() or None

    try:
        with _connect() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    INSERT INTO etl_datasets (dataset_id, data_class, owner_user, status, created_at, updated_at)
                    VALUES (%s, %s, %s, %s, NOW(), NOW())
                    ON CONFLICT (dataset_id)
                    DO UPDATE SET
                        data_class = COALESCE(EXCLUDED.data_class, etl_datasets.data_class),
                        owner_user = COALESCE(EXCLUDED.owner_user, etl_datasets.owner_user),
                        status = EXCLUDED.status,
                        updated_at = NOW()
                    """,
                    (ds_id, data_class_text, owner_user_text, "active"),
                )
                cur.execute(
                    """
                    INSERT INTO etl_dataset_versions (
                        dataset_id, version_label, is_immutable, schema_hash, created_by_run_id, created_at
                    )
                    VALUES (%s, %s, %s, %s, %s, NOW())
                    ON CONFLICT (dataset_id, version_label)
                    DO UPDATE SET dataset_id = EXCLUDED.dataset_id
                    RETURNING dataset_version_id
                    """,
                    (ds_id, ver, bool(stage_text == "published"), None, None),
                )
                row = cur.fetchone()
                if not row:
                    raise DatasetServiceError("Failed to create or load dataset version row.")
                dataset_version_id = int(row[0])
                cur.execute(
                    """
                    INSERT INTO etl_dataset_locations (
                        dataset_version_id, environment, location_type, uri, is_canonical, checksum, size_bytes, created_at
                    )
                    VALUES (%s, %s, %s, %s, %s, %s, %s, NOW())
                    ON CONFLICT (dataset_version_id, environment, location_type, uri)
                    DO UPDATE SET
                        is_canonical = EXCLUDED.is_canonical,
                        checksum = COALESCE(EXCLUDED.checksum, etl_dataset_locations.checksum),
                        size_bytes = COALESCE(EXCLUDED.size_bytes, etl_dataset_locations.size_bytes)
                    """,
                    (dataset_version_id, env_name, loc_type, persisted_uri, canonical, checksum, size_bytes),
                )
                cur.execute(
                    """
                    INSERT INTO etl_dataset_events (
                        dataset_id, version_label, event_type, run_id, payload_json, created_at
                    )
                    VALUES (%s, %s, %s, %s, %s::jsonb, NOW())
                    """,
                    (
                        ds_id,
                        ver,
                        "stored" if not canonical else "published",
                        None,
                        json.dumps(
                            {
                                "source_path": str(src),
                                "target_uri": persisted_uri,
                                "transport": chosen_transport,
                                "environment": env_name,
                                "location_type": loc_type,
                                "dry_run": bool(dry_run),
                            }
                        ),
                    ),
                )
            conn.commit()
    except DatasetServiceError:
        raise
    except Exception as exc:  # noqa: BLE001
        raise DatasetServiceError(f"Failed to persist dataset storage metadata: {exc}") from exc

    return {
        "dataset_id": ds_id,
        "version_label": ver,
        "stage": stage_text,
        "environment": env_name,
        "location_type": loc_type,
        "target_uri": persisted_uri,
        "transport": chosen_transport,
        "checksum": checksum,
        "size_bytes": size_bytes,
        "dry_run": bool(dry_run),
    }


def get_data(
    *,
    dataset_id: str,
    version: str = "latest",
    environment: Optional[str] = None,
    runtime_context: str = "local",
    location_type: Optional[str] = None,
    cache_dir: Optional[str] = None,
    transport: Optional[str] = None,
    dry_run: bool = False,
    prefer_direct_local: bool = True,
    transport_options: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    ds_id = str(dataset_id or "").strip()
    if not ds_id:
        raise DatasetServiceError("dataset_id is required")
    ver_req = str(version or "latest").strip() or "latest"
    env_name = str(environment or runtime_context or "").strip().lower() or None
    policy = _load_policy_or_none()
    resolved_version = ver_req

    try:
        with _connect() as conn:
            with conn.cursor() as cur:
                if ver_req.lower() == "latest":
                    cur.execute(
                        """
                        SELECT dataset_version_id, version_label
                        FROM etl_dataset_versions
                        WHERE dataset_id = %s
                        ORDER BY created_at DESC, dataset_version_id DESC
                        LIMIT 1
                        """,
                        (ds_id,),
                    )
                else:
                    cur.execute(
                        """
                        SELECT dataset_version_id, version_label
                        FROM etl_dataset_versions
                        WHERE dataset_id = %s AND version_label = %s
                        ORDER BY created_at DESC, dataset_version_id DESC
                        LIMIT 1
                        """,
                        (ds_id, ver_req),
                    )
                version_row = cur.fetchone()
                if not version_row:
                    raise DatasetServiceError(
                        f"Dataset version not found for dataset_id='{ds_id}' version='{ver_req}'."
                    )
                dataset_version_id = int(version_row[0])
                resolved_version = str(version_row[1])

                where_loc = "dataset_version_id = %s"
                params: list[Any] = [dataset_version_id]
                if location_type:
                    where_loc += " AND location_type = %s"
                    params.append(str(location_type).strip().lower())
                cur.execute(
                    f"""
                    SELECT
                        dataset_location_id,
                        environment,
                        location_type,
                        uri,
                        is_canonical,
                        checksum,
                        size_bytes,
                        created_at
                    FROM etl_dataset_locations
                    WHERE {where_loc}
                    ORDER BY
                        CASE WHEN environment = %s THEN 0 ELSE 1 END,
                        CASE WHEN is_canonical THEN 0 ELSE 1 END,
                        created_at DESC,
                        dataset_location_id DESC
                    """,
                    (*params, env_name),
                )
                location_rows = cur.fetchall()
                if not location_rows:
                    raise DatasetServiceError(
                        f"No locations found for dataset_id='{ds_id}' version='{resolved_version}'."
                    )
                loc = location_rows[0]
                location_id = int(loc[0])
                src_env = loc[1]
                src_loc_type = str(loc[2])
                src_uri = str(loc[3])
                src_checksum = loc[5]
                src_size_bytes = int(loc[6]) if loc[6] is not None else None
    except DatasetServiceError:
        raise
    except Exception as exc:  # noqa: BLE001
        raise DatasetServiceError(f"Failed to resolve dataset retrieval: {exc}") from exc

    local_cache_root = Path(cache_dir or ".runs/datasets_cache").expanduser().resolve()
    target_name = _location_name_from_uri(src_uri)
    local_target = local_cache_root / ds_id.replace(".", "/") / resolved_version / target_name

    if prefer_direct_local and src_loc_type in {"local_cache", "hpcc_cache", "local_tmp", "run_artifact"}:
        src_path = Path(src_uri).expanduser()
        if src_path.exists():
            local_target = src_path.resolve()
            chosen_transport = "none"
            fetched = False
            receipt = {"transport": "none", "target_path": str(local_target), "dry_run": bool(dry_run)}
        else:
            chosen_transport = _infer_fetch_transport(location_type=src_loc_type, policy=policy, explicit=transport)
            try:
                receipt = fetch_via_transport(
                    transport=chosen_transport,
                    source_uri=src_uri,
                    target_path=str(local_target),
                    dry_run=bool(dry_run),
                    options=transport_options or {},
                )
            except (DatasetTransportError, FileNotFoundError, ValueError, RuntimeError) as exc:
                raise DatasetServiceError(f"get_data fetch failed: {exc}") from exc
            fetched = True
    else:
        chosen_transport = _infer_fetch_transport(location_type=src_loc_type, policy=policy, explicit=transport)
        try:
            receipt = fetch_via_transport(
                transport=chosen_transport,
                source_uri=src_uri,
                target_path=str(local_target),
                dry_run=bool(dry_run),
                options=transport_options or {},
            )
        except (DatasetTransportError, FileNotFoundError, ValueError, RuntimeError) as exc:
            raise DatasetServiceError(f"get_data fetch failed: {exc}") from exc
        fetched = True

    result_path = str(receipt.get("target_path") or local_target)

    try:
        with _connect() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    INSERT INTO etl_dataset_events (
                        dataset_id, version_label, event_type, run_id, payload_json, created_at
                    )
                    VALUES (%s, %s, %s, %s, %s::jsonb, NOW())
                    """,
                    (
                        ds_id,
                        resolved_version,
                        "retrieved",
                        None,
                        json.dumps(
                            {
                                "dataset_location_id": location_id,
                                "source_environment": src_env,
                                "source_location_type": src_loc_type,
                                "source_uri": src_uri,
                                "transport": chosen_transport,
                                "target_path": result_path,
                                "fetched": bool(fetched),
                                "dry_run": bool(dry_run),
                            }
                        ),
                    ),
                )
            conn.commit()
    except Exception:
        # Retrieval event logging should not block data access.
        pass

    return {
        "dataset_id": ds_id,
        "version_label": resolved_version,
        "environment": src_env,
        "location_type": src_loc_type,
        "source_uri": src_uri,
        "local_path": result_path,
        "transport": chosen_transport,
        "fetched": bool(fetched),
        "checksum": src_checksum,
        "size_bytes": src_size_bytes,
        "dry_run": bool(dry_run),
    }


__all__ = ["DatasetServiceError", "list_datasets", "get_dataset", "store_data", "get_data"]

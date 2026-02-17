from __future__ import annotations

from typing import Any, Dict, List, Optional

import psycopg

from etl.db import get_database_url


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


__all__ = ["DatasetServiceError", "list_datasets", "get_dataset"]

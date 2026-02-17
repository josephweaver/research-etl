"""
Database schema bootstrap/migration support.

Behavior:
- If ETL_DATABASE_URL is not set, no-op.
- If ETL_DATABASE_URL is set, ensure migration metadata tables exist,
  then apply all scripts in db/ddl/*.sql in lexical order.
- Each script is recorded with checksum. Existing scripts with changed
  checksum are rejected to prevent silent drift.
"""

from __future__ import annotations

import hashlib
import os
from dataclasses import dataclass
from pathlib import Path
from typing import List, Optional, Tuple
from urllib.parse import urlparse

import psycopg


class DatabaseError(RuntimeError):
    """Raised when database bootstrap/migration fails."""


@dataclass(frozen=True)
class AppliedMigration:
    version_name: str
    checksum: str


def _db_mode_is_offline(raw: Optional[str]) -> bool:
    text = str(raw or "").strip().lower()
    if not text:
        return False
    return text in {"offline", "off", "disabled", "none", "false", "0"}


def _normalize_database_url(raw: Optional[str]) -> Optional[str]:
    if raw is None:
        return None
    value = raw.strip()
    if not value:
        return None
    if (value.startswith('"') and value.endswith('"')) or (value.startswith("'") and value.endswith("'")):
        value = value[1:-1].strip()
    if not value:
        return None
    parsed = urlparse(value)
    if not parsed.scheme or not parsed.netloc:
        raise DatabaseError("ETL_DATABASE_URL is set but is not a valid URL.")
    return value


def _load_database_url() -> Optional[str]:
    if _db_mode_is_offline(os.environ.get("ETL_DB_MODE")) or _db_mode_is_offline(
        os.environ.get("ETL_DATABASE_MODE")
    ):
        return None
    raw = os.environ.get("ETL_DATABASE_URL")
    if raw is None:
        # Windows fallback for values persisted by `setx`.
        try:
            import winreg  # type: ignore

            for hive, subkey in (
                (winreg.HKEY_CURRENT_USER, r"Environment"),
                (winreg.HKEY_LOCAL_MACHINE, r"SYSTEM\CurrentControlSet\Control\Session Manager\Environment"),
            ):
                try:
                    with winreg.OpenKey(hive, subkey) as key:
                        val, _ = winreg.QueryValueEx(key, "ETL_DATABASE_URL")
                        if isinstance(val, str) and val.strip():
                            raw = val
                            break
                except OSError:
                    continue
        except Exception:
            pass
    return _normalize_database_url(raw)


def get_database_url() -> Optional[str]:
    """Return normalized DB URL (or None if not configured)."""
    return _load_database_url()


def _discover_ddl_scripts(ddl_dir: Path) -> List[Path]:
    if not ddl_dir.exists():
        raise DatabaseError(f"DDL directory not found: {ddl_dir}")
    scripts = sorted([p for p in ddl_dir.iterdir() if p.is_file() and p.suffix.lower() == ".sql"])
    if not scripts:
        raise DatabaseError(f"No .sql DDL scripts found in {ddl_dir}")
    return scripts


def _sha256(text: str) -> str:
    return hashlib.sha256(text.encode("utf-8")).hexdigest()


def _ensure_migration_tables(conn: psycopg.Connection) -> None:
    with conn.cursor() as cur:
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS etl_schema_versions (
                version_name TEXT PRIMARY KEY,
                checksum TEXT NOT NULL,
                applied_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
            );
            """
        )
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS etl_schema_state (
                singleton_id SMALLINT PRIMARY KEY CHECK (singleton_id = 1),
                latest_version TEXT NOT NULL,
                latest_checksum TEXT NOT NULL,
                updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
            );
            """
        )


def _fetch_existing_migration(conn: psycopg.Connection, version_name: str) -> Optional[Tuple[str, str]]:
    with conn.cursor() as cur:
        cur.execute(
            "SELECT version_name, checksum FROM etl_schema_versions WHERE version_name = %s",
            (version_name,),
        )
        row = cur.fetchone()
        if not row:
            return None
        return (row[0], row[1])


def _record_migration(conn: psycopg.Connection, version_name: str, checksum: str) -> None:
    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO etl_schema_versions (version_name, checksum)
            VALUES (%s, %s)
            """,
            (version_name, checksum),
        )
        cur.execute(
            """
            INSERT INTO etl_schema_state (singleton_id, latest_version, latest_checksum, updated_at)
            VALUES (1, %s, %s, NOW())
            ON CONFLICT (singleton_id)
            DO UPDATE SET
                latest_version = EXCLUDED.latest_version,
                latest_checksum = EXCLUDED.latest_checksum,
                updated_at = NOW()
            """,
            (version_name, checksum),
        )


def ensure_database_schema(ddl_dir: Path = Path("db/ddl")) -> List[AppliedMigration]:
    """
    Ensure ETL schema exists and migrations are applied.

    Returns list of migrations newly applied during this run.
    """
    db_url = _load_database_url()
    if not db_url:
        return []

    scripts = _discover_ddl_scripts(ddl_dir)
    applied: List[AppliedMigration] = []

    try:
        with psycopg.connect(db_url) as conn:
            with conn.cursor() as cur:
                # Serialize migration bootstrap across concurrent processes.
                cur.execute("SELECT pg_advisory_lock(918273645)")
            try:
                _ensure_migration_tables(conn)
                conn.commit()

                for script in scripts:
                    sql_text = script.read_text(encoding="utf-8")
                    checksum = _sha256(sql_text)
                    existing = _fetch_existing_migration(conn, script.name)

                    if existing:
                        _, existing_checksum = existing
                        if existing_checksum != checksum:
                            raise DatabaseError(
                                f"Migration checksum mismatch for {script.name}. "
                                "Do not edit applied scripts; add a new incremental .sql migration."
                            )
                        continue

                    with conn.cursor() as cur:
                        cur.execute(sql_text)
                    _record_migration(conn, script.name, checksum)
                    conn.commit()
                    applied.append(AppliedMigration(version_name=script.name, checksum=checksum))
            finally:
                with conn.cursor() as cur:
                    cur.execute("SELECT pg_advisory_unlock(918273645)")
                conn.commit()
    except DatabaseError:
        raise
    except Exception as exc:  # noqa: BLE001
        raise DatabaseError(f"Database schema bootstrap failed: {exc}") from exc

    return applied


__all__ = ["DatabaseError", "AppliedMigration", "ensure_database_schema", "get_database_url"]

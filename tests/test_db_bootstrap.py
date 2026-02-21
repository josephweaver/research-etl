# research-etl
# Copyright (c) 2026 Joseph Weaver
# This file is part of the research-etl project and is licensed under the MIT License.
# You may not use this file except in compliance with the License.
# See https://github.com/josephweaver/research-etl for details.

from __future__ import annotations

import hashlib
from pathlib import Path

import pytest

from etl import db as db_module


class _FakeCursor:
    def __init__(self, conn: "_FakeConn") -> None:
        self._conn = conn
        self._row = None

    def __enter__(self) -> "_FakeCursor":
        return self

    def __exit__(self, exc_type, exc, tb) -> bool:
        return False

    def execute(self, sql: str, params=None) -> None:
        self._conn.executed.append((sql, params))
        if "SELECT version_name, checksum FROM etl_schema_versions" in sql:
            version = params[0]
            checksum = self._conn.migrations.get(version)
            self._row = (version, checksum) if checksum else None
        elif "INSERT INTO etl_schema_versions" in sql:
            version, checksum = params
            self._conn.migrations[version] = checksum
            self._row = None
        else:
            self._row = None

    def fetchone(self):
        return self._row


class _FakeConn:
    def __init__(self, migrations=None) -> None:
        self.migrations = dict(migrations or {})
        self.executed = []

    def __enter__(self) -> "_FakeConn":
        return self

    def __exit__(self, exc_type, exc, tb) -> bool:
        return False

    def cursor(self) -> _FakeCursor:
        return _FakeCursor(self)

    def commit(self) -> None:
        self.executed.append(("COMMIT", None))


def _write_sql(path: Path, text: str) -> None:
    path.write_text(text, encoding="utf-8")


def test_ensure_database_schema_applies_new_sql_scripts(monkeypatch, tmp_path: Path) -> None:
    ddl_dir = tmp_path / "ddl"
    ddl_dir.mkdir()
    _write_sql(ddl_dir / "001_init.sql", "CREATE TABLE IF NOT EXISTS t1(id INT);")
    _write_sql(ddl_dir / "002_more.sql", "CREATE TABLE IF NOT EXISTS t2(id INT);")

    fake_conn = _FakeConn()
    monkeypatch.setattr(db_module, "_load_database_url", lambda: "postgresql://u:p@h/db")
    monkeypatch.setattr(db_module.psycopg, "connect", lambda _: fake_conn)

    applied = db_module.ensure_database_schema(ddl_dir)

    assert [m.version_name for m in applied] == ["001_init.sql", "002_more.sql"]
    assert fake_conn.migrations["001_init.sql"] == hashlib.sha256(
        "CREATE TABLE IF NOT EXISTS t1(id INT);".encode("utf-8")
    ).hexdigest()
    assert fake_conn.migrations["002_more.sql"] == hashlib.sha256(
        "CREATE TABLE IF NOT EXISTS t2(id INT);".encode("utf-8")
    ).hexdigest()


def test_ensure_database_schema_rejects_checksum_drift(monkeypatch, tmp_path: Path) -> None:
    ddl_dir = tmp_path / "ddl"
    ddl_dir.mkdir()
    sql = "CREATE TABLE IF NOT EXISTS t1(id INT);"
    _write_sql(ddl_dir / "001_init.sql", sql)

    fake_conn = _FakeConn(migrations={"001_init.sql": "deadbeef"})
    monkeypatch.setattr(db_module, "_load_database_url", lambda: "postgresql://u:p@h/db")
    monkeypatch.setattr(db_module.psycopg, "connect", lambda _: fake_conn)

    with pytest.raises(db_module.DatabaseError, match="checksum mismatch"):
        db_module.ensure_database_schema(ddl_dir)


def test_get_database_url_respects_offline_db_mode(monkeypatch) -> None:
    monkeypatch.setenv("ETL_DATABASE_URL", "postgresql://u:p@h/db")
    monkeypatch.setenv("ETL_DB_MODE", "offline")
    assert db_module.get_database_url() is None

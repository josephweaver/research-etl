from __future__ import annotations

from datetime import datetime

import pytest

from etl.datasets import service as ds


class _FakeCursor:
    def __init__(self, conn):
        self._conn = conn
        self._last_sql = ""
        self._last_params = None
        self._row = None

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        return False

    def execute(self, sql, params=None):
        self._last_sql = sql
        self._last_params = params
        self._conn.last_sql = sql
        self._conn.last_params = params
        if "FROM etl_datasets\n                    WHERE dataset_id = %s" in sql:
            if params and params[0] == "serve.demo":
                self._row = (
                    "serve.demo",
                    "SERVE",
                    "land-core",
                    "active",
                    datetime(2026, 2, 17, 1, 2, 3),
                    datetime(2026, 2, 17, 1, 3, 3),
                )
            else:
                self._row = None
        else:
            self._row = None

    def fetchall(self):
        if "FROM etl_datasets d" in self._last_sql:
            return [
                (
                    "serve.demo",
                    "SERVE",
                    "land-core",
                    "active",
                    datetime(2026, 2, 17, 1, 2, 3),
                    datetime(2026, 2, 17, 1, 3, 3),
                    2,
                    "v2",
                )
            ]
        if "FROM etl_dataset_versions" in self._last_sql:
            return [
                (2, "v2", True, "sch2", "r2", datetime(2026, 2, 17, 1, 3, 3)),
                (1, "v1", True, "sch1", "r1", datetime(2026, 2, 17, 1, 2, 3)),
            ]
        if "FROM etl_dataset_locations" in self._last_sql:
            return [
                (2, "v2", "local", "local_cache", "file:///tmp/demo", True, "abc", 123, datetime(2026, 2, 17, 1, 3, 4)),
            ]
        return []

    def fetchone(self):
        return self._row


class _FakeConn:
    def __init__(self):
        self.last_sql = ""
        self.last_params = None

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        return False

    def cursor(self):
        return _FakeCursor(self)


def test_list_datasets_returns_rows(monkeypatch):
    conn = _FakeConn()
    monkeypatch.setattr(ds, "get_database_url", lambda: "postgresql://u:p@h/db")
    monkeypatch.setattr(ds.psycopg, "connect", lambda *_: conn)
    rows = ds.list_datasets(limit=10)
    assert len(rows) == 1
    assert rows[0]["dataset_id"] == "serve.demo"
    assert rows[0]["version_count"] == 2
    assert rows[0]["latest_version"] == "v2"


def test_list_datasets_applies_query_filter(monkeypatch):
    conn = _FakeConn()
    monkeypatch.setattr(ds, "get_database_url", lambda: "postgresql://u:p@h/db")
    monkeypatch.setattr(ds.psycopg, "connect", lambda *_: conn)
    _ = ds.list_datasets(limit=7, q="demo")
    assert "WHERE (d.dataset_id ILIKE %s OR COALESCE(d.owner_user, '') ILIKE %s)" in conn.last_sql
    assert conn.last_params == ("%demo%", "%demo%", 7)


def test_get_dataset_returns_payload(monkeypatch):
    monkeypatch.setattr(ds, "get_database_url", lambda: "postgresql://u:p@h/db")
    monkeypatch.setattr(ds.psycopg, "connect", lambda *_: _FakeConn())
    payload = ds.get_dataset("serve.demo")
    assert payload is not None
    assert payload["dataset_id"] == "serve.demo"
    assert len(payload["versions"]) == 2
    assert payload["locations"][0]["location_type"] == "local_cache"


def test_get_dataset_missing_returns_none(monkeypatch):
    monkeypatch.setattr(ds, "get_database_url", lambda: "postgresql://u:p@h/db")
    monkeypatch.setattr(ds.psycopg, "connect", lambda *_: _FakeConn())
    assert ds.get_dataset("serve.missing") is None


def test_list_datasets_requires_db_url(monkeypatch):
    monkeypatch.setattr(ds, "get_database_url", lambda: None)
    with pytest.raises(ds.DatasetServiceError):
        ds.list_datasets(limit=5)

# research-etl
# Copyright (c) 2026 Joseph Weaver
# This file is part of the research-etl project and is licensed under the MIT License.
# You may not use this file except in compliance with the License.
# See https://github.com/josephweaver/research-etl for details.

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
        self._conn.executed.append((sql, params))
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
        elif "INSERT INTO etl_dataset_versions" in sql:
            self._row = (2,)
        elif "SELECT dataset_version_id, version_label" in sql and "FROM etl_dataset_versions" in sql:
            if params and params[0] == "serve.demo":
                self._row = (2, "v2")
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
            if "dataset_version_id = %s" in self._last_sql:
                return [
                    (10, "local", "local_cache", "C:/tmp/demo/file.txt", True, "abc", 123, datetime(2026, 2, 17, 1, 3, 4)),
                ]
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
        self.executed = []

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        return False

    def cursor(self):
        return _FakeCursor(self)

    def commit(self):
        return None


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


def test_store_data_persists_records(monkeypatch, tmp_path):
    conn = _FakeConn()
    src = tmp_path / "payload.txt"
    src.write_text("hello", encoding="utf-8")

    def _fake_transfer(**kwargs):
        return {
            "transport": kwargs["transport"],
            "target_uri": str(tmp_path / "stored" / "payload.txt"),
            "dry_run": False,
        }

    monkeypatch.setattr(ds, "_connect", lambda: conn)
    monkeypatch.setattr(ds, "_load_policy_or_none", lambda: None)
    monkeypatch.setattr(ds, "transfer_via_transport", _fake_transfer)
    monkeypatch.setattr(
        ds,
        "_infer_dataset_profile",
        lambda _src: {
            "format": "delimited",
            "sample_file": str(src),
            "columns": [{"name": "id", "type": "BIGINT"}],
            "row_count": 1,
            "schema_hash": "abc123",
        },
    )

    out = ds.store_data(
        dataset_id="serve.demo",
        source_path=str(src),
        stage="staging",
        runtime_context="local",
        version_label="v1",
    )

    assert out["dataset_id"] == "serve.demo"
    assert out["version_label"] == "v1"
    assert out["transport"] == "local_fs"
    assert any("INSERT INTO etl_datasets" in sql for sql, _ in conn.executed)
    assert any("INSERT INTO etl_dataset_versions" in sql for sql, _ in conn.executed)
    assert any("INSERT INTO etl_dataset_locations" in sql for sql, _ in conn.executed)
    assert any("INSERT INTO etl_dataset_events" in sql for sql, _ in conn.executed)
    assert any("INSERT INTO etl_dataset_profiles" in sql for sql, _ in conn.executed)
    assert any("UPDATE etl_dataset_versions" in sql and "schema_hash" in sql for sql, _ in conn.executed)
    assert out["schema_hash"] == "abc123"


def test_store_data_resolves_location_alias(monkeypatch, tmp_path):
    conn = _FakeConn()
    src = tmp_path / "payload.txt"
    src.write_text("hello", encoding="utf-8")
    captured = {}

    def _fake_transfer(**kwargs):
        captured.update(kwargs)
        return {
            "transport": kwargs["transport"],
            "target_uri": kwargs["target_uri"],
            "dry_run": False,
        }

    monkeypatch.setattr(ds, "_connect", lambda: conn)
    monkeypatch.setattr(ds, "_load_policy_or_none", lambda: None)
    monkeypatch.setattr(ds, "_infer_dataset_profile", lambda _src: {"columns": [], "schema_hash": None})
    monkeypatch.setattr(
        ds,
        "resolve_data_location_alias",
        lambda alias, config_path=None: {
            "alias": alias,
            "location_type": "gdrive",
            "transport": "rclone",
            "target_uri": "gdrive://LandCore/ETL",
            "options": {"rclone_bin": "bin/rclone"},
        },
    )
    monkeypatch.setattr(ds, "transfer_via_transport", _fake_transfer)

    out = ds.store_data(
        dataset_id="serve.demo",
        source_path=str(src),
        stage="published",
        runtime_context="local",
        version_label="v1",
        location_alias="LC_GDrive",
        transport_options={"shared_drive_id": "team123"},
    )
    assert out["location_alias"] == "LC_GDrive"
    assert out["location_type"] == "gdrive"
    assert out["transport"] == "rclone"
    assert captured["target_uri"] == "gdrive://LandCore/ETL"
    assert captured["options"]["rclone_bin"] == "bin/rclone"
    assert captured["options"]["shared_drive_id"] == "team123"


def test_store_data_auto_registers_workspace_entry(monkeypatch, tmp_path):
    conn = _FakeConn()
    src = tmp_path / "payload.csv"
    src.write_text("id,name\n1,a\n", encoding="utf-8")
    ws = tmp_path / "db" / "duckdb" / "workspace.yml"

    def _fake_transfer(**kwargs):
        return {
            "transport": kwargs["transport"],
            "target_uri": str(tmp_path / "datasets" / "raw.demo_v1"),
            "dry_run": False,
        }

    monkeypatch.setattr(ds, "_connect", lambda: conn)
    monkeypatch.setattr(ds, "_load_policy_or_none", lambda: None)
    monkeypatch.setattr(ds, "transfer_via_transport", _fake_transfer)
    monkeypatch.setattr(
        ds,
        "_infer_dataset_profile",
        lambda _src: {
            "format": "delimited",
            "sample_file": str(src),
            "columns": [{"name": "id", "type": "BIGINT"}, {"name": "name", "type": "VARCHAR"}],
            "row_count": 1,
            "schema_hash": "sch-demo",
        },
    )

    out = ds.store_data(
        dataset_id="raw.demo_v1",
        source_path=str(src),
        stage="staging",
        runtime_context="local",
        version_label="v1",
        project_id="crop_insurance",
        workspace_auto_register=True,
        workspace_config_path=str(ws),
    )

    assert out["profile"]["workspace_auto_register"]["updated"] is True
    auto_ws = Path(out["profile"]["workspace_auto_register"]["workspace_path"])
    assert auto_ws.exists()
    text = auto_ws.read_text(encoding="utf-8")
    assert "raw_demo_v1" in text
    assert "dataset_store_auto" in text


def test_store_data_policy_violation_raises(monkeypatch, tmp_path):
    src = tmp_path / "payload.txt"
    src.write_text("hello", encoding="utf-8")
    monkeypatch.setattr(ds, "_load_policy_or_none", lambda: {
        "classes": {"cache": {"allowed_location_types": ["hpcc_cache"]}},
        "locations": {"local_cache": {"kind": "filesystem", "root_path": str(tmp_path)}},
    })
    with pytest.raises(ds.DatasetServiceError) as ex:
        ds.store_data(
            dataset_id="serve.demo",
            source_path=str(src),
            stage="staging",
            runtime_context="local",
            location_type="local_cache",
            target_uri=str(tmp_path / "payload.txt"),
        )
    assert len(ex.value.details.get("operation_log") or []) > 0


def test_get_data_returns_direct_local_path(monkeypatch, tmp_path):
    conn = _FakeConn()
    src = tmp_path / "file.txt"
    src.write_text("hello", encoding="utf-8")

    class _Cursor(_FakeCursor):
        def fetchall(self):
            if "dataset_version_id = %s" in self._last_sql:
                return [
                    (10, "local", "local_cache", str(src), True, "abc", 5, datetime(2026, 2, 17, 1, 3, 4)),
                ]
            return super().fetchall()

    class _Conn(_FakeConn):
        def cursor(self):
            return _Cursor(self)

    monkeypatch.setattr(ds, "_connect", lambda: _Conn())
    out = ds.get_data(dataset_id="serve.demo", version="latest", runtime_context="local")
    assert out["dataset_id"] == "serve.demo"
    assert out["version_label"] == "v2"
    assert out["transport"] == "none"
    assert out["fetched"] is False
    assert out["local_path"] == str(src.resolve())


def test_get_data_fetches_when_no_direct(monkeypatch, tmp_path):
    conn = _FakeConn()
    src_uri = "gdrive://data/etl/serve/demo/v2/file.txt"
    captured = {}

    class _Cursor(_FakeCursor):
        def fetchall(self):
            if "dataset_version_id = %s" in self._last_sql:
                return [
                    (11, "local", "gdrive", src_uri, True, "abc", 5, datetime(2026, 2, 17, 1, 3, 4)),
                ]
            return super().fetchall()

    class _Conn(_FakeConn):
        def cursor(self):
            return _Cursor(self)

    def _fake_fetch(**kwargs):
        captured.update(kwargs)
        return {"target_path": str(tmp_path / "cache" / "file.txt"), "transport": kwargs["transport"], "dry_run": False}

    monkeypatch.setattr(ds, "_connect", lambda: _Conn())
    monkeypatch.setattr(ds, "fetch_via_transport", _fake_fetch)
    out = ds.get_data(dataset_id="serve.demo", version="latest", runtime_context="local", cache_dir=str(tmp_path / "cache"))
    assert out["transport"] == "rclone"
    assert out["fetched"] is True
    assert captured["source_uri"] == src_uri


def test_get_data_applies_alias_location_type_filter(monkeypatch, tmp_path):
    src_uri = "gdrive://data/etl/serve/demo/v2/file.txt"
    captured = {}

    class _Cursor(_FakeCursor):
        def fetchall(self):
            if "dataset_version_id = %s" in self._last_sql:
                return [
                    (11, "local", "gdrive", src_uri, True, "abc", 5, datetime(2026, 2, 17, 1, 3, 4)),
                ]
            return super().fetchall()

    class _Conn(_FakeConn):
        def cursor(self):
            return _Cursor(self)

    def _fake_fetch(**kwargs):
        captured.update(kwargs)
        return {"target_path": str(tmp_path / "cache" / "file.txt"), "transport": kwargs["transport"], "dry_run": False}

    monkeypatch.setattr(ds, "_connect", lambda: _Conn())
    monkeypatch.setattr(ds, "fetch_via_transport", _fake_fetch)
    monkeypatch.setattr(
        ds,
        "resolve_data_location_alias",
        lambda alias, config_path=None: {
            "alias": alias,
            "location_type": "gdrive",
            "transport": "rclone",
            "options": {"rclone_bin": "bin/rclone"},
        },
    )

    out = ds.get_data(
        dataset_id="serve.demo",
        version="latest",
        runtime_context="local",
        cache_dir=str(tmp_path / "cache"),
        location_alias="LC_GDrive",
    )
    assert out["location_alias"] == "LC_GDrive"
    assert captured["transport"] == "rclone"
    assert captured["options"]["rclone_bin"] == "bin/rclone"


def test_get_data_missing_version_raises(monkeypatch):
    class _Cursor(_FakeCursor):
        def execute(self, sql, params=None):
            super().execute(sql, params)
            if "SELECT dataset_version_id, version_label" in sql:
                self._row = None

    class _Conn(_FakeConn):
        def cursor(self):
            return _Cursor(self)

    monkeypatch.setattr(ds, "_connect", lambda: _Conn())
    with pytest.raises(ds.DatasetServiceError) as ex:
        ds.get_data(dataset_id="serve.demo", version="latest")
    assert len(ex.value.details.get("operation_log") or []) > 0

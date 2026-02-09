from __future__ import annotations

from datetime import datetime

import pytest

import etl.web_queries as wq


class _FakeCursor:
    def __init__(self, conn):
        self._conn = conn
        self._last_sql = ""
        self._last_params = None

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        return False

    def execute(self, sql, params=None):
        self._last_sql = sql
        self._last_params = params
        self._conn.last_sql = sql
        self._conn.last_params = params

    def fetchall(self):
        if "WITH grouped AS" in self._last_sql:
            return [
                ("pipelines/sample.yml", 3, 1, datetime(2026, 2, 8, 1, 2, 3), "failed", "local"),
            ]
        if "WHERE pipeline = %s" in self._last_sql and "SELECT\n                        run_id" in self._last_sql:
            return [
                ("r1", "pipelines/sample.yml", "failed", False, datetime(2026, 2, 8, 1, 2, 3), datetime(2026, 2, 8, 1, 2, 4), "", "local", ".runs/x")
            ]
        if "FROM etl_runs" in self._last_sql:
            return [
                ("r1", "p.yml", "succeeded", True, datetime(2026, 2, 8, 1, 2, 3), datetime(2026, 2, 8, 1, 2, 4), "", "local", ".runs/x")
            ]
        if "FROM etl_run_steps" in self._last_sql:
            return [("s1", "echo.py", True, False, None, {"ok": True})]
        if "FROM etl_run_step_attempts" in self._last_sql:
            return [("s1", 1, True, False, None, {"ok": True}, datetime(2026, 2, 8, 1, 2, 3), datetime(2026, 2, 8, 1, 2, 4))]
        if "FROM etl_run_events" in self._last_sql:
            return [(1, "run_completed", datetime(2026, 2, 8, 1, 2, 4), {"status": "succeeded"})]
        return []

    def fetchone(self):
        if "WITH stats AS" in self._last_sql:
            return (
                "pipelines/sample.yml",
                5,
                2,
                datetime(2026, 2, 8, 1, 2, 3),
                "r5",
                "succeeded",
                "slurm",
                datetime(2026, 2, 8, 1, 2, 3),
                datetime(2026, 2, 8, 1, 2, 4),
                "abc",
                "main",
                None,
                False,
                "etl run pipelines/sample.yml",
                "pchk",
                None,
                None,
                {"echo.py": "chk"},
            )
        if "FROM etl_runs" in self._last_sql:
            if "artifact_dir" in self._last_sql and "git_commit_sha" not in self._last_sql:
                return ("r1", "p.yml", "local", "succeeded", ".runs/x")
            return (
                "r1",
                "p.yml",
                "succeeded",
                True,
                datetime(2026, 2, 8, 1, 2, 3),
                datetime(2026, 2, 8, 1, 2, 4),
                "",
                "local",
                ".runs/x",
                "abc",
                "main",
                None,
                False,
                "etl run p.yml",
                "pchk",
                None,
                None,
                {"echo.py": "chk"},
            )
        return None


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


def test_fetch_runs_returns_rows(monkeypatch):
    monkeypatch.setattr(wq, "get_database_url", lambda: "postgresql://u:p@h/db")
    monkeypatch.setattr(wq.psycopg, "connect", lambda *_: _FakeConn())
    rows = wq.fetch_runs(limit=10)
    assert len(rows) == 1
    assert rows[0]["run_id"] == "r1"
    assert rows[0]["success"] is True


def test_fetch_runs_applies_filters(monkeypatch):
    conn = _FakeConn()
    monkeypatch.setattr(wq, "get_database_url", lambda: "postgresql://u:p@h/db")
    monkeypatch.setattr(wq.psycopg, "connect", lambda *_: conn)
    _ = wq.fetch_runs(limit=10, status="failed", executor="local", q="sample")
    assert "WHERE status = %s AND executor = %s AND (run_id ILIKE %s OR pipeline ILIKE %s)" in conn.last_sql
    assert conn.last_params == ("failed", "local", "%sample%", "%sample%", 10)


def test_fetch_run_detail_returns_payload(monkeypatch):
    monkeypatch.setattr(wq, "get_database_url", lambda: "postgresql://u:p@h/db")
    monkeypatch.setattr(wq.psycopg, "connect", lambda *_: _FakeConn())
    payload = wq.fetch_run_detail("r1")
    assert payload is not None
    assert payload["run_id"] == "r1"
    assert payload["provenance"]["git_commit_sha"] == "abc"
    assert payload["steps"][0]["step_name"] == "s1"
    assert payload["attempts"][0]["attempt_no"] == 1
    assert payload["events"][0]["event_type"] == "run_completed"


def test_fetch_run_header(monkeypatch):
    monkeypatch.setattr(wq, "get_database_url", lambda: "postgresql://u:p@h/db")
    monkeypatch.setattr(wq.psycopg, "connect", lambda *_: _FakeConn())
    hdr = wq.fetch_run_header("r1")
    assert hdr is not None
    assert hdr["run_id"] == "r1"
    assert hdr["artifact_dir"] == ".runs/x"


def test_fetch_pipelines_returns_rows(monkeypatch):
    monkeypatch.setattr(wq, "get_database_url", lambda: "postgresql://u:p@h/db")
    monkeypatch.setattr(wq.psycopg, "connect", lambda *_: _FakeConn())
    rows = wq.fetch_pipelines(limit=10)
    assert len(rows) == 1
    assert rows[0]["pipeline"] == "pipelines/sample.yml"
    assert rows[0]["total_runs"] == 3
    assert rows[0]["failed_runs"] == 1
    assert rows[0]["failure_rate"] == pytest.approx(1 / 3)


def test_fetch_pipelines_applies_filter(monkeypatch):
    conn = _FakeConn()
    monkeypatch.setattr(wq, "get_database_url", lambda: "postgresql://u:p@h/db")
    monkeypatch.setattr(wq.psycopg, "connect", lambda *_: conn)
    _ = wq.fetch_pipelines(limit=7, q="sample")
    assert "WHERE pipeline ILIKE %s" in conn.last_sql
    assert conn.last_params == ("%sample%", 7)


def test_fetch_pipeline_detail(monkeypatch):
    monkeypatch.setattr(wq, "get_database_url", lambda: "postgresql://u:p@h/db")
    monkeypatch.setattr(wq.psycopg, "connect", lambda *_: _FakeConn())
    payload = wq.fetch_pipeline_detail("pipelines/sample.yml")
    assert payload is not None
    assert payload["pipeline"] == "pipelines/sample.yml"
    assert payload["total_runs"] == 5
    assert payload["failed_runs"] == 2
    assert payload["latest_run"]["run_id"] == "r5"
    assert payload["latest_provenance"]["git_commit_sha"] == "abc"


def test_fetch_pipeline_runs(monkeypatch):
    monkeypatch.setattr(wq, "get_database_url", lambda: "postgresql://u:p@h/db")
    monkeypatch.setattr(wq.psycopg, "connect", lambda *_: _FakeConn())
    rows = wq.fetch_pipeline_runs("pipelines/sample.yml", limit=10)
    assert len(rows) == 1
    assert rows[0]["pipeline"] == "pipelines/sample.yml"
    assert rows[0]["status"] == "failed"


def test_fetch_runs_requires_db_url(monkeypatch):
    monkeypatch.setattr(wq, "get_database_url", lambda: None)
    with pytest.raises(wq.WebQueryError):
        wq.fetch_runs(limit=5)

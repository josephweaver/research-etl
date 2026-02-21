# research-etl
# Copyright (c) 2026 Joseph Weaver
# This file is part of the research-etl project and is licensed under the MIT License.
# You may not use this file except in compliance with the License.
# See https://github.com/josephweaver/research-etl for details.

from __future__ import annotations

import json
from pathlib import Path

from etl.pipeline import Step
from etl.runner import RunResult, StepResult
from etl import tracking


class _FakeCursor:
    def __init__(self, conn: "_FakeConn") -> None:
        self._conn = conn

    def __enter__(self) -> "_FakeCursor":
        return self

    def __exit__(self, exc_type, exc, tb) -> bool:
        return False

    def execute(self, sql: str, params=None) -> None:
        self._conn.executed.append((sql, params))

    def fetchone(self):
        return None


class _FakeConn:
    def __init__(self) -> None:
        self.executed = []

    def __enter__(self) -> "_FakeConn":
        return self

    def __exit__(self, exc_type, exc, tb) -> bool:
        return False

    def cursor(self) -> _FakeCursor:
        return _FakeCursor(self)

    def commit(self) -> None:
        self.executed.append(("COMMIT", None))


def test_record_run_writes_jsonl_and_db(monkeypatch, tmp_path: Path) -> None:
    fake_conn = _FakeConn()
    monkeypatch.setattr(tracking, "get_database_url", lambda: "postgresql://u:p@h/db")
    monkeypatch.setattr(tracking.psycopg, "connect", lambda _: fake_conn)

    step = Step(name="echo", script="echo.py")
    result = RunResult(
        run_id="run_001",
        steps=[StepResult(step=step, success=True, outputs={"message": "ok"})],
        artifact_dir=".runs/260208/010203-run_001",
    )
    result.started_at = "2026-02-08T01:02:03Z"  # type: ignore[attr-defined]
    result.ended_at = "2026-02-08T01:02:04Z"  # type: ignore[attr-defined]

    store = tmp_path / "runs.jsonl"
    tracking.record_run(
        result,
        "pipelines/sample.yml",
        store,
        executor="local",
        artifact_dir=result.artifact_dir,
    )

    lines = store.read_text(encoding="utf-8").strip().splitlines()
    assert len(lines) == 1
    payload = json.loads(lines[0])
    assert payload["run_id"] == "run_001"

    sql_text = "\n".join(sql for sql, _ in fake_conn.executed)
    assert "INSERT INTO etl_runs" in sql_text
    assert "INSERT INTO etl_run_steps" in sql_text
    assert "INSERT INTO etl_run_step_attempts" in sql_text
    assert "INSERT INTO etl_run_events" in sql_text


def test_upsert_step_attempt_updates_summary_and_attempt_tables(monkeypatch) -> None:
    fake_conn = _FakeConn()
    monkeypatch.setattr(tracking, "get_database_url", lambda: "postgresql://u:p@h/db")
    monkeypatch.setattr(tracking.psycopg, "connect", lambda _: fake_conn)

    tracking.upsert_step_attempt(
        run_id="run_002",
        step_name="echo",
        attempt_no=2,
        script="echo.py",
        success=True,
        outputs={"value": 123},
    )

    sql_text = "\n".join(sql for sql, _ in fake_conn.executed)
    assert "INSERT INTO etl_run_steps" in sql_text
    assert "INSERT INTO etl_run_step_attempts" in sql_text


def test_record_run_triggers_artifact_registration(monkeypatch, tmp_path: Path) -> None:
    called = {}
    monkeypatch.setattr(tracking, "_upsert_run_db", lambda *a, **k: None)

    def _register(**kwargs):
        called.update(kwargs)
        return 1

    monkeypatch.setattr(tracking, "register_run_artifacts", _register)
    step = Step(name="echo", script="echo.py")
    result = RunResult(
        run_id="run_003",
        steps=[StepResult(step=step, success=True, outputs={"path": str(tmp_path / "x.txt")})],
        artifact_dir=str(tmp_path / ".runs" / "r3"),
    )
    store = tmp_path / "runs.jsonl"
    tracking.record_run(result, "pipelines/sample.yml", store, executor="local", artifact_dir=result.artifact_dir)
    assert called["run_id"] == "run_003"
    assert called["pipeline"] == "pipelines/sample.yml"

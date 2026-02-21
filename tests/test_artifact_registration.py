# research-etl
# Copyright (c) 2026 Joseph Weaver
# This file is part of the research-etl project and is licensed under the MIT License.
# You may not use this file except in compliance with the License.
# See https://github.com/josephweaver/research-etl for details.

from __future__ import annotations

from pathlib import Path

from etl import artifacts as artifacts_module
from etl import tracking


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
        if "RETURNING artifact_id" in sql:
            self._conn.next_artifact_id += 1
            self._row = (self._conn.next_artifact_id,)
        elif "INSERT INTO etl_artifact_locations" in sql:
            self._conn.location_inserts.append(params)
            self._row = None
        else:
            self._row = None

    def fetchone(self):
        return self._row


class _FakeConn:
    def __init__(self) -> None:
        self.executed = []
        self.commits = 0
        self.next_artifact_id = 100
        self.location_inserts = []

    def __enter__(self) -> "_FakeConn":
        return self

    def __exit__(self, exc_type, exc, tb) -> bool:
        return False

    def cursor(self) -> _FakeCursor:
        return _FakeCursor(self)

    def commit(self) -> None:
        self.commits += 1


def test_register_step_artifacts_supports_explicit_and_inferred(monkeypatch, tmp_path: Path) -> None:
    fake_conn = _FakeConn()
    local_file = tmp_path / "cache" / "x.txt"
    local_file.parent.mkdir(parents=True, exist_ok=True)
    local_file.write_text("ok", encoding="utf-8")

    monkeypatch.setattr(artifacts_module, "get_database_url", lambda: "postgresql://u:p@h/db")
    monkeypatch.setattr(artifacts_module.psycopg, "connect", lambda _: fake_conn)

    inserted = artifacts_module.register_step_artifacts(
        run_id="r1",
        pipeline="pipelines/sample.yml",
        step_name="download",
        outputs={
            "_artifacts": [
                {
                    "uri": "gdrive://ResearchETL/published/file.tif",
                    "class": "published",
                    "location_type": "gdrive",
                    "canonical": True,
                }
            ],
            "downloaded_files": [str(local_file)],
        },
        executor="local",
    )
    assert inserted >= 2
    sql_text = "\n".join(sql for sql, _ in fake_conn.executed)
    assert "INSERT INTO etl_artifacts" in sql_text
    assert "INSERT INTO etl_artifact_locations" in sql_text


def test_register_step_artifacts_marks_policy_violation_for_out_of_root(monkeypatch, tmp_path: Path) -> None:
    fake_conn = _FakeConn()
    out_of_root = tmp_path / "other" / "bad.txt"
    out_of_root.parent.mkdir(parents=True, exist_ok=True)
    out_of_root.write_text("bad", encoding="utf-8")

    monkeypatch.setattr(artifacts_module, "get_database_url", lambda: "postgresql://u:p@h/db")
    monkeypatch.setattr(artifacts_module.psycopg, "connect", lambda _: fake_conn)
    monkeypatch.setattr(
        artifacts_module,
        "_registration_policy",
        lambda: {
            "classes": {"cache": {"allowed_location_types": ["local_cache"]}},
            "locations": {
                "local_cache": {
                    "kind": "filesystem",
                    "root_path": str((tmp_path / "cache").resolve()),
                }
            },
        },
    )

    inserted = artifacts_module.register_step_artifacts(
        run_id="r-root",
        pipeline="pipelines/sample.yml",
        step_name="s1",
        outputs={
            "_artifacts": [
                {
                    "uri": str(out_of_root),
                    "class": "cache",
                    "location_type": "local_cache",
                }
            ]
        },
        executor="local",
    )
    assert inserted == 1
    assert fake_conn.location_inserts
    # params: artifact_id, location_type, location_uri, is_canonical, state, last_error
    assert fake_conn.location_inserts[0][4] == "policy_violation"
    assert "outside root_path" in str(fake_conn.location_inserts[0][5])


def test_tracking_upsert_step_attempt_registers_artifacts(monkeypatch) -> None:
    called = {}
    fake_conn = _FakeConn()
    monkeypatch.setattr(tracking, "get_database_url", lambda: "postgresql://u:p@h/db")
    monkeypatch.setattr(tracking.psycopg, "connect", lambda _: fake_conn)

    def _register(**kwargs):
        called.update(kwargs)
        return 1

    monkeypatch.setattr(tracking, "register_step_artifacts", _register)
    tracking.upsert_step_attempt(
        run_id="r2",
        step_name="publish",
        attempt_no=1,
        script="publish.py",
        success=True,
        outputs={"uri": "gdrive://ResearchETL/published/x.parquet"},
        pipeline="pipelines/publish.yml",
        executor="slurm",
    )
    assert called["run_id"] == "r2"
    assert called["step_name"] == "publish"
    assert called["pipeline"] == "pipelines/publish.yml"

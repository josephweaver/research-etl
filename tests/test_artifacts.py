# research-etl
# Copyright (c) 2026 Joseph Weaver
# This file is part of the research-etl project and is licensed under the MIT License.
# You may not use this file except in compliance with the License.
# See https://github.com/josephweaver/research-etl for details.

from __future__ import annotations

from datetime import datetime, timedelta
from pathlib import Path

import pytest

import cli
from etl import artifacts as artifacts_module


class _FakeCursor:
    def __init__(self, conn: "_FakeConn") -> None:
        self._conn = conn
        self._rows = []

    def __enter__(self) -> "_FakeCursor":
        return self

    def __exit__(self, exc_type, exc, tb) -> bool:
        return False

    def execute(self, sql: str, params=None) -> None:
        self._conn.executed.append((sql, params))
        if "FROM etl_artifacts" in sql:
            limit = int(params[0]) if params else 2000
            self._rows = self._conn.artifacts[:limit]
            return
        if "FROM etl_artifact_locations" in sql and "WHERE artifact_id = ANY" in sql:
            wanted = {int(v) for v in (params[0] or [])}
            self._rows = [row for row in self._conn.locations if int(row[1]) in wanted]
            return
        if "UPDATE etl_artifact_locations" in sql:
            if "SET state = %s" in sql:
                new_state, _, location_id = params
                self._conn.location_state[int(location_id)] = str(new_state)
            elif "SET last_verified_at = NOW()," in sql:
                _, location_id = params
                self._conn.location_errors[int(location_id)] = True
            self._rows = []
            return
        self._rows = []

    def fetchall(self):
        return self._rows


class _FakeConn:
    def __init__(self, artifacts=None, locations=None) -> None:
        self.artifacts = list(artifacts or [])
        self.locations = list(locations or [])
        self.executed = []
        self.location_state = {}
        self.location_errors = {}
        self.commits = 0

    def __enter__(self) -> "_FakeConn":
        return self

    def __exit__(self, exc_type, exc, tb) -> bool:
        return False

    def cursor(self) -> _FakeCursor:
        return _FakeCursor(self)

    def commit(self) -> None:
        self.commits += 1


def test_load_artifact_policy_rejects_negative_retention(tmp_path: Path) -> None:
    cfg = tmp_path / "artifacts.yml"
    cfg.write_text(
        "classes:\n  cache:\n    retention_days: -1\n",
        encoding="utf-8",
    )
    with pytest.raises(artifacts_module.ArtifactPolicyError, match="retention_days"):
        artifacts_module.load_artifact_policy(cfg)


def test_load_artifact_policy_rejects_relative_root_path(tmp_path: Path) -> None:
    cfg = tmp_path / "artifacts.yml"
    cfg.write_text(
        "locations:\n  local_cache:\n    kind: filesystem\n    root_path: relative/cache\n",
        encoding="utf-8",
    )
    with pytest.raises(artifacts_module.ArtifactPolicyError, match="absolute path"):
        artifacts_module.load_artifact_policy(cfg)


def test_load_artifact_policy_accepts_posix_absolute_root_path(tmp_path: Path) -> None:
    cfg = tmp_path / "artifacts.yml"
    cfg.write_text(
        "locations:\n  hpcc_cache:\n    kind: filesystem\n    root_path: /mnt/scratch/weave151/cache\n",
        encoding="utf-8",
    )
    loaded = artifacts_module.load_artifact_policy(cfg)
    assert loaded["locations"]["hpcc_cache"]["root_path"] == "/mnt/scratch/weave151/cache"


def test_enforce_artifact_policies_flags_missing_canonical(monkeypatch) -> None:
    created = datetime.utcnow() - timedelta(days=2)
    fake_conn = _FakeConn(
        artifacts=[
            (1, "dataset/prism/2026-02-01", "published", created),
        ],
        locations=[
            (10, 1, "local_cache", "/tmp/prism", False, "present"),
        ],
    )
    monkeypatch.setattr(artifacts_module, "get_database_url", lambda: "postgresql://u:p@h/db")
    monkeypatch.setattr(artifacts_module.psycopg, "connect", lambda _: fake_conn)

    summary = artifacts_module.enforce_artifact_policies(
        config={
            "default_publish_location_type": "gdrive",
            "classes": {"published": {"require_canonical": True, "canonical_location_type": "gdrive"}},
            "locations": {"local_cache": {"kind": "filesystem"}, "gdrive": {"kind": "gdrive"}},
        },
        dry_run=True,
    )
    assert summary["violations_missing_canonical"] == 1
    assert summary["inspected_artifacts"] == 1


def test_enforce_artifact_policies_dry_run_does_not_delete_file(monkeypatch, tmp_path: Path) -> None:
    target = tmp_path / "cache.bin"
    target.write_text("x", encoding="utf-8")
    created = datetime.utcnow() - timedelta(days=40)
    fake_conn = _FakeConn(
        artifacts=[(7, "cache/item-7", "cache", created)],
        locations=[(70, 7, "local_cache", str(target), False, "present")],
    )
    monkeypatch.setattr(artifacts_module, "get_database_url", lambda: "postgresql://u:p@h/db")
    monkeypatch.setattr(artifacts_module.psycopg, "connect", lambda _: fake_conn)

    summary = artifacts_module.enforce_artifact_policies(
        config={
            "classes": {"cache": {"retention_days": 30, "enforce_delete": True}},
            "locations": {"local_cache": {"kind": "filesystem"}},
        },
        dry_run=True,
    )
    assert summary["would_delete_files"] == 1
    assert target.exists()
    assert fake_conn.location_state == {}


def test_cli_parser_includes_artifacts_enforce() -> None:
    parser = cli.build_parser()
    args = parser.parse_args(["artifacts", "enforce", "--dry-run", "--limit", "123"])
    assert args.command == "artifacts"
    assert args.artifacts_command == "enforce"
    assert args.dry_run is True
    assert args.limit == 123

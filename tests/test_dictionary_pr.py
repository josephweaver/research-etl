from __future__ import annotations

import subprocess
from pathlib import Path

import pytest

from etl import dictionary_pr as dpr


class _FakeCursor:
    def __init__(self, conn):
        self._conn = conn
        self._row = None
        self._rows = []

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        return False

    def execute(self, sql, params=None):
        self._conn.executed.append((sql, params))
        if "FROM etl_dictionary_repos" in sql:
            key = params[0]
            self._row = self._conn.repo_rows.get(key)
            self._rows = []
            return
        if "FROM etl_dataset_dictionary_entries" in sql and "SELECT file_path" in sql:
            self._row = self._conn.file_path_row
            self._rows = []
            return
        self._row = None
        self._rows = []

    def fetchone(self):
        return self._row

    def fetchall(self):
        return self._rows


class _FakeConn:
    def __init__(self, repo_rows=None, file_path_row=None):
        self.repo_rows = dict(repo_rows or {})
        self.file_path_row = file_path_row
        self.executed = []
        self.committed = False

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        return False

    def cursor(self):
        return _FakeCursor(self)

    def commit(self):
        self.committed = True


def test_create_dictionary_pr_happy_path(monkeypatch, tmp_path: Path) -> None:
    repo = tmp_path / "catalog_repo"
    repo.mkdir(parents=True, exist_ok=True)
    src = tmp_path / "entry.yml"
    src.write_text("dataset_id: serve.demo\n", encoding="utf-8")

    fake_conn = _FakeConn(
        repo_rows={
            "catalog": (
                1,
                "catalog",
                "github",
                "landcore",
                "landcore-data-catalog",
                "main",
                str(repo),
                True,
            )
        },
        file_path_row=("datasets/serve.demo.yml",),
    )
    monkeypatch.setattr(dpr, "_connect", lambda: fake_conn)

    def _fake_run_cmd(cmd, *, cwd, trace, check=True):
        text = " ".join(cmd)
        if "diff --cached --quiet" in text:
            return subprocess.CompletedProcess(cmd, 1, "", "")
        if "rev-parse HEAD:" in text:
            return subprocess.CompletedProcess(cmd, 0, "filesha123\n", "")
        if "rev-parse HEAD" in text:
            return subprocess.CompletedProcess(cmd, 0, "commitsha123\n", "")
        return subprocess.CompletedProcess(cmd, 0, "", "")

    monkeypatch.setattr(dpr, "_run_cmd", _fake_run_cmd)
    monkeypatch.setattr(
        dpr,
        "_github_api_create_pr",
        lambda **kwargs: ("https://github.com/landcore/landcore-data-catalog/pull/9", 9),
    )

    out = dpr.create_dictionary_pr(
        dataset_id="serve.demo",
        repo_key="catalog",
        source_file=str(src),
        create_pr=True,
        use_github_api=True,
        dry_run=False,
    )
    assert out["has_changes"] is True
    assert out["commit_sha"] == "commitsha123"
    assert out["pr_number"] == 9
    assert out["file_path"] == "datasets/serve.demo.yml"
    assert fake_conn.committed is True


def test_create_dictionary_pr_missing_repo_raises(monkeypatch, tmp_path: Path) -> None:
    src = tmp_path / "entry.yml"
    src.write_text("dataset_id: serve.demo\n", encoding="utf-8")
    monkeypatch.setattr(dpr, "_connect", lambda: _FakeConn())
    with pytest.raises(dpr.DictionaryPRError):
        dpr.create_dictionary_pr(
            dataset_id="serve.demo",
            repo_key="missing",
            source_file=str(src),
        )

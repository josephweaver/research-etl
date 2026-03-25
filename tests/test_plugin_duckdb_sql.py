# research-etl
# Copyright (c) 2026 Joseph Weaver
# This file is part of the research-etl project and is licensed under the MIT License.
# You may not use this file except in compliance with the License.
# See https://github.com/josephweaver/research-etl for details.

from __future__ import annotations

from pathlib import Path

from etl.plugins.base import PluginContext
from plugins import duckdb_sql


def _ctx(tmp_path: Path) -> PluginContext:
    return PluginContext(run_id="t1", workdir=tmp_path / "work", log=lambda *_a, **_k: None)


def test_duckdb_sql_runs_inline_create_and_select(tmp_path: Path) -> None:
    db_path = tmp_path / "db" / "test.duckdb"
    out = duckdb_sql.run(
        {
            "db_path": str(db_path),
            "sql": """
                CREATE TABLE demo AS SELECT 1 AS id, 'a' AS label;
                SELECT COUNT(*) AS n FROM demo;
            """,
        },
        _ctx(tmp_path),
    )
    assert out["db_path"].endswith("test.duckdb")
    assert out["statements_executed"] == 2
    assert out["columns"] == ["n"]
    assert out["rows"] == [[1]]


def test_duckdb_sql_resolves_sql_file_from_workspace_root(tmp_path: Path) -> None:
    repo = tmp_path / "landcore-duckdb"
    tables = repo / "tables" / "yanroy"
    tables.mkdir(parents=True)
    workspace = repo / "workspace.yml"
    sql_file = repo / "views" / "yanroy" / "count.sql"
    sql_file.parent.mkdir(parents=True)
    workspace.write_text("name: demo\n", encoding="utf-8")
    sql_file.write_text("SELECT 42 AS answer;", encoding="utf-8")

    out = duckdb_sql.run(
        {
            "workspace_config_path": str(workspace),
            "sql_file": "views/yanroy/count.sql",
            "db_path": ":memory:",
        },
        _ctx(tmp_path),
    )
    assert out["workspace_root"] == repo.as_posix()
    assert out["columns"] == ["answer"]
    assert out["rows"] == [[42]]

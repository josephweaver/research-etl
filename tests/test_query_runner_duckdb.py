from __future__ import annotations

from pathlib import Path

import pytest

from etl.query.errors import QueryExecutionError
from etl.query.planner_duckdb import build_duckdb_query_plan
from etl.query.runners.duckdb_runner import run_duckdb_query_plan
from etl.query.spec import validate_query_spec


def test_run_duckdb_query_plan_executes(tmp_path: Path) -> None:
    pytest.importorskip("duckdb")
    csv_path = tmp_path / "demo.csv"
    csv_path.write_text("id,name\n1,A\n2,B\n", encoding="utf-8")
    spec = validate_query_spec({"source": str(csv_path), "order_by": [{"column": "id", "direction": "asc"}]})
    plan = build_duckdb_query_plan(spec)
    result = run_duckdb_query_plan(plan)
    assert result["engine"] == "duckdb"
    assert [c["name"] for c in result["columns"]] == ["id", "name"]
    assert result["rows"] == [[1, "A"], [2, "B"]]


def test_run_duckdb_query_plan_wraps_sql_errors() -> None:
    pytest.importorskip("duckdb")
    with pytest.raises(QueryExecutionError) as exc:
        run_duckdb_query_plan({"sql": "SELECT * FROM missing_table", "params": []})
    assert exc.value.error_code == "execution_error"
    assert "sql" in (exc.value.detail or {})


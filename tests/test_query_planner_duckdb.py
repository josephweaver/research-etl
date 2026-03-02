from __future__ import annotations

import pytest

from etl.query.errors import QueryPlannerError
from etl.query.planner_duckdb import build_duckdb_query_plan
from etl.query.spec import validate_query_spec


def test_build_duckdb_query_plan_with_filters_and_order(tmp_path) -> None:
    src = tmp_path / "demo.csv"
    spec = validate_query_spec(
        {
            "source": str(src),
            "select": ["id", "name"],
            "filter": [{"column": "id", "op": "in", "value": [1, 2, 3]}],
            "order_by": [{"column": "id", "direction": "desc"}],
            "limit": 5,
            "offset": 1,
        }
    )
    plan = build_duckdb_query_plan(spec)
    assert plan["engine"] == "duckdb"
    assert "read_csv_auto" in plan["sql"]
    assert '"id" IN (?, ?, ?)' in plan["sql"]
    assert 'ORDER BY "id" DESC' in plan["sql"]
    assert "LIMIT 5 OFFSET 1" in plan["sql"]
    assert plan["params"] == [1, 2, 3]


def test_build_duckdb_query_plan_rejects_unsafe_derive_expr() -> None:
    spec = validate_query_spec(
        {
            "source": "data/demo.csv",
            "derive": [{"name": "x", "expr": "upper(name); drop table t"}],
        }
    )
    with pytest.raises(QueryPlannerError) as exc:
        build_duckdb_query_plan(spec)
    assert exc.value.error_code == "planner_error"
    assert str(exc.value.detail.get("field")) == "derive[0].expr"


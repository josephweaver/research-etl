from __future__ import annotations

import pytest

from etl.query.errors import QueryPlannerError
from etl.query.spec import validate_query_spec


def test_validate_query_spec_normalizes_minimal_source() -> None:
    out = validate_query_spec({"source": "data/demo.parquet"})
    assert out["source"] == {"path": "data/demo.parquet"}
    assert out["select"] == ["*"]
    assert out["derive"] == []
    assert out["filter"] == []
    assert out["order_by"] == []
    assert out["limit"] == 1000
    assert out["offset"] == 0


def test_validate_query_spec_normalizes_full_payload() -> None:
    out = validate_query_spec(
        {
            "source": {"alias": "serve.demo_v1", "format": "parquet", "options": {"hive_partitioning": True}},
            "select": ["id", "name"],
            "derive": [{"name": "name_upper", "expr": "upper(name)"}],
            "filter": [{"column": "id", "op": "gt", "value": 10}],
            "order_by": [{"column": "id", "direction": "DESC"}],
            "limit": 25,
            "offset": 5,
        }
    )
    assert out["source"]["alias"] == "serve.demo_v1"
    assert out["source"]["format"] == "parquet"
    assert out["select"] == ["id", "name"]
    assert out["derive"][0]["name"] == "name_upper"
    assert out["filter"][0]["op"] == "gt"
    assert out["order_by"][0]["direction"] == "desc"
    assert out["limit"] == 25
    assert out["offset"] == 5


def test_validate_query_spec_rejects_missing_source() -> None:
    with pytest.raises(QueryPlannerError) as exc:
        validate_query_spec({"select": ["id"]})
    assert exc.value.error_code == "planner_error"
    assert exc.value.detail["field"] == "source"


def test_validate_query_spec_rejects_bad_filter_op() -> None:
    with pytest.raises(QueryPlannerError) as exc:
        validate_query_spec(
            {"source": "data/demo.csv", "filter": [{"column": "id", "op": "between", "value": [1, 2]}]}
        )
    assert exc.value.detail["field"] == "filter[0].op"


def test_validate_query_spec_rejects_limit_out_of_bounds() -> None:
    with pytest.raises(QueryPlannerError) as exc:
        validate_query_spec({"source": "data/demo.csv", "limit": 0})
    assert exc.value.detail["field"] == "limit"


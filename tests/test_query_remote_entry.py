from __future__ import annotations

from pathlib import Path

import pytest

from etl.query.remote_entry import run_query_request


def test_remote_entry_runs_query_success(tmp_path: Path) -> None:
    pytest.importorskip("duckdb")
    csv_path = tmp_path / "demo.csv"
    csv_path.write_text("id,name\n1,A\n2,B\n", encoding="utf-8")

    out = run_query_request({"query_spec": {"source": str(csv_path), "select": ["id"], "limit": 1}})
    assert out["ok"] is True
    result = out["result"]
    assert result["engine"] == "duckdb"
    assert result["rows"] == [[1]]


def test_remote_entry_returns_planner_error_for_invalid_payload() -> None:
    out = run_query_request({"query_spec": "bad"})
    assert out["ok"] is False
    err = out["error"]
    assert err["error_code"] == "planner_error"

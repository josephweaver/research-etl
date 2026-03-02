from __future__ import annotations

from pathlib import Path

import pytest

from etl.executors.local import LocalExecutor


def test_local_executor_query_capability_enabled() -> None:
    ex = LocalExecutor()
    caps = ex.capabilities()
    assert caps["query_data"] is True


def test_local_executor_query_data_executes_csv_preview(tmp_path: Path) -> None:
    pytest.importorskip("duckdb")
    csv_path = tmp_path / "demo.csv"
    csv_path.write_text("id,name\n1,A\n2,B\n3,C\n", encoding="utf-8")

    ex = LocalExecutor()
    payload = ex.query_data(
        {
            "source": str(csv_path),
            "select": ["id", "name"],
            "filter": [{"column": "id", "op": "gte", "value": 2}],
            "order_by": [{"column": "id", "direction": "desc"}],
            "limit": 2,
        }
    )
    assert payload["engine"] == "duckdb"
    assert payload["executor"] == "local"
    assert [c["name"] for c in payload["columns"]] == ["id", "name"]
    assert payload["rows"] == [[3, "C"], [2, "B"]]

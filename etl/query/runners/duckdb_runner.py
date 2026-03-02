# research-etl
# Copyright (c) 2026 Joseph Weaver
# This file is part of the research-etl project and is licensed under the MIT License.
# You may not use this file except in compliance with the License.
# See https://github.com/josephweaver/research-etl for details.

"""DuckDB query plan runner."""

from __future__ import annotations

from datetime import date, datetime, time
from decimal import Decimal
from time import perf_counter
from typing import Any, Dict, List

from ..errors import QueryExecutionError


def _to_jsonable(value: Any) -> Any:
    if value is None:
        return None
    if isinstance(value, (str, int, float, bool)):
        return value
    if isinstance(value, (datetime, date, time)):
        return value.isoformat()
    if isinstance(value, Decimal):
        return float(value)
    if isinstance(value, (list, tuple)):
        return [_to_jsonable(v) for v in value]
    if isinstance(value, dict):
        return {str(k): _to_jsonable(v) for k, v in value.items()}
    return str(value)


def run_duckdb_query_plan(plan: Dict[str, Any]) -> Dict[str, Any]:
    try:
        import duckdb
    except Exception as exc:  # noqa: BLE001
        raise QueryExecutionError("DuckDB runner requires the `duckdb` package.") from exc

    sql = str(plan.get("sql") or "").strip()
    if not sql:
        raise QueryExecutionError("Query plan is missing SQL text.", detail={"field": "plan.sql"})
    params = list(plan.get("params") or [])

    started = perf_counter()
    try:
        con = duckdb.connect(":memory:")
        try:
            cur = con.execute(sql, params)
            raw_rows = cur.fetchall()
            desc = list(cur.description or [])
        finally:
            con.close()
    except QueryExecutionError:
        raise
    except Exception as exc:  # noqa: BLE001
        raise QueryExecutionError(
            "DuckDB execution failed.",
            detail={"sql": sql, "params_count": len(params), "error": str(exc)},
        ) from exc

    elapsed_ms = int((perf_counter() - started) * 1000.0)
    columns: List[Dict[str, Any]] = []
    for item in desc:
        name = str(item[0]) if item else ""
        dtype = str(item[1]) if len(item) > 1 else ""
        columns.append({"name": name, "type": dtype})

    rows = [[_to_jsonable(v) for v in row] for row in raw_rows]
    return {
        "columns": columns,
        "rows": rows,
        "row_count_estimate": len(rows),
        "elapsed_ms": elapsed_ms,
        "engine": "duckdb",
    }


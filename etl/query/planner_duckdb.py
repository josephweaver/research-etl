# research-etl
# Copyright (c) 2026 Joseph Weaver
# This file is part of the research-etl project and is licensed under the MIT License.
# You may not use this file except in compliance with the License.
# See https://github.com/josephweaver/research-etl for details.

"""DuckDB planner for query_spec v1."""

from __future__ import annotations

from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

from .errors import QueryPlannerError


def _quote_ident(name: str) -> str:
    text = str(name or "").strip()
    if not text:
        raise QueryPlannerError("Empty identifier is not allowed.", detail={"field": "identifier"})
    return '"' + text.replace('"', '""') + '"'


def _quote_sql_literal(text: str) -> str:
    return "'" + str(text or "").replace("'", "''") + "'"


def _resolve_source_path(source: Dict[str, Any], context: Optional[Dict[str, Any]] = None) -> str:
    if source.get("dataset_id") or source.get("alias"):
        raise QueryPlannerError(
            "dataset_id/alias sources are not implemented yet for DuckDB planner.",
            detail={"field": "source", "source": source},
        )
    raw = str(source.get("path") or source.get("uri") or "").strip()
    if not raw:
        raise QueryPlannerError("Source path/uri is required.", detail={"field": "source"})

    p = Path(raw).expanduser()
    base = Path(".").resolve()
    if isinstance(context, dict):
        for key in ("repo_root", "workdir", "base_dir"):
            candidate = str(context.get(key) or "").strip()
            if candidate:
                base = Path(candidate).expanduser().resolve()
                break
    if not p.is_absolute():
        p = (base / p).resolve()
    return p.as_posix()


def _detect_format(source: Dict[str, Any], source_path: str) -> str:
    configured = str(source.get("format") or "").strip().lower()
    if configured:
        if configured not in {"csv", "parquet"}:
            raise QueryPlannerError(
                "Unsupported source format; expected csv or parquet.",
                detail={"field": "source.format", "value": configured},
            )
        return configured
    suffix = Path(source_path).suffix.lower()
    if suffix in {".parquet", ".pq"}:
        return "parquet"
    return "csv"


def _build_source_expr(source: Dict[str, Any], context: Optional[Dict[str, Any]] = None) -> Tuple[str, Dict[str, Any]]:
    path = _resolve_source_path(source, context=context)
    fmt = _detect_format(source, path)
    path_lit = _quote_sql_literal(path)
    if fmt == "parquet":
        expr = f"read_parquet({path_lit}, union_by_name=true)"
    else:
        expr = f"read_csv_auto({path_lit}, header=true)"
    return expr, {"path": path, "format": fmt}


def _validate_derive_expr(expr: str, *, field: str) -> None:
    text = str(expr or "")
    blocked = [";", "--", "/*", "*/"]
    for token in blocked:
        if token in text:
            raise QueryPlannerError(
                "Unsafe token in derive expression.",
                detail={"field": field, "token": token},
            )


def _build_projection(spec: Dict[str, Any]) -> str:
    select = list(spec.get("select") or [])
    derive = list(spec.get("derive") or [])
    parts: List[str] = []

    if select:
        if "*" in select and len(select) > 1:
            raise QueryPlannerError("`select` cannot mix '*' with named columns.", detail={"field": "select"})
        if select == ["*"]:
            parts.append("*")
        else:
            parts.extend(_quote_ident(col) for col in select)

    for idx, entry in enumerate(derive):
        expr = str(entry.get("expr") or "").strip()
        _validate_derive_expr(expr, field=f"derive[{idx}].expr")
        name = _quote_ident(str(entry.get("name") or "").strip())
        parts.append(f"({expr}) AS {name}")

    if not parts:
        return "*"
    return ", ".join(parts)


def _build_where(spec: Dict[str, Any]) -> Tuple[str, List[Any]]:
    clauses: List[str] = []
    params: List[Any] = []
    for idx, entry in enumerate(spec.get("filter") or []):
        col = _quote_ident(str(entry.get("column") or ""))
        op = str(entry.get("op") or "").strip().lower()
        field = f"filter[{idx}].op"
        if op == "eq":
            clauses.append(f"{col} = ?")
            params.append(entry.get("value"))
        elif op == "ne":
            clauses.append(f"{col} <> ?")
            params.append(entry.get("value"))
        elif op == "gt":
            clauses.append(f"{col} > ?")
            params.append(entry.get("value"))
        elif op == "gte":
            clauses.append(f"{col} >= ?")
            params.append(entry.get("value"))
        elif op == "lt":
            clauses.append(f"{col} < ?")
            params.append(entry.get("value"))
        elif op == "lte":
            clauses.append(f"{col} <= ?")
            params.append(entry.get("value"))
        elif op in {"in", "not_in"}:
            values = list(entry.get("value") or [])
            if not values:
                raise QueryPlannerError("Filter list cannot be empty.", detail={"field": f"filter[{idx}].value"})
            placeholders = ", ".join("?" for _ in values)
            comparator = "IN" if op == "in" else "NOT IN"
            clauses.append(f"{col} {comparator} ({placeholders})")
            params.extend(values)
        elif op == "contains":
            clauses.append(f"CAST({col} AS VARCHAR) LIKE ?")
            params.append(f"%{entry.get('value')}%")
        elif op == "starts_with":
            clauses.append(f"CAST({col} AS VARCHAR) LIKE ?")
            params.append(f"{entry.get('value')}%")
        elif op == "ends_with":
            clauses.append(f"CAST({col} AS VARCHAR) LIKE ?")
            params.append(f"%{entry.get('value')}")
        elif op == "is_null":
            clauses.append(f"{col} IS NULL")
        elif op == "not_null":
            clauses.append(f"{col} IS NOT NULL")
        else:
            raise QueryPlannerError("Unsupported filter operation.", detail={"field": field, "value": op})

    if not clauses:
        return "", params
    return " WHERE " + " AND ".join(clauses), params


def _build_order_by(spec: Dict[str, Any]) -> str:
    order = list(spec.get("order_by") or [])
    if not order:
        return ""
    parts = [f"{_quote_ident(str(x.get('column') or ''))} {str(x.get('direction') or 'asc').upper()}" for x in order]
    return " ORDER BY " + ", ".join(parts)


def build_duckdb_query_plan(query_spec: Dict[str, Any], context: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
    source_expr, source_meta = _build_source_expr(dict(query_spec.get("source") or {}), context=context)
    projection = _build_projection(query_spec)
    where_sql, params = _build_where(query_spec)
    order_sql = _build_order_by(query_spec)
    limit = int(query_spec.get("limit") or 1000)
    offset = int(query_spec.get("offset") or 0)
    sql = (
        f"SELECT {projection} "
        f"FROM ({source_expr}) AS src"
        f"{where_sql}"
        f"{order_sql}"
        f" LIMIT {limit} OFFSET {offset}"
    )
    return {
        "engine": "duckdb",
        "sql": sql,
        "params": params,
        "source": source_meta,
        "limit": limit,
        "offset": offset,
    }


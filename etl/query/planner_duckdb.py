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


def _quote_column_ref(name: str) -> str:
    text = str(name or "").strip()
    if text == "*":
        return "*"
    if text.endswith(".*") and "." in text:
        left = text[:-2]
        return f'{_quote_ident(left)}.*'
    parts = [p.strip() for p in text.split(".") if str(p).strip()]
    if len(parts) == 1:
        return _quote_ident(parts[0])
    if len(parts) == 2:
        return f"{_quote_ident(parts[0])}.{_quote_ident(parts[1])}"
    raise QueryPlannerError("Invalid dotted column reference.", detail={"field": "column", "value": text})


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
            parts.extend(_quote_column_ref(col) for col in select)

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
        col = _quote_column_ref(str(entry.get("column") or ""))
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
    parts = [f"{_quote_column_ref(str(x.get('column') or ''))} {str(x.get('direction') or 'asc').upper()}" for x in order]
    return " ORDER BY " + ", ".join(parts)


def _coerce_type_name(text: str) -> str:
    raw = str(text or "").strip()
    if not raw:
        raise QueryPlannerError("Column type override cannot be empty.", detail={"field": "tables.columns.type"})
    allowed = set("ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789_(), ")
    if any(ch not in allowed for ch in raw):
        raise QueryPlannerError("Unsafe token in column type override.", detail={"field": "tables.columns.type", "value": raw})
    return raw


def _build_table_ctes(tables: List[Dict[str, Any]], *, context: Optional[Dict[str, Any]]) -> Tuple[List[str], Dict[str, Any]]:
    ctes: List[str] = []
    meta_tables: List[Dict[str, Any]] = []
    for idx, table in enumerate(tables):
        table_name = str(table.get("name") or "").strip()
        if not table_name:
            raise QueryPlannerError("Table name is required.", detail={"field": f"tables[{idx}].name"})
        source_expr, source_meta = _build_source_expr(dict(table.get("source") or {}), context=context)
        cols = list(table.get("columns") or [])
        if cols:
            proj: List[str] = []
            for cidx, col in enumerate(cols):
                col_name = str((col or {}).get("name") or "").strip()
                if not col_name:
                    raise QueryPlannerError(
                        "Table column override is missing name.",
                        detail={"field": f"tables[{idx}].columns[{cidx}].name"},
                    )
                col_ref = _quote_ident(col_name)
                col_type = str((col or {}).get("type") or "").strip()
                if col_type:
                    proj.append(f"CAST({col_ref} AS {_coerce_type_name(col_type)}) AS {col_ref}")
                else:
                    proj.append(f"{col_ref}")
            cte_sql = f"{_quote_ident(table_name)} AS (SELECT {', '.join(proj)} FROM ({source_expr}) AS src)"
        else:
            cte_sql = f"{_quote_ident(table_name)} AS (SELECT * FROM ({source_expr}) AS src)"
        ctes.append(cte_sql)
        meta_tables.append(
            {
                "name": table_name,
                "source": source_meta,
                "column_overrides": cols,
            }
        )
    return ctes, {"tables": meta_tables}


def _join_comparator(op: str) -> str:
    mapping = {
        "eq": "=",
        "ne": "<>",
        "gt": ">",
        "gte": ">=",
        "lt": "<",
        "lte": "<=",
    }
    out = mapping.get(str(op or "").strip().lower())
    if not out:
        raise QueryPlannerError("Unsupported join operation.", detail={"field": "joins.on.op", "value": op})
    return out


def _build_from_and_joins(spec: Dict[str, Any]) -> str:
    tables = list(spec.get("tables") or [])
    if not tables:
        return ""
    from_table = str(spec.get("from_table") or "").strip()
    sql = f" FROM {_quote_ident(from_table)}"
    joins = list(spec.get("joins") or [])
    for idx, join in enumerate(joins):
        jt = str(join.get("type") or "inner").strip().lower().upper()
        right = _quote_ident(str(join.get("right_table") or "").strip())
        if jt == "CROSS":
            sql += f" CROSS JOIN {right}"
            continue
        clauses = []
        for cidx, cond in enumerate(join.get("on") or []):
            left = _quote_column_ref(str(cond.get("left") or ""))
            right_ref = _quote_column_ref(str(cond.get("right") or ""))
            op = _join_comparator(str(cond.get("op") or "eq"))
            clauses.append(f"{left} {op} {right_ref}")
            if not clauses[-1]:
                raise QueryPlannerError("Invalid join condition.", detail={"field": f"joins[{idx}].on[{cidx}]"})
        if not clauses:
            raise QueryPlannerError("Join requires at least one ON clause.", detail={"field": f"joins[{idx}].on"})
        sql += f" {jt} JOIN {right} ON " + " AND ".join(clauses)
    return sql


def build_duckdb_query_plan(query_spec: Dict[str, Any], context: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
    projection = _build_projection(query_spec)
    where_sql, params = _build_where(query_spec)
    order_sql = _build_order_by(query_spec)
    limit = int(query_spec.get("limit") or 1000)
    offset = int(query_spec.get("offset") or 0)
    tables = list(query_spec.get("tables") or [])
    if tables:
        ctes, source_meta = _build_table_ctes(tables, context=context)
        from_sql = _build_from_and_joins(query_spec)
        sql = (
            f"WITH {', '.join(ctes)} "
            f"SELECT {projection}"
            f"{from_sql}"
            f"{where_sql}"
            f"{order_sql}"
            f" LIMIT {limit} OFFSET {offset}"
        )
    else:
        source_expr, single_source_meta = _build_source_expr(dict(query_spec.get("source") or {}), context=context)
        sql = (
            f"SELECT {projection} "
            f"FROM ({source_expr}) AS src"
            f"{where_sql}"
            f"{order_sql}"
            f" LIMIT {limit} OFFSET {offset}"
        )
        source_meta = {"source": single_source_meta}
    return {
        "engine": "duckdb",
        "sql": sql,
        "params": params,
        "source": source_meta,
        "limit": limit,
        "offset": offset,
    }

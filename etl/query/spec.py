# research-etl
# Copyright (c) 2026 Joseph Weaver
# This file is part of the research-etl project and is licensed under the MIT License.
# You may not use this file except in compliance with the License.
# See https://github.com/josephweaver/research-etl for details.

"""Validation and normalization for query_spec v1."""

from __future__ import annotations

from typing import Any, Dict, List

from .errors import QueryPlannerError

_QUERY_SPEC_ALLOWED_KEYS = {"source", "select", "derive", "filter", "order_by", "limit", "offset"}
_FILTER_ALLOWED_OPS = {
    "eq",
    "ne",
    "gt",
    "gte",
    "lt",
    "lte",
    "in",
    "not_in",
    "contains",
    "starts_with",
    "ends_with",
    "is_null",
    "not_null",
}
_ORDER_ALLOWED = {"asc", "desc"}


def _planner_error(message: str, *, field: str, value: Any = None) -> QueryPlannerError:
    detail = {"field": field}
    if value is not None:
        detail["value"] = value
    return QueryPlannerError(message, detail=detail)


def _require_non_empty_str(value: Any, *, field: str) -> str:
    if not isinstance(value, str):
        raise _planner_error("Expected a string.", field=field, value=value)
    out = value.strip()
    if not out:
        raise _planner_error("Value cannot be empty.", field=field, value=value)
    return out


def _normalize_source(value: Any) -> Dict[str, Any]:
    if isinstance(value, str):
        return {"path": _require_non_empty_str(value, field="source")}
    if not isinstance(value, dict):
        raise _planner_error("`source` must be a string path or object.", field="source", value=value)
    raw = dict(value)
    for key in raw.keys():
        if key not in {"path", "uri", "dataset_id", "alias", "format", "options"}:
            raise _planner_error("Unsupported source field.", field=f"source.{key}", value=raw.get(key))
    selectors = [k for k in ("path", "uri", "dataset_id", "alias") if str(raw.get(k) or "").strip()]
    if len(selectors) != 1:
        raise _planner_error(
            "Source must provide exactly one selector: path, uri, dataset_id, or alias.",
            field="source",
            value={k: raw.get(k) for k in ("path", "uri", "dataset_id", "alias")},
        )
    out: Dict[str, Any] = {}
    selected = selectors[0]
    out[selected] = _require_non_empty_str(raw.get(selected), field=f"source.{selected}")
    if "format" in raw and raw.get("format") is not None:
        out["format"] = _require_non_empty_str(raw.get("format"), field="source.format").lower()
    if "options" in raw and raw.get("options") is not None:
        if not isinstance(raw.get("options"), dict):
            raise _planner_error("`source.options` must be an object.", field="source.options", value=raw.get("options"))
        out["options"] = dict(raw.get("options") or {})
    return out


def _normalize_select(value: Any) -> List[str]:
    if value is None:
        return []
    if not isinstance(value, list):
        raise _planner_error("`select` must be a list of column names.", field="select", value=value)
    out: List[str] = []
    for idx, item in enumerate(value):
        out.append(_require_non_empty_str(item, field=f"select[{idx}]"))
    return out


def _normalize_derive(value: Any) -> List[Dict[str, str]]:
    if value is None:
        return []
    if not isinstance(value, list):
        raise _planner_error("`derive` must be a list.", field="derive", value=value)
    out: List[Dict[str, str]] = []
    for idx, item in enumerate(value):
        if not isinstance(item, dict):
            raise _planner_error("Each derive entry must be an object.", field=f"derive[{idx}]", value=item)
        unknown = [k for k in item.keys() if k not in {"name", "expr"}]
        if unknown:
            raise _planner_error(
                "Unsupported derive field.",
                field=f"derive[{idx}].{unknown[0]}",
                value=item.get(unknown[0]),
            )
        out.append(
            {
                "name": _require_non_empty_str(item.get("name"), field=f"derive[{idx}].name"),
                "expr": _require_non_empty_str(item.get("expr"), field=f"derive[{idx}].expr"),
            }
        )
    return out


def _normalize_filter(value: Any) -> List[Dict[str, Any]]:
    if value is None:
        return []
    if not isinstance(value, list):
        raise _planner_error("`filter` must be a list.", field="filter", value=value)
    out: List[Dict[str, Any]] = []
    for idx, item in enumerate(value):
        if not isinstance(item, dict):
            raise _planner_error("Each filter entry must be an object.", field=f"filter[{idx}]", value=item)
        unknown = [k for k in item.keys() if k not in {"column", "op", "value"}]
        if unknown:
            raise _planner_error(
                "Unsupported filter field.",
                field=f"filter[{idx}].{unknown[0]}",
                value=item.get(unknown[0]),
            )
        op = _require_non_empty_str(item.get("op"), field=f"filter[{idx}].op").lower()
        if op not in _FILTER_ALLOWED_OPS:
            raise _planner_error("Unsupported filter operation.", field=f"filter[{idx}].op", value=op)
        entry: Dict[str, Any] = {
            "column": _require_non_empty_str(item.get("column"), field=f"filter[{idx}].column"),
            "op": op,
        }
        if op not in {"is_null", "not_null"}:
            if "value" not in item:
                raise _planner_error("Filter operation requires `value`.", field=f"filter[{idx}].value")
            if op in {"in", "not_in"} and not isinstance(item.get("value"), list):
                raise _planner_error(
                    "Filter `in`/`not_in` requires list `value`.",
                    field=f"filter[{idx}].value",
                    value=item.get("value"),
                )
            entry["value"] = item.get("value")
        out.append(entry)
    return out


def _normalize_order_by(value: Any) -> List[Dict[str, str]]:
    if value is None:
        return []
    if not isinstance(value, list):
        raise _planner_error("`order_by` must be a list.", field="order_by", value=value)
    out: List[Dict[str, str]] = []
    for idx, item in enumerate(value):
        if not isinstance(item, dict):
            raise _planner_error("Each order_by entry must be an object.", field=f"order_by[{idx}]", value=item)
        unknown = [k for k in item.keys() if k not in {"column", "direction"}]
        if unknown:
            raise _planner_error(
                "Unsupported order_by field.",
                field=f"order_by[{idx}].{unknown[0]}",
                value=item.get(unknown[0]),
            )
        direction = str(item.get("direction") or "asc").strip().lower()
        if direction not in _ORDER_ALLOWED:
            raise _planner_error(
                "Invalid order direction; expected asc or desc.",
                field=f"order_by[{idx}].direction",
                value=direction,
            )
        out.append(
            {
                "column": _require_non_empty_str(item.get("column"), field=f"order_by[{idx}].column"),
                "direction": direction,
            }
        )
    return out


def _normalize_limit(value: Any) -> int:
    if value is None:
        return 1000
    try:
        out = int(value)
    except (TypeError, ValueError) as exc:
        raise _planner_error("`limit` must be an integer.", field="limit", value=value) from exc
    if out < 1 or out > 10000:
        raise _planner_error("`limit` out of bounds (1..10000).", field="limit", value=out)
    return out


def _normalize_offset(value: Any) -> int:
    if value is None:
        return 0
    try:
        out = int(value)
    except (TypeError, ValueError) as exc:
        raise _planner_error("`offset` must be an integer.", field="offset", value=value) from exc
    if out < 0:
        raise _planner_error("`offset` must be >= 0.", field="offset", value=out)
    return out


def validate_query_spec(query_spec: Dict[str, Any]) -> Dict[str, Any]:
    """
    Validate and normalize query_spec v1.

    Supported fields:
    - source: string path or object selector
    - select: [str]
    - derive: [{name, expr}]
    - filter: [{column, op, value?}]
    - order_by: [{column, direction}]
    - limit: int (1..10000, default 1000)
    - offset: int (>=0, default 0)
    """

    if not isinstance(query_spec, dict):
        raise _planner_error("`query_spec` must be an object.", field="query_spec", value=query_spec)
    unknown = [k for k in query_spec.keys() if k not in _QUERY_SPEC_ALLOWED_KEYS]
    if unknown:
        raise _planner_error("Unsupported query_spec field.", field=unknown[0], value=query_spec.get(unknown[0]))
    if "source" not in query_spec:
        raise _planner_error("Missing required field: source.", field="source")

    normalized = {
        "source": _normalize_source(query_spec.get("source")),
        "select": _normalize_select(query_spec.get("select")),
        "derive": _normalize_derive(query_spec.get("derive")),
        "filter": _normalize_filter(query_spec.get("filter")),
        "order_by": _normalize_order_by(query_spec.get("order_by")),
        "limit": _normalize_limit(query_spec.get("limit")),
        "offset": _normalize_offset(query_spec.get("offset")),
    }

    # Avoid accidental full-table scans in preview mode when no projection/filter is set.
    if not normalized["select"] and not normalized["derive"]:
        normalized["select"] = ["*"]
    return normalized


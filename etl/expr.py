# research-etl
# Copyright (c) 2026 Joseph Weaver
# This file is part of the research-etl project and is licensed under the MIT License.
# You may not use this file except in compliance with the License.
# See https://github.com/josephweaver/research-etl for details.

from __future__ import annotations

import ast
import calendar
import copy
from datetime import date, datetime, timedelta
import itertools
from typing import Any, Dict, List, Tuple


class ExprEvalError(ValueError):
    """Raised when a safe expression cannot be evaluated."""


def _lookup_path(ctx: Dict[str, Any], path: str) -> Tuple[Any, bool]:
    current: Any = ctx
    for part in str(path or "").split("."):
        if isinstance(current, dict) and part in current:
            current = current[part]
            continue
        return None, False
    return current, True


def _coerce_int(value: Any, *, name: str) -> int:
    try:
        return int(value)
    except (TypeError, ValueError) as exc:
        raise ExprEvalError(f"{name} must be an integer") from exc


def _normalize_datetime(value: Any) -> datetime:
    if isinstance(value, datetime):
        return value
    if isinstance(value, date):
        return datetime(value.year, value.month, value.day)
    if isinstance(value, dict):
        iso = str(value.get("iso_utc") or value.get("iso") or "").strip()
        if iso:
            text = iso[:-1] + "+00:00" if iso.endswith("Z") else iso
            try:
                return datetime.fromisoformat(text)
            except ValueError:
                pass
        yymmdd = str(value.get("yymmdd") or "").strip()
        if yymmdd:
            hhmmss = str(value.get("hhmmss") or "000000").strip() or "000000"
            try:
                return datetime.strptime(f"{yymmdd}{hhmmss}", "%y%m%d%H%M%S")
            except ValueError:
                pass
        if all(k in value for k in ("year", "month", "day")):
            return datetime(
                _coerce_int(value.get("year"), name="year"),
                _coerce_int(value.get("month"), name="month"),
                _coerce_int(value.get("day"), name="day"),
                _coerce_int(value.get("hour", 0), name="hour"),
                _coerce_int(value.get("minute", 0), name="minute"),
                _coerce_int(value.get("second", 0), name="second"),
            )
    text = str(value or "").strip()
    if text:
        normalized = text[:-1] + "+00:00" if text.endswith("Z") else text
        try:
            return datetime.fromisoformat(normalized)
        except ValueError:
            pass
    raise ExprEvalError(f"unsupported date/datetime value: {value!r}")


def _normalize_format(fmt: str) -> str:
    text = str(fmt or "")
    replacements = [
        ("%YYYY", "%Y"),
        ("YYYY", "%Y"),
        ("%YY", "%y"),
        ("YY", "%y"),
        ("%MM", "%m"),
        ("MM", "%m"),
        ("%DD", "%d"),
        ("DD", "%d"),
        ("%hh", "%H"),
        ("hh", "%H"),
        ("%mm", "%M"),
        ("mm", "%M"),
        ("%ss", "%S"),
        ("ss", "%S"),
        ("%dd", "%S"),
        ("dd", "%S"),
    ]
    out = text
    for old, new in replacements:
        out = out.replace(old, new)
    return out


def _add_months(value: datetime, months: int) -> datetime:
    idx = (value.month - 1) + int(months)
    year = value.year + (idx // 12)
    month = (idx % 12) + 1
    last_day = calendar.monthrange(year, month)[1]
    day = min(value.day, last_day)
    return value.replace(year=year, month=month, day=day)


def _add_years(value: datetime, years: int) -> datetime:
    year = value.year + int(years)
    last_day = calendar.monthrange(year, value.month)[1]
    day = min(value.day, last_day)
    return value.replace(year=year, day=day)


def _fn_range(*args: Any, **kwargs: Any) -> List[int]:
    if len(args) == 1:
        stop = _coerce_int(args[0], name="stop")
        return list(range(stop))
    if len(args) < 2:
        raise ExprEvalError("expr.range requires at least start and end")
    start = _coerce_int(args[0], name="start")
    end = _coerce_int(args[1], name="end")
    step = _coerce_int(kwargs.get("step", args[2] if len(args) >= 3 else 1), name="step")
    if step == 0:
        raise ExprEvalError("step must not be 0")
    inclusive = bool(kwargs.get("inclusive", True))
    stop = end + (1 if step > 0 else -1) if inclusive else end
    return list(range(start, stop, step))


def _fn_join(values: Any, sep: str = ",") -> str:
    if isinstance(values, (list, tuple)):
        return str(sep).join(str(v) for v in values)
    return str(values)


def _fn_product(*values: Any) -> List[List[Any]]:
    parts: List[List[Any]] = []
    for v in values:
        if isinstance(v, (list, tuple)):
            parts.append(list(v))
        else:
            parts.append([v])
    return [list(combo) for combo in itertools.product(*parts)]


def _fn_date(year: Any, month: Any, day: Any, hour: Any = 0, minute: Any = 0, second: Any = 0) -> datetime:
    return datetime(
        _coerce_int(year, name="year"),
        _coerce_int(month, name="month"),
        _coerce_int(day, name="day"),
        _coerce_int(hour, name="hour"),
        _coerce_int(minute, name="minute"),
        _coerce_int(second, name="second"),
    )


def _fn_datediff(value: Any, amount: Any, unit: str = "D") -> datetime:
    dt = _normalize_datetime(value)
    delta = _coerce_int(amount, name="amount")
    code = str(unit or "D").strip().upper()
    if code == "Y":
        return _add_years(dt, delta)
    if code == "M":
        return _add_months(dt, delta)
    if code == "W":
        return dt + timedelta(weeks=delta)
    if code == "D":
        return dt + timedelta(days=delta)
    if code == "H":
        return dt + timedelta(hours=delta)
    if code == "MIN":
        return dt + timedelta(minutes=delta)
    if code == "S":
        return dt + timedelta(seconds=delta)
    raise ExprEvalError("unit must be one of: Y, M, W, D, H, MIN, S")


def _fn_dateformat(value: Any, fmt: str) -> str:
    dt = _normalize_datetime(value)
    return dt.strftime(_normalize_format(str(fmt or "")))


def _fn_daterange(
    start: Any,
    end: Any,
    step: Any = 1,
    unit: str = "D",
    fmt: str = "%Y%m%d",
    inclusive: bool = True,
) -> List[str]:
    start_dt = _normalize_datetime(start)
    end_dt = _normalize_datetime(end)
    delta = _coerce_int(step, name="step")
    if delta == 0:
        raise ExprEvalError("step must not be 0")
    code = str(unit or "D").strip().upper()
    if code not in {"Y", "M", "W", "D", "H", "MIN", "S"}:
        raise ExprEvalError("unit must be one of: Y, M, W, D, H, MIN, S")

    def _in_bounds(cur: datetime) -> bool:
        if delta > 0:
            return cur <= end_dt if inclusive else cur < end_dt
        return cur >= end_dt if inclusive else cur > end_dt

    if delta > 0 and start_dt > end_dt:
        return []
    if delta < 0 and start_dt < end_dt:
        return []

    out: List[str] = []
    current = start_dt
    max_items = 200000
    while _in_bounds(current):
        out.append(current.strftime(_normalize_format(fmt)))
        current = _fn_datediff(current, delta, code)
        if len(out) > max_items:
            raise ExprEvalError("expr.daterange exceeded max generated items")
    return out


_EXPR_FUNCS = {
    "range": _fn_range,
    "join": _fn_join,
    "product": _fn_product,
    "date": _fn_date,
    "datediff": _fn_datediff,
    "dateformat": _fn_dateformat,
    "daterange": _fn_daterange,
}


def _eval_node(node: ast.AST, ctx: Dict[str, Any]) -> Any:
    if isinstance(node, ast.Constant):
        return node.value
    if isinstance(node, ast.List):
        return [_eval_node(v, ctx) for v in node.elts]
    if isinstance(node, ast.Tuple):
        return tuple(_eval_node(v, ctx) for v in node.elts)
    if isinstance(node, ast.Dict):
        return {_eval_node(k, ctx): _eval_node(v, ctx) for k, v in zip(node.keys, node.values)}
    if isinstance(node, ast.Name):
        value, ok = _lookup_path(ctx, node.id)
        if not ok:
            raise ExprEvalError(f"unknown name: {node.id}")
        return copy.deepcopy(value)
    if isinstance(node, ast.Attribute):
        base = _eval_node(node.value, ctx)
        if isinstance(base, dict) and node.attr in base:
            return copy.deepcopy(base[node.attr])
        raise ExprEvalError(f"unknown attribute: {node.attr}")
    if isinstance(node, ast.UnaryOp) and isinstance(node.op, ast.USub):
        return -_eval_node(node.operand, ctx)
    if isinstance(node, ast.Call):
        if not isinstance(node.func, ast.Attribute):
            raise ExprEvalError("only expr.<fn>(...) calls are allowed")
        if not isinstance(node.func.value, ast.Name) or node.func.value.id != "expr":
            raise ExprEvalError("only expr.<fn>(...) calls are allowed")
        fn_name = str(node.func.attr or "").strip()
        fn = _EXPR_FUNCS.get(fn_name)
        if fn is None:
            raise ExprEvalError(f"unknown expr function: {fn_name}")
        args = [_eval_node(v, ctx) for v in node.args]
        kwargs = {str(kw.arg): _eval_node(kw.value, ctx) for kw in node.keywords if kw.arg}
        return fn(*args, **kwargs)
    raise ExprEvalError(f"unsupported expression node: {type(node).__name__}")


def eval_expr_text(expr_text: str, ctx: Dict[str, Any]) -> Any:
    text = str(expr_text or "").strip()
    tree = ast.parse(text, mode="eval")
    return _eval_node(tree.body, ctx)


def try_eval_expr_text(expr_text: str, ctx: Dict[str, Any]) -> Tuple[Any, bool]:
    text = str(expr_text or "").strip()
    if not text.startswith("expr."):
        return None, False
    try:
        return eval_expr_text(text, ctx), True
    except Exception:
        return None, False

# research-etl
# Copyright (c) 2026 Joseph Weaver
# This file is part of the research-etl project and is licensed under the MIT License.
# You may not use this file except in compliance with the License.
# See https://github.com/josephweaver/research-etl for details.

from __future__ import annotations

import ast
import csv
import re
from pathlib import Path
from typing import Any

try:
    import geopandas as gpd  # type: ignore
except Exception:  # noqa: BLE001
    gpd = None


meta = {
    "name": "geo_vector_filter",
    "version": "0.1.0",
    "description": "Filter vector features by attribute conditions and write to a new vector file.",
    "inputs": [],
    "outputs": [
        "input_path",
        "output_path",
        "input_feature_count",
        "output_feature_count",
        "filter_key",
        "filter_op",
        "filter_values",
    ],
    "params": {
        "input_path": {"type": "str", "default": ""},
        "output_path": {"type": "str", "default": ""},
        "where": {"type": "str", "default": ""},
        "key": {"type": "str", "default": ""},
        "op": {"type": "str", "default": "eq"},
        "value": {"type": "str", "default": ""},
        "values": {"type": "str", "default": ""},
        "case_insensitive": {"type": "bool", "default": True},
        "fail_on_empty": {"type": "bool", "default": False},
        "fail_on_unresolved_tokens": {"type": "bool", "default": True},
        "verbose": {"type": "bool", "default": False},
    },
    "idempotent": True,
}


_WHERE_IN = re.compile(r"^\s*([A-Za-z_][A-Za-z0-9_]*)\s+in\s*\((.*)\)\s*$", re.IGNORECASE)
_WHERE_EQ = re.compile(r"^\s*([A-Za-z_][A-Za-z0-9_]*)\s*(==|=)\s*(.+?)\s*$")
_WHERE_NE = re.compile(r"^\s*([A-Za-z_][A-Za-z0-9_]*)\s*(!=|<>)\s*(.+?)\s*$")


def _resolve_path(path_text: str, ctx) -> Path:
    p = Path(str(path_text or "")).expanduser()
    if p.is_absolute():
        return p
    repo_rel = (Path(".").resolve() / p).resolve()
    if repo_rel.exists():
        return repo_rel
    text = str(path_text or "").replace("\\", "/")
    if text.startswith(".") or "/" in text:
        return repo_rel
    return (ctx.workdir / p).resolve()


def _parse_csv_values(text: str) -> list[str]:
    raw = str(text or "").strip()
    if not raw:
        return []
    row = next(csv.reader([raw], skipinitialspace=True), [])
    return [str(v).strip().strip("\"'") for v in row if str(v).strip()]


def _coerce_values(values_arg: Any) -> list[str]:
    if values_arg is None:
        return []
    if isinstance(values_arg, str):
        raw = values_arg.strip()
        if raw.startswith("[") and raw.endswith("]"):
            try:
                parsed = ast.literal_eval(raw)
            except Exception:  # noqa: BLE001
                parsed = None
            if isinstance(parsed, (list, tuple, set)):
                return _coerce_values(parsed)
        return _parse_csv_values(raw)
    if isinstance(values_arg, (list, tuple, set)):
        out: list[str] = []
        for raw in values_arg:
            text = str(raw or "").strip().strip("\"'")
            if text:
                out.append(text)
        return out
    text = str(values_arg).strip().strip("\"'")
    return [text] if text else []


def _parse_where(where: str) -> tuple[str, str, list[str]]:
    expr = str(where or "").strip()
    if not expr:
        return "", "", []
    m = _WHERE_IN.match(expr)
    if m:
        return m.group(1), "in", _parse_csv_values(m.group(2))
    m = _WHERE_EQ.match(expr)
    if m:
        return m.group(1), "eq", [m.group(3).strip().strip("\"'")]
    m = _WHERE_NE.match(expr)
    if m:
        return m.group(1), "ne", [m.group(3).strip().strip("\"'")]
    raise ValueError("Unsupported where expression. Use: COL in (a,b), COL == x, or COL != x")


def _normalize_op(op: str) -> str:
    token = str(op or "").strip().lower().replace("-", "_")
    if token in {"eq", "=", "=="}:
        return "eq"
    if token in {"ne", "!=", "<>"}:
        return "ne"
    if token in {"in"}:
        return "in"
    if token in {"not_in", "nin"}:
        return "not_in"
    raise ValueError("op must be one of: eq, ne, in, not_in")


def _normalize_value(value: Any, case_insensitive: bool) -> str:
    text = "" if value is None else str(value)
    return text.casefold() if case_insensitive else text


def _remove_existing_output(output_path: Path) -> None:
    ext = output_path.suffix.lower()
    if ext == ".shp":
        for sidecar in [".shp", ".shx", ".dbf", ".prj", ".cpg", ".qix"]:
            candidate = output_path.with_suffix(sidecar)
            if candidate.exists():
                candidate.unlink()
        return
    if output_path.exists():
        output_path.unlink()


def run(args, ctx):
    if gpd is None:
        raise RuntimeError(
            "geo_vector_filter requires geopandas. Install dependencies from requirements.txt in the active environment."
        )

    input_text = str(args.get("input_path") or "").strip()
    output_text = str(args.get("output_path") or "").strip()
    if not input_text:
        raise ValueError("input_path is required")
    if not output_text:
        raise ValueError("output_path is required")

    input_path = _resolve_path(input_text, ctx)
    output_path = _resolve_path(output_text, ctx)
    if not input_path.exists():
        raise FileNotFoundError(f"input_path not found: {input_path}")
    output_path.parent.mkdir(parents=True, exist_ok=True)

    where = str(args.get("where") or "").strip()
    key = str(args.get("key") or "").strip()
    op = str(args.get("op") or "eq").strip()
    value = str(args.get("value") or "").strip()
    values_arg = args.get("values")
    case_insensitive = bool(args.get("case_insensitive", True))
    fail_on_empty = bool(args.get("fail_on_empty", False))
    fail_on_unresolved_tokens = bool(args.get("fail_on_unresolved_tokens", True))
    verbose = bool(args.get("verbose", False))

    if where:
        key, op, parsed_values = _parse_where(where)
        filter_values = parsed_values
    else:
        if not key:
            raise ValueError("Provide either where or key")
        op = _normalize_op(op)
        filter_values = _coerce_values(values_arg)
        if not filter_values and value:
            filter_values = [value]
        if op in {"eq", "ne"} and len(filter_values) != 1:
            raise ValueError("eq/ne require exactly one value")
        if op in {"in", "not_in"} and not filter_values:
            raise ValueError("in/not_in require one or more values (use values=...)")
    if fail_on_unresolved_tokens:
        unresolved = [v for v in filter_values if "{" in str(v) and "}" in str(v)]
        if unresolved:
            raise ValueError(
                f"geo_vector_filter received unresolved template token(s) in values: {unresolved}. "
                "Check project_id/projects_config and variable names."
            )

    gdf = gpd.read_file(input_path)
    if key not in gdf.columns:
        raise ValueError(f"key '{key}' not found in input columns: {', '.join(map(str, gdf.columns))}")

    gdf_cmp = gdf.copy()
    cmp_col = gdf_cmp[key].map(lambda v: _normalize_value(v, case_insensitive))
    cmp_values = [_normalize_value(v, case_insensitive) for v in filter_values]

    if op == "eq":
        mask = cmp_col == cmp_values[0]
    elif op == "ne":
        mask = cmp_col != cmp_values[0]
    elif op == "in":
        mask = cmp_col.isin(cmp_values)
    else:
        mask = ~cmp_col.isin(cmp_values)

    out = gdf.loc[mask].copy()
    if fail_on_empty and len(out) == 0:
        raise ValueError(
            f"geo_vector_filter matched 0 features for key={key} op={op} values={filter_values}. "
            "Check project vars/key/value format."
        )
    _remove_existing_output(output_path)
    out.to_file(output_path)

    ctx.log(
        f"[geo_vector_filter] input={input_path.as_posix()} output={output_path.as_posix()} "
        f"key={key} op={op} values={filter_values} in_count={len(gdf)} out_count={len(out)}"
    )
    if verbose and len(out) > 0:
        ctx.log(f"[geo_vector_filter] output_columns={list(out.columns)}")

    return {
        "input_path": input_path.resolve().as_posix(),
        "output_path": output_path.resolve().as_posix(),
        "input_feature_count": int(len(gdf)),
        "output_feature_count": int(len(out)),
        "filter_key": key,
        "filter_op": op,
        "filter_values": list(filter_values),
    }


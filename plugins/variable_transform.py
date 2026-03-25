from __future__ import annotations

import json
from typing import Any

from etl.variable_transform import apply_operations


meta = {
    "name": "variable_transform",
    "version": "0.1.0",
    "description": "EXPERIMENTAL DO NOT USE: lightweight variable/path/file transforms prototype.",
    "inputs": [],
    "outputs": ["value", "value_type", "operations_applied"],
    "params": {
        "value": {"type": "str", "default": ""},
        "op": {"type": "str", "default": ""},
        "operations": {"type": "str", "default": ""},
        "pattern": {"type": "str", "default": ""},
        "group": {"type": "str", "default": "1"},
        "start": {"type": "str", "default": ""},
        "end": {"type": "str", "default": ""},
        "namespace": {"type": "str", "default": ""},
        "parts": {"type": "str", "default": ""},
        "encoding": {"type": "str", "default": "utf-8"},
        "separator": {"type": "str", "default": ""},
        "verbose": {"type": "bool", "default": False},
    },
    "idempotent": True,
}


def _parse_operations(args: dict[str, Any]) -> list[dict[str, Any]]:
    operations_text = str(args.get("operations") or "").strip()
    if operations_text:
        payload = json.loads(operations_text)
        if isinstance(payload, dict):
            return [dict(payload)]
        if isinstance(payload, list):
            return [dict(item) for item in payload if isinstance(item, dict)]
        raise ValueError("operations must decode to an object or array of objects")

    op = str(args.get("op") or "").strip()
    if not op:
        return []
    spec: dict[str, Any] = {"op": op}
    for key in ("pattern", "group", "start", "end", "namespace", "parts", "encoding", "separator"):
        value = args.get(key)
        if value not in (None, ""):
            spec[key] = value
    return [spec]


def run(args, ctx):
    ctx.warn("[variable_transform] experimental plugin; do not use in production pipelines")
    ops = _parse_operations(args)
    if not ops:
        raise ValueError("either op or operations is required")
    seed = args.get("value")
    verbose = bool(args.get("verbose", False))
    if verbose:
        ctx.log(f"[variable_transform] operations={len(ops)}")
    result = apply_operations(seed, ops, workdir=ctx.workdir)
    if verbose:
        ctx.log(f"[variable_transform] result_type={type(result).__name__}")
    if isinstance(result, dict):
        out = dict(result)
        out["value"] = result
        out["value_type"] = "dict"
        out["operations_applied"] = len(ops)
        return out
    return {
        "value": result,
        "value_type": type(result).__name__,
        "operations_applied": len(ops),
    }

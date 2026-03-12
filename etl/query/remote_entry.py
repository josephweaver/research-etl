# research-etl
# Copyright (c) 2026 Joseph Weaver
# This file is part of the research-etl project and is licensed under the MIT License.
# You may not use this file except in compliance with the License.
# See https://github.com/josephweaver/research-etl for details.

"""Remote query entrypoint with structured JSON input/output."""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import Any, Dict

from .errors import QueryError, QueryExecutionError, QueryPlannerError
from .planner_duckdb import build_duckdb_query_plan
from .runners.duckdb_runner import run_duckdb_query_plan
from .spec import validate_query_spec


def run_query_request(payload: Dict[str, Any]) -> Dict[str, Any]:
    try:
        if not isinstance(payload, dict):
            raise QueryPlannerError("Request payload must be an object.", detail={"field": "payload"})
        query_spec = payload.get("query_spec")
        if not isinstance(query_spec, dict):
            raise QueryPlannerError("`query_spec` is required and must be an object.", detail={"field": "query_spec"})
        context = payload.get("context")
        context_dict = dict(context or {}) if isinstance(context, dict) else {}
        normalized = validate_query_spec(query_spec)
        plan = build_duckdb_query_plan(normalized, context=context_dict)
        result = run_duckdb_query_plan(plan)
        return {"ok": True, "result": result}
    except QueryError as exc:
        return {"ok": False, "error": exc.to_payload()}
    except Exception as exc:  # noqa: BLE001
        wrapped = QueryExecutionError("Unhandled query failure.", detail={"error": str(exc)})
        return {"ok": False, "error": wrapped.to_payload()}


def _read_input(path_text: str) -> Dict[str, Any]:
    if path_text:
        return json.loads(Path(path_text).read_text(encoding="utf-8"))
    return json.load(sys.stdin)


def _write_output(path_text: str, payload: Dict[str, Any]) -> None:
    text = json.dumps(payload, separators=(",", ":"), ensure_ascii=True)
    if path_text:
        p = Path(path_text)
        p.parent.mkdir(parents=True, exist_ok=True)
        p.write_text(text + "\n", encoding="utf-8")
        return
    sys.stdout.write(text + "\n")
    sys.stdout.flush()


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Run a structured query request and return JSON.")
    parser.add_argument("--input", default="", help="Path to JSON request payload file. Defaults to stdin.")
    parser.add_argument("--output", default="", help="Path to write JSON response payload. Defaults to stdout.")
    args = parser.parse_args(argv)

    try:
        request_payload = _read_input(str(args.input or "").strip())
    except Exception as exc:  # noqa: BLE001
        error_payload = QueryPlannerError("Failed to read query request payload.", detail={"error": str(exc)}).to_payload()
        _write_output(str(args.output or "").strip(), {"ok": False, "error": error_payload})
        return 0

    response = run_query_request(request_payload)
    _write_output(str(args.output or "").strip(), response)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

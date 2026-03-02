# research-etl
# Copyright (c) 2026 Joseph Weaver
# This file is part of the research-etl project and is licensed under the MIT License.
# You may not use this file except in compliance with the License.
# See https://github.com/josephweaver/research-etl for details.

"""Shared query error model."""

from __future__ import annotations

from typing import Any, Dict, Optional


class QueryError(RuntimeError):
    """Base class for query-layer failures."""

    error_code = "query_error"
    http_status = 500

    def __init__(self, message: str, *, detail: Optional[Dict[str, Any]] = None) -> None:
        super().__init__(str(message))
        self.detail = dict(detail or {})

    def to_payload(self) -> Dict[str, Any]:
        payload: Dict[str, Any] = {"error_code": self.error_code, "message": str(self)}
        if self.detail:
            payload["detail"] = self.detail
        return payload


class QueryPlannerError(QueryError):
    """Raised when query specification/planning fails."""

    error_code = "planner_error"
    http_status = 400


class QueryExecutionError(QueryError):
    """Raised when execution/engine runtime fails."""

    error_code = "execution_error"
    http_status = 422


class QueryTransportError(QueryError):
    """Raised when executor transport/remote invocation fails."""

    error_code = "transport_error"
    http_status = 502


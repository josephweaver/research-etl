# research-etl
# Copyright (c) 2026 Joseph Weaver
# This file is part of the research-etl project and is licensed under the MIT License.
# You may not use this file except in compliance with the License.
# See https://github.com/josephweaver/research-etl for details.

"""Query foundation modules."""

from .errors import (
    QueryError,
    QueryExecutionError,
    QueryPlannerError,
    QueryTransportError,
)
from .planner_duckdb import build_duckdb_query_plan
from .runners import run_duckdb_query_plan
from .spec import validate_query_spec

__all__ = [
    "QueryError",
    "QueryPlannerError",
    "QueryExecutionError",
    "QueryTransportError",
    "validate_query_spec",
    "build_duckdb_query_plan",
    "run_duckdb_query_plan",
]

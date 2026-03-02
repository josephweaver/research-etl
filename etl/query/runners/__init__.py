# research-etl
# Copyright (c) 2026 Joseph Weaver
# This file is part of the research-etl project and is licensed under the MIT License.
# You may not use this file except in compliance with the License.
# See https://github.com/josephweaver/research-etl for details.

"""Query runners."""

from .duckdb_runner import run_duckdb_query_plan

__all__ = ["run_duckdb_query_plan"]


# research-etl
# Copyright (c) 2026 Joseph Weaver
# This file is part of the research-etl project and is licensed under the MIT License.
# You may not use this file except in compliance with the License.
# See https://github.com/josephweaver/research-etl for details.

"""
ETL package bootstrap.

Provides shared types and simple helpers used by the CLI, plugin loader,
and executor interfaces. The implementation is intentionally lightweight
so that downstream projects can extend it without pulling in heavy
dependencies by default.
"""

__all__ = ["__version__"]

__version__ = "0.0.1"

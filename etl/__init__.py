"""
ETL package bootstrap.

Provides shared types and simple helpers used by the CLI, plugin loader,
and executor interfaces. The implementation is intentionally lightweight
so that downstream projects can extend it without pulling in heavy
dependencies by default.
"""

__all__ = ["__version__"]

__version__ = "0.0.1"

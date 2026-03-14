# research-etl
# Copyright (c) 2026 Joseph Weaver
# This file is part of the research-etl project and is licensed under the MIT License.
# You may not use this file except in compliance with the License.
# See https://github.com/josephweaver/research-etl for details.

from __future__ import annotations

import re
from typing import Any, Callable, Optional

from ..common.parsing import parse_bool as _parse_bool
from ..variable_solver import VariableSolver

_TPL_RE = re.compile(r"\{([^{}]+)\}")


def parse_bool(value: Any, *, default: bool = False) -> bool:
    return _parse_bool(value, default=default)


def extract_unresolved_tokens(value: Any) -> list[str]:
    if not isinstance(value, str):
        return []
    seen: set[str] = set()
    out: list[str] = []
    for token in _TPL_RE.findall(value):
        tok = str(token or "").strip()
        if not tok or tok in seen:
            continue
        seen.add(tok)
        out.append(tok)
    return out


def resolve_workdir_from_solver(
    *,
    solver: VariableSolver,
    pipeline_workdir: Optional[str] = None,
    context_vars: Optional[dict[str, Any]] = None,
    resolve_text_iterative: Optional[Callable[[str, dict[str, Any]], str]] = None,
    fallback: str = ".runs",
) -> str:
    cli_workdir = str(solver.get("commandline.workdir", "", resolve=True) or "").strip()
    if cli_workdir and not extract_unresolved_tokens(cli_workdir):
        return cli_workdir
    pipeline_text = str(pipeline_workdir or "").strip()
    if pipeline_text:
        if context_vars and resolve_text_iterative is not None:
            text_from_context = resolve_text_iterative(pipeline_text, context_vars)
            if text_from_context and not extract_unresolved_tokens(text_from_context):
                return text_from_context
        resolved = solver.resolve(pipeline_text, context=solver.resolved_context())
        text = str(resolved or "").strip()
        if text and extract_unresolved_tokens(text) and resolve_text_iterative is not None:
            text = resolve_text_iterative(text, solver.resolved_context())
        if text and not extract_unresolved_tokens(text):
            return text
    env_workdir = str(solver.get("env.workdir", "", resolve=True) or "").strip()
    if env_workdir:
        return env_workdir
    global_workdir = str(solver.get("global.workdir", "", resolve=True) or "").strip()
    if global_workdir:
        return global_workdir
    return fallback

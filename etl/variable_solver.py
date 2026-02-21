# research-etl
# Copyright (c) 2026 Joseph Weaver
# This file is part of the research-etl project and is licensed under the MIT License.
# You may not use this file except in compliance with the License.
# See https://github.com/josephweaver/research-etl for details.

from __future__ import annotations

import copy
import os
import re
from typing import Any, Dict, Optional

from .expr import try_eval_expr_text

_PLACEHOLDER_RE = re.compile(r"\{([^{}]+)\}")


class VariableSolver:
    """
    Layered variable context with iterative placeholder resolution.

    Each overlay can be loaded into:
    - a namespaced mapping (e.g., `globals.workdir`)
    - flat keys (e.g., `workdir`)
    """

    def __init__(self, *, max_passes: int = 20, initial: Optional[Dict[str, Any]] = None) -> None:
        self.max_passes = max(1, int(max_passes or 20))
        self._ctx: Dict[str, Any] = copy.deepcopy(initial or {})

    @staticmethod
    def _lookup_path(ctx: Dict[str, Any], path: str) -> tuple[Any, bool]:
        current: Any = ctx
        for part in str(path or "").split("."):
            if isinstance(current, dict) and part in current:
                current = current[part]
                continue
            return None, False
        return current, True

    @staticmethod
    def _resolve_string_once(value: str, ctx: Dict[str, Any]) -> Any:
        exact = _PLACEHOLDER_RE.fullmatch(value)
        if exact:
            token = exact.group(1)
            resolved, ok = VariableSolver._lookup_path(ctx, token)
            if ok:
                if isinstance(resolved, (dict, list)):
                    return copy.deepcopy(resolved)
                return str(resolved)
            expr_value, expr_ok = try_eval_expr_text(token, ctx)
            if expr_ok:
                if isinstance(expr_value, (dict, list)):
                    return copy.deepcopy(expr_value)
                return expr_value
            return value

        def _repl(match: re.Match[str]) -> str:
            token = match.group(1)
            resolved, ok = VariableSolver._lookup_path(ctx, token)
            if not ok or isinstance(resolved, (dict, list)):
                expr_value, expr_ok = try_eval_expr_text(token, ctx)
                if not expr_ok or isinstance(expr_value, (dict, list)):
                    return match.group(0)
                return str(expr_value)
            return str(resolved)

        return _PLACEHOLDER_RE.sub(_repl, value)

    @classmethod
    def _walk_once(cls, value: Any, ctx: Dict[str, Any]) -> Any:
        if isinstance(value, str):
            return cls._resolve_string_once(value, ctx)
        if isinstance(value, list):
            return [cls._walk_once(v, ctx) for v in value]
        if isinstance(value, dict):
            return {k: cls._walk_once(v, ctx) for k, v in value.items()}
        return value

    @classmethod
    def resolve_iterative(cls, value: Any, ctx: Dict[str, Any], *, max_passes: int = 20) -> Any:
        current = copy.deepcopy(value)
        passes = max(1, int(max_passes or 20))
        for _ in range(passes):
            nxt = cls._walk_once(current, ctx)
            if nxt == current:
                return current
            current = nxt
        return current

    @classmethod
    def resolve_context_iterative(cls, ctx: Dict[str, Any], *, max_passes: int = 20) -> Dict[str, Any]:
        current = copy.deepcopy(ctx)
        passes = max(1, int(max_passes or 20))
        for _ in range(passes):
            nxt = cls._walk_once(current, current)
            if nxt == current:
                return current
            current = nxt
        return current

    def overlay(
        self,
        name: str,
        values: Optional[Dict[str, Any]],
        *,
        add_namespace: bool = True,
        add_flat: bool = True,
    ) -> "VariableSolver":
        payload = copy.deepcopy(values or {})
        ns = str(name or "").strip()
        if ns and add_namespace:
            self._ctx[ns] = payload
        if add_flat:
            for k, v in payload.items():
                self._ctx[str(k)] = copy.deepcopy(v)
        return self

    def update(self, values: Optional[Dict[str, Any]]) -> "VariableSolver":
        for k, v in dict(values or {}).items():
            self._ctx[str(k)] = copy.deepcopy(v)
        return self

    def with_sys(self, values: Optional[Dict[str, Any]]) -> "VariableSolver":
        """Attach static runtime/system variables under `sys.*` namespace."""
        return self.overlay("sys", values, add_namespace=True, add_flat=False)

    def context(self) -> Dict[str, Any]:
        return copy.deepcopy(self._ctx)

    def resolved_context(self) -> Dict[str, Any]:
        return self.resolve_context_iterative(self._ctx, max_passes=self.max_passes)

    def resolve(self, value: Any, *, context: Optional[Dict[str, Any]] = None) -> Any:
        return self.resolve_iterative(
            value,
            context if context is not None else self.resolved_context(),
            max_passes=self.max_passes,
        )

    def get(self, key: str, default: Any = None, *, resolve: bool = True) -> Any:
        ctx = self.resolved_context() if resolve else self.context()
        value, ok = self._lookup_path(ctx, str(key or ""))
        return value if ok else default

    @staticmethod
    def _normalize_path_style(raw: str) -> str:
        text = str(raw or "").strip().lower()
        if text in {"windows", "win", "nt"}:
            return "windows"
        if text in {"unix", "posix", "linux", "mac", "darwin"}:
            return "unix"
        return ""

    @classmethod
    def _normalize_path_text(cls, value: str, style: str) -> str:
        text = str(value or "")
        if style == "windows":
            return text.replace("/", "\\")
        return text.replace("\\", "/")

    def get_path(
        self,
        key: str,
        default: Any = None,
        *,
        resolve: bool = True,
        path_style: Optional[str] = None,
    ) -> str:
        ctx = self.resolved_context() if resolve else self.context()
        value, ok = self._lookup_path(ctx, str(key or ""))
        raw = value if ok else default
        text = str(raw or "")
        preferred = self._normalize_path_style(str(path_style or ""))
        if not preferred:
            preferred = self._normalize_path_style(str((ctx.get("env") or {}).get("path_style") or ""))
        if not preferred:
            preferred = self._normalize_path_style(str(ctx.get("path_style") or ""))
        if not preferred:
            preferred = "windows" if os.name == "nt" else "unix"
        return self._normalize_path_text(text, preferred)

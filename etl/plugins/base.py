"""
Plugin interface definitions and loader utilities.

A plugin is a Python module under `plugins/` that exposes:
- `meta`: metadata dict describing inputs, outputs, params, etc.
- `run(args, ctx) -> dict`: main execution function.
- Optional `validate(args, outputs, ctx)` and `cleanup(ctx)`.

This module keeps the contract minimal so authors can write small,
single-file plugins without inheriting heavy classes.
"""

from __future__ import annotations

import importlib.util
import inspect
import types
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Callable, Dict, List, Optional


@dataclass
class PluginMeta:
    """Lightweight metadata describing a plugin."""

    name: str
    version: str = "0.0.0"
    description: str = ""
    inputs: List[str] = field(default_factory=list)
    outputs: List[str] = field(default_factory=list)
    params: Dict[str, Any] = field(default_factory=dict)
    deps: List[str] = field(default_factory=list)
    idempotent: bool = True
    resources: Dict[str, Any] = field(default_factory=dict)


@dataclass
class PluginContext:
    """
    Runtime context passed to plugins.

    Attributes
    ----------
    run_id: str
        Unique identifier for the pipeline run.
    workdir: Path
        Directory where the plugin can read/write work files.
    log: Callable[[str], None]
        Simple logger; expected signature log(message: str) -> None.
    """

    run_id: str
    workdir: Path
    log: Callable[[str], None]

    def temp_path(self, name: str) -> Path:
        """Return a path under the workdir for temporary artifacts."""
        return self.workdir / name


@dataclass
class PluginDefinition:
    """In-memory representation of a plugin."""

    meta: PluginMeta
    run: Callable[[Dict[str, Any], PluginContext], Dict[str, Any]]
    validate: Optional[
        Callable[[Dict[str, Any], Dict[str, Any], PluginContext], Any]
    ] = None
    cleanup: Optional[Callable[[PluginContext], Any]] = None
    module: Optional[types.ModuleType] = None


class PluginLoadError(RuntimeError):
    """Raised when a plugin module is missing required members."""


def _load_module_from_path(path: Path) -> types.ModuleType:
    spec = importlib.util.spec_from_file_location(path.stem, path)
    if spec is None or spec.loader is None:
        raise PluginLoadError(f"Cannot load module from {path}")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)  # type: ignore[call-arg]
    return module


def _require_callable(module: types.ModuleType, name: str) -> Callable:
    fn = getattr(module, name, None)
    if not callable(fn):
        raise PluginLoadError(f"Plugin {module.__name__} missing callable `{name}`")
    return fn


def _parse_meta(module: types.ModuleType) -> PluginMeta:
    raw = getattr(module, "meta", None)
    if not isinstance(raw, dict):
        raise PluginLoadError(f"Plugin {module.__name__} missing dict `meta`")
    try:
        return PluginMeta(**raw)
    except TypeError as exc:
        raise PluginLoadError(f"Invalid meta for {module.__name__}: {exc}") from exc


def load_plugin(path: Path) -> PluginDefinition:
    """Load a plugin module from a file path."""
    module = _load_module_from_path(path)
    meta = _parse_meta(module)
    run_fn = _require_callable(module, "run")
    validate_fn = getattr(module, "validate", None)
    cleanup_fn = getattr(module, "cleanup", None)
    return PluginDefinition(
        meta=meta,
        run=run_fn,
        validate=validate_fn if callable(validate_fn) else None,
        cleanup=cleanup_fn if callable(cleanup_fn) else None,
        module=module,
    )


def discover_plugins(directory: Path) -> List[PluginDefinition]:
    """
    Discover plugins in a directory.

    A plugin is any `.py` file not starting with `_`.
    """
    plugins: List[PluginDefinition] = []
    for path in sorted(directory.glob("*.py")):
        if path.name.startswith("_"):
            continue
        plugins.append(load_plugin(path))
    return plugins


def describe_plugin(defn: PluginDefinition) -> Dict[str, Any]:
    """Return a serializable description of a plugin."""
    return {
        "name": defn.meta.name,
        "version": defn.meta.version,
        "description": defn.meta.description,
        "inputs": defn.meta.inputs,
        "outputs": defn.meta.outputs,
        "params": defn.meta.params,
        "deps": defn.meta.deps,
        "idempotent": defn.meta.idempotent,
        "resources": defn.meta.resources,
        "module": defn.module.__file__ if defn.module else None,
        "has_validate": defn.validate is not None,
        "has_cleanup": defn.cleanup is not None,
    }

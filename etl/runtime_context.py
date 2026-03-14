# research-etl
# Copyright (c) 2026 Joseph Weaver
# This file is part of the research-etl project and is licensed under the MIT License.
# You may not use this file except in compliance with the License.
# See https://github.com/josephweaver/research-etl for details.

"""Shared runtime context builder used by CLI, web handlers, and batch entrypoints.

This module centralizes early config/variable loading and provides a two-phase
logging lifecycle:
1) bootstrap logger (available before full runtime resolution),
2) runtime logger (promoted after run-specific paths are known).
"""

from __future__ import annotations

import logging
import os
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, Optional

from .app_logging import configure_app_logger
from .config import ConfigError, load_global_config, resolve_global_config_path
from .execution_config import (
    ExecutionConfigError,
    apply_execution_env_overrides,
    load_execution_config,
    resolve_execution_config_path,
    resolve_execution_env_templates,
    validate_environment_executor,
)
from .pipeline import PipelineError, parse_pipeline
from .pipeline_assets import PipelineAssetError, resolve_pipeline_path_from_project_sources
from .projects import (
    ProjectConfigError,
    load_project_vars,
    resolve_project_id,
    resolve_projects_config_path,
)
from .variable_solver import VariableSolver

DEFAULT_SECRET_ENV_KEYS = ("ETL_DATABASE_URL", "OPENAI_API_KEY", "GITHUB_TOKEN")


def _pipeline_assets_cache_root(*, global_vars: Dict[str, Any], exec_env: Dict[str, Any]) -> Optional[Path]:
    raw = (
        exec_env.get("pipeline_assets_cache_root")
        or exec_env.get("source_root")
        or global_vars.get("pipeline_assets_cache_root")
        or global_vars.get("source_root")
    )
    text = str(raw or "").strip()
    if not text:
        return None
    return Path(text).expanduser()


class RuntimeContextError(RuntimeError):
    """Raised when runtime context loading fails."""


@dataclass
class VariableCatalog:
    """Snapshot of unresolved/resolved variable maps built via VariableSolver."""

    max_passes: int
    raw_context: Dict[str, Any]
    resolved_context: Dict[str, Any]


@dataclass
class LoggingContext:
    """Tracks bootstrap/runtime log files and logger instances."""

    bootstrap_log_file: Path
    bootstrap_logger: logging.Logger
    runtime_log_file: Optional[Path] = None
    runtime_logger: Optional[logging.Logger] = None

    @property
    def logger(self) -> logging.Logger:
        return self.runtime_logger or self.bootstrap_logger

    def promote_runtime(
        self,
        runtime_log_file: Path,
        *,
        logger_name: str = "etl",
        level: str | None = None,
    ) -> logging.Logger:
        path = Path(runtime_log_file).expanduser().resolve()
        runtime_logger = configure_app_logger(
            logger_name=logger_name,
            level=level,
            log_file=path,
            force=True,
        )
        self.runtime_log_file = path
        self.runtime_logger = runtime_logger
        runtime_logger.info(
            "Runtime logging enabled bootstrap_log=%s runtime_log=%s",
            self.bootstrap_log_file.as_posix(),
            path.as_posix(),
        )
        return runtime_logger


@dataclass
class RuntimeContext:
    """Resolved context shared by submit-time and runtime callers."""

    global_config_path: Optional[Path]
    projects_config_path: Optional[Path]
    environments_config_path: Optional[Path]
    env_name: Optional[str]
    selected_executor: Optional[str]
    pipeline_path: Optional[Path]
    project_id: Optional[str]
    local_env_vars: Dict[str, str]
    global_vars: Dict[str, Any]
    exec_env: Dict[str, Any]
    project_vars: Dict[str, Any]
    commandline_vars: Dict[str, Any]
    parse_context_vars: Dict[str, Any]
    pipeline: Optional[Any]
    solvers: Dict[str, VariableSolver]
    variable_catalogs: Dict[str, VariableCatalog]
    variable_catalog: VariableCatalog
    logging: LoggingContext

    def solver(self, scope: str = "target") -> VariableSolver:
        key = str(scope or "target").strip().lower() or "target"
        if key not in self.solvers:
            raise RuntimeContextError(f"Unknown runtime solver scope: {scope}")
        return self.solvers[key]

    def catalog(self, scope: str = "target") -> VariableCatalog:
        key = str(scope or "target").strip().lower() or "target"
        if key not in self.variable_catalogs:
            raise RuntimeContextError(f"Unknown runtime variable catalog scope: {scope}")
        return self.variable_catalogs[key]

    def default_runtime_log_file(
        self,
        *,
        label: str = "runtime",
        run_id: Optional[str] = None,
    ) -> Path:
        base = str(
            self.exec_env.get("logdir")
            or self.global_vars.get("logdir")
            or self.global_vars.get("log")
            or ".runs/logs"
        ).strip()
        suffix = f"-{run_id}" if str(run_id or "").strip() else ""
        return (Path(base).expanduser() / f"{label}{suffix}.log").resolve()


@dataclass
class RuntimeContextRequest:
    """Input shape for building shared runtime context."""

    global_config: Optional[Path] = None
    projects_config: Optional[Path] = None
    environments_config: Optional[Path] = None
    env_name: Optional[str] = None
    executor: Optional[str] = None
    pipeline_path: Optional[Path] = None
    project_id: Optional[str] = None
    commandline_vars: Optional[Dict[str, Any]] = None
    commandline_var_entries: Optional[list[str]] = None
    local_env_vars: Optional[Dict[str, str]] = None
    include_secret_vars: bool = True
    bootstrap_label: str = "bootstrap"
    logger_name: str = "etl"


def _assign_dotted_path(target: Dict[str, Any], dotted_key: str, value: Any) -> None:
    parts = [p.strip() for p in str(dotted_key or "").split(".")]
    if not parts or any(not p for p in parts):
        raise RuntimeContextError(f"Invalid --var key: '{dotted_key}'")
    cur: Dict[str, Any] = target
    for part in parts[:-1]:
        nxt = cur.get(part)
        if not isinstance(nxt, dict):
            nxt = {}
            cur[part] = nxt
        cur = nxt
    cur[parts[-1]] = value


def parse_cli_var_overrides(entries: list[str] | None) -> Dict[str, Any]:
    """Parse KEY=VALUE runtime overrides into nested mappings."""
    out: Dict[str, Any] = {}
    for raw in list(entries or []):
        text = str(raw or "").strip()
        if not text:
            continue
        if "=" not in text:
            raise RuntimeContextError(f"Invalid --var '{text}': expected KEY=VALUE")
        key, value = text.split("=", 1)
        key_text = key.strip()
        if not key_text:
            raise RuntimeContextError(f"Invalid --var '{text}': key may not be empty")
        _assign_dotted_path(out, key_text, value)
    return out


def parse_secret_env_keys(exec_env: Dict[str, Any]) -> list[str]:
    raw = exec_env.get("secret_env_keys")
    if isinstance(raw, (list, tuple, set)):
        items = [str(x).strip() for x in raw]
    elif raw is None:
        env_raw = str(os.environ.get("ETL_SECRET_ENV_KEYS") or "").strip()
        if env_raw:
            items = [x.strip() for x in env_raw.replace(";", ",").split(",")]
        else:
            items = list(DEFAULT_SECRET_ENV_KEYS)
    else:
        items = [x.strip() for x in str(raw).replace(";", ",").split(",")]

    out: list[str] = []
    seen: set[str] = set()
    for key in items:
        if not key or key in seen:
            continue
        seen.add(key)
        out.append(key)
    return out


def collect_secret_vars(exec_env: Dict[str, Any]) -> Dict[str, str]:
    out: Dict[str, str] = {}
    for key in parse_secret_env_keys(exec_env):
        val = os.environ.get(key)
        if val is None:
            continue
        text = str(val).strip()
        if not text:
            continue
        out[key] = text
    return out


def merge_context_with_secrets(context_vars: Dict[str, Any], secret_vars: Dict[str, str]) -> Dict[str, Any]:
    merged = dict(context_vars or {})
    if not secret_vars:
        return merged
    secret_ns: Dict[str, Any] = dict(secret_vars)
    existing = merged.get("secret")
    if isinstance(existing, dict):
        secret_ns.update({str(k): v for k, v in existing.items()})
    merged["secret"] = secret_ns
    return merged


def apply_db_mode_from_exec_env(exec_env: Dict[str, Any]) -> None:
    mode = str(exec_env.get("db_mode") or "").strip()
    if mode:
        os.environ["ETL_DB_MODE"] = mode
    verbose = exec_env.get("db_verbose")
    if verbose is not None:
        os.environ["ETL_DB_VERBOSE"] = "1" if bool(verbose) else "0"


def _resolve_max_passes(*, global_vars: Dict[str, Any], exec_env: Dict[str, Any]) -> int:
    raw = exec_env.get("resolve_max_passes", global_vars.get("resolve_max_passes", 20))
    try:
        value = int(raw)
    except Exception:
        value = 20
    return max(1, min(100, value))


def _resolve_bootstrap_log_file(
    *,
    global_vars: Dict[str, Any],
    label: str,
) -> Path:
    base = str(global_vars.get("logdir") or global_vars.get("log") or ".runs/bootstrap_logs").strip()
    return (Path(base).expanduser() / "bootstrap" / f"{label}.log").resolve()


def _build_bootstrap_logging(
    *,
    global_vars: Dict[str, Any],
    label: str,
    logger_name: str,
) -> LoggingContext:
    bootstrap_log_file = _resolve_bootstrap_log_file(global_vars=global_vars, label=label)
    bootstrap_logger = configure_app_logger(
        logger_name=logger_name,
        log_file=bootstrap_log_file,
        force=True,
    )
    return LoggingContext(
        bootstrap_log_file=bootstrap_log_file,
        bootstrap_logger=bootstrap_logger,
    )


def _build_variable_catalog(
    *,
    global_vars: Dict[str, Any],
    local_env_vars: Dict[str, str],
    exec_env: Dict[str, Any],
    project_vars: Dict[str, Any],
    commandline_vars: Dict[str, Any],
    parse_context_vars: Dict[str, Any],
    pipeline: Optional[Any],
    project_id: Optional[str],
    env_name: Optional[str],
    executor: Optional[str],
) -> tuple[VariableCatalog, VariableSolver]:
    max_passes = _resolve_max_passes(global_vars=global_vars, exec_env=exec_env)
    solver = VariableSolver(max_passes=max_passes)
    solver.overlay("global", global_vars, add_namespace=True, add_flat=True)
    solver.overlay("globals", global_vars, add_namespace=True, add_flat=False)
    solver.overlay("local_env", local_env_vars, add_namespace=True, add_flat=False)
    solver.overlay("env", exec_env, add_namespace=True, add_flat=True)
    solver.overlay("project", project_vars, add_namespace=True, add_flat=True)
    solver.overlay("commandline", commandline_vars, add_namespace=True, add_flat=True)
    solver.overlay("context", parse_context_vars, add_namespace=True, add_flat=False)
    if pipeline is not None:
        solver.overlay("pipe", dict(getattr(pipeline, "vars", {}) or {}), add_namespace=True, add_flat=True)
        solver.overlay("vars", dict(getattr(pipeline, "vars", {}) or {}), add_namespace=True, add_flat=False)
        solver.overlay("dirs", dict(getattr(pipeline, "dirs", {}) or {}), add_namespace=True, add_flat=True)
    solver.with_sys(
        {
            "project": {"id": str(project_id or "")},
            "env": {"name": str(env_name or "")},
            "executor": {"name": str(executor or "")},
        }
    )
    return VariableCatalog(
        max_passes=max_passes,
        raw_context=solver.context(),
        resolved_context=solver.resolved_context(),
    ), solver


def build_runtime_context(req: RuntimeContextRequest) -> RuntimeContext:
    """Resolve global/env/project/pipeline-aware context for CLI/web/batch."""
    try:
        global_config_path = resolve_global_config_path(Path(req.global_config) if req.global_config else None)
    except ConfigError as exc:
        raise RuntimeContextError(f"Global config error: {exc}") from exc

    global_vars: Dict[str, Any] = {}
    if global_config_path:
        try:
            global_vars = load_global_config(global_config_path)
        except ConfigError as exc:
            raise RuntimeContextError(f"Global config error: {exc}") from exc

    logging_ctx = _build_bootstrap_logging(
        global_vars=global_vars,
        label=req.bootstrap_label,
        logger_name=req.logger_name,
    )
    logging_ctx.bootstrap_logger.info("Bootstrap context initialization started")

    try:
        projects_config_path = resolve_projects_config_path(
            Path(req.projects_config) if req.projects_config else None
        )
    except ProjectConfigError as exc:
        raise RuntimeContextError(f"Projects config error: {exc}") from exc

    try:
        environments_config_path = resolve_execution_config_path(
            Path(req.environments_config) if req.environments_config else None
        )
    except ExecutionConfigError as exc:
        raise RuntimeContextError(f"Environments config error: {exc}") from exc

    selected_env_name = str(req.env_name or "").strip() or None
    selected_executor = str(req.executor or "").strip().lower() or None

    exec_env: Dict[str, Any] = {}
    if environments_config_path and selected_env_name:
        try:
            envs = load_execution_config(environments_config_path)
            exec_env = dict(envs.get(selected_env_name, {}) or {})
            if not exec_env:
                raise RuntimeContextError(f"Execution env '{selected_env_name}' not found in config")
            validate_environment_executor(selected_env_name, exec_env, executor=selected_executor)
            exec_env = apply_execution_env_overrides(exec_env)
            exec_env = resolve_execution_env_templates(exec_env, global_vars=global_vars)
        except ExecutionConfigError as exc:
            raise RuntimeContextError(f"Environments config error: {exc}") from exc
    elif selected_env_name and not environments_config_path:
        raise RuntimeContextError("Environments config error: `env_name` provided but no environments config was found.")

    if not selected_executor:
        env_executor = str(exec_env.get("executor") or "").strip().lower()
        selected_executor = env_executor or None

    apply_db_mode_from_exec_env(exec_env)
    local_env_vars = dict(req.local_env_vars or {str(k): str(v) for k, v in os.environ.items()})

    if req.commandline_vars is not None:
        commandline_vars = dict(req.commandline_vars or {})
    else:
        commandline_vars = parse_cli_var_overrides(req.commandline_var_entries)
    parse_context_vars = merge_context_with_secrets(
        commandline_vars,
        collect_secret_vars(exec_env) if req.include_secret_vars else {},
    )

    pipeline_path: Optional[Path] = Path(req.pipeline_path).expanduser() if req.pipeline_path else None
    project_id = resolve_project_id(
        explicit_project_id=req.project_id,
        pipeline_project_id=None,
        pipeline_path=pipeline_path if pipeline_path else None,
    )
    project_vars = {}

    pipeline_obj: Optional[Any] = None
    if pipeline_path is not None:
        try:
            project_vars = load_project_vars(project_id=project_id, projects_config_path=projects_config_path)
        except ProjectConfigError as exc:
            raise RuntimeContextError(f"Projects config error: {exc}") from exc

        try:
            pipeline_path = resolve_pipeline_path_from_project_sources(
                pipeline_path,
                project_vars=project_vars,
                repo_root=Path(".").resolve(),
                cache_root=_pipeline_assets_cache_root(global_vars=global_vars, exec_env=exec_env),
            )
        except PipelineAssetError as exc:
            raise RuntimeContextError(f"Pipeline asset resolution error: {exc}") from exc

        try:
            pre_pipeline = parse_pipeline(
                pipeline_path,
                global_vars=global_vars,
                env_vars=exec_env,
                context_vars=parse_context_vars,
            )
        except (PipelineError, FileNotFoundError) as exc:
            raise RuntimeContextError(f"Invalid pipeline: {exc}") from exc

        project_id = resolve_project_id(
            explicit_project_id=req.project_id,
            pipeline_project_id=getattr(pre_pipeline, "project_id", None),
            pipeline_path=pipeline_path,
        )
        try:
            project_vars = load_project_vars(project_id=project_id, projects_config_path=projects_config_path)
        except ProjectConfigError as exc:
            raise RuntimeContextError(f"Projects config error: {exc}") from exc
        try:
            pipeline_obj = parse_pipeline(
                pipeline_path,
                global_vars=global_vars,
                env_vars=exec_env,
                project_vars=project_vars,
                context_vars=parse_context_vars,
            )
        except (PipelineError, FileNotFoundError) as exc:
            raise RuntimeContextError(f"Invalid pipeline: {exc}") from exc

    logging_ctx.bootstrap_logger.info(
        "Bootstrap context loaded env=%s executor=%s project_id=%s",
        selected_env_name or "",
        selected_executor or "",
        project_id or "",
    )
    target_catalog, target_solver = _build_variable_catalog(
        global_vars=global_vars,
        local_env_vars=local_env_vars,
        exec_env=exec_env,
        project_vars=project_vars,
        commandline_vars=commandline_vars,
        parse_context_vars=parse_context_vars,
        pipeline=pipeline_obj,
        project_id=project_id,
        env_name=selected_env_name,
        executor=selected_executor,
    )
    # Control scope represents the local/orchestrator process context.
    control_catalog, control_solver = _build_variable_catalog(
        global_vars=global_vars,
        local_env_vars=local_env_vars,
        exec_env={},
        project_vars=project_vars,
        commandline_vars=commandline_vars,
        parse_context_vars=parse_context_vars,
        pipeline=pipeline_obj,
        project_id=project_id,
        env_name="control",
        executor="local",
    )
    logging_ctx.bootstrap_logger.info(
        "Variable catalog built max_passes=%s keys=%s",
        target_catalog.max_passes,
        len(target_catalog.resolved_context.keys()),
    )
    return RuntimeContext(
        global_config_path=global_config_path,
        projects_config_path=projects_config_path,
        environments_config_path=environments_config_path,
        env_name=selected_env_name,
        selected_executor=selected_executor,
        pipeline_path=pipeline_path.resolve() if pipeline_path else None,
        project_id=project_id,
        local_env_vars=local_env_vars,
        global_vars=global_vars,
        exec_env=exec_env,
        project_vars=project_vars,
        commandline_vars=commandline_vars,
        parse_context_vars=parse_context_vars,
        pipeline=pipeline_obj,
        solvers={"control": control_solver, "target": target_solver},
        variable_catalogs={"control": control_catalog, "target": target_catalog},
        variable_catalog=target_catalog,
        logging=logging_ctx,
    )


__all__ = [
    "RuntimeContextError",
    "LoggingContext",
    "RuntimeContext",
    "VariableCatalog",
    "RuntimeContextRequest",
    "build_runtime_context",
    "parse_cli_var_overrides",
    "parse_secret_env_keys",
    "collect_secret_vars",
    "merge_context_with_secrets",
    "apply_db_mode_from_exec_env",
]

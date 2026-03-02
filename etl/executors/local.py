# research-etl
# Copyright (c) 2026 Joseph Weaver
# This file is part of the research-etl project and is licensed under the MIT License.
# You may not use this file except in compliance with the License.
# See https://github.com/josephweaver/research-etl for details.

"""
Local executor: runs pipelines synchronously in-process.
"""

from __future__ import annotations

from datetime import datetime
from dataclasses import replace
from pathlib import Path
from typing import Any, Dict, Optional

from .base import Executor, RunState, RunStatus, SubmissionResult
from ..git_checkout import GitCheckoutError, GitExecutionSpec
from ..pipeline import Pipeline, parse_pipeline, PipelineError
from ..runner import run_pipeline, RunResult
from ..source_control import SourceExecutionSpec, make_git_source_provider
from ..tracking import record_run, load_run_step_states, upsert_run_context_snapshot
from ..variable_solver import VariableSolver

_SOURCE_PROVIDER = make_git_source_provider()


def _to_source_spec(spec: SourceExecutionSpec | GitExecutionSpec) -> SourceExecutionSpec:
    if isinstance(spec, SourceExecutionSpec):
        return spec
    return SourceExecutionSpec(
        provider="git",
        revision=str(spec.commit_sha or ""),
        origin_url=spec.origin_url,
        repo_name=spec.repo_name,
        is_dirty=spec.git_is_dirty,
        extra={"commit_sha": str(spec.commit_sha or "")},
    )


def resolve_execution_spec(**kwargs) -> SourceExecutionSpec:
    return _SOURCE_PROVIDER.resolve_execution_spec(**kwargs)


def ensure_repo_checkout(base_dir: Path, spec: SourceExecutionSpec | GitExecutionSpec) -> Path:
    return _SOURCE_PROVIDER.ensure_repo_checkout(base_dir, _to_source_spec(spec))


def ensure_bundle_checkout(base_dir: Path, spec: SourceExecutionSpec | GitExecutionSpec, bundle_path: Path) -> Path:
    return _SOURCE_PROVIDER.ensure_bundle_checkout(base_dir, _to_source_spec(spec), bundle_path)


def ensure_snapshot_checkout(base_dir: Path, spec: SourceExecutionSpec | GitExecutionSpec, snapshot_path: Path) -> Path:
    return _SOURCE_PROVIDER.ensure_snapshot_checkout(base_dir, _to_source_spec(spec), snapshot_path)


def map_to_checkout(path: Path, repo_root: Path, checkout_root: Path, label: str) -> Path:
    rel = _SOURCE_PROVIDER.repo_relative_path(path, repo_root, label)
    return checkout_root / rel


class LocalExecutor(Executor):
    name = "local"

    def __init__(
        self,
        plugin_dir: Path = Path("plugins"),
        workdir: Path = Path(".runs"),
        dry_run: bool = False,
        max_retries: int = 0,
        retry_delay_seconds: float = 0.0,
        enforce_git_checkout: bool = False,
        require_clean_git: bool = True,
        execution_source: str = "auto",
        source_bundle: Optional[str] = None,
        source_snapshot: Optional[str] = None,
        allow_workspace_source: bool = False,
    ):
        self.plugin_dir = plugin_dir
        self.workdir = workdir
        self.dry_run = dry_run
        self.max_retries = max_retries
        self.retry_delay_seconds = retry_delay_seconds
        self.enforce_git_checkout = enforce_git_checkout
        self.require_clean_git = require_clean_git
        self.execution_source = (execution_source or "auto").strip().lower()
        self.source_bundle = source_bundle
        self.source_snapshot = source_snapshot
        self.allow_workspace_source = allow_workspace_source
        self._statuses: Dict[str, RunStatus] = {}

    def _record_status(self, run_result: RunResult, message: str = "") -> RunStatus:
        state = RunState.SUCCEEDED if run_result.success else RunState.FAILED
        status = RunStatus(run_id=run_result.run_id, state=state, message=message)
        self._statuses[run_result.run_id] = status
        return status

    def capabilities(self) -> Dict[str, bool]:
        return {
            "cancel": False,
            "artifact_tree": True,
            "artifact_file": True,
            "query_data": False,
        }

    def submit(self, pipeline_path: str, context: Dict[str, Any] | None = None) -> SubmissionResult:
        context = context or {}
        pipeline_ref = Path(pipeline_path)
        plugin_dir = self.plugin_dir
        global_vars = context.get("global_vars") or {}
        execution_env = context.get("execution_env") or {}
        project_vars = context.get("project_vars") or {}
        commandline_vars = context.get("commandline_vars") or {}
        path_style = str(commandline_vars.get("path_style") or execution_env.get("path_style") or "").strip()

        def _resolve_effective_workdir(pipeline_obj: Optional[Pipeline]) -> Path:
            solver = VariableSolver(max_passes=20)
            solver.overlay("global", dict(global_vars), add_namespace=True, add_flat=True)
            solver.overlay("globals", dict(global_vars), add_namespace=True, add_flat=False)
            solver.overlay("env", dict(execution_env), add_namespace=True, add_flat=True)
            solver.overlay("project", dict(project_vars), add_namespace=True, add_flat=True)
            if pipeline_obj is not None:
                solver.overlay("pipe", dict(getattr(pipeline_obj, "vars", {}) or {}), add_namespace=True, add_flat=True)
                solver.overlay("vars", dict(getattr(pipeline_obj, "vars", {}) or {}), add_namespace=True, add_flat=False)
                solver.overlay("dirs", dict(getattr(pipeline_obj, "dirs", {}) or {}), add_namespace=True, add_flat=True)
                pipeline_workdir = str(getattr(pipeline_obj, "workdir", "") or "").strip()
                if pipeline_workdir:
                    solver.update({"workdir": pipeline_workdir})
            solver.overlay("commandline", dict(commandline_vars), add_namespace=True, add_flat=True)
            default_workdir = str(self.workdir)
            existing = str(solver.get("workdir", "", resolve=False) or "").strip()
            if not existing:
                solver.update({"workdir": default_workdir})
            resolved = str(solver.get_path("workdir", default_workdir, path_style=path_style) or "").strip()
            return Path(resolved or default_workdir)

        preparse_workdir = _resolve_effective_workdir(None)

        if self.enforce_git_checkout:
            repo_root = Path(context.get("repo_root") or Path(".").resolve()).resolve()
            provenance = context.get("provenance") or {}
            source_mode = self.execution_source
            bundle = context.get("source_bundle") or self.source_bundle
            snapshot = context.get("source_snapshot") or self.source_snapshot
            allow_workspace = bool(context.get("allow_workspace_source", self.allow_workspace_source))
            git_remote_override = str(context.get("execution_env", {}).get("git_remote_url") or "").strip() or None

            modes = self._resolve_mode_order(source_mode, allow_workspace)
            checkout_root = None
            selected_mode = None
            last_error: Optional[Exception] = None

            for mode in modes:
                try:
                    if mode == "workspace":
                        checkout_root = repo_root
                    elif mode == "git_remote":
                        spec = resolve_execution_spec(
                            repo_root=repo_root,
                            provenance=provenance,
                            require_clean=self.require_clean_git,
                            require_origin=not bool(git_remote_override),
                        )
                        if git_remote_override:
                            spec = replace(spec, origin_url=git_remote_override)
                        checkout_root = ensure_repo_checkout(preparse_workdir / "_code", spec)
                    elif mode == "git_bundle":
                        if not bundle:
                            raise GitCheckoutError("No source bundle configured.")
                        spec = resolve_execution_spec(
                            repo_root=repo_root,
                            provenance=provenance,
                            require_clean=self.require_clean_git,
                            require_origin=False,
                        )
                        checkout_root = ensure_bundle_checkout(preparse_workdir / "_code", spec, Path(bundle))
                    elif mode == "snapshot":
                        if not snapshot:
                            raise GitCheckoutError("No source snapshot configured.")
                        spec = resolve_execution_spec(
                            repo_root=repo_root,
                            provenance=provenance,
                            require_clean=False,
                            require_origin=False,
                        )
                        checkout_root = ensure_snapshot_checkout(preparse_workdir / "_code", spec, Path(snapshot))
                    else:
                        raise GitCheckoutError(f"Unsupported execution_source: {mode}")
                except Exception as exc:  # noqa: BLE001
                    last_error = exc
                    continue
                selected_mode = mode
                break

            if not checkout_root or not selected_mode:
                raise RuntimeError(f"Could not prepare execution source ({source_mode}): {last_error}")
            if isinstance(provenance, dict):
                provenance["source_mode"] = selected_mode
                if selected_mode == "git_bundle" and bundle:
                    provenance["source_uri"] = str(bundle)
                if selected_mode == "snapshot" and snapshot:
                    provenance["source_uri"] = str(snapshot)
                if selected_mode == "workspace":
                    provenance["source_uri"] = str(repo_root)
            pipeline_ref = map_to_checkout(pipeline_ref, repo_root, checkout_root, "pipeline")
            plugin_dir = map_to_checkout(self.plugin_dir, repo_root, checkout_root, "plugins_dir")

        try:
            pipeline: Pipeline = parse_pipeline(
                pipeline_ref,
                global_vars=global_vars,
                env_vars=execution_env,
                project_vars=project_vars,
                context_vars=commandline_vars,
            )
        except PipelineError as exc:
            raise RuntimeError(f"Pipeline parse failed: {exc}") from exc
        requested_step_indices = _parse_step_indices(context.get("step_indices"), len(pipeline.steps))
        if requested_step_indices:
            pipeline.steps = [pipeline.steps[i] for i in requested_step_indices]
        seed_context = dict(context.get("seed_context") or {})
        if seed_context:
            pipeline.vars = dict(getattr(pipeline, "vars", {}) or {})
            pipeline.vars.update(seed_context)
        effective_workdir = _resolve_effective_workdir(pipeline)
        resume_run_id = context.get("resume_run_id")
        resume_succeeded_steps = None
        prior_step_outputs = None
        if resume_run_id:
            states = load_run_step_states(str(resume_run_id))
            resume_succeeded_steps = {name for name, st in states.items() if st.success}
            prior_step_outputs = {name: st.outputs for name, st in states.items()}

        run_started: Optional[datetime] = None
        raw_started = context.get("run_started_at")
        if isinstance(raw_started, datetime):
            run_started = raw_started
        elif raw_started:
            text = str(raw_started).strip()
            if text:
                try:
                    run_started = datetime.fromisoformat(text.replace("Z", "+00:00")).replace(tzinfo=None)
                except Exception:
                    run_started = None

        run_result = run_pipeline(
            pipeline,
            plugin_dir=plugin_dir,
            workdir=effective_workdir,
            run_id=str(context.get("run_id") or "").strip() or None,
            run_started=run_started,
            dry_run=self.dry_run,
            max_retries=self.max_retries,
            retry_delay_seconds=self.retry_delay_seconds,
            resume_succeeded_steps=resume_succeeded_steps,
            prior_step_outputs=prior_step_outputs,
            log_func=context.get("log"),
            step_log_func=context.get("step_log"),
            context_snapshot_func=lambda **snap: upsert_run_context_snapshot(
                run_id=str((snap.get("context") or {}).get("sys", {}).get("run", {}).get("id") or context.get("run_id") or ""),
                pipeline=str(pipeline_path),
                project_id=context.get("project_id"),
                executor=self.name,
                event_type=str(snap.get("event_type") or "snapshot"),
                context=dict(snap.get("context") or {}),
                step_name=str(snap.get("step_name") or "").strip() or None,
                step_index=snap.get("step_index"),
                snapshot_file=(
                    effective_workdir
                    / "_context_snapshots"
                    / f"{str((snap.get('context') or {}).get('sys', {}).get('run', {}).get('id') or context.get('run_id') or 'run').strip()}.jsonl"
                ),
            ),
        )
        # attach timestamps
        run_result.started_at = context.get("started_at") or datetime.utcnow().isoformat() + "Z"  # type: ignore[attr-defined]
        run_result.ended_at = datetime.utcnow().isoformat() + "Z"  # type: ignore[attr-defined]

        status = self._record_status(run_result)
        record_run(
            run_result,
            pipeline_path,
            effective_workdir / "runs.jsonl",
            executor=self.name,
            artifact_dir=getattr(run_result, "artifact_dir", None),
            provenance=context.get("provenance"),
            project_id=context.get("project_id"),
        )
        return SubmissionResult(run_id=run_result.run_id, message=status.message)

    @staticmethod
    def _resolve_mode_order(source_mode: str, allow_workspace: bool) -> list[str]:
        mode = (source_mode or "auto").strip().lower()
        if mode == "auto":
            order = ["git_remote", "git_bundle", "snapshot"]
            if allow_workspace:
                order.append("workspace")
            return order
        if mode == "workspace" and not allow_workspace:
            raise RuntimeError("execution_source=workspace requires allow_workspace_source=true")
        return [mode]

    def status(self, run_id: str) -> RunStatus:
        if run_id not in self._statuses:
            return RunStatus(run_id=run_id, state=RunState.FAILED, message="Unknown run_id")
        return self._statuses[run_id]

    def artifact_tree(self, artifact_dir: str) -> Dict[str, Any]:
        root = Path(artifact_dir).expanduser()
        if not root.is_absolute():
            root = (Path(".").resolve() / root).resolve()
        if not root.exists() or not root.is_dir():
            raise RuntimeError(f"Artifact directory not found: {root}")

        def walk(path: Path, rel: str) -> Dict[str, Any]:
            if path.is_dir():
                children = []
                for child in sorted(path.iterdir(), key=lambda p: (not p.is_dir(), p.name.lower())):
                    child_rel = f"{rel}/{child.name}" if rel else child.name
                    children.append(walk(child, child_rel))
                return {"name": path.name or ".", "path": rel, "type": "dir", "children": children}
            size = path.stat().st_size if path.exists() else 0
            return {"name": path.name, "path": rel, "type": "file", "size": size}

        return walk(root, "")

    def artifact_file(self, artifact_dir: str, relative_path: str, max_bytes: int = 256 * 1024) -> Dict[str, Any]:
        root = Path(artifact_dir).expanduser()
        if not root.is_absolute():
            root = (Path(".").resolve() / root).resolve()
        rel = (relative_path or "").strip().lstrip("/").replace("\\", "/")
        candidate = (root / rel).resolve()
        if root not in candidate.parents and candidate != root:
            raise RuntimeError("Invalid artifact path")
        if not candidate.exists() or not candidate.is_file():
            raise RuntimeError(f"Artifact file not found: {relative_path}")
        data = candidate.read_bytes()[:max_bytes]
        text = data.decode("utf-8", errors="replace")
        truncated = candidate.stat().st_size > max_bytes
        if truncated:
            text += "\n\n...[truncated]"
        return {"path": rel, "content": text, "truncated": truncated}


def _parse_step_indices(value: Any, step_count: int) -> list[int]:
    if value is None or value == "":
        return []
    raw_items: list[Any]
    if isinstance(value, (list, tuple, set)):
        raw_items = list(value)
    else:
        raw_items = [x.strip() for x in str(value).replace(";", ",").split(",")]
    out: list[int] = []
    seen: set[int] = set()
    for raw in raw_items:
        text = str(raw).strip()
        if not text:
            continue
        try:
            idx = int(text)
        except (TypeError, ValueError) as exc:
            raise RuntimeError(f"Invalid step index in context.step_indices: {text}") from exc
        if idx < 0 or idx >= int(step_count):
            raise RuntimeError(f"Step index out of range in context.step_indices: {idx} (step_count={step_count})")
        if idx in seen:
            continue
        seen.add(idx)
        out.append(idx)
    out.sort()
    return out

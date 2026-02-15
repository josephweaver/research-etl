"""
Local executor: runs pipelines synchronously in-process.
"""

from __future__ import annotations

from datetime import datetime
from pathlib import Path
from typing import Any, Dict, Optional

from .base import Executor, RunState, RunStatus, SubmissionResult
from ..git_checkout import (
    GitCheckoutError,
    ensure_bundle_checkout,
    ensure_repo_checkout,
    ensure_snapshot_checkout,
    map_to_checkout,
    resolve_execution_spec,
)
from ..pipeline import Pipeline, parse_pipeline, PipelineError
from ..runner import run_pipeline, RunResult
from ..tracking import record_run, load_run_step_states


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

    def submit(self, pipeline_path: str, context: Dict[str, Any] | None = None) -> SubmissionResult:
        context = context or {}
        pipeline_ref = Path(pipeline_path)
        plugin_dir = self.plugin_dir
        global_vars = context.get("global_vars") or {}
        execution_env = context.get("execution_env") or {}

        if self.enforce_git_checkout:
            repo_root = Path(context.get("repo_root") or Path(".").resolve()).resolve()
            provenance = context.get("provenance") or {}
            source_mode = self.execution_source
            bundle = context.get("source_bundle") or self.source_bundle
            snapshot = context.get("source_snapshot") or self.source_snapshot
            allow_workspace = bool(context.get("allow_workspace_source", self.allow_workspace_source))

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
                            require_origin=True,
                        )
                        checkout_root = ensure_repo_checkout(self.workdir / "_code", spec)
                    elif mode == "git_bundle":
                        if not bundle:
                            raise GitCheckoutError("No source bundle configured.")
                        spec = resolve_execution_spec(
                            repo_root=repo_root,
                            provenance=provenance,
                            require_clean=self.require_clean_git,
                            require_origin=False,
                        )
                        checkout_root = ensure_bundle_checkout(self.workdir / "_code", spec, Path(bundle))
                    elif mode == "snapshot":
                        if not snapshot:
                            raise GitCheckoutError("No source snapshot configured.")
                        spec = resolve_execution_spec(
                            repo_root=repo_root,
                            provenance=provenance,
                            require_clean=False,
                            require_origin=False,
                        )
                        checkout_root = ensure_snapshot_checkout(self.workdir / "_code", spec, Path(snapshot))
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
            )
        except PipelineError as exc:
            raise RuntimeError(f"Pipeline parse failed: {exc}") from exc
        resume_run_id = context.get("resume_run_id")
        resume_succeeded_steps = None
        prior_step_outputs = None
        if resume_run_id:
            states = load_run_step_states(str(resume_run_id))
            resume_succeeded_steps = {name for name, st in states.items() if st.success}
            prior_step_outputs = {name: st.outputs for name, st in states.items()}

        run_result = run_pipeline(
            pipeline,
            plugin_dir=plugin_dir,
            workdir=self.workdir,
            run_id=str(context.get("run_id") or "").strip() or None,
            dry_run=self.dry_run,
            max_retries=self.max_retries,
            retry_delay_seconds=self.retry_delay_seconds,
            resume_succeeded_steps=resume_succeeded_steps,
            prior_step_outputs=prior_step_outputs,
            log_func=context.get("log"),
            step_log_func=context.get("step_log"),
        )
        # attach timestamps
        run_result.started_at = context.get("started_at") or datetime.utcnow().isoformat() + "Z"  # type: ignore[attr-defined]
        run_result.ended_at = datetime.utcnow().isoformat() + "Z"  # type: ignore[attr-defined]

        status = self._record_status(run_result)
        record_run(
            run_result,
            pipeline_path,
            self.workdir / "runs.jsonl",
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

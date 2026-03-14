from __future__ import annotations

import uuid
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, Optional

from .base import RunState, RunStatus, SubmissionResult
from .local import (
    LocalExecutor,
    _parse_step_indices,
    ensure_bundle_checkout,
    ensure_repo_checkout,
    ensure_snapshot_checkout,
    map_to_checkout,
    overlay_pipeline_asset_checkout,
    parse_pipeline,
    resolve_execution_spec,
)
from .local_batch_adapter import LocalBatchAdapter
from ..git_checkout import GitCheckoutError
from ..job_specs import ResolvedPaths, RunSelection, RunSpec, SourceRef
from ..pipeline_assets import PipelineAssetMatch, infer_pipeline_asset_match
from ..pipeline import PipelineError
from ..provisioners import LocalProvisioner, ProvisionState
from ..tracking import upsert_run_status
from ..variable_solver import VariableSolver


def _group_steps_with_indices(steps: list[Any]) -> list[list[tuple[int, Any]]]:
    batches: list[list[tuple[int, Any]]] = []
    i = 0
    while i < len(steps):
        current = steps[i]
        group = [(i, current)]
        if getattr(current, "parallel_with", None):
            key = current.parallel_with
            j = i + 1
            while j < len(steps) and getattr(steps[j], "parallel_with", None) == key:
                group.append((j, steps[j]))
                j += 1
            i = j
        else:
            i += 1
        batches.append(group)
    return batches


class LocalBatchExecutor(LocalExecutor):
    name = "local_batch"

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
        *,
        array_task_limit: int = 1000,
        enable_controller_job: bool = False,
        provisioner: Optional[LocalProvisioner] = None,
        python_bin: Optional[str] = None,
        verbose: bool = False,
        global_config: Optional[Path] = None,
        projects_config: Optional[Path] = None,
        environments_config: Optional[Path] = None,
        env_name: Optional[str] = None,
    ) -> None:
        super().__init__(
            plugin_dir=plugin_dir,
            workdir=workdir,
            dry_run=dry_run,
            max_retries=max_retries,
            retry_delay_seconds=retry_delay_seconds,
            enforce_git_checkout=enforce_git_checkout,
            require_clean_git=require_clean_git,
            execution_source=execution_source,
            source_bundle=source_bundle,
            source_snapshot=source_snapshot,
            allow_workspace_source=allow_workspace_source,
        )
        self.array_task_limit = int(array_task_limit)
        self.enable_controller_job = bool(enable_controller_job)
        self.provisioner = provisioner or LocalProvisioner(dry_run=dry_run)
        self.python_bin = str(python_bin or "").strip() or None
        self.verbose = bool(verbose)
        self.global_config = Path(global_config) if global_config else None
        self.projects_config = Path(projects_config) if projects_config else None
        self.environments_config = Path(environments_config) if environments_config else None
        self.env_name = str(env_name or "").strip() or None

    def submit(self, pipeline_path: str, context: Dict[str, Any] | None = None) -> SubmissionResult:
        ctx = dict(context or {})
        ts = datetime.utcnow()
        run_id = str(ctx.get("run_id") or "").strip() or uuid.uuid4().hex
        run_started_at = str(ctx.get("run_started_at") or "").strip() or (ts.isoformat() + "Z")
        provenance = dict(ctx.get("provenance") or {})
        global_vars = dict(ctx.get("global_vars") or {})
        execution_env = dict(ctx.get("execution_env") or {})
        project_vars = dict(ctx.get("project_vars") or {})
        commandline_vars = dict(ctx.get("commandline_vars") or {})
        path_style = str(commandline_vars.get("path_style") or execution_env.get("path_style") or "").strip()

        pipeline_ref = Path(pipeline_path)
        plugin_dir = self.plugin_dir
        preparse_workdir = self._resolve_effective_workdir(
            None,
            global_vars=global_vars,
            execution_env=execution_env,
            project_vars=project_vars,
            commandline_vars=commandline_vars,
            path_style=path_style,
        )
        source_root_raw = (
            commandline_vars.get("source_root")
            or execution_env.get("source_root")
            or global_vars.get("source_root")
        )
        source_root_text = str(source_root_raw or "").strip()
        source_root = Path(source_root_text).expanduser() if source_root_text else (preparse_workdir / "_code")
        pipeline_assets_root_raw = (
            commandline_vars.get("pipeline_assets_cache_root")
            or execution_env.get("pipeline_assets_cache_root")
            or global_vars.get("pipeline_assets_cache_root")
            or source_root_text
        )
        pipeline_assets_root_text = str(pipeline_assets_root_raw or "").strip()
        pipeline_assets_root = Path(pipeline_assets_root_text).expanduser() if pipeline_assets_root_text else source_root

        if self.enforce_git_checkout:
            repo_root = Path(ctx.get("repo_root") or Path(".").resolve()).resolve()
            source_mode = self.execution_source
            bundle = ctx.get("source_bundle") or self.source_bundle
            snapshot = ctx.get("source_snapshot") or self.source_snapshot
            allow_workspace = bool(ctx.get("allow_workspace_source", self.allow_workspace_source))
            git_remote_override = (
                str(ctx.get("execution_env", {}).get("git_remote_url") or "").strip()
                or str(global_vars.get("etl_git_remote_url") or "").strip()
                or None
            )

            modes = self._resolve_mode_order(source_mode, allow_workspace)
            checkout_root = None
            selected_mode = None
            last_error: Optional[Exception] = None
            pipeline_asset_match: Optional[PipelineAssetMatch] = None

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
                            from dataclasses import replace

                            spec = replace(spec, origin_url=git_remote_override)
                        checkout_root = ensure_repo_checkout(source_root, spec)
                    elif mode == "git_bundle":
                        if not bundle:
                            raise GitCheckoutError("No source bundle configured.")
                        spec = resolve_execution_spec(
                            repo_root=repo_root,
                            provenance=provenance,
                            require_clean=self.require_clean_git,
                            require_origin=False,
                        )
                        checkout_root = ensure_bundle_checkout(source_root, spec, Path(bundle))
                    elif mode == "snapshot":
                        if not snapshot:
                            raise GitCheckoutError("No source snapshot configured.")
                        spec = resolve_execution_spec(
                            repo_root=repo_root,
                            provenance=provenance,
                            require_clean=False,
                            require_origin=False,
                        )
                        checkout_root = ensure_snapshot_checkout(source_root, spec, Path(snapshot))
                    else:
                        raise GitCheckoutError(f"Unsupported execution_source: {mode}")
                    if mode != "workspace":
                        pipeline_asset_match = infer_pipeline_asset_match(
                            pipeline_ref,
                            project_vars=project_vars,
                            repo_root=repo_root,
                            cache_root=pipeline_assets_root,
                        )
                        if pipeline_asset_match is not None:
                            overlay_pipeline_asset_checkout(checkout_root, pipeline_asset_match)
                except Exception as exc:  # noqa: BLE001
                    last_error = exc
                    continue
                selected_mode = mode
                break
            if not checkout_root or not selected_mode:
                raise RuntimeError(f"Could not prepare execution source ({source_mode}): {last_error}")
            provenance["source_mode"] = selected_mode
            try:
                pipeline_ref = map_to_checkout(pipeline_ref, repo_root, checkout_root, "pipeline")
            except Exception:
                if selected_mode == "workspace":
                    pipeline_ref = pipeline_ref.resolve()
                elif pipeline_asset_match is not None:
                    pipeline_ref = checkout_root / Path(pipeline_asset_match.pipeline_remote_hint)
                else:
                    raise
            plugin_dir = map_to_checkout(self.plugin_dir, repo_root, checkout_root, "plugins_dir")
        else:
            checkout_root = Path(ctx.get("repo_root") or Path(".").resolve()).resolve()
            selected_mode = "workspace"

        try:
            pipeline = parse_pipeline(
                pipeline_ref,
                global_vars=global_vars,
                env_vars=execution_env,
                project_vars=project_vars,
                context_vars=commandline_vars,
            )
        except PipelineError as exc:
            raise RuntimeError(f"Pipeline parse failed: {exc}") from exc

        effective_workdir = self._resolve_effective_workdir(
            pipeline,
            global_vars=global_vars,
            execution_env=execution_env,
            project_vars=project_vars,
            commandline_vars=commandline_vars,
            path_style=path_style,
        )
        run_root = self._compute_run_root(effective_workdir, run_id, ts)
        requested_step_indices = _parse_step_indices(ctx.get("step_indices"), len(pipeline.steps))
        batches = _group_steps_with_indices(pipeline.steps)
        if requested_step_indices:
            wanted = set(requested_step_indices)
            batches = [[pair for pair in batch if int(pair[0]) in wanted] for batch in batches]
            batches = [batch for batch in batches if batch]

        base_logdir = run_root / "logs"
        paths = ResolvedPaths(
            workdir=run_root.as_posix(),
            base_logdir=base_logdir.as_posix(),
            setup_logdir=(base_logdir / "setup").as_posix(),
            context_file=(run_root / "context.json").as_posix(),
            child_jobs_file=(run_root / "child_jobs.txt").as_posix(),
            checkout_root=Path(checkout_root).as_posix(),
            pipeline_path=Path(pipeline_ref).as_posix(),
            plugins_dir=Path(plugin_dir).as_posix(),
            venv_path=((Path(checkout_root) / ".venv").as_posix()),
            requirements_path=((Path(checkout_root) / "requirements.txt").as_posix()),
            python_bin=self.python_bin or Path.cwd().joinpath(".venv").joinpath("Scripts").joinpath("python.exe").as_posix() if False else (self.python_bin or "python"),
            workdirs_to_create=[run_root.as_posix()],
            logdirs_to_create=[base_logdir.as_posix(), (base_logdir / "setup").as_posix()],
            global_config_path=(self._map_optional_path(self.global_config, checkout_root) if self.global_config else None),
            projects_config_path=(self._map_optional_path(self.projects_config, checkout_root) if self.projects_config else None),
            environments_config_path=(self._map_optional_path(self.environments_config, checkout_root) if self.environments_config else None),
        )
        selection = RunSelection(
            selected_step_indices=requested_step_indices,
            resume_run_id=(str(ctx.get("resume_run_id") or "").strip() or None),
            run_started_at=run_started_at,
            max_retries=int(self.max_retries or 0),
            retry_delay_seconds=float(self.retry_delay_seconds or 0.0),
            verbose=bool(self.verbose),
            dry_run=bool(self.dry_run),
        )
        run_spec = RunSpec(
            run_id=run_id,
            project_id=(str(ctx.get("project_id") or "").strip() or None),
            pipeline_path_input=str(pipeline_path),
            pipeline_name=(str(getattr(pipeline, "name", "") or "").strip() or None),
            job_name=(str((getattr(pipeline, "vars", {}) or {}).get("jobname") or (getattr(pipeline, "vars", {}) or {}).get("name") or "run")),
            source_repo_root=Path(checkout_root).as_posix(),
            created_at=ts,
            run_date=ts.strftime("%y%m%d"),
            run_stamp=ts.strftime("%H%M%S"),
            run_fs_id=run_root.name,
            pipeline=pipeline,
            batches=batches,
            etl_source=SourceRef(
                provider="git",
                mode=selected_mode,
                checkout_root=Path(checkout_root).as_posix(),
                local_repo_path=Path(checkout_root).as_posix() if selected_mode == "workspace" else None,
            ),
            paths=paths,
            selection=selection,
            execution_env_name=(str(ctx.get("env_name") or self.env_name or "").strip() or None),
            execution_env=execution_env,
            global_vars=global_vars,
            project_vars=project_vars,
            commandline_vars=commandline_vars,
            provenance=provenance,
        )

        adapter = LocalBatchAdapter(
            array_task_limit=self.array_task_limit,
            enable_controller_job=self.enable_controller_job,
            foreach_count_fn=self._foreach_count_from_pipeline,
            foreach_max_concurrency_fn=self._foreach_array_max_concurrency,
            resolve_batch_resources_fn=self._resolve_batch_resources,
            python_bin=self.python_bin,
            verbose=self.verbose,
        )
        job_specs = adapter.build_job_specs(run_spec)
        if not any(spec.kind == "step" for spec in job_specs):
            status = RunStatus(run_id=run_id, state=RunState.SUCCEEDED, message="No selected steps to run.")
            self._statuses[run_id] = status
            return SubmissionResult(run_id=run_id, message=status.message)

        self._statuses[run_id] = RunStatus(run_id=run_id, state=RunState.RUNNING, message="local batch running")
        job_ids: list[str] = []
        failed_message = ""
        for job_spec in job_specs:
            handle = self.provisioner.submit(adapter.to_workload(job_spec))
            job_ids.extend(list(handle.job_ids or []))
            status = self.provisioner.status(handle)
            if status.state not in {ProvisionState.SUCCEEDED, ProvisionState.QUEUED}:
                failed_message = status.message or f"{job_spec.kind} failed"
                self._statuses[run_id] = RunStatus(run_id=run_id, state=RunState.FAILED, message=failed_message)
                upsert_run_status(
                    run_id=run_id,
                    pipeline=str(pipeline_path),
                    project_id=run_spec.project_id,
                    status="failed",
                    success=False,
                    started_at=run_started_at,
                    ended_at=datetime.utcnow().isoformat() + "Z",
                    message=failed_message,
                    executor=self.name,
                    artifact_dir=run_root.as_posix(),
                    provenance=provenance,
                    event_type="run_failed",
                    event_details={"job_ids": job_ids, "failed_job_id": job_spec.job_id},
                )
                return SubmissionResult(
                    run_id=run_id,
                    backend_run_id=",".join(job_ids),
                    job_ids=job_ids,
                    message=failed_message,
                )

        success_message = f"completed {len(job_specs)} local batch jobs"
        self._statuses[run_id] = RunStatus(run_id=run_id, state=RunState.SUCCEEDED, message=success_message)
        upsert_run_status(
            run_id=run_id,
            pipeline=str(pipeline_path),
            project_id=run_spec.project_id,
            status="succeeded",
            success=True,
            started_at=run_started_at,
            ended_at=datetime.utcnow().isoformat() + "Z",
            message=success_message,
            executor=self.name,
            artifact_dir=run_root.as_posix(),
            provenance=provenance,
            event_type="run_completed",
            event_details={"job_ids": job_ids},
        )
        return SubmissionResult(
            run_id=run_id,
            backend_run_id=",".join(job_ids),
            job_ids=job_ids,
            message=success_message,
        )

    def _resolve_effective_workdir(
        self,
        pipeline_obj: Optional[Any],
        *,
        global_vars: Dict[str, Any],
        execution_env: Dict[str, Any],
        project_vars: Dict[str, Any],
        commandline_vars: Dict[str, Any],
        path_style: str = "",
    ) -> Path:
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

    @staticmethod
    def _compute_run_root(base_workdir: Path, run_id: str, started_at: datetime) -> Path:
        date_dir = started_at.strftime("%y%m%d")
        run_dir = f"{started_at.strftime('%H%M%S')}-{run_id[:8]}"
        is_same_stamp = base_workdir.name == run_dir and base_workdir.parent.name == date_dir
        if is_same_stamp:
            return base_workdir
        return base_workdir / date_dir / run_dir

    def _map_optional_path(self, path: Optional[Path], checkout_root: Path) -> Optional[str]:
        if not path:
            return None
        candidate = Path(path)
        repo_root = Path(checkout_root)
        if not candidate.is_absolute():
            return (repo_root / candidate).as_posix()
        return candidate.as_posix()

    @staticmethod
    def _foreach_count_from_pipeline(step: Any, pipeline: Any) -> Optional[int]:
        foreach_key = str(getattr(step, "foreach", "") or getattr(step, "sequential_foreach", "") or "").strip()
        if not foreach_key:
            return None
        vars_ns = dict(getattr(pipeline, "vars", {}) or {})
        current: Any = vars_ns
        for part in foreach_key.split("."):
            if isinstance(current, dict) and part in current:
                current = current[part]
            else:
                return None
        if isinstance(current, (list, tuple)):
            return len(current)
        return None

    @staticmethod
    def _foreach_array_max_concurrency(step: Any) -> Optional[int]:
        resources = dict(getattr(step, "resources", {}) or {})
        value = resources.get("foreach_max_concurrency")
        if value in (None, ""):
            return None
        try:
            return int(value)
        except (TypeError, ValueError):
            return None

    @staticmethod
    def _resolve_batch_resources(_steps: list[Any]) -> Dict[str, Optional[Any]]:
        return {"time": None, "cpus_per_task": None, "mem": None}

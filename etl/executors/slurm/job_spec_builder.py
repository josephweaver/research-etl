from __future__ import annotations

from typing import Any, Optional

from ...job_planning import RunJobPlanner
from ...job_specs import JobResources, JobSpec, PlannedJob, RunSpec


class SlurmJobSpecBuilder:
    def __init__(self, executor: Any) -> None:
        self.executor = executor

    def build(
        self,
        run_spec: RunSpec,
        *,
        source_bundle_path: Optional[str] = None,
        source_snapshot_path: Optional[str] = None,
    ) -> list[JobSpec]:
        executor = self.executor
        if not run_spec.paths:
            raise RuntimeError("RunSpec.paths is required to build SLURM job specs")

        paths = run_spec.paths
        commandline_vars = dict(run_spec.commandline_vars)
        resume_run_id = run_spec.selection.resume_run_id
        run_started_at = run_spec.selection.run_started_at or (run_spec.created_at.isoformat() + "Z")
        selected_source_mode = str(run_spec.metadata.get("selected_source_mode") or run_spec.etl_source.mode or "workspace")
        allow_workspace_source = bool(run_spec.metadata.get("allow_workspace_source", False))
        pipeline_asset_overlays = [
            {
                "repo_url": asset.repo_url,
                "ref": asset.revision,
                "pipelines_dir": asset.pipelines_dir,
                "scripts_dir": asset.scripts_dir,
            }
            for asset in run_spec.pipeline_assets
        ]

        planned_jobs = RunJobPlanner(
            array_task_limit=executor.array_task_limit,
            enable_controller_job=executor.enable_controller_job,
            controller_logdir_kind="setup",
            foreach_count_fn=executor._foreach_count_from_pipeline,
            foreach_max_concurrency_fn=executor._foreach_array_max_concurrency,
            resolve_batch_resources_fn=executor._resolve_batch_resources,
        ).build(run_spec)

        return [
            self._render_job_spec(
                run_spec,
                planned_job,
                source_bundle_path=source_bundle_path,
                source_snapshot_path=source_snapshot_path,
                selected_source_mode=selected_source_mode,
                allow_workspace_source=allow_workspace_source,
                pipeline_asset_overlays=pipeline_asset_overlays,
                resume_run_id=resume_run_id,
                run_started_at=run_started_at,
                commandline_vars=commandline_vars,
            )
            for planned_job in planned_jobs
        ]

    def _render_job_spec(
        self,
        run_spec: RunSpec,
        planned_job: PlannedJob,
        *,
        source_bundle_path: Optional[str] = None,
        source_snapshot_path: Optional[str] = None,
        selected_source_mode: str,
        allow_workspace_source: bool,
        pipeline_asset_overlays: list[dict[str, str]],
        resume_run_id: Optional[str] = None,
        run_started_at: Optional[str] = None,
        commandline_vars: Optional[dict[str, Any]] = None,
    ) -> JobSpec:
        executor = self.executor
        paths = run_spec.paths
        if not paths:
            raise RuntimeError("RunSpec.paths is required to build a JobSpec")

        if planned_job.kind == "setup":
            return JobSpec(
                job_id=planned_job.job_id,
                run_id=run_spec.run_id,
                name=planned_job.name,
                kind=planned_job.kind,
                script_text=executor._render_setup_script(
                    run_spec.run_id,
                    paths.checkout_root,
                    paths.workdir,
                    paths.setup_logdir,
                    paths.venv_path,
                    paths.requirements_path,
                    paths.python_bin,
                    list(paths.workdirs_to_create),
                    list(paths.logdirs_to_create),
                    execution_source=selected_source_mode,
                    git_origin_url=run_spec.etl_source.repo_url,
                    git_commit_sha=run_spec.etl_source.revision,
                    source_bundle_path=source_bundle_path,
                    source_snapshot_path=source_snapshot_path,
                    allow_workspace_source=allow_workspace_source,
                    pipeline_asset_overlays=pipeline_asset_overlays,
                ),
                workdir=planned_job.workdir,
                logdir=planned_job.logdir,
                dependencies=list(planned_job.dependencies),
                metadata=dict(planned_job.metadata),
                backend_options={
                    "label": planned_job.label,
                    "remote_dest_dir": planned_job.workdir,
                },
            )

        if planned_job.kind == "controller":
            return JobSpec(
                job_id=planned_job.job_id,
                run_id=run_spec.run_id,
                name=planned_job.name,
                kind=planned_job.kind,
                script_text=executor._render_controller_script(
                    run_id=run_spec.run_id,
                    workdir=paths.workdir,
                    logdir=planned_job.logdir or paths.setup_logdir,
                    child_jobs_file=paths.child_jobs_file,
                    wait_children=executor.controller_wait_children,
                    poll_seconds=executor.controller_poll_seconds,
                ),
                workdir=planned_job.workdir,
                logdir=planned_job.logdir,
                dependencies=list(planned_job.dependencies),
                metadata=dict(planned_job.metadata),
                backend_options={
                    "label": planned_job.label,
                    "remote_dest_dir": planned_job.workdir,
                },
            )

        resource_metadata = dict((planned_job.resources.metadata if planned_job.resources else {}) or {})
        return JobSpec(
            job_id=planned_job.job_id,
            run_id=run_spec.run_id,
            name=planned_job.name,
            kind="step",
            script_text=executor._render_batch_script(
                run_spec.run_id,
                paths.checkout_root,
                paths.pipeline_path,
                planned_job.steps,
                planned_job.step_indices,
                paths.context_file,
                paths.workdir,
                paths.plugins_dir,
                planned_job.logdir or paths.base_logdir,
                paths.venv_path,
                paths.requirements_path,
                paths.python_bin,
                project_id=run_spec.project_id,
                resume_run_id=resume_run_id,
                run_started_at=run_started_at,
                global_config_path=paths.global_config_path,
                projects_config_path=paths.projects_config_path,
                environments_config_path=paths.environments_config_path,
                commandline_vars=commandline_vars,
                child_jobs_file=paths.child_jobs_file,
                sbatch_time=resource_metadata.get("time"),
                sbatch_cpus_per_task=resource_metadata.get("cpus_per_task"),
                sbatch_mem=resource_metadata.get("mem"),
                array_index=bool(planned_job.metadata.get("array_index", False)),
                array_count=planned_job.metadata.get("array_count"),
                array_max_parallel=planned_job.metadata.get("array_max_parallel"),
                foreach_from_array=bool(planned_job.metadata.get("foreach_from_array", False)),
                foreach_item_offset=int(planned_job.metadata.get("foreach_item_offset", 0) or 0),
            ),
            workdir=planned_job.workdir,
            logdir=planned_job.logdir,
            dependencies=list(planned_job.dependencies),
            resources=planned_job.resources or JobResources(metadata={}),
            step_indices=list(planned_job.step_indices),
            metadata=dict(planned_job.metadata),
            backend_options={
                "label": planned_job.label,
                "remote_dest_dir": planned_job.workdir,
                "array_bounds": planned_job.metadata.get("array_bounds"),
            },
        )

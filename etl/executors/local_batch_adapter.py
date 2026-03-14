from __future__ import annotations

import json
import sys
from typing import Any, Optional

from ..job_planning import RunJobPlanner
from ..job_specs import JobResources, JobSpec, PlannedJob, RunSpec
from ..provisioners import ResourceRequest, WorkloadSpec


def _flatten_vars_for_cli(values: dict[str, Any], prefix: str = "") -> list[tuple[str, str]]:
    items: list[tuple[str, str]] = []
    for key in sorted(values.keys()):
        k = str(key).strip()
        if not k:
            continue
        full_key = f"{prefix}.{k}" if prefix else k
        raw = values.get(key)
        if isinstance(raw, dict):
            items.extend(_flatten_vars_for_cli(raw, prefix=full_key))
            continue
        items.append((full_key, "" if raw is None else str(raw)))
    return items


class LocalBatchAdapter:
    def __init__(
        self,
        *,
        array_task_limit: int,
        enable_controller_job: bool = False,
        foreach_count_fn,
        foreach_max_concurrency_fn,
        resolve_batch_resources_fn,
        python_bin: Optional[str] = None,
        verbose: bool = False,
    ) -> None:
        self.array_task_limit = int(array_task_limit)
        self.enable_controller_job = bool(enable_controller_job)
        self.foreach_count_fn = foreach_count_fn
        self.foreach_max_concurrency_fn = foreach_max_concurrency_fn
        self.resolve_batch_resources_fn = resolve_batch_resources_fn
        self.python_bin = str(python_bin or sys.executable or "python").strip() or "python"
        self.verbose = bool(verbose)

    def plan(self, run_spec: RunSpec) -> list[PlannedJob]:
        return RunJobPlanner(
            array_task_limit=self.array_task_limit,
            enable_controller_job=self.enable_controller_job,
            controller_logdir_kind="setup",
            foreach_count_fn=self.foreach_count_fn,
            foreach_max_concurrency_fn=self.foreach_max_concurrency_fn,
            resolve_batch_resources_fn=self.resolve_batch_resources_fn,
        ).build(run_spec)

    def build_job_specs(self, run_spec: RunSpec, planned_jobs: Optional[list[PlannedJob]] = None) -> list[JobSpec]:
        if not run_spec.paths:
            raise RuntimeError("RunSpec.paths is required to build local batch job specs")
        planned = planned_jobs or self.plan(run_spec)
        all_workdirs = [p.workdir for p in planned if p.workdir]
        all_logdirs = [p.logdir for p in planned if p.logdir]
        specs: list[JobSpec] = []
        last_concrete_by_logical: dict[str, str] = {}
        for planned_job in planned:
            concrete = self._materialize_planned_job(
                run_spec,
                planned_job,
                last_concrete_by_logical,
                all_workdirs=all_workdirs,
                all_logdirs=all_logdirs,
            )
            if not concrete:
                continue
            specs.extend(concrete)
            last_concrete_by_logical[planned_job.job_id] = concrete[-1].job_id
        return specs

    def to_workload(self, job_spec: JobSpec) -> WorkloadSpec:
        return WorkloadSpec(
            name=job_spec.name,
            command=list(job_spec.command) if job_spec.command else None,
            script_text=job_spec.script_text,
            cwd=job_spec.cwd,
            env=dict(job_spec.env),
            resources=self._to_resource_request(job_spec.resources),
            metadata=dict(job_spec.metadata),
            backend_options={
                **dict(job_spec.backend_options),
                "dependencies": list(job_spec.dependencies),
            },
        )

    def _materialize_planned_job(
        self,
        run_spec: RunSpec,
        planned_job: PlannedJob,
        last_concrete_by_logical: dict[str, str],
        *,
        all_workdirs: list[str],
        all_logdirs: list[str],
    ) -> list[JobSpec]:
        if not run_spec.paths:
            raise RuntimeError("RunSpec.paths is required to build local batch jobs")
        deps = [last_concrete_by_logical[d] for d in planned_job.dependencies if d in last_concrete_by_logical]
        if planned_job.kind == "setup":
            return [self._build_setup_job(run_spec, planned_job, deps, all_workdirs=all_workdirs, all_logdirs=all_logdirs)]
        if planned_job.kind == "controller":
            return [self._build_controller_job(run_spec, planned_job, deps)]
        if bool(planned_job.metadata.get("array_index", False)):
            return self._build_array_step_jobs(run_spec, planned_job, deps)
        return [self._build_step_job(run_spec, planned_job, deps)]

    def _build_setup_job(
        self,
        run_spec: RunSpec,
        planned_job: PlannedJob,
        deps: list[str],
        *,
        all_workdirs: list[str],
        all_logdirs: list[str],
    ) -> JobSpec:
        paths = run_spec.paths
        assert paths is not None
        setup_payload = {
            "dirs": list(
                dict.fromkeys(
                    [
                        paths.workdir,
                        paths.base_logdir,
                        paths.setup_logdir,
                        *paths.workdirs_to_create,
                        *paths.logdirs_to_create,
                        *all_workdirs,
                        *all_logdirs,
                    ]
                )
            ),
            "context_file": paths.context_file,
            "child_jobs_file": paths.child_jobs_file,
        }
        command = [
            self.python_bin,
            "-c",
            (
                "import json; from pathlib import Path; "
                f"payload=json.loads({json.dumps(json.dumps(setup_payload))}); "
                "[Path(p).mkdir(parents=True, exist_ok=True) for p in payload['dirs'] if p]; "
                "cf=Path(payload['context_file']); cf.parent.mkdir(parents=True, exist_ok=True); "
                "cf.write_text(cf.read_text(encoding='utf-8') if cf.exists() else '{}', encoding='utf-8'); "
                "jf=Path(payload['child_jobs_file']); jf.parent.mkdir(parents=True, exist_ok=True); jf.touch(); "
                "print('setup complete')"
            ),
        ]
        return JobSpec(
            job_id=planned_job.job_id,
            run_id=run_spec.run_id,
            name=planned_job.name,
            kind="setup",
            command=command,
            workdir=planned_job.workdir,
            cwd=None,
            logdir=planned_job.logdir,
            dependencies=list(deps),
            metadata=dict(planned_job.metadata),
            backend_options={"label": planned_job.label},
        )

    def _build_controller_job(self, run_spec: RunSpec, planned_job: PlannedJob, deps: list[str]) -> JobSpec:
        command = [self.python_bin, "-c", "print('controller complete')"]
        return JobSpec(
            job_id=planned_job.job_id,
            run_id=run_spec.run_id,
            name=planned_job.name,
            kind="controller",
            command=command,
            workdir=planned_job.workdir,
            cwd=planned_job.workdir,
            logdir=planned_job.logdir,
            dependencies=list(deps),
            metadata=dict(planned_job.metadata),
            backend_options={"label": planned_job.label},
        )

    def _build_array_step_jobs(self, run_spec: RunSpec, planned_job: PlannedJob, deps: list[str]) -> list[JobSpec]:
        array_count = int(planned_job.metadata.get("array_count") or 0)
        foreach_from_array = bool(planned_job.metadata.get("foreach_from_array", False))
        foreach_offset = int(planned_job.metadata.get("foreach_item_offset") or 0)
        step_indices = list(planned_job.step_indices)
        specs: list[JobSpec] = []
        prev_deps = list(deps)
        if foreach_from_array:
            for i in range(array_count):
                foreach_index = foreach_offset + i
                label = f"{planned_job.label}_task{foreach_index}"
                spec = self._build_step_job(
                    run_spec,
                    planned_job,
                    prev_deps,
                    job_id=f"{planned_job.job_id}:task{foreach_index}",
                    name=f"{planned_job.name}-task{foreach_index}",
                    label=label,
                    step_indices=step_indices,
                    foreach_item_index=foreach_index,
                )
                specs.append(spec)
                prev_deps = [spec.job_id]
            return specs
        for idx in step_indices:
            label = f"{planned_job.label}_task{idx}"
            spec = self._build_step_job(
                run_spec,
                planned_job,
                prev_deps,
                job_id=f"{planned_job.job_id}:task{idx}",
                name=f"{planned_job.name}-task{idx}",
                label=label,
                step_indices=[idx],
            )
            specs.append(spec)
            prev_deps = [spec.job_id]
        return specs

    def _build_step_job(
        self,
        run_spec: RunSpec,
        planned_job: PlannedJob,
        deps: list[str],
        *,
        job_id: Optional[str] = None,
        name: Optional[str] = None,
        label: Optional[str] = None,
        step_indices: Optional[list[int]] = None,
        foreach_item_index: Optional[int] = None,
    ) -> JobSpec:
        paths = run_spec.paths
        assert paths is not None
        argv = [
            self.python_bin,
            "-m",
            "etl.run_batch",
            paths.pipeline_path,
            "--steps",
            ",".join(str(i) for i in (step_indices or planned_job.step_indices)),
            "--plugins-dir",
            paths.plugins_dir,
            "--workdir",
            planned_job.workdir or paths.workdir,
            "--context-file",
            paths.context_file,
            "--run-id",
            run_spec.run_id,
            "--executor-type",
            "local",
            "--tracking-executor",
            "local_batch",
            "--max-retries",
            str(run_spec.selection.max_retries),
            "--retry-delay-seconds",
            str(run_spec.selection.retry_delay_seconds),
        ]
        if run_spec.project_id:
            argv += ["--project-id", str(run_spec.project_id)]
        if run_spec.selection.resume_run_id:
            argv += ["--resume-run-id", str(run_spec.selection.resume_run_id)]
        if run_spec.selection.run_started_at:
            argv += ["--run-started-at", str(run_spec.selection.run_started_at)]
        if paths.global_config_path:
            argv += ["--global-config", paths.global_config_path]
        if paths.projects_config_path:
            argv += ["--projects-config", paths.projects_config_path]
        if paths.environments_config_path and run_spec.execution_env_name:
            argv += ["--environments-config", paths.environments_config_path, "--env", run_spec.execution_env_name]
        if foreach_item_index is not None:
            argv += ["--foreach-item-index", str(foreach_item_index)]
        for key, value in _flatten_vars_for_cli(dict(run_spec.commandline_vars or {})):
            argv += ["--var", f"{key}={value}"]
        if self.verbose or run_spec.selection.verbose:
            argv += ["--verbose"]
        return JobSpec(
            job_id=job_id or planned_job.job_id,
            run_id=run_spec.run_id,
            name=name or planned_job.name,
            kind="step",
            command=argv,
            workdir=planned_job.workdir,
            cwd=planned_job.workdir,
            logdir=planned_job.logdir,
            dependencies=list(deps),
            resources=planned_job.resources or JobResources(metadata={}),
            step_indices=list(step_indices or planned_job.step_indices),
            metadata={
                **dict(planned_job.metadata),
                **({"foreach_item_index": foreach_item_index} if foreach_item_index is not None else {}),
            },
            backend_options={"label": label or planned_job.label},
        )

    @staticmethod
    def _to_resource_request(resources: Optional[JobResources]) -> Optional[ResourceRequest]:
        if resources is None:
            return None
        return ResourceRequest(
            cpu_cores=resources.cpu_cores,
            memory_gb=resources.memory_gb,
            wall_minutes=resources.wall_minutes,
            metadata=dict(resources.metadata),
        )

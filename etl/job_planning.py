from __future__ import annotations

from pathlib import Path
from typing import Any, Optional

from .job_specs import JobResources, PlannedJob, RunSpec


class RunJobPlanner:
    def __init__(
        self,
        *,
        array_task_limit: int,
        enable_controller_job: bool = False,
        controller_logdir_kind: str = "setup",
        foreach_count_fn,
        foreach_max_concurrency_fn,
        resolve_batch_resources_fn,
    ) -> None:
        self.array_task_limit = int(array_task_limit)
        self.enable_controller_job = bool(enable_controller_job)
        self.controller_logdir_kind = str(controller_logdir_kind or "setup")
        self.foreach_count_fn = foreach_count_fn
        self.foreach_max_concurrency_fn = foreach_max_concurrency_fn
        self.resolve_batch_resources_fn = resolve_batch_resources_fn

    def build(self, run_spec: RunSpec) -> list[PlannedJob]:
        if not run_spec.paths:
            raise RuntimeError("RunSpec.paths is required to plan jobs")

        pipeline = run_spec.pipeline
        batches = run_spec.batches
        run_id = run_spec.run_id
        paths = run_spec.paths
        remote_workdir_root = Path(paths.workdir)
        base_logdir = Path(paths.base_logdir)
        setup_logdir = paths.setup_logdir

        jobs: list[PlannedJob] = []
        prev_job_id: Optional[str] = None

        setup_job = PlannedJob(
            job_id="setup",
            run_id=run_id,
            name=f"etl-{run_id}-setup",
            kind="setup",
            label="setup",
            workdir=paths.workdir,
            logdir=setup_logdir,
        )
        jobs.append(setup_job)
        prev_job_id = setup_job.job_id

        for batch_idx, batch in enumerate(batches):
            if len(batch) == 1:
                step_idx, step = batch[0]
                step_indices = [step_idx]
                steps = [step]
                step_name = getattr(step, "name", f"step{step_idx}")
                foreach_count = self.foreach_count_fn(step, pipeline)
                if foreach_count and foreach_count > 1:
                    foreach_array_max_parallel = self.foreach_max_concurrency_fn(step)
                    chunk_size = min(self.array_task_limit, int(foreach_count))
                    start = 0
                    while start < int(foreach_count):
                        chunk_n = min(chunk_size, int(foreach_count) - start)
                        jobs.append(
                            self._planned_step_job(
                                run_spec,
                                job_id=f"step:{step_name}_foreach_chunk{start}",
                                label=f"{step_name}_foreach_chunk{start}",
                                prev_job_id=prev_job_id,
                                workdir=(remote_workdir_root / step_name).as_posix(),
                                logdir=(base_logdir / step_name).as_posix(),
                                step_indices=step_indices,
                                steps=steps,
                                array_bounds=(0, chunk_n - 1),
                                array_index=True,
                                array_count=chunk_n,
                                array_max_parallel=foreach_array_max_parallel,
                                foreach_from_array=True,
                                foreach_item_offset=start,
                            )
                        )
                        prev_job_id = jobs[-1].job_id
                        start += chunk_size
                    continue

                jobs.append(
                    self._planned_step_job(
                        run_spec,
                        job_id=f"step:{step_name}",
                        label=step_name,
                        prev_job_id=prev_job_id,
                        workdir=(remote_workdir_root / step_name).as_posix(),
                        logdir=(base_logdir / step_name).as_posix(),
                        step_indices=step_indices,
                        steps=steps,
                        array_index=False,
                    )
                )
                prev_job_id = jobs[-1].job_id
                continue

            chunk_size = min(self.array_task_limit, len(batch))
            start = 0
            while start < len(batch):
                chunk = batch[start : start + chunk_size]
                chunk_steps = [s for _, s in chunk]
                chunk_indices = [idx for idx, _ in chunk]
                first_name = getattr(chunk_steps[0], "name", f"step{chunk_indices[0]}")
                label = f"{first_name}_array{batch_idx}_chunk{start}"
                jobs.append(
                    self._planned_step_job(
                        run_spec,
                        job_id=f"step:{label}",
                        label=label,
                        prev_job_id=prev_job_id,
                        workdir=(remote_workdir_root / label).as_posix(),
                        logdir=(base_logdir / label).as_posix(),
                        step_indices=chunk_indices,
                        steps=chunk_steps,
                        array_bounds=(0, len(chunk) - 1),
                        array_index=True,
                    )
                )
                prev_job_id = jobs[-1].job_id
                start += chunk_size

        if self.enable_controller_job:
            controller_logdir = setup_logdir if self.controller_logdir_kind == "setup" else paths.base_logdir
            jobs.append(
                PlannedJob(
                    job_id="controller",
                    run_id=run_id,
                    name=f"etl-{run_id}-controller",
                    kind="controller",
                    label="controller",
                    workdir=paths.workdir,
                    logdir=controller_logdir,
                    dependencies=[prev_job_id] if prev_job_id else [],
                )
            )

        return jobs

    def _planned_step_job(
        self,
        run_spec: RunSpec,
        *,
        job_id: str,
        label: str,
        prev_job_id: Optional[str],
        workdir: str,
        logdir: str,
        step_indices: list[int],
        steps: list[Any],
        array_bounds: Optional[tuple[int, int]] = None,
        array_index: bool = False,
        array_count: Optional[int] = None,
        array_max_parallel: Optional[int] = None,
        foreach_from_array: bool = False,
        foreach_item_offset: int = 0,
    ) -> PlannedJob:
        batch_resources = self.resolve_batch_resources_fn(steps)
        return PlannedJob(
            job_id=job_id,
            run_id=run_spec.run_id,
            name=f"etl-{run_spec.run_id}-{label}",
            kind="step",
            label=label,
            workdir=workdir,
            logdir=logdir,
            dependencies=[prev_job_id] if prev_job_id else [],
            resources=JobResources(
                metadata={
                    "time": batch_resources.get("time"),
                    "cpus_per_task": batch_resources.get("cpus_per_task"),
                    "mem": batch_resources.get("mem"),
                }
            ),
            step_indices=list(step_indices),
            steps=list(steps),
            metadata={
                "step_names": [getattr(step, "name", None) for step in steps],
                "array_bounds": array_bounds,
                "array_index": bool(array_index),
                "array_count": array_count,
                "array_max_parallel": array_max_parallel,
                "foreach_from_array": bool(foreach_from_array),
                "foreach_item_offset": foreach_item_offset,
            },
        )

from __future__ import annotations

from datetime import datetime

from etl.job_planning import RunJobPlanner
from etl.job_specs import ResolvedPaths, RunSelection, RunSpec, SourceRef
from etl.pipeline import Pipeline, Step


def _sample_run_spec() -> RunSpec:
    pipeline = Pipeline(
        vars={"jobname": "sample"},
        steps=[
            Step(name="s1", script="echo.py"),
            Step(name="p1", script="echo.py", parallel_with="grp"),
            Step(name="p2", script="echo.py", parallel_with="grp"),
            Step(name="fan", script="echo.py", foreach="items"),
        ],
    )
    return RunSpec(
        run_id="runabc1234",
        project_id="proj",
        pipeline_path_input="pipelines/sample.yml",
        pipeline_name="sample",
        job_name="sample",
        source_repo_root="/repo",
        created_at=datetime.utcnow(),
        run_date="260313",
        run_stamp="101010",
        run_fs_id="101010-runabc12",
        pipeline=pipeline,
        batches=[
            [(0, pipeline.steps[0])],
            [(1, pipeline.steps[1]), (2, pipeline.steps[2])],
            [(3, pipeline.steps[3])],
        ],
        etl_source=SourceRef(provider="git", mode="workspace"),
        paths=ResolvedPaths(
            workdir="/tmp/work/sample/260313/101010-runabc12",
            base_logdir="/tmp/work/sample/260313/101010-runabc12/logs",
            setup_logdir="/tmp/work/sample/260313/101010-runabc12/logs/setup",
            context_file="/tmp/work/sample/260313/101010-runabc12/context.json",
            child_jobs_file="/tmp/work/sample/260313/101010-runabc12/child_jobs.txt",
            checkout_root="/tmp/work/_code/repo",
            pipeline_path="/tmp/work/_code/repo/pipelines/sample.yml",
            plugins_dir="/tmp/work/_code/repo/plugins",
            venv_path="/tmp/work/_code/repo/.venv",
            requirements_path="/tmp/work/_code/repo/requirements.txt",
            python_bin="python3",
        ),
        selection=RunSelection(),
    )


def test_run_job_planner_builds_setup_step_array_foreach_and_controller() -> None:
    run_spec = _sample_run_spec()

    planner = RunJobPlanner(
        array_task_limit=10,
        enable_controller_job=True,
        foreach_count_fn=lambda step, _pipeline: 3 if getattr(step, "name", "") == "fan" else None,
        foreach_max_concurrency_fn=lambda _step: 2,
        resolve_batch_resources_fn=lambda steps: {
            "time": "00:10:00",
            "cpus_per_task": 2,
            "mem": "4G",
        },
    )
    jobs = planner.build(run_spec)

    assert [job.kind for job in jobs] == ["setup", "step", "step", "step", "controller"]
    assert jobs[1].label == "s1"
    assert jobs[1].dependencies == ["setup"]
    assert jobs[2].label == "p1_array1_chunk0"
    assert jobs[2].metadata["array_bounds"] == (0, 1)
    assert jobs[3].label == "fan_foreach_chunk0"
    assert jobs[3].metadata["array_count"] == 3
    assert jobs[3].metadata["array_max_parallel"] == 2
    assert jobs[4].dependencies == [jobs[3].job_id]

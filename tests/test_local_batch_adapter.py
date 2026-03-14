from __future__ import annotations

from datetime import datetime

from etl.executors.local_batch_adapter import LocalBatchAdapter
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
            python_bin="python",
            global_config_path="/tmp/work/_code/repo/config/global.yml",
            projects_config_path="/tmp/work/_code/repo/config/projects.yml",
            environments_config_path="/tmp/work/_code/repo/config/environments.yml",
        ),
        selection=RunSelection(
            resume_run_id="olderrun",
            run_started_at="2026-03-13T10:10:10Z",
            max_retries=2,
            retry_delay_seconds=1.5,
            verbose=True,
        ),
        execution_env_name="local_windows",
        commandline_vars={"source": {"pipeline": {"revision": "feature/test"}}},
    )


def test_local_batch_adapter_expands_planned_arrays_into_concrete_jobs() -> None:
    run_spec = _sample_run_spec()
    adapter = LocalBatchAdapter(
        array_task_limit=10,
        enable_controller_job=True,
        foreach_count_fn=lambda step, _pipeline: 3 if getattr(step, "name", "") == "fan" else None,
        foreach_max_concurrency_fn=lambda _step: 2,
        resolve_batch_resources_fn=lambda _steps: {
            "time": "00:10:00",
            "cpus_per_task": 2,
            "mem": "4G",
        },
        python_bin="python",
        verbose=True,
    )

    specs = adapter.build_job_specs(run_spec)

    assert [spec.kind for spec in specs] == ["setup", "step", "step", "step", "step", "step", "step", "controller"]
    assert specs[1].dependencies == ["setup"]
    assert "--executor-type" in (specs[1].command or [])
    assert "local" in (specs[1].command or [])
    assert "--resume-run-id" in (specs[1].command or [])
    assert "--run-started-at" in (specs[1].command or [])
    assert "--var" in (specs[1].command or [])
    assert specs[2].step_indices == [1]
    assert specs[3].step_indices == [2]
    assert specs[4].metadata["foreach_item_index"] == 0
    assert specs[5].metadata["foreach_item_index"] == 1
    assert specs[6].metadata["foreach_item_index"] == 2
    assert specs[7].kind == "controller"
    assert specs[7].dependencies == [specs[6].job_id]


def test_local_batch_adapter_converts_job_spec_to_workload() -> None:
    run_spec = _sample_run_spec()
    adapter = LocalBatchAdapter(
        array_task_limit=10,
        enable_controller_job=False,
        foreach_count_fn=lambda _step, _pipeline: None,
        foreach_max_concurrency_fn=lambda _step: None,
        resolve_batch_resources_fn=lambda _steps: {
            "time": "00:10:00",
            "cpus_per_task": 2,
            "mem": "4G",
        },
        python_bin="python",
    )

    specs = adapter.build_job_specs(run_spec)
    workload = adapter.to_workload(specs[1])

    assert workload.command is not None
    assert workload.command[:3] == ["python", "-m", "etl.run_batch"]
    assert workload.cwd == specs[1].workdir
    assert workload.backend_options["dependencies"] == ["setup"]

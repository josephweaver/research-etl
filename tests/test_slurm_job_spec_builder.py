from __future__ import annotations

from pathlib import Path

from etl.git_checkout import GitExecutionSpec
from etl.executors.slurm.executor import SlurmExecutor
from etl.executors.slurm.job_spec_builder import SlurmJobSpecBuilder
from etl.executors.slurm.run_spec_builder import SlurmRunSpecBuilder
from etl.pipeline import Pipeline, Step


def test_slurm_job_spec_builder_creates_setup_step_and_controller_jobs(monkeypatch, tmp_path: Path) -> None:
    pipeline = Pipeline(
        vars={"jobname": "sample"},
        steps=[
            Step(name="setup", script="echo.py"),
            Step(name="p1", script="echo.py", parallel_with="grp"),
            Step(name="p2", script="echo.py", parallel_with="grp"),
        ],
    )
    monkeypatch.setattr(
        "etl.executors.slurm.run_spec_builder.parse_pipeline",
        lambda *_args, **_kwargs: pipeline,
    )

    ex = SlurmExecutor(
        {
            "workdir": "/tmp/work",
            "logdir": "/tmp/logs",
            "enable_controller_job": True,
        },
        repo_root=tmp_path,
        plugins_dir=Path("plugins"),
        dry_run=False,
    )

    run_spec = SlurmRunSpecBuilder(ex).build("pipelines/sample.yml", {"run_id": "runabc1234"})
    job_specs = SlurmJobSpecBuilder(ex).build(run_spec)

    assert [spec.kind for spec in job_specs] == ["setup", "step", "step", "controller"]
    assert job_specs[0].job_id == "setup"
    assert job_specs[1].dependencies == ["setup"]
    assert job_specs[2].dependencies == [job_specs[1].job_id]
    assert job_specs[3].dependencies == [job_specs[2].job_id]
    assert job_specs[2].backend_options["array_bounds"] == (0, 1)
    assert "#SBATCH --array=0-1" in (job_specs[2].script_text or "")


def test_slurm_job_spec_builder_propagates_staged_source_paths(monkeypatch, tmp_path: Path) -> None:
    pipeline = Pipeline(
        vars={"jobname": "sample"},
        steps=[Step(name="s1", script="echo.py")],
    )
    monkeypatch.setattr(
        "etl.executors.slurm.run_spec_builder.parse_pipeline",
        lambda *_args, **_kwargs: pipeline,
    )

    ex = SlurmExecutor(
        {"workdir": "/tmp/work", "logdir": "/tmp/logs"},
        repo_root=tmp_path,
        plugins_dir=Path("plugins"),
        dry_run=False,
        enforce_git_checkout=True,
        require_clean_git=False,
    )
    monkeypatch.setattr(
        "etl.executors.slurm.run_spec_builder._SOURCE_PROVIDER.resolve_execution_spec",
        lambda **_kwargs: GitExecutionSpec(
            commit_sha="abc123def4567890",
            origin_url="git@github.com:org/research-etl.git",
            repo_name="research-etl",
            git_is_dirty=False,
        ),
    )

    run_spec = SlurmRunSpecBuilder(ex).build(
        "pipelines/sample.yml",
        {
            "run_id": "runabc1234",
            "execution_source": "git_bundle",
            "source_bundle": "/tmp/local/source.bundle",
        },
    )
    job_specs = SlurmJobSpecBuilder(ex).build(
        run_spec,
        source_bundle_path="/remote/work/source/source.bundle",
    )

    setup_script = job_specs[0].script_text or ""
    assert "SOURCE_BUNDLE=/remote/work/source/source.bundle" in setup_script

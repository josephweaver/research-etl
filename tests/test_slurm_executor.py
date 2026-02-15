from __future__ import annotations

from pathlib import Path

import etl.executors.slurm as slurm_mod
from etl.executors.slurm import SlurmExecutor
from etl.pipeline import Pipeline, Step
from etl.git_checkout import GitExecutionSpec


def test_slurm_submit_uses_array_batches_and_passes_resume_retry(monkeypatch, tmp_path: Path) -> None:
    pipeline = Pipeline(
        vars={"items": ["a", "b"]},
        steps=[
            Step(name="setup", script="echo.py"),
            Step(name="p1", script="echo.py", parallel_with="grp"),
            Step(name="p2", script="echo.py", parallel_with="grp"),
            Step(name="fan", script="echo.py", foreach="items"),
        ],
    )
    monkeypatch.setattr(slurm_mod, "parse_pipeline", lambda *_args, **_kwargs: pipeline)
    monkeypatch.setattr(slurm_mod, "upsert_run_status", lambda **_: None)

    calls = []

    def _fake_submit_script(
        self,
        script_text,
        run_id,
        label="job",
        prev_dependency=None,
        array_bounds=None,
        remote_dest_dir=None,
    ):
        jobid = f"job{len(calls) + 1}"
        calls.append(
            {
                "jobid": jobid,
                "label": label,
                "prev_dependency": prev_dependency,
                "array_bounds": array_bounds,
                "remote_dest_dir": remote_dest_dir,
                "script_text": script_text,
            }
        )
        return jobid

    monkeypatch.setattr(SlurmExecutor, "_submit_script", _fake_submit_script)

    ex = SlurmExecutor(
        {
            "workdir": "/tmp/work",
            "logdir": "/tmp/logs",
            "step_max_retries": 2,
            "step_retry_delay_seconds": 1.5,
            "array_task_limit": 10,
        },
        repo_root=tmp_path,
        plugins_dir=Path("plugins"),
        dry_run=False,
    )
    res = ex.submit(
        "pipelines/sample_parallel.yml",
        {"run_id": "runabc1234", "resume_run_id": "olderrun"},
    )

    assert len(calls) == 4
    assert res.job_ids == ["job1", "job2", "job3", "job4"]

    # setup has no dependency; all later jobs chain after previous job.
    assert calls[0]["label"] == "setup"
    assert calls[0]["prev_dependency"] is None
    assert calls[1]["prev_dependency"] == "job1"
    assert calls[2]["prev_dependency"] == "job2"
    assert calls[3]["prev_dependency"] == "job3"

    # parallel batch of p1/p2 is submitted as an array script.
    array_call = calls[2]
    assert array_call["label"].startswith("p1_array")
    assert "#SBATCH --array=0-1" in array_call["script_text"]
    assert "--steps ${step_indices[$SLURM_ARRAY_TASK_ID]}" in array_call["script_text"]

    # Retry/resume flags are carried into batch scripts.
    for idx in (1, 2, 3):
        script = calls[idx]["script_text"]
        assert "--resume-run-id olderrun" in script
        assert "--max-retries 2" in script
        assert "--retry-delay-seconds 1.5" in script

    # foreach remains a single pipeline step index here; fan-out happens in run_batch.
    assert "--steps 3" in calls[3]["script_text"]


def test_slurm_submit_applies_step_resource_hints_and_env_caps(monkeypatch, tmp_path: Path) -> None:
    plugins_dir = tmp_path / "plugins"
    plugins_dir.mkdir(parents=True, exist_ok=True)
    (plugins_dir / "heavy.py").write_text(
        "\n".join(
            [
                "meta = {",
                "  'name': 'heavy',",
                "  'version': '1.2.3',",
                "  'description': 'heavy test',",
                "  'resources': {'cpu_cores': 32, 'memory_gb': 96, 'wall_minutes': 720},",
                "}",
                "def run(args, ctx):",
                "  return {'ok': True}",
            ]
        ),
        encoding="utf-8",
    )

    pipeline = Pipeline(
        steps=[
            Step(
                name="heavy_step",
                script="heavy.py",
                resources={"cpu_cores": 48, "memory_gb": 140, "wall_minutes": 6000},
            )
        ],
    )
    monkeypatch.setattr(slurm_mod, "parse_pipeline", lambda *_args, **_kwargs: pipeline)
    monkeypatch.setattr(slurm_mod, "upsert_run_status", lambda **_: None)

    calls = []

    def _fake_submit_script(
        self,
        script_text,
        run_id,
        label="job",
        prev_dependency=None,
        array_bounds=None,
        remote_dest_dir=None,
    ):
        calls.append({"label": label, "script_text": script_text})
        return f"job{len(calls)}"

    monkeypatch.setattr(SlurmExecutor, "_submit_script", _fake_submit_script)

    ex = SlurmExecutor(
        {
            "workdir": "/tmp/work",
            "logdir": "/tmp/logs",
            "time": "72:00:00",
            "cpus_per_task": 4,
            "mem": "8G",
            "max_time": "10:00:00",
            "max_cpus_per_task": 20,
            "max_mem": "64G",
        },
        repo_root=tmp_path,
        plugins_dir=plugins_dir,
        dry_run=False,
    )
    ex.submit("pipelines/sample.yml", {"run_id": "runabc1234"})

    # call[0]=setup, call[1]=batch
    assert len(calls) == 2
    batch_script = calls[1]["script_text"]
    assert "#SBATCH -c 20" in batch_script
    assert "#SBATCH --mem=64G" in batch_script
    assert "#SBATCH -t 10:00:00" in batch_script


def test_slurm_submit_uses_db_metrics_with_low_sample_1_5x(monkeypatch, tmp_path: Path) -> None:
    plugins_dir = tmp_path / "plugins"
    plugins_dir.mkdir(parents=True, exist_ok=True)
    (plugins_dir / "smart.py").write_text(
        "\n".join(
            [
                "meta = {",
                "  'name': 'smart',",
                "  'version': '2.0.0',",
                "  'description': 'smart test',",
                "}",
                "def run(args, ctx):",
                "  return {'ok': True}",
            ]
        ),
        encoding="utf-8",
    )
    pipeline = Pipeline(steps=[Step(name="smart_step", script="smart.py")])
    monkeypatch.setattr(slurm_mod, "parse_pipeline", lambda *_args, **_kwargs: pipeline)
    monkeypatch.setattr(slurm_mod, "upsert_run_status", lambda **_: None)
    monkeypatch.setattr(
        slurm_mod,
        "fetch_plugin_resource_stats",
        lambda **_k: {
            "samples": 3,
            "wall_minutes_mean": 20.0,
            "wall_minutes_std": 4.0,
            "wall_minutes_samples": 3,
            "memory_gb_mean": 10.0,
            "memory_gb_std": 2.0,
            "memory_gb_samples": 3,
            "cpu_cores_mean": 2.0,
            "cpu_cores_std": 1.0,
            "cpu_cores_samples": 3,
        },
    )

    calls = []

    def _fake_submit_script(
        self,
        script_text,
        run_id,
        label="job",
        prev_dependency=None,
        array_bounds=None,
        remote_dest_dir=None,
    ):
        calls.append({"label": label, "script_text": script_text})
        return f"job{len(calls)}"

    monkeypatch.setattr(SlurmExecutor, "_submit_script", _fake_submit_script)

    ex = SlurmExecutor(
        {
            "workdir": "/tmp/work",
            "logdir": "/tmp/logs",
            "time": "00:05:00",
            "cpus_per_task": 1,
            "mem": "2G",
            "resource_low_sample_multiplier": 1.5,
        },
        repo_root=tmp_path,
        plugins_dir=plugins_dir,
        dry_run=False,
    )
    ex.submit("pipelines/sample.yml", {"run_id": "runabc1234"})
    batch_script = calls[1]["script_text"]
    assert "#SBATCH -t 00:30:00" in batch_script  # 20 * 1.5
    assert "#SBATCH -c 3" in batch_script  # ceil(2 * 1.5)
    assert "#SBATCH --mem=15G" in batch_script  # 10 * 1.5


def test_slurm_submit_prefers_execution_env_git_remote_url(monkeypatch, tmp_path: Path) -> None:
    pipeline = Pipeline(steps=[Step(name="s1", script="echo.py")])
    monkeypatch.setattr(slurm_mod, "parse_pipeline", lambda *_args, **_kwargs: pipeline)
    monkeypatch.setattr(slurm_mod, "upsert_run_status", lambda **_: None)
    monkeypatch.setattr(
        slurm_mod,
        "resolve_execution_spec",
        lambda **_: GitExecutionSpec(
            commit_sha="abc123def456",
            origin_url="https://github.com/org/repo.git",
            repo_name="repo",
            git_is_dirty=False,
        ),
    )

    calls = []

    def _fake_submit_script(
        self,
        script_text,
        run_id,
        label="job",
        prev_dependency=None,
        array_bounds=None,
        remote_dest_dir=None,
    ):
        calls.append({"label": label, "script_text": script_text})
        return f"job{len(calls)}"

    monkeypatch.setattr(SlurmExecutor, "_submit_script", _fake_submit_script)

    ex = SlurmExecutor(
        {"workdir": "/tmp/work", "logdir": "/tmp/logs", "git_remote_url": "git@github.com:wrong/wrong.git"},
        repo_root=tmp_path,
        plugins_dir=Path("plugins"),
        dry_run=False,
        enforce_git_checkout=True,
        require_clean_git=False,
    )
    ex.submit(
        "pipelines/sample.yml",
        {
            "run_id": "runabc1234",
            "execution_env": {"git_remote_url": "git@github.com:josephweaver/research-etl.git"},
        },
    )
    setup_script = calls[0]["script_text"]
    assert "REPO_URL=git@github.com:josephweaver/research-etl.git" in setup_script

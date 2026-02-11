from __future__ import annotations

from pathlib import Path

import etl.executors.slurm as slurm_mod
from etl.executors.slurm import SlurmExecutor
from etl.pipeline import Pipeline, Step


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

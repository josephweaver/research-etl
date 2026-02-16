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
        assert "--run-started-at" in script
        assert "RUN_STARTED_OPT" in script
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


def test_slurm_submit_script_skips_secret_propagation_when_disabled(monkeypatch, tmp_path: Path) -> None:
    ex = SlurmExecutor(
        {
            "ssh_host": "example.org",
            "ssh_user": "alice",
            "propagate_db_secret": False,
            "workdir": "/tmp/work",
            "logdir": "/tmp/logs",
        },
        repo_root=tmp_path,
        plugins_dir=Path("plugins"),
        dry_run=False,
    )

    called = {"secret": 0}

    def _fake_secret(_target: str) -> None:
        called["secret"] += 1

    class _P:
        def __init__(self, rc=0, out="Submitted batch job 123\n", err=""):
            self.returncode = rc
            self.stdout = out
            self.stderr = err

    monkeypatch.setattr(ex, "_ensure_remote_secrets_file", _fake_secret)
    monkeypatch.setattr(slurm_mod.subprocess, "run", lambda *a, **k: _P())

    jobid = ex._submit_script("#!/bin/bash\necho ok\n", run_id="r123", label="x")
    assert jobid == "123"
    assert called["secret"] == 0


def test_slurm_submit_script_retries_transient_remote_failures(monkeypatch, tmp_path: Path) -> None:
    ex = SlurmExecutor(
        {
            "ssh_host": "example.org",
            "ssh_user": "alice",
            "propagate_db_secret": False,
            "workdir": "/tmp/work",
            "logdir": "/tmp/logs",
            "ssh_retries": 2,
            "scp_retries": 2,
            "remote_retry_delay_seconds": 0.0,
        },
        repo_root=tmp_path,
        plugins_dir=Path("plugins"),
        dry_run=False,
    )

    calls = {"mkdir": 0}

    class _P:
        def __init__(self, rc=0, out="Submitted batch job 123\n", err=""):
            self.returncode = rc
            self.stdout = out
            self.stderr = err

    def _fake_run(cmd, **_kwargs):
        text = " ".join(str(x) for x in cmd)
        if "mkdir -p" in text and calls["mkdir"] == 0:
            calls["mkdir"] += 1
            return _P(rc=255, out="", err="ssh: connect to host example.org port 22: Connection timed out")
        return _P()

    monkeypatch.setattr(slurm_mod.subprocess, "run", _fake_run)

    jobid = ex._submit_script("#!/bin/bash\necho ok\n", run_id="r123", label="x")
    assert jobid == "123"
    assert calls["mkdir"] == 1


def test_slurm_remote_commands_use_noninteractive_ssh_options(monkeypatch, tmp_path: Path) -> None:
    ex = SlurmExecutor(
        {
            "ssh_host": "example.org",
            "ssh_user": "alice",
            "propagate_db_secret": False,
            "workdir": "/tmp/work",
            "logdir": "/tmp/logs",
            "ssh_connect_timeout": 17,
            "ssh_strict_host_key_checking": "accept-new",
        },
        repo_root=tmp_path,
        plugins_dir=Path("plugins"),
        dry_run=False,
    )

    observed_cmds = []

    class _P:
        def __init__(self, rc=0, out="Submitted batch job 123\n", err=""):
            self.returncode = rc
            self.stdout = out
            self.stderr = err

    def _fake_run(cmd, **_kwargs):
        observed_cmds.append([str(x) for x in cmd])
        return _P()

    monkeypatch.setattr(slurm_mod.subprocess, "run", _fake_run)
    ex._submit_script("#!/bin/bash\necho ok\n", run_id="r123", label="x")

    flat = " ".join(" ".join(c) for c in observed_cmds)
    assert "-o BatchMode=yes" in flat
    assert "-o ConnectTimeout=17" in flat
    assert "-o StrictHostKeyChecking=accept-new" in flat


def test_slurm_verbose_scripts_include_safe_step_logs(monkeypatch, tmp_path: Path) -> None:
    pipeline = Pipeline(steps=[Step(name="s1", script="echo.py")])
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
        {"workdir": "/tmp/work", "logdir": "/tmp/logs"},
        repo_root=tmp_path,
        plugins_dir=Path("plugins"),
        dry_run=False,
        verbose=True,
    )
    ex.submit("pipelines/sample.yml", {"run_id": "runabc1234"})

    setup_script = calls[0]["script_text"]
    batch_script = calls[1]["script_text"]
    assert "log_step(){" in setup_script
    assert "log_step(){" in batch_script
    assert "loading optional secrets file (values hidden)" in setup_script
    assert "loading optional secrets file (values hidden)" in batch_script
    assert "--verbose" in batch_script
    assert "ETL_DATABASE_URL=" not in setup_script
    assert "ETL_DATABASE_URL=" not in batch_script


def test_slurm_setup_time_defaults_to_ten_minutes(monkeypatch, tmp_path: Path) -> None:
    pipeline = Pipeline(steps=[Step(name="s1", script="echo.py")])
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
        {"workdir": "/tmp/work", "logdir": "/tmp/logs"},
        repo_root=tmp_path,
        plugins_dir=Path("plugins"),
        dry_run=False,
    )
    ex.submit("pipelines/sample.yml", {"run_id": "runabc1234"})

    setup_script = calls[0]["script_text"]
    assert "#SBATCH -t 00:10:00" in setup_script


def test_slurm_setup_time_can_be_overridden(monkeypatch, tmp_path: Path) -> None:
    pipeline = Pipeline(steps=[Step(name="s1", script="echo.py")])
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
        {"workdir": "/tmp/work", "logdir": "/tmp/logs", "setup_time": "00:06:00"},
        repo_root=tmp_path,
        plugins_dir=Path("plugins"),
        dry_run=False,
    )
    ex.submit("pipelines/sample.yml", {"run_id": "runabc1234"})

    setup_script = calls[0]["script_text"]
    assert "#SBATCH -t 00:06:00" in setup_script


def test_slurm_pipeline_logdir_overrides_env_logdir(monkeypatch, tmp_path: Path) -> None:
    pipeline = Pipeline(
        vars={"jobname": "tiger_state"},
        dirs={"logdir": "{workdir}/logs"},
        steps=[Step(name="s1", script="echo.py")],
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
        {"workdir": "/tmp/work", "logdir": "/tmp/env_logs"},
        repo_root=tmp_path,
        plugins_dir=Path("plugins"),
        dry_run=False,
    )
    ex.submit("pipelines/tiger/state.yml", {"run_id": "runabc1234"})

    setup_script = calls[0]["script_text"]
    batch_script = calls[1]["script_text"]
    assert "/tmp/env_logs" not in setup_script
    assert "/tmp/env_logs" not in batch_script
    assert "/logs/setup/" in setup_script
    assert "/logs/s1/" in batch_script
    assert "mkdir -p /tmp/work" in batch_script


def test_slurm_uses_pipeline_name_as_jobname_fallback(monkeypatch, tmp_path: Path) -> None:
    pipeline = Pipeline(
        vars={"name": "tiger_state"},
        steps=[Step(name="s1", script="echo.py")],
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
        calls.append({"label": label, "remote_dest_dir": remote_dest_dir, "script_text": script_text})
        return f"job{len(calls)}"

    monkeypatch.setattr(SlurmExecutor, "_submit_script", _fake_submit_script)

    ex = SlurmExecutor(
        {"workdir": "/tmp/work", "logdir": "/tmp/logs"},
        repo_root=tmp_path,
        plugins_dir=Path("plugins"),
        dry_run=False,
    )
    ex.submit("pipelines/tiger/state.yml", {"run_id": "runabc1234"})

    setup_call = calls[0]
    assert "/tmp/work/tiger_state/" in str(setup_call["remote_dest_dir"])


def test_slurm_can_disable_loading_remote_secrets_file(monkeypatch, tmp_path: Path) -> None:
    pipeline = Pipeline(steps=[Step(name="s1", script="echo.py")])
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
        {"workdir": "/tmp/work", "logdir": "/tmp/logs", "load_secrets_file": False},
        repo_root=tmp_path,
        plugins_dir=Path("plugins"),
        dry_run=False,
    )
    ex.submit("pipelines/sample.yml", {"run_id": "runabc1234"})

    setup_script = calls[0]["script_text"]
    batch_script = calls[1]["script_text"]
    assert "$HOME/.secrets/etl" not in setup_script
    assert "$HOME/.secrets/etl" not in batch_script


def test_slurm_workdir_prefers_pipeline_dirs_workdir_over_env_defaults(monkeypatch, tmp_path: Path) -> None:
    pipeline = Pipeline(
        vars={"name": "yanroy.extract_fields"},
        dirs={"workdir": "{env.workdir}/yanroy/fields/{sys.now.yymmdd}/{sys.now.hhmmss}-{sys.run.short_id}"},
        steps=[Step(name="s1", script="echo.py")],
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
        calls.append({"label": label, "remote_dest_dir": remote_dest_dir, "script_text": script_text})
        return f"job{len(calls)}"

    monkeypatch.setattr(SlurmExecutor, "_submit_script", _fake_submit_script)

    ex = SlurmExecutor(
        {"workdir": "/tmp/env_work", "logdir": "/tmp/logs"},
        repo_root=tmp_path,
        plugins_dir=Path("plugins"),
        dry_run=False,
    )
    ex.submit(
        "pipelines/yanroy/extract_fields.yml",
        {
            "run_id": "runabc1234",
            "execution_env": {"workdir": "/hpcc/work"},
            "global_vars": {"workdir": "/global/work"},
        },
    )

    setup_call = calls[0]
    # submit destination should be rooted in resolved pipeline dirs.workdir (env.workdir + pipeline suffix),
    # not in executor default /tmp/env_work.
    assert str(setup_call["remote_dest_dir"]).startswith("/hpcc/work/yanroy/fields/")
    assert "/tmp/env_work/" not in str(setup_call["remote_dest_dir"])


def test_slurm_workdir_respects_unix_path_style_normalization(monkeypatch, tmp_path: Path) -> None:
    pipeline = Pipeline(
        vars={"name": "yanroy.extract_fields"},
        dirs={"workdir": r"{env.workdir}\yanroy\fields\{sys.now.yymmdd}\{sys.now.hhmmss}-{sys.run.short_id}"},
        steps=[Step(name="s1", script="echo.py")],
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
        calls.append({"label": label, "remote_dest_dir": remote_dest_dir, "script_text": script_text})
        return f"job{len(calls)}"

    monkeypatch.setattr(SlurmExecutor, "_submit_script", _fake_submit_script)

    ex = SlurmExecutor(
        {"workdir": "/tmp/env_work", "logdir": "/tmp/logs"},
        repo_root=tmp_path,
        plugins_dir=Path("plugins"),
        dry_run=False,
    )
    ex.submit(
        "pipelines/yanroy/extract_fields.yml",
        {
            "run_id": "runabc1234",
            "execution_env": {"workdir": "/hpcc/work", "path_style": "unix"},
            "global_vars": {},
        },
    )

    setup_call = calls[0]
    assert "\\" not in str(setup_call["remote_dest_dir"])
    assert str(setup_call["remote_dest_dir"]).startswith("/hpcc/work/yanroy/fields/")


def test_slurm_workdir_prefers_commandline_vars_override(monkeypatch, tmp_path: Path) -> None:
    pipeline = Pipeline(
        vars={"name": "yanroy.extract_fields"},
        dirs={"workdir": "{workdir}/yanroy/fields/{sys.now.yymmdd}/{sys.now.hhmmss}-{sys.run.short_id}"},
        steps=[Step(name="s1", script="echo.py")],
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
        calls.append({"label": label, "remote_dest_dir": remote_dest_dir, "script_text": script_text})
        return f"job{len(calls)}"

    monkeypatch.setattr(SlurmExecutor, "_submit_script", _fake_submit_script)

    ex = SlurmExecutor(
        {"workdir": "/tmp/env_work", "logdir": "/tmp/logs"},
        repo_root=tmp_path,
        plugins_dir=Path("plugins"),
        dry_run=False,
    )
    ex.submit(
        "pipelines/yanroy/extract_fields.yml",
        {
            "run_id": "runabc1234",
            "execution_env": {"workdir": "/hpcc/work"},
            "global_vars": {"workdir": "/global/work"},
            "commandline_vars": {"workdir": "/cli/work"},
        },
    )

    setup_call = calls[0]
    assert str(setup_call["remote_dest_dir"]).startswith("/cli/work/yanroy/fields/")


def test_slurm_controller_job_submitted_when_enabled(monkeypatch, tmp_path: Path) -> None:
    pipeline = Pipeline(
        vars={"name": "sample"},
        steps=[Step(name="s1", script="echo.py")],
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
        calls.append({"label": label, "prev_dependency": prev_dependency, "script_text": script_text})
        return f"job{len(calls)}"

    monkeypatch.setattr(SlurmExecutor, "_submit_script", _fake_submit_script)

    ex = SlurmExecutor(
        {"workdir": "/tmp/work", "logdir": "/tmp/logs", "enable_controller_job": True},
        repo_root=tmp_path,
        plugins_dir=Path("plugins"),
        dry_run=False,
    )
    res = ex.submit("pipelines/sample.yml", {"run_id": "runabc1234"})

    assert len(calls) == 3  # setup + step + controller
    assert calls[2]["label"] == "controller"
    assert calls[2]["prev_dependency"] == "job2"
    assert "CHILD_FILE=" in calls[2]["script_text"]
    assert "squeue -h -j" in calls[2]["script_text"]
    assert res.job_ids[-1] == "job3"
    assert "ETL_CHILD_JOBS_FILE=" in calls[1]["script_text"]

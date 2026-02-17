from __future__ import annotations

import subprocess
from pathlib import Path

import etl.executors.hpcc_direct as hpcc_mod
from etl.executors.hpcc_direct import HpccDirectExecutor
from etl.pipeline import Pipeline, Step


def test_hpcc_direct_submit_runs_remote_run_batch(monkeypatch, tmp_path: Path) -> None:
    repo = tmp_path / "repo"
    (repo / "pipelines").mkdir(parents=True, exist_ok=True)
    (repo / "plugins").mkdir(parents=True, exist_ok=True)
    (repo / "config").mkdir(parents=True, exist_ok=True)
    pipeline_path = repo / "pipelines" / "sample.yml"
    pipeline_path.write_text("steps: []\n", encoding="utf-8")
    global_cfg = repo / "config" / "global.yml"
    global_cfg.write_text("workdir: /tmp/work\n", encoding="utf-8")
    env_cfg = repo / "config" / "environments.yml"
    env_cfg.write_text("environments: {}\n", encoding="utf-8")

    monkeypatch.setattr(
        hpcc_mod,
        "parse_pipeline",
        lambda *_a, **_k: Pipeline(steps=[Step(name="s1", script="echo.py"), Step(name="s2", script="echo.py")]),
    )
    seen = {}

    def _fake_run(cmd, capture_output, text, timeout, check):
        seen["cmd"] = cmd
        return subprocess.CompletedProcess(cmd, 0, "ok", "")

    monkeypatch.setattr(hpcc_mod.subprocess, "run", _fake_run)

    ex = HpccDirectExecutor(
        env_config={
            "ssh_host": "dev.hpcc.local",
            "ssh_user": "alice",
            "remote_repo": "/scratch/alice/research-etl",
            "python": "python3",
        },
        repo_root=repo,
        plugins_dir=repo / "plugins",
        workdir=Path("/scratch/alice/runs"),
        global_config=global_cfg,
        environments_config=env_cfg,
        env_name="hpcc_dev",
        dry_run=False,
        verbose=True,
    )
    res = ex.submit(
        str(pipeline_path),
        {
            "run_id": "runabc",
            "run_started_at": "2026-02-17T01:02:03Z",
            "execution_env": {"step_max_retries": 2, "step_retry_delay_seconds": 1.5},
            "commandline_vars": {"foo": "bar"},
            "project_id": "land_core",
        },
    )
    assert res.run_id == "runabc"
    status = ex.status("runabc")
    assert status.state.value == "succeeded"

    remote_script = str(seen["cmd"][-1])
    assert "etl.run_batch" in remote_script
    assert "--steps 0,1" in remote_script
    assert "--executor-type hpcc_direct" in remote_script
    assert "--tracking-executor hpcc_direct" in remote_script
    assert "--project-id land_core" in remote_script


def test_hpcc_direct_submit_records_failed_status(monkeypatch, tmp_path: Path) -> None:
    repo = tmp_path / "repo"
    (repo / "pipelines").mkdir(parents=True, exist_ok=True)
    pipeline_path = repo / "pipelines" / "sample.yml"
    pipeline_path.write_text("steps: []\n", encoding="utf-8")
    monkeypatch.setattr(
        hpcc_mod,
        "parse_pipeline",
        lambda *_a, **_k: Pipeline(steps=[Step(name="s1", script="echo.py")]),
    )
    monkeypatch.setattr(
        hpcc_mod.subprocess,
        "run",
        lambda *a, **k: subprocess.CompletedProcess(a[0], 1, "", "boom"),
    )
    ex = HpccDirectExecutor(
        env_config={"ssh_host": "dev.hpcc.local", "remote_repo": "/scratch/alice/research-etl"},
        repo_root=repo,
        dry_run=False,
    )
    _ = ex.submit(str(pipeline_path), {"run_id": "r1"})
    status = ex.status("r1")
    assert status.state.value == "failed"
    assert "rc=1" in status.message


def test_hpcc_direct_requires_ssh_host(tmp_path: Path) -> None:
    repo = tmp_path / "repo"
    (repo / "pipelines").mkdir(parents=True, exist_ok=True)
    pipeline_path = repo / "pipelines" / "sample.yml"
    pipeline_path.write_text("steps: []\n", encoding="utf-8")
    ex = HpccDirectExecutor(env_config={}, repo_root=repo)
    hpcc_mod.parse_pipeline = lambda *_a, **_k: Pipeline(steps=[Step(name="s1", script="echo.py")])  # type: ignore[assignment]
    try:
        ex.submit(str(pipeline_path), {})
        assert False, "expected missing ssh_host error"
    except RuntimeError as exc:
        assert "ssh_host" in str(exc)

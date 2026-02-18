from __future__ import annotations

import subprocess
from pathlib import Path

import etl.executors.hpcc_direct as hpcc_mod
from etl.executors.hpcc_direct import HpccDirectExecutor
from etl.git_checkout import GitExecutionSpec
from etl.pipeline import Pipeline, Step


def _fake_ssh_runner(seen_scripts: list[str], *, returncode: int = 0, stdout: str = "ok", stderr: str = ""):
    def _runner(self, target, script, stream_output=False):
        seen_scripts.append(str(script))
        return subprocess.CompletedProcess(["ssh", target], returncode, stdout, stderr)

    return _runner


def test_hpcc_direct_submit_runs_remote_run_batch(monkeypatch, tmp_path: Path) -> None:
    monkeypatch.delenv("OPENAI_API_KEY", raising=False)
    monkeypatch.delenv("ETL_DATABASE_URL", raising=False)
    monkeypatch.delenv("GITHUB_TOKEN", raising=False)
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
    monkeypatch.setattr(
        hpcc_mod,
        "resolve_execution_spec",
        lambda **_k: GitExecutionSpec(
            commit_sha="32adb6b10db9aaaa111122223333444455556666",
            origin_url="git@github.com:org/research-etl.git",
            repo_name="research-etl",
            git_is_dirty=False,
        ),
    )
    seen_scripts: list[str] = []
    monkeypatch.setattr(HpccDirectExecutor, "_run_ssh_script", _fake_ssh_runner(seen_scripts))

    ex = HpccDirectExecutor(
        env_config={
            "ssh_host": "dev.hpcc.local",
            "ssh_user": "alice",
            "remote_repo": "/scratch/alice/research-etl",
            "python": "python3",
            "propagate_secrets": False,
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
            "execution_env": {
                "step_max_retries": 2,
                "step_retry_delay_seconds": 1.5,
                "db_mode": "offline",
                "db_verbose": True,
            },
            "commandline_vars": {"foo": "bar"},
            "project_id": "land_core",
        },
    )
    assert res.run_id == "runabc"
    status = ex.status("runabc")
    assert status.state.value == "succeeded"

    remote_script = str(seen_scripts[-1])
    assert "etl.run_batch" in remote_script
    assert "python -u -m etl.run_batch" in remote_script
    assert "python3 -m etl.run_batch" not in remote_script
    assert "export PYTHONUNBUFFERED=1" in remote_script
    assert "export ETL_DB_MODE=offline" in remote_script
    assert "export ETL_DB_VERBOSE=1" in remote_script
    assert "--steps 0,1" in remote_script
    assert "--project-id land_core" in remote_script


def test_hpcc_direct_submit_records_failed_status(monkeypatch, tmp_path: Path) -> None:
    monkeypatch.delenv("OPENAI_API_KEY", raising=False)
    monkeypatch.delenv("ETL_DATABASE_URL", raising=False)
    monkeypatch.delenv("GITHUB_TOKEN", raising=False)
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
        hpcc_mod,
        "resolve_execution_spec",
        lambda **_k: GitExecutionSpec(
            commit_sha="32adb6b10db9aaaa111122223333444455556666",
            origin_url="git@github.com:org/research-etl.git",
            repo_name="research-etl",
            git_is_dirty=False,
        ),
    )
    seen_scripts: list[str] = []
    monkeypatch.setattr(
        HpccDirectExecutor,
        "_run_ssh_script",
        _fake_ssh_runner(seen_scripts, returncode=1, stdout="", stderr="boom"),
    )
    ex = HpccDirectExecutor(
        env_config={"ssh_host": "dev.hpcc.local", "remote_repo": "/scratch/alice/research-etl", "propagate_secrets": False},
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
    ex = HpccDirectExecutor(env_config={"propagate_secrets": False}, repo_root=repo)
    hpcc_mod.parse_pipeline = lambda *_a, **_k: Pipeline(steps=[Step(name="s1", script="echo.py")])  # type: ignore[assignment]
    try:
        ex.submit(str(pipeline_path), {})
        assert False, "expected missing ssh_host error"
    except RuntimeError as exc:
        assert "ssh_host" in str(exc)


def test_hpcc_direct_allow_dirty_git_overlays_local_changes(monkeypatch, tmp_path: Path) -> None:
    monkeypatch.delenv("OPENAI_API_KEY", raising=False)
    monkeypatch.delenv("ETL_DATABASE_URL", raising=False)
    monkeypatch.delenv("GITHUB_TOKEN", raising=False)
    repo = tmp_path / "repo"
    (repo / "pipelines").mkdir(parents=True, exist_ok=True)
    (repo / "plugins").mkdir(parents=True, exist_ok=True)
    (repo / "requirements.txt").write_text("pyyaml>=6.0\n", encoding="utf-8")
    pipeline_path = repo / "pipelines" / "sample.yml"
    pipeline_path.write_text("steps: []\n", encoding="utf-8")

    monkeypatch.setattr(
        hpcc_mod,
        "parse_pipeline",
        lambda *_a, **_k: Pipeline(steps=[Step(name="s1", script="echo.py")]),
    )
    monkeypatch.setattr(
        hpcc_mod,
        "resolve_execution_spec",
        lambda **_k: GitExecutionSpec(
            commit_sha="32adb6b10db9aaaa111122223333444455556666",
            origin_url="git@github.com:org/research-etl.git",
            repo_name="research-etl",
            git_is_dirty=True,
        ),
    )
    monkeypatch.setattr(
        HpccDirectExecutor,
        "_collect_dirty_overlay_paths",
        lambda self: ([Path("requirements.txt")], ["deleted.txt"]),
    )

    seen_cmds: list[list[str]] = []
    seen_scripts: list[str] = []

    def _fake_run(cmd, capture_output, text, timeout, check):
        seen_cmds.append(list(cmd))
        return subprocess.CompletedProcess(cmd, 0, "ok", "")

    monkeypatch.setattr(hpcc_mod.subprocess, "run", _fake_run)
    monkeypatch.setattr(HpccDirectExecutor, "_run_ssh_script", _fake_ssh_runner(seen_scripts))

    ex = HpccDirectExecutor(
        env_config={
            "ssh_host": "dev.hpcc.local",
            "ssh_user": "alice",
            "remote_repo": "/scratch/alice/research-etl",
            "propagate_secrets": False,
        },
        repo_root=repo,
        plugins_dir=repo / "plugins",
        dry_run=False,
    )
    _ = ex.submit(str(pipeline_path), {"run_id": "dirty1", "allow_dirty_git": True})
    assert any(cmd and cmd[0] == "scp" for cmd in seen_cmds)
    assert seen_scripts
    assert any("DIRTY_OVERLAY_TAR=" in s for s in seen_scripts)
    assert any("rm -f -- deleted.txt" in s for s in seen_scripts)


def test_hpcc_direct_propagates_secrets_file(monkeypatch, tmp_path: Path) -> None:
    monkeypatch.setenv("OPENAI_API_KEY", "sk-test-value")
    monkeypatch.delenv("ETL_DATABASE_URL", raising=False)
    monkeypatch.delenv("GITHUB_TOKEN", raising=False)
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
        hpcc_mod,
        "resolve_execution_spec",
        lambda **_k: GitExecutionSpec(
            commit_sha="32adb6b10db9aaaa111122223333444455556666",
            origin_url="git@github.com:org/research-etl.git",
            repo_name="research-etl",
            git_is_dirty=False,
        ),
    )

    seen_cmds: list[list[str]] = []
    seen_scripts: list[str] = []

    def _fake_run(cmd, capture_output, text, timeout, check):
        seen_cmds.append(list(cmd))
        return subprocess.CompletedProcess(cmd, 0, "ok", "")

    monkeypatch.setattr(hpcc_mod.subprocess, "run", _fake_run)
    monkeypatch.setattr(HpccDirectExecutor, "_run_ssh_script", _fake_ssh_runner(seen_scripts))

    ex = HpccDirectExecutor(
        env_config={"ssh_host": "dev.hpcc.local", "ssh_user": "alice", "remote_repo": "/scratch/alice/research-etl"},
        repo_root=repo,
        dry_run=False,
    )
    _ = ex.submit(str(pipeline_path), {"run_id": "sec1"})
    assert any(cmd and cmd[0] == "scp" for cmd in seen_cmds)
    assert seen_scripts
    assert any("STAGED_SECRETS=" in s for s in seen_scripts)
    assert any("source \"$HOME/.secrets/etl\"" in s for s in seen_scripts)


def test_hpcc_direct_streams_run_batch_output_by_default(monkeypatch, tmp_path: Path) -> None:
    monkeypatch.delenv("OPENAI_API_KEY", raising=False)
    monkeypatch.delenv("ETL_DATABASE_URL", raising=False)
    monkeypatch.delenv("GITHUB_TOKEN", raising=False)
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
        hpcc_mod,
        "resolve_execution_spec",
        lambda **_k: GitExecutionSpec(
            commit_sha="32adb6b10db9aaaa111122223333444455556666",
            origin_url="git@github.com:org/research-etl.git",
            repo_name="research-etl",
            git_is_dirty=False,
        ),
    )

    stage_calls: list[tuple[str, bool]] = []

    def _fake_run_stage(self, *, target, stage_name, lines, stream_output=False):
        stage_calls.append((stage_name, bool(stream_output)))
        return subprocess.CompletedProcess(["ssh"], 0, "", "")

    monkeypatch.setattr(HpccDirectExecutor, "_run_stage", _fake_run_stage)

    ex = HpccDirectExecutor(
        env_config={"ssh_host": "dev.hpcc.local", "propagate_secrets": False},
        repo_root=repo,
        dry_run=False,
        verbose=False,
    )
    _ = ex.submit(str(pipeline_path), {"run_id": "stream1"})
    assert ("run_batch", True) in stage_calls

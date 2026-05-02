from __future__ import annotations

import argparse
from pathlib import Path
from types import SimpleNamespace

from etl.cli_cmd import run as run_cli
from etl.executors.base import SubmissionResult
from etl.pipeline import Pipeline, Step


class _Status:
    def __init__(self, state: str, message: str = "") -> None:
        self.state = state
        self.message = message


def test_submit_pipeline_run_allows_external_pipeline_for_slurm_with_project_asset_match(
    monkeypatch,
    tmp_path: Path,
) -> None:
    repo_root = Path(".").resolve()
    external_pipeline = tmp_path / "shared-etl-pipelines" / "pipelines" / "sample.yml"
    external_pipeline.parent.mkdir(parents=True, exist_ok=True)
    external_pipeline.write_text("steps:\n  - name: s1\n    script: echo.py\n", encoding="utf-8")

    monkeypatch.setattr(
        run_cli,
        "parse_pipeline",
        lambda *a, **k: Pipeline(steps=[Step(name="s1", script="echo.py")]),
    )
    monkeypatch.setattr(run_cli, "collect_run_provenance", lambda **k: {"git_commit_sha": "abc123"})
    monkeypatch.setattr(
        run_cli,
        "load_project_vars",
        lambda **k: {
            "pipeline_asset_sources": [
                {
                    "repo_url": "https://github.com/josephweaver/shared-etl-pipelines.git",
                    "pipelines_dir": "pipelines",
                    "scripts_dir": "scripts",
                    "ref": "main",
                }
            ]
        },
    )
    monkeypatch.setattr(
        run_cli,
        "infer_pipeline_asset_match",
        lambda *a, **k: SimpleNamespace(pipeline_remote_hint="pipelines/sample.yml"),
    )

    captured: dict[str, object] = {}

    class _SlurmExecutor:
        def __init__(self, **kwargs) -> None:
            captured["executor_kwargs"] = kwargs

        def submit(self, pipeline_path, context):
            captured["pipeline_path"] = pipeline_path
            captured["context"] = context
            return SubmissionResult(run_id="run_slurm")

        def status(self, run_id):
            return _Status(state="queued")

    monkeypatch.setattr(run_cli, "SlurmExecutor", _SlurmExecutor)

    args = argparse.Namespace(
        pipeline=str(external_pipeline),
        plugins_dir="plugins",
        global_config=None,
        projects_config="config/projects.yml",
        environments_config="config/environments.yml",
        env="hpcc_msu",
        project_id="land_core",
        dry_run=True,
        verbose=False,
        allow_dirty_git=True,
        execution_mode=None,
        execution_source=None,
        source_bundle=None,
        source_snapshot=None,
        allow_workspace_source=False,
        executor="slurm",
        step_indices=None,
        state_run_id=None,
        _raw_argv=["run", str(external_pipeline), "--env", "hpcc_msu", "--project-id", "land_core", "--dry-run"],
    )
    vars_ctx = run_cli._build_run_variable_context(
        base_solver=None,
        global_vars={"execution_mode": "immutable", "source_root": str(repo_root.parent / ".out" / "src")},
        env_vars={"execution_source": "git_remote", "allow_workspace_source": False},
        commandline_vars={},
        parse_context_vars={},
    )

    rc = run_cli._submit_pipeline_run(
        args,
        pipeline_path=external_pipeline,
        vars_ctx=vars_ctx,
        resume_run_id=None,
    )

    assert rc == 0
    context = dict(captured["context"] or {})
    assert context["pipeline_remote_hint"] == "pipelines/sample.yml"
    assert context["execution_source"] == "git_remote"


def test_submit_pipeline_run_uses_local_sbatch_for_hpcc_control_env(
    monkeypatch,
    tmp_path: Path,
) -> None:
    external_pipeline = tmp_path / "shared-etl-pipelines" / "pipelines" / "sample.yml"
    external_pipeline.parent.mkdir(parents=True, exist_ok=True)
    external_pipeline.write_text("steps:\n  - name: s1\n    script: echo.py\n", encoding="utf-8")

    monkeypatch.setattr(
        run_cli,
        "parse_pipeline",
        lambda *a, **k: Pipeline(steps=[Step(name="s1", script="echo.py")]),
    )
    monkeypatch.setattr(run_cli, "collect_run_provenance", lambda **k: {"git_commit_sha": "abc123"})
    monkeypatch.setattr(
        run_cli,
        "load_project_vars",
        lambda **k: {
            "pipeline_asset_sources": [
                {
                    "repo_url": "https://github.com/josephweaver/shared-etl-pipelines.git",
                    "pipelines_dir": "pipelines",
                    "scripts_dir": "scripts",
                    "ref": "main",
                }
            ]
        },
    )
    monkeypatch.setattr(
        run_cli,
        "infer_pipeline_asset_match",
        lambda *a, **k: SimpleNamespace(pipeline_remote_hint="pipelines/sample.yml"),
    )

    captured: dict[str, object] = {}

    class _SlurmExecutor:
        def __init__(self, **kwargs) -> None:
            captured["executor_kwargs"] = kwargs

        def submit(self, pipeline_path, context):
            captured["context"] = context
            return SubmissionResult(run_id="run_slurm")

        def status(self, run_id):
            return _Status(state="queued")

    monkeypatch.setattr(run_cli, "SlurmExecutor", _SlurmExecutor)

    args = argparse.Namespace(
        pipeline=str(external_pipeline),
        plugins_dir="plugins",
        global_config=None,
        projects_config="config/projects.yml",
        environments_config="config/environments.yml",
        env="hpcc_msu",
        project_id="land_core",
        dry_run=True,
        verbose=False,
        allow_dirty_git=True,
        execution_mode=None,
        execution_source=None,
        source_bundle=None,
        source_snapshot=None,
        allow_workspace_source=False,
        executor="slurm",
        step_indices=None,
        state_run_id=None,
        _raw_argv=["run", str(external_pipeline), "--env", "hpcc_msu", "--project-id", "land_core", "--dry-run"],
    )
    vars_ctx = run_cli._build_run_variable_context(
        base_solver=None,
        global_vars={"execution_mode": "immutable"},
        env_vars={
            "executor": "slurm",
            "ssh_host": "dev-amd20",
            "ssh_user": "f0108939",
            "sync": True,
            "execution_source": "git_remote",
        },
        commandline_vars={},
        parse_context_vars={},
        control_env={"role": "local", "executor": "local", "slurm_submit_mode": "local"},
    )

    rc = run_cli._submit_pipeline_run(
        args,
        pipeline_path=external_pipeline,
        vars_ctx=vars_ctx,
        resume_run_id=None,
    )

    assert rc == 0
    env_config = dict(captured["executor_kwargs"]["env_config"])
    assert "ssh_host" not in env_config
    assert "ssh_user" not in env_config
    assert env_config["sync"] is False
    context_env = dict(captured["context"]["execution_env"])
    assert "ssh_host" not in context_env


def test_submit_pipeline_run_overlays_hpcc_control_paths_for_local_sbatch(
    monkeypatch,
    tmp_path: Path,
) -> None:
    external_pipeline = tmp_path / "shared-etl-pipelines" / "pipelines" / "sample.yml"
    external_pipeline.parent.mkdir(parents=True, exist_ok=True)
    external_pipeline.write_text("steps:\n  - name: s1\n    script: echo.py\n", encoding="utf-8")

    monkeypatch.setattr(
        run_cli,
        "parse_pipeline",
        lambda *a, **k: Pipeline(steps=[Step(name="s1", script="echo.py")]),
    )
    monkeypatch.setattr(run_cli, "collect_run_provenance", lambda **k: {"git_commit_sha": "abc123"})
    monkeypatch.setattr(run_cli, "load_project_vars", lambda **k: {})
    monkeypatch.setattr(
        run_cli,
        "infer_pipeline_asset_match",
        lambda *a, **k: SimpleNamespace(pipeline_remote_hint="pipelines/sample.yml"),
    )

    captured: dict[str, object] = {}

    class _SlurmExecutor:
        def __init__(self, **kwargs) -> None:
            captured["executor_kwargs"] = kwargs

        def submit(self, pipeline_path, context):
            captured["context"] = context
            return SubmissionResult(run_id="run_slurm")

        def status(self, run_id):
            return _Status(state="queued")

    monkeypatch.setattr(run_cli, "SlurmExecutor", _SlurmExecutor)

    args = argparse.Namespace(
        pipeline=str(external_pipeline),
        plugins_dir="plugins",
        global_config=None,
        projects_config="config/projects.yml",
        environments_config="config/environments.yml",
        env="hpcc_msu",
        project_id="land_core",
        dry_run=True,
        verbose=False,
        allow_dirty_git=True,
        execution_mode=None,
        execution_source="workspace",
        source_bundle=None,
        source_snapshot=None,
        allow_workspace_source=True,
        executor="slurm",
        step_indices=None,
        state_run_id=None,
        _raw_argv=["run", str(external_pipeline), "--env", "hpcc_msu"],
    )
    vars_ctx = run_cli._build_run_variable_context(
        base_solver=None,
        global_vars={"execution_mode": "workspace"},
        env_vars={
            "executor": "slurm",
            "ssh_host": "dev-amd20",
            "basedir": "/mnt/gs21/scratch/Viens_AgroEco_Lab/etl",
            "workdir": "/mnt/gs21/scratch/Viens_AgroEco_Lab/etl/work",
            "execution_source": "workspace",
            "allow_workspace_source": True,
        },
        commandline_vars={},
        parse_context_vars={},
        control_env={
            "role": "local",
            "executor": "local",
            "slurm_submit_mode": "local",
            "basedir": "/mnt/gs21/scratch/weave151/etl",
            "workdir": "/mnt/gs21/scratch/weave151/etl/work",
            "source_root": "/mnt/gs21/scratch/weave151/etl/src",
            "remote_repo": "/mnt/gs21/scratch/weave151/etl/src",
        },
    )

    rc = run_cli._submit_pipeline_run(
        args,
        pipeline_path=external_pipeline,
        vars_ctx=vars_ctx,
        resume_run_id=None,
    )

    assert rc == 0
    env_config = dict(captured["executor_kwargs"]["env_config"])
    assert env_config["workdir"] == "/mnt/gs21/scratch/weave151/etl/work"
    assert env_config["source_root"] == "/mnt/gs21/scratch/weave151/etl/src"
    assert env_config["remote_repo"] == "/mnt/gs21/scratch/weave151/etl/src"

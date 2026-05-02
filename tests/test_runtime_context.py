from __future__ import annotations

import os
from pathlib import Path
from types import SimpleNamespace

import etl.pipeline_assets as pipeline_assets
import etl.runtime_context as runtime_context
from etl.runtime_context import (
    RuntimeContextRequest,
    apply_db_mode_from_exec_env,
    apply_control_project_paths,
    build_runtime_context,
    resolve_pipeline_assets_cache_root,
)


def test_resolve_pipeline_assets_cache_root_prefers_environment_override(monkeypatch, tmp_path: Path) -> None:
    env_root = tmp_path / "env-cache"
    monkeypatch.setenv("ETL_PIPELINE_ASSET_CACHE_ROOT", str(env_root))

    out = resolve_pipeline_assets_cache_root(
        global_vars={"source_root": "../.out/src"},
        exec_env={"source_root": "/mnt/gs21/scratch/weave151/etl/src"},
    )

    assert out == env_root.resolve()


def test_apply_db_mode_from_exec_env_does_not_export_tunnel_settings(monkeypatch) -> None:
    monkeypatch.delenv("ETL_DB_MODE", raising=False)
    monkeypatch.delenv("ETL_DB_VERBOSE", raising=False)
    monkeypatch.delenv("ETL_DB_TUNNEL_MODE", raising=False)
    monkeypatch.delenv("ETL_DB_TUNNEL_COMMAND_RAW", raising=False)
    monkeypatch.delenv("ETL_DB_TUNNEL_HOST", raising=False)

    apply_db_mode_from_exec_env(
        {
            "db_mode": "online",
            "db_verbose": True,
            "db_tunnel_mode": "process",
            "db_tunnel_command": "ssh -N -L 6543:db:5432 user@login",
            "db_tunnel_host": "127.0.0.1",
        }
    )

    assert os.environ.get("ETL_DB_MODE") == "online"
    assert os.environ.get("ETL_DB_VERBOSE") == "1"
    assert os.environ.get("ETL_DB_TUNNEL_MODE") is None
    assert os.environ.get("ETL_DB_TUNNEL_COMMAND_RAW") is None
    assert os.environ.get("ETL_DB_TUNNEL_HOST") is None


def test_build_runtime_context_exposes_sys_apppath(monkeypatch, tmp_path: Path) -> None:
    repo_root = tmp_path / "repo"
    (repo_root / "config").mkdir(parents=True, exist_ok=True)
    (repo_root / "config" / "global.yml").write_text("basedir: ./out\n", encoding="utf-8")
    project_root = tmp_path / "landcore-etl-pipelines"
    pipeline_file = project_root / "pipelines" / "yanroy" / "db_fields.yml"
    (project_root / ".git").mkdir(parents=True, exist_ok=True)
    pipeline_file.parent.mkdir(parents=True, exist_ok=True)
    pipeline_file.write_text("steps:\n  - name: s1\n    script: echo.py\n", encoding="utf-8")
    monkeypatch.setenv("ETL_REPO_ROOT", str(repo_root))
    monkeypatch.setenv("ETL_PROJECTS_DIR", str(tmp_path))

    ctx = build_runtime_context(
        RuntimeContextRequest(
            global_config=repo_root / "config" / "global.yml",
            pipeline_path=pipeline_file,
        )
    )

    assert ctx.variable_catalog.resolved_context["sys"]["apppath"] == repo_root.resolve().as_posix()
    assert ctx.variable_catalog.resolved_context["sys"]["appdir"] == repo_root.resolve().as_posix()
    assert ctx.variable_catalog.resolved_context["sys"]["projectsdir"] == tmp_path.resolve().as_posix()
    assert ctx.variable_catalog.resolved_context["sys"]["projectdir"] == project_root.resolve().as_posix()
    assert ctx.variable_catalog.resolved_context["sys"]["pipelinefile"] == pipeline_file.resolve().as_posix()


def test_apply_control_project_paths_fills_sibling_asset_repos(tmp_path: Path) -> None:
    projects_root = tmp_path / "projects"
    project_vars = {
        "pipeline_asset_sources": [
            {
                "repo_url": "https://github.com/josephweaver/shared-etl-pipelines.git",
                "pipelines_dir": "pipelines",
            }
        ],
        "pipeline_assets_repo_url": "https://github.com/josephweaver/shared-etl-pipelines.git",
    }

    out = apply_control_project_paths(
        project_vars,
        control_env={"role": "local", "executor": "local", "projects_root": str(projects_root)},
    )

    expected = (projects_root / "shared-etl-pipelines").as_posix()
    assert out["pipeline_asset_sources"][0]["local_repo_path"] == expected
    assert out["pipeline_assets_local_repo_path"] == expected


def test_build_runtime_context_tracks_control_env_for_remote_target(monkeypatch, tmp_path: Path) -> None:
    repo_root = tmp_path / "research-etl"
    cfg = repo_root / "config"
    cfg.mkdir(parents=True)
    (cfg / "global.yml").write_text("basedir: ./out\n", encoding="utf-8")
    (cfg / "environments.yml").write_text(
        "\n".join(
            [
                "environments:",
                "  unix_local:",
                "    role: local",
                "    executor: local",
                "    path_style: unix",
                f"    projects_root: {tmp_path.as_posix()}",
                "  hpcc_msu:",
                "    role: remote",
                "    executor: slurm",
                "    path_style: unix",
                "    basedir: /mnt/scratch/etl",
                "    workdir: '{basedir}/work'",
            ]
        )
        + "\n",
        encoding="utf-8",
    )
    (cfg / "projects.yml").write_text(
        "\n".join(
            [
                "projects:",
                "  default:",
                "    vars:",
                "      pipeline_asset_sources:",
                "        - repo_url: https://github.com/josephweaver/shared-etl-pipelines.git",
                "          local_repo_path: ''",
                "          pipelines_dir: pipelines",
            ]
        )
        + "\n",
        encoding="utf-8",
    )
    shared = tmp_path / "shared-etl-pipelines"
    pipeline_file = shared / "pipelines" / "sample.yml"
    (shared / ".git").mkdir(parents=True)
    pipeline_file.parent.mkdir(parents=True)
    pipeline_file.write_text("steps:\n  - name: s1\n    script: echo.py\n", encoding="utf-8")
    monkeypatch.setattr(
        pipeline_assets,
        "run_logged_subprocess",
        lambda *a, **k: SimpleNamespace(returncode=0, stdout="true\n", stderr=""),
    )
    monkeypatch.setenv("ETL_REPO_ROOT", str(repo_root))
    monkeypatch.setenv("ETL_PROJECTS_DIR", str(tmp_path))
    monkeypatch.chdir(repo_root)

    ctx = build_runtime_context(
        RuntimeContextRequest(
            global_config=cfg / "global.yml",
            projects_config=cfg / "projects.yml",
            environments_config=cfg / "environments.yml",
            env_name="hpcc_msu",
            control_env_name="unix_local",
            pipeline_path=Path("sample.yml"),
        )
    )

    assert ctx.env_name == "hpcc_msu"
    assert ctx.control_env_name == "unix_local"
    assert ctx.pipeline_path == pipeline_file.resolve()
    assert ctx.project_vars["pipeline_asset_sources"][0]["local_repo_path"] == shared.as_posix()


def test_control_env_templates_can_use_local_env_vars(tmp_path: Path) -> None:
    repo_root = tmp_path / "research-etl"
    cfg = repo_root / "config"
    cfg.mkdir(parents=True)
    (cfg / "global.yml").write_text("basedir: ./out\n", encoding="utf-8")
    (cfg / "environments.yml").write_text(
        "\n".join(
            [
                "environments:",
                "  hpcc_local:",
                "    role: local",
                "    executor: local",
                "    path_style: unix",
                "    basedir: /mnt/gs21/scratch/{local_env.USER}/etl",
                "    workdir: '{basedir}/work'",
                "  hpcc_msu:",
                "    role: remote",
                "    executor: slurm",
            ]
        )
        + "\n",
        encoding="utf-8",
    )

    ctx = build_runtime_context(
        RuntimeContextRequest(
            global_config=cfg / "global.yml",
            environments_config=cfg / "environments.yml",
            env_name="hpcc_msu",
            control_env_name="hpcc_local",
            local_env_vars={"USER": "monica"},
        )
    )

    assert ctx.control_env["basedir"] == "/mnt/gs21/scratch/monica/etl"
    assert ctx.control_env["workdir"] == "/mnt/gs21/scratch/monica/etl/work"


def test_bootstrap_logging_falls_back_when_configured_path_denied(monkeypatch, tmp_path: Path) -> None:
    calls: list[Path] = []
    real_configure = runtime_context.configure_app_logger
    fallback_root = tmp_path / "fallback-bootstrap"
    monkeypatch.setenv("ETL_BOOTSTRAP_LOG_DIR", str(fallback_root))

    def fake_configure_app_logger(**kwargs):
        path = Path(kwargs["log_file"])
        calls.append(path)
        if len(calls) == 1:
            raise PermissionError("denied")
        return real_configure(**kwargs)

    monkeypatch.setattr(runtime_context, "configure_app_logger", fake_configure_app_logger)

    logging_ctx = runtime_context._build_bootstrap_logging(
        global_vars={"logdir": "/not/writable/logs"},
        label="cli-run",
        logger_name="test-bootstrap-fallback",
    )

    assert calls[0].as_posix().endswith("/not/writable/logs/bootstrap/cli-run.log")
    assert logging_ctx.bootstrap_log_file == (fallback_root / "cli-run.log").resolve()
    assert logging_ctx.bootstrap_log_file.exists()

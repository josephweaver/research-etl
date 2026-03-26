from __future__ import annotations

import os
from pathlib import Path

from etl.runtime_context import (
    RuntimeContextRequest,
    apply_db_mode_from_exec_env,
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

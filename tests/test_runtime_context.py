from __future__ import annotations

import os
from pathlib import Path

from etl.runtime_context import apply_db_mode_from_exec_env, resolve_pipeline_assets_cache_root


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

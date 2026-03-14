from __future__ import annotations

from pathlib import Path

from etl.runtime_context import resolve_pipeline_assets_cache_root


def test_resolve_pipeline_assets_cache_root_prefers_environment_override(monkeypatch, tmp_path: Path) -> None:
    env_root = tmp_path / "env-cache"
    monkeypatch.setenv("ETL_PIPELINE_ASSET_CACHE_ROOT", str(env_root))

    out = resolve_pipeline_assets_cache_root(
        global_vars={"source_root": "../.out/src"},
        exec_env={"source_root": "/mnt/gs21/scratch/weave151/etl/src"},
    )

    assert out == env_root.resolve()

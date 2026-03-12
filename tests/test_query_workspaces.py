from __future__ import annotations

from pathlib import Path

from etl.query.workspaces import (
    deep_merge_config,
    load_workspace_config_file,
    resolve_repo_workspace_config_path,
)


def test_resolve_repo_workspace_config_path_prefers_explicit(tmp_path: Path) -> None:
    repo_root = tmp_path / "repo"
    repo_root.mkdir(parents=True, exist_ok=True)
    p = resolve_repo_workspace_config_path(
        project_id="crop_insurance",
        project_vars={"duckdb_workspace_config_path": "configs/query/workspace.yml"},
        repo_root=repo_root,
    )
    assert p == (repo_root / "configs/query/workspace.yml").resolve()


def test_resolve_repo_workspace_config_path_uses_pipeline_assets_repo(tmp_path: Path) -> None:
    repo_root = tmp_path / "repo"
    repo_root.mkdir(parents=True, exist_ok=True)
    p = resolve_repo_workspace_config_path(
        project_id="crop_insurance",
        project_vars={"pipeline_assets_local_repo_path": "../crop-insurance-etl-pipelines"},
        repo_root=repo_root,
    )
    assert p == (repo_root / "../crop-insurance-etl-pipelines/db/duckdb/workspace.yml").resolve()


def test_load_workspace_config_file_yaml(tmp_path: Path) -> None:
    cfg = tmp_path / "duckdb.workspace.yml"
    cfg.write_text("tables:\n  - name: t1\n    path: data/a.csv\njoins: []\n", encoding="utf-8")
    out = load_workspace_config_file(cfg)
    assert out["tables"][0]["name"] == "t1"
    assert out["tables"][0]["path"] == "data/a.csv"


def test_resolve_repo_workspace_config_path_falls_back_to_legacy_when_new_missing(tmp_path: Path) -> None:
    repo_root = tmp_path / "repo"
    assets = (repo_root / "../crop-insurance-etl-pipelines").resolve()
    (assets / "query").mkdir(parents=True, exist_ok=True)
    legacy = assets / "query" / "duckdb.workspace.yml"
    legacy.write_text("tables: []\n", encoding="utf-8")
    p = resolve_repo_workspace_config_path(
        project_id="crop_insurance",
        project_vars={"pipeline_assets_local_repo_path": "../crop-insurance-etl-pipelines"},
        repo_root=repo_root,
    )
    assert p == legacy


def test_deep_merge_config_nested() -> None:
    base = {"tables": [{"name": "t1"}], "settings": {"a": 1, "b": 2}}
    override = {"settings": {"b": 3, "c": 4}, "executor": "hpcc_direct"}
    out = deep_merge_config(base, override)
    assert out["tables"] == [{"name": "t1"}]
    assert out["settings"] == {"a": 1, "b": 3, "c": 4}
    assert out["executor"] == "hpcc_direct"

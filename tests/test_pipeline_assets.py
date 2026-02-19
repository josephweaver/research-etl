from pathlib import Path

import pytest

from etl.pipeline_assets import (
    PipelineAssetError,
    pipeline_asset_sources_from_project_vars,
    resolve_pipeline_path_from_project_sources,
)


def test_pipeline_asset_sources_from_project_vars_merges_and_sorts() -> None:
    sources = pipeline_asset_sources_from_project_vars(
        {
            "pipeline_asset_sources": [
                {"repo_url": "https://example.com/repo-a.git", "priority": 20},
                {"repo_url": "https://example.com/repo-b.git", "priority": 10},
            ],
            "pipeline_assets_repo_url": "https://example.com/repo-a.git",
        }
    )
    assert [s.repo_url for s in sources] == [
        "https://example.com/repo-b.git",
        "https://example.com/repo-a.git",
    ]


def test_pipeline_asset_sources_from_project_vars_rejects_invalid_entries() -> None:
    with pytest.raises(PipelineAssetError):
        pipeline_asset_sources_from_project_vars({"pipeline_asset_sources": ["bad"]})


def test_resolve_pipeline_path_from_project_sources_prefers_repo_local_path(tmp_path: Path) -> None:
    repo_root = tmp_path / "repo"
    local_pipeline = repo_root / "pipelines" / "local.yml"
    local_pipeline.parent.mkdir(parents=True, exist_ok=True)
    local_pipeline.write_text("steps: []\n", encoding="utf-8")

    resolved = resolve_pipeline_path_from_project_sources(
        Path("pipelines/local.yml"),
        project_vars={},
        repo_root=repo_root,
    )
    assert resolved == local_pipeline.resolve()


def test_resolve_pipeline_path_from_project_sources_uses_external_source(monkeypatch, tmp_path: Path) -> None:
    repo_root = tmp_path / "workspace"
    repo_root.mkdir(parents=True, exist_ok=True)
    ext_repo = tmp_path / "external"
    ext_pipeline = ext_repo / "pipelines" / "folder" / "download.yml"
    ext_pipeline.parent.mkdir(parents=True, exist_ok=True)
    ext_pipeline.write_text("steps: []\n", encoding="utf-8")

    monkeypatch.setattr(
        "etl.pipeline_assets.sync_pipeline_asset_source",
        lambda source, cache_root: ext_repo,
    )

    resolved = resolve_pipeline_path_from_project_sources(
        Path("folder/download"),
        project_vars={
            "pipeline_asset_sources": [
                {"repo_url": "https://example.com/shared-etl-pipelines.git", "pipelines_dir": "pipelines"}
            ]
        },
        repo_root=repo_root,
    )
    assert resolved == ext_pipeline.resolve()

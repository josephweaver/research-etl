# research-etl
# Copyright (c) 2026 Joseph Weaver
# This file is part of the research-etl project and is licensed under the MIT License.
# You may not use this file except in compliance with the License.
# See https://github.com/josephweaver/research-etl for details.

from pathlib import Path

import pytest

from etl.pipeline_assets import (
    PipelineAssetError,
    PipelineAssetSource,
    pipeline_asset_sources_from_project_vars,
    resolve_pipeline_path_from_project_sources,
    sync_pipeline_asset_source,
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


def test_sync_pipeline_asset_source_falls_back_when_local_repo_path_missing(monkeypatch, tmp_path: Path) -> None:
    calls: list[list[str]] = []

    def _fake_run_git(args, *, cwd=None):
        calls.append(list(args))
        return ""

    monkeypatch.setattr("etl.pipeline_assets._run_git", _fake_run_git)
    monkeypatch.setattr("etl.pipeline_assets._SYNCED_REPOS", set())

    src = PipelineAssetSource(
        repo_url="https://example.com/shared.git",
        ref="main",
        local_repo_path="does/not/exist",
    )
    out = sync_pipeline_asset_source(src, cache_root=tmp_path / "cache", repo_root=tmp_path / "repo_root")

    assert out.parent == (tmp_path / "cache").resolve()
    assert any(cmd[:2] == ["clone", "https://example.com/shared.git"] for cmd in calls)

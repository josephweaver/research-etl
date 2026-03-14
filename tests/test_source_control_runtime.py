from __future__ import annotations

from pathlib import Path

import etl.source_control.runtime as rt
from etl.source_control import SourceExecutionSpec


def test_merge_source_commandline_vars_seeds_etl_and_pipeline_defaults(monkeypatch, tmp_path: Path) -> None:
    repo_root = tmp_path / "etl"
    repo_root.mkdir(parents=True, exist_ok=True)
    pipeline_root = (tmp_path / "crop-insurance-etl-pipelines").resolve()
    pipeline_root.mkdir(parents=True, exist_ok=True)

    def _fake_resolve_execution_spec(*, repo_root: Path, **_kwargs) -> SourceExecutionSpec:
        name = repo_root.name
        return SourceExecutionSpec(
            provider="git",
            revision=f"{name}-sha",
            origin_url=f"git@github.com:org/{name}.git",
            repo_name=name,
            is_dirty=False,
        )

    monkeypatch.setattr(rt, "_SOURCE_PROVIDER", type("P", (), {"resolve_execution_spec": staticmethod(_fake_resolve_execution_spec)})())

    merged = rt.merge_source_commandline_vars(
        {"workdir": "/tmp/work"},
        repo_root=repo_root,
        project_vars={
            "pipeline_asset_sources": [
                {
                    "repo_url": "git@github.com:org/crop-insurance-etl-pipelines.git",
                    "local_repo_path": "../crop-insurance-etl-pipelines",
                    "ref": "main",
                }
            ]
        },
        provenance={},
    )

    assert merged["source"]["etl"]["revision"] == "etl-sha"
    assert merged["source"]["pipeline"]["revision"] == "crop-insurance-etl-pipelines-sha"
    assert merged["source"]["pipeline"]["repo_url"] == "git@github.com:org/crop-insurance-etl-pipelines.git"

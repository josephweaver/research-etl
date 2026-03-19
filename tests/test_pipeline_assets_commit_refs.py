from __future__ import annotations

import etl.pipeline_assets as pipeline_assets_mod


def test_resolve_repo_commit_returns_sha_refs_without_ls_remote(monkeypatch) -> None:
    calls: list[list[str]] = []

    def _fake_run_git(args, *, cwd=None):
        calls.append(list(args))
        return ""

    monkeypatch.setattr(pipeline_assets_mod, "_run_git", _fake_run_git)

    sha = "178969886c4891b3261cdfbb9ead1b6c921516d2"
    resolved = pipeline_assets_mod._resolve_repo_commit(
        repo_url="https://github.com/josephweaver/shared-etl-pipelines.git",
        ref=sha,
    )

    assert resolved == sha
    assert calls == []

# research-etl
# Copyright (c) 2026 Joseph Weaver
# This file is part of the research-etl project and is licensed under the MIT License.
# You may not use this file except in compliance with the License.
# See https://github.com/josephweaver/research-etl for details.

from __future__ import annotations

from pathlib import Path

import pytest

from etl.git_checkout import GitCheckoutError, GitExecutionSpec
from etl.source_control import GitSourceProvider, SourceControlError, SourceExecutionSpec
from etl.source_control import git_provider as gp


def test_git_source_provider_resolve_execution_spec_maps_fields(monkeypatch) -> None:
    provider = GitSourceProvider()

    def _fake_resolve(**kwargs):
        return GitExecutionSpec(
            commit_sha="abc123def456",
            origin_url="git@github.com:org/repo.git",
            repo_name="repo",
            git_is_dirty=False,
        )

    monkeypatch.setattr(gp, "resolve_execution_spec", _fake_resolve)
    spec = provider.resolve_execution_spec(repo_root=Path("."), provenance={})
    assert spec.provider == "git"
    assert spec.revision == "abc123def456"
    assert spec.origin_url == "git@github.com:org/repo.git"
    assert spec.repo_name == "repo"
    assert spec.is_dirty is False
    assert spec.extra["commit_sha"] == "abc123def456"


def test_git_source_provider_to_git_spec_rejects_non_git_provider() -> None:
    provider = GitSourceProvider()
    with pytest.raises(SourceControlError, match="cannot use provider"):
        provider._to_git_spec(  # noqa: SLF001
            SourceExecutionSpec(
                provider="svn",
                revision="r1",
                origin_url=None,
                repo_name="repo",
            )
        )


def test_git_source_provider_wraps_git_checkout_error(monkeypatch) -> None:
    provider = GitSourceProvider()

    def _raise(*args, **kwargs):
        raise GitCheckoutError("boom")

    monkeypatch.setattr(gp, "repo_relative_path", _raise)
    with pytest.raises(SourceControlError, match="boom"):
        provider.repo_relative_path(Path("x"), Path("."), "path")

from __future__ import annotations

from pathlib import Path

import pytest

from etl.git_checkout import GitCheckoutError, infer_repo_name, resolve_execution_spec


def test_infer_repo_name_handles_scp_style_origin() -> None:
    assert infer_repo_name("git@github.com:org/research-etl.git") == "research-etl"


def test_resolve_execution_spec_rejects_dirty_repo_when_required(tmp_path: Path) -> None:
    with pytest.raises(GitCheckoutError, match="dirty"):
        resolve_execution_spec(
            repo_root=tmp_path,
            provenance={
                "git_commit_sha": "abc123",
                "git_origin_url": "https://github.com/org/repo.git",
                "git_is_dirty": True,
            },
            require_clean=True,
        )

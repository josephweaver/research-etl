from __future__ import annotations

from pathlib import Path

import etl.executors.local as local_mod
from etl.executors.local import LocalExecutor
from etl.git_checkout import GitExecutionSpec


def test_local_executor_runs_from_checkout_when_enforced(tmp_path: Path, monkeypatch) -> None:
    repo_root = tmp_path / "repo"
    repo_root.mkdir(parents=True, exist_ok=True)
    source_pipeline = repo_root / "pipelines" / "sample.yml"
    source_pipeline.parent.mkdir(parents=True, exist_ok=True)
    # Do not create source_pipeline; strict checkout mode should use checkout copy.

    checkout_root = tmp_path / "checkout"
    (checkout_root / "pipelines").mkdir(parents=True, exist_ok=True)
    (checkout_root / "plugins").mkdir(parents=True, exist_ok=True)
    (checkout_root / "pipelines" / "sample.yml").write_text(
        "steps:\n  - name: echo\n    script: echo.py\n",
        encoding="utf-8",
    )
    (checkout_root / "plugins" / "echo.py").write_text(
        "\n".join(
            [
                "meta = {'name': 'echo', 'version': '0.1.0', 'description': 'test'}",
                "def run(args, ctx):",
                "    return {'ok': True}",
            ]
        ),
        encoding="utf-8",
    )

    monkeypatch.setattr(
        local_mod,
        "resolve_execution_spec",
        lambda **_: GitExecutionSpec(
            commit_sha="abc123",
            origin_url="https://github.com/org/repo.git",
            repo_name="repo",
            git_is_dirty=False,
        ),
    )
    monkeypatch.setattr(local_mod, "ensure_repo_checkout", lambda *a, **k: checkout_root)

    ex = LocalExecutor(
        plugin_dir=Path("plugins"),
        workdir=tmp_path / ".runs",
        dry_run=True,
        enforce_git_checkout=True,
        require_clean_git=True,
    )
    submit = ex.submit(
        str(source_pipeline),
        context={
            "repo_root": repo_root,
            "provenance": {
                "git_commit_sha": "abc123",
                "git_origin_url": "https://github.com/org/repo.git",
                "git_is_dirty": False,
            },
            "global_vars": {},
        },
    )
    assert submit.run_id
    assert ex.status(submit.run_id).state.value == "succeeded"

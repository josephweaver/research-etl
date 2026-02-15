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


def test_local_executor_applies_execution_env_vars_in_parse(tmp_path: Path, monkeypatch) -> None:
    pipeline_path = tmp_path / "pipeline.yml"
    pipeline_path.write_text(
        "steps:\n  - name: echo\n    script: 'echo.py message=\"{env.msg}\"'\n",
        encoding="utf-8",
    )
    plugins_dir = tmp_path / "plugins"
    plugins_dir.mkdir(parents=True, exist_ok=True)
    (plugins_dir / "echo.py").write_text(
        "\n".join(
            [
                "meta = {'name': 'echo', 'version': '0.1.0', 'description': 'test'}",
                "def run(args, ctx):",
                "    return {'message': args.get('message', '')}",
            ]
        ),
        encoding="utf-8",
    )

    captured = {}

    def _fake_record_run(run_result, pipeline_path, store, **kwargs):
        captured["script"] = run_result.steps[0].step.script

    monkeypatch.setattr(local_mod, "record_run", _fake_record_run)

    ex = LocalExecutor(
        plugin_dir=plugins_dir,
        workdir=tmp_path / ".runs",
        dry_run=True,
    )
    submit = ex.submit(
        str(pipeline_path),
        context={
            "global_vars": {},
            "execution_env": {"msg": "HELLO_ENV"},
        },
    )
    assert submit.run_id
    assert captured["script"] == 'echo.py message="HELLO_ENV"'


def test_local_executor_prefers_execution_env_git_remote_url(tmp_path: Path, monkeypatch) -> None:
    repo_root = tmp_path / "repo"
    repo_root.mkdir(parents=True, exist_ok=True)
    source_pipeline = repo_root / "pipelines" / "sample.yml"
    source_pipeline.parent.mkdir(parents=True, exist_ok=True)

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
    seen = {}

    def _fake_checkout(_base, spec):
        seen["origin_url"] = spec.origin_url
        return checkout_root

    monkeypatch.setattr(local_mod, "ensure_repo_checkout", _fake_checkout)

    ex = LocalExecutor(
        plugin_dir=Path("plugins"),
        workdir=tmp_path / ".runs",
        dry_run=True,
        enforce_git_checkout=True,
        require_clean_git=False,
    )
    submit = ex.submit(
        str(source_pipeline),
        context={
            "repo_root": repo_root,
            "execution_env": {"git_remote_url": "git@github.com:josephweaver/research-etl.git"},
            "global_vars": {},
        },
    )
    assert submit.run_id
    assert seen["origin_url"] == "git@github.com:josephweaver/research-etl.git"

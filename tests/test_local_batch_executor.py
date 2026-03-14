from __future__ import annotations

from pathlib import Path

import etl.executors.local_batch as local_batch_mod
import etl.pipeline_assets as pipeline_assets_mod
from etl.executors.local_batch import LocalBatchExecutor
from etl.git_checkout import GitExecutionSpec
from etl.provisioners import ProvisionHandle, ProvisionState, ProvisionStatus


class _FakeProvisioner:
    def __init__(self) -> None:
        self.submitted = []

    def submit(self, spec):
        self.submitted.append(spec)
        idx = len(self.submitted)
        return ProvisionHandle(provisioner="local", backend_run_id=f"job{idx}", job_ids=[f"job{idx}"])

    def status(self, handle):
        return ProvisionStatus(backend_run_id=handle.backend_run_id, state=ProvisionState.SUCCEEDED, message="ok")


def test_local_batch_executor_submits_planned_workloads(monkeypatch, tmp_path: Path) -> None:
    pipeline_path = tmp_path / "pipeline.yml"
    pipeline_path.write_text(
        "\n".join(
            [
                "vars:",
                "  items: [a, b]",
                "steps:",
                "  - name: first",
                "    script: echo.py",
                "  - name: fan",
                "    script: echo.py",
                "    foreach: items",
            ]
        ),
        encoding="utf-8",
    )
    plugins_dir = tmp_path / "plugins"
    plugins_dir.mkdir(parents=True, exist_ok=True)
    (plugins_dir / "echo.py").write_text(
        "\n".join(
            [
                "meta = {'name': 'echo', 'version': '0.1.0', 'description': 'test'}",
                "def run(args, ctx):",
                "    return {'ok': True}",
            ]
        ),
        encoding="utf-8",
    )
    fake = _FakeProvisioner()
    upserts = []
    monkeypatch.setattr(local_batch_mod, "upsert_run_status", lambda **kw: upserts.append(kw))

    ex = LocalBatchExecutor(
        plugin_dir=plugins_dir,
        workdir=tmp_path / ".runs",
        provisioner=fake,
        verbose=True,
    )
    res = ex.submit(str(pipeline_path), {"run_id": "runabc1234"})

    assert res.run_id == "runabc1234"
    assert len(fake.submitted) == 4  # setup + first + foreach(2)
    assert fake.submitted[0].name.endswith("setup")
    assert fake.submitted[1].command[1:3] == ["-m", "etl.run_batch"]
    assert "--steps" in fake.submitted[1].command
    assert "--foreach-item-index" in fake.submitted[2].command
    assert ex.status("runabc1234").state.value == "succeeded"
    assert upserts[-1]["status"] == "succeeded"


def test_local_batch_executor_runs_real_batch_job(tmp_path: Path, monkeypatch) -> None:
    pipeline_path = tmp_path / "pipeline.yml"
    pipeline_path.write_text(
        "\n".join(
            [
                "steps:",
                "  - name: write",
                "    script: writer.py",
            ]
        ),
        encoding="utf-8",
    )
    plugins_dir = tmp_path / "plugins"
    plugins_dir.mkdir(parents=True, exist_ok=True)
    marker = tmp_path / "marker.txt"
    (plugins_dir / "writer.py").write_text(
        "\n".join(
            [
                "meta = {'name': 'writer', 'version': '0.1.0', 'description': 'test'}",
                "def run(args, ctx):",
                f"    from pathlib import Path; Path(r'{marker}').write_text('ok', encoding='utf-8')",
                "    return {'ok': True}",
            ]
        ),
        encoding="utf-8",
    )
    monkeypatch.setattr(local_batch_mod, "upsert_run_status", lambda **_: None)

    ex = LocalBatchExecutor(
        plugin_dir=plugins_dir,
        workdir=tmp_path / ".runs",
        python_bin="python",
    )
    res = ex.submit(str(pipeline_path), {"run_id": "runabc1234"})

    assert res.run_id == "runabc1234"
    assert marker.read_text(encoding="utf-8") == "ok"
    status = ex.status("runabc1234")
    assert status.state.value == "succeeded"


def test_local_batch_executor_prefers_execution_env_source_root_for_checkout(tmp_path: Path, monkeypatch) -> None:
    repo_root = tmp_path / "repo"
    repo_root.mkdir(parents=True, exist_ok=True)
    pipeline_path = repo_root / "pipelines" / "pipeline.yml"
    pipeline_path.parent.mkdir(parents=True, exist_ok=True)
    pipeline_path.write_text("steps:\n  - name: write\n    script: writer.py\n", encoding="utf-8")
    checkout_root = tmp_path / "src" / "repo-abc123"
    (checkout_root / "plugins").mkdir(parents=True, exist_ok=True)
    (checkout_root / "pipelines").mkdir(parents=True, exist_ok=True)
    (checkout_root / "pipelines" / "pipeline.yml").write_text(
        "steps:\n  - name: write\n    script: writer.py\n",
        encoding="utf-8",
    )
    marker = tmp_path / "marker_src.txt"
    (checkout_root / "plugins" / "writer.py").write_text(
        "\n".join(
            [
                "meta = {'name': 'writer', 'version': '0.1.0', 'description': 'test'}",
                "def run(args, ctx):",
                f"    from pathlib import Path; Path(r'{marker}').write_text('ok', encoding='utf-8')",
                "    return {'ok': True}",
            ]
        ),
        encoding="utf-8",
    )
    monkeypatch.setattr(local_batch_mod, "upsert_run_status", lambda **_: None)
    monkeypatch.setattr(
        local_batch_mod,
        "resolve_execution_spec",
        lambda **_: GitExecutionSpec(
            commit_sha="abc123",
            origin_url="https://github.com/org/repo.git",
            repo_name="repo",
            git_is_dirty=False,
        ),
    )
    seen = {}

    def _fake_checkout(base_dir, spec):
        seen["base_dir"] = Path(base_dir)
        return checkout_root

    monkeypatch.setattr(local_batch_mod, "ensure_repo_checkout", _fake_checkout)

    ex = LocalBatchExecutor(
        plugin_dir=Path("plugins"),
        workdir=tmp_path / ".runs",
        python_bin="python",
        enforce_git_checkout=True,
        require_clean_git=False,
    )
    res = ex.submit(
        str(pipeline_path),
        {
            "run_id": "runabc1234",
            "repo_root": repo_root,
            "execution_env": {"source_root": str(tmp_path / "src")},
        },
    )
    assert res.run_id == "runabc1234"
    assert seen["base_dir"].resolve() == (tmp_path / "src").resolve()


def test_local_batch_executor_uses_matching_pipeline_asset_repo_for_external_pipeline(tmp_path: Path, monkeypatch) -> None:
    repo_root = tmp_path / "repo"
    repo_root.mkdir(parents=True, exist_ok=True)
    external_repo = tmp_path / "shared-etl-pipelines"
    external_pipeline = external_repo / "pipelines" / "sample.yml"
    external_pipeline.parent.mkdir(parents=True, exist_ok=True)
    external_pipeline.write_text(
        "steps:\n  - name: write\n    script: writer.py\n",
        encoding="utf-8",
    )

    checkout_root = tmp_path / "checkout"
    (checkout_root / "plugins").mkdir(parents=True, exist_ok=True)
    marker = tmp_path / "marker_ext.txt"
    (checkout_root / "plugins" / "writer.py").write_text(
        "\n".join(
            [
                "meta = {'name': 'writer', 'version': '0.1.0', 'description': 'test'}",
                "def run(args, ctx):",
                f"    from pathlib import Path; Path(r'{marker}').write_text('ok', encoding='utf-8')",
                "    return {'ok': True}",
            ]
        ),
        encoding="utf-8",
    )

    monkeypatch.setattr(
        local_batch_mod,
        "resolve_execution_spec",
        lambda **_: GitExecutionSpec(
            commit_sha="abc123",
            origin_url="https://github.com/org/repo.git",
            repo_name="repo",
            git_is_dirty=False,
        ),
    )
    monkeypatch.setattr(local_batch_mod, "ensure_repo_checkout", lambda *a, **k: checkout_root)
    monkeypatch.setattr(pipeline_assets_mod, "sync_pipeline_asset_source", lambda *a, **k: external_repo)
    monkeypatch.setattr(local_batch_mod, "upsert_run_status", lambda **_: None)

    ex = LocalBatchExecutor(
        plugin_dir=Path("plugins"),
        workdir=tmp_path / ".runs",
        python_bin="python",
        enforce_git_checkout=True,
        require_clean_git=False,
    )
    res = ex.submit(
        str(external_pipeline),
        {
            "run_id": "runabc1234",
            "repo_root": repo_root,
            "project_vars": {
                "pipeline_asset_sources": [
                    {
                        "repo_url": "https://github.com/josephweaver/shared-etl-pipelines.git",
                        "local_repo_path": str(external_repo),
                        "pipelines_dir": "pipelines",
                        "scripts_dir": "scripts",
                        "ref": "main",
                    }
                ]
            },
        },
    )

    assert res.run_id == "runabc1234"
    assert marker.read_text(encoding="utf-8") == "ok"

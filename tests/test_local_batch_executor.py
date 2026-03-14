from __future__ import annotations

from pathlib import Path

import etl.executors.local_batch as local_batch_mod
from etl.executors.local_batch import LocalBatchExecutor
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

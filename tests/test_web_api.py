from __future__ import annotations

from pathlib import Path

import pytest


def test_web_api_endpoints(monkeypatch):
    fastapi = pytest.importorskip("fastapi", exc_type=ImportError)
    testclient = pytest.importorskip("fastapi.testclient", exc_type=ImportError)
    assert fastapi is not None
    assert testclient is not None

    import etl.web_api as web_api
    from fastapi.testclient import TestClient

    monkeypatch.setattr(
        web_api,
        "fetch_runs",
        lambda limit=50, status=None, executor=None, q=None: [{"run_id": "r1", "status": "succeeded"}],
    )
    monkeypatch.setattr(web_api, "fetch_run_detail", lambda run_id: {"run_id": run_id, "status": "succeeded"})

    client = TestClient(web_api.app)
    r1 = client.get("/api/runs")
    assert r1.status_code == 200
    assert r1.json()[0]["run_id"] == "r1"

    r2 = client.get("/api/runs/r1")
    assert r2.status_code == 200
    assert r2.json()["run_id"] == "r1"


def test_web_api_404(monkeypatch):
    pytest.importorskip("fastapi", exc_type=ImportError)
    import etl.web_api as web_api
    from fastapi.testclient import TestClient

    monkeypatch.setattr(web_api, "fetch_run_detail", lambda run_id: None)
    client = TestClient(web_api.app)
    r = client.get("/api/runs/missing")
    assert r.status_code == 404


def test_web_api_resume_local(monkeypatch):
    pytest.importorskip("fastapi", exc_type=ImportError)
    import etl.web_api as web_api
    from etl.executors.base import SubmissionResult, RunState, RunStatus
    from etl.pipeline import Pipeline, Step
    from fastapi.testclient import TestClient

    monkeypatch.setattr(
        web_api,
        "fetch_run_header",
        lambda run_id: {"run_id": run_id, "pipeline": "pipelines/sample.yml", "executor": "local", "status": "failed"},
    )
    monkeypatch.setattr(web_api, "parse_pipeline", lambda *a, **k: Pipeline(steps=[Step(name="s1", script="echo.py")]))
    monkeypatch.setattr(web_api, "collect_run_provenance", lambda **k: {"git_commit_sha": "abc"})

    class _FakeLocalExecutor:
        def __init__(self, *a, **k):
            pass

        def submit(self, *a, **k):
            return SubmissionResult(run_id="new_run_1")

        def status(self, run_id):
            return RunStatus(run_id=run_id, state=RunState.SUCCEEDED)

    monkeypatch.setattr(web_api, "LocalExecutor", _FakeLocalExecutor)
    client = TestClient(web_api.app)
    r = client.post("/api/runs/old_run/resume")
    assert r.status_code == 200
    payload = r.json()
    assert payload["run_id"] == "new_run_1"
    assert payload["state"] == "succeeded"


def test_web_api_resume_rejects_non_local(monkeypatch):
    pytest.importorskip("fastapi", exc_type=ImportError)
    import etl.web_api as web_api
    from fastapi.testclient import TestClient

    monkeypatch.setattr(
        web_api,
        "fetch_run_header",
        lambda run_id: {"run_id": run_id, "pipeline": "pipelines/sample.yml", "executor": "slurm", "status": "failed"},
    )
    client = TestClient(web_api.app)
    r = client.post("/api/runs/old_run/resume")
    assert r.status_code == 400


def test_web_api_files_and_file_view(monkeypatch, tmp_path: Path):
    pytest.importorskip("fastapi", exc_type=ImportError)
    import etl.web_api as web_api
    from fastapi.testclient import TestClient

    art = tmp_path / ".runs" / "x"
    (art / "logs").mkdir(parents=True, exist_ok=True)
    (art / "logs" / "job.out").write_text("hello log", encoding="utf-8")
    monkeypatch.setattr(
        web_api,
        "fetch_run_header",
        lambda run_id: {
            "run_id": run_id,
            "pipeline": "pipelines/sample.yml",
            "executor": "local",
            "status": "failed",
            "artifact_dir": str(art),
        },
    )
    client = TestClient(web_api.app)
    tree = client.get("/api/runs/r1/files")
    assert tree.status_code == 200
    payload = tree.json()
    assert payload["type"] == "dir"

    view = client.get("/api/runs/r1/file", params={"path": "logs/job.out"})
    assert view.status_code == 200
    assert "hello log" in view.json()["content"]

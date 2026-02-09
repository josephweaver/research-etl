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
    monkeypatch.setattr(
        web_api,
        "fetch_pipelines",
        lambda limit=100, q=None: [{"pipeline": "pipelines/sample.yml", "total_runs": 2, "failed_runs": 0}],
    )
    monkeypatch.setattr(
        web_api,
        "fetch_pipeline_detail",
        lambda pipeline_id: {"pipeline": pipeline_id, "total_runs": 2, "failed_runs": 0, "latest_provenance": {}},
    )
    monkeypatch.setattr(
        web_api,
        "fetch_pipeline_runs",
        lambda pipeline_id, limit=50, status=None, executor=None: [{"run_id": "r1", "pipeline": pipeline_id}],
    )
    monkeypatch.setattr(web_api, "fetch_run_detail", lambda run_id: {"run_id": run_id, "status": "succeeded"})

    client = TestClient(web_api.app)
    r1 = client.get("/api/runs")
    assert r1.status_code == 200
    assert r1.json()[0]["run_id"] == "r1"

    r2 = client.get("/api/runs/r1")
    assert r2.status_code == 200
    assert r2.json()["run_id"] == "r1"

    r3 = client.get("/api/pipelines")
    assert r3.status_code == 200
    assert r3.json()[0]["pipeline"] == "pipelines/sample.yml"

    r4 = client.get("/pipelines")
    assert r4.status_code == 200

    r5 = client.get("/pipelines/pipelines%2Fsample.yml")
    assert r5.status_code == 200

    r6 = client.get("/api/pipelines/pipelines%2Fsample.yml")
    assert r6.status_code == 200
    assert r6.json()["pipeline"] == "pipelines/sample.yml"

    r7 = client.get("/api/pipelines/pipelines%2Fsample.yml/runs")
    assert r7.status_code == 200
    assert r7.json()[0]["pipeline"] == "pipelines/sample.yml"


def test_web_api_404(monkeypatch):
    pytest.importorskip("fastapi", exc_type=ImportError)
    import etl.web_api as web_api
    from fastapi.testclient import TestClient

    monkeypatch.setattr(web_api, "fetch_run_detail", lambda run_id: None)
    client = TestClient(web_api.app)
    r = client.get("/api/runs/missing")
    assert r.status_code == 404


def test_web_api_validate_action(monkeypatch, tmp_path: Path):
    pytest.importorskip("fastapi", exc_type=ImportError)
    import etl.web_api as web_api
    from etl.pipeline import Pipeline, Step
    from fastapi.testclient import TestClient

    pipeline_path = tmp_path / "p.yml"
    pipeline_path.write_text("steps: []", encoding="utf-8")
    monkeypatch.setattr(web_api, "parse_pipeline", lambda *a, **k: Pipeline(steps=[Step(name="s1", script="echo.py")]))

    client = TestClient(web_api.app)
    r = client.post("/api/actions/validate", json={"pipeline": str(pipeline_path)})
    assert r.status_code == 200
    payload = r.json()
    assert payload["valid"] is True
    assert payload["step_count"] == 1


def test_web_api_validate_requires_pipeline(monkeypatch):
    pytest.importorskip("fastapi", exc_type=ImportError)
    import etl.web_api as web_api
    from fastapi.testclient import TestClient

    client = TestClient(web_api.app)
    r = client.post("/api/actions/validate", json={})
    assert r.status_code == 400


def test_web_api_run_action_local(monkeypatch, tmp_path: Path):
    pytest.importorskip("fastapi", exc_type=ImportError)
    import etl.web_api as web_api
    from etl.executors.base import SubmissionResult, RunState, RunStatus
    from etl.pipeline import Pipeline, Step
    from fastapi.testclient import TestClient

    pipeline_path = tmp_path / "p.yml"
    pipeline_path.write_text("steps: []", encoding="utf-8")
    monkeypatch.setattr(web_api, "parse_pipeline", lambda *a, **k: Pipeline(steps=[Step(name="s1", script="echo.py")]))
    monkeypatch.setattr(web_api, "collect_run_provenance", lambda **k: {"git_commit_sha": "abc"})

    class _FakeLocalExecutor:
        def __init__(self, *a, **k):
            pass

        def submit(self, *a, **k):
            return SubmissionResult(run_id="new_run_local")

        def status(self, run_id):
            return RunStatus(run_id=run_id, state=RunState.SUCCEEDED, message="ok")

    monkeypatch.setattr(web_api, "LocalExecutor", _FakeLocalExecutor)

    client = TestClient(web_api.app)
    r = client.post(
        "/api/actions/run",
        json={
            "pipeline": str(pipeline_path),
            "executor": "local",
            "plugins_dir": "plugins",
            "workdir": ".runs",
            "dry_run": True,
        },
    )
    assert r.status_code == 200
    payload = r.json()
    assert payload["run_id"] == "new_run_local"
    assert payload["executor"] == "local"
    assert payload["state"] == "succeeded"


def test_web_api_pipeline_scoped_actions(monkeypatch, tmp_path: Path):
    pytest.importorskip("fastapi", exc_type=ImportError)
    import etl.web_api as web_api
    from etl.executors.base import SubmissionResult, RunState, RunStatus
    from etl.pipeline import Pipeline, Step
    from fastapi.testclient import TestClient
    from urllib.parse import quote

    pipeline_path = tmp_path / "p.yml"
    pipeline_path.write_text("steps: []", encoding="utf-8")
    monkeypatch.setattr(web_api, "parse_pipeline", lambda *a, **k: Pipeline(steps=[Step(name="s1", script="echo.py")]))
    monkeypatch.setattr(web_api, "collect_run_provenance", lambda **k: {"git_commit_sha": "abc"})

    class _FakeLocalExecutor:
        def __init__(self, *a, **k):
            pass

        def submit(self, *a, **k):
            return SubmissionResult(run_id="new_run_local")

        def status(self, run_id):
            return RunStatus(run_id=run_id, state=RunState.SUCCEEDED, message="ok")

    monkeypatch.setattr(web_api, "LocalExecutor", _FakeLocalExecutor)

    client = TestClient(web_api.app)
    pipeline_id = quote(pipeline_path.as_posix(), safe="")
    v = client.post(f"/api/pipelines/{pipeline_id}/validate", json={})
    assert v.status_code == 200
    assert Path(v.json()["pipeline"]).resolve() == pipeline_path.resolve()

    r = client.post(
        f"/api/pipelines/{pipeline_id}/run",
        json={"executor": "local", "dry_run": True},
    )
    assert r.status_code == 200
    assert Path(r.json()["pipeline"]).resolve() == pipeline_path.resolve()


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

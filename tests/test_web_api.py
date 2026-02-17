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
        lambda limit=50, status=None, executor=None, q=None, project_id=None: [{"run_id": "r1", "status": "succeeded"}],
    )
    monkeypatch.setattr(
        web_api,
        "fetch_pipelines",
        lambda limit=100, q=None, project_id=None: [{"pipeline": "pipelines/sample.yml", "total_runs": 2, "failed_runs": 0}],
    )
    monkeypatch.setattr(
        web_api,
        "fetch_pipeline_detail",
        lambda pipeline_id, project_id=None: {"pipeline": pipeline_id, "total_runs": 2, "failed_runs": 0, "latest_provenance": {}},
    )
    monkeypatch.setattr(
        web_api,
        "fetch_pipeline_runs",
        lambda pipeline_id, limit=50, status=None, executor=None, project_id=None: [{"run_id": "r1", "pipeline": pipeline_id}],
    )
    monkeypatch.setattr(
        web_api,
        "fetch_pipeline_validations",
        lambda pipeline_id, limit=50, project_id=None: [{"validation_id": 1, "pipeline": pipeline_id, "valid": True}],
    )
    monkeypatch.setattr(
        web_api,
        "fetch_datasets",
        lambda limit=100, q=None: [{"dataset_id": "serve.demo", "status": "active", "latest_version": "v2"}],
    )
    monkeypatch.setattr(
        web_api,
        "fetch_dataset_detail",
        lambda dataset_id: {"dataset_id": dataset_id, "versions": [], "locations": [], "dictionary_entries": []},
    )
    monkeypatch.setattr(web_api, "fetch_run_detail", lambda run_id: {"run_id": run_id, "status": "succeeded"})

    client = TestClient(web_api.app)
    r0 = client.get("/")
    assert r0.status_code == 200
    assert "Failed/running triage inbox" in r0.text

    r1 = client.get("/api/runs")
    assert r1.status_code == 200
    assert r1.json()[0]["run_id"] == "r1"

    r2 = client.get("/api/runs/r1")
    assert r2.status_code == 200
    assert r2.json()["run_id"] == "r1"

    r2b = client.get("/api/runs/r1/live")
    assert r2b.status_code == 200
    assert r2b.json()["run_id"] == "r1"

    r3 = client.get("/api/pipelines")
    assert r3.status_code == 200
    assert r3.json()[0]["pipeline"] == "pipelines/sample.yml"

    r4 = client.get("/pipelines")
    assert r4.status_code == 200

    r4c = client.get("/plugins")
    assert r4c.status_code == 200

    r4d = client.get("/datasets")
    assert r4d.status_code == 200

    r4b = client.get("/pipelines/new")
    assert r4b.status_code == 200

    r5 = client.get("/pipelines/pipelines%2Fsample.yml")
    assert r5.status_code == 200

    r5c = client.get("/pipelines/pipelines%2Fsample.yml/edit")
    assert r5c.status_code == 200

    r5b = client.get("/runs/r1/live")
    assert r5b.status_code == 200

    r6 = client.get("/api/pipelines/pipelines%2Fsample.yml")
    assert r6.status_code == 200
    assert r6.json()["pipeline"] == "pipelines/sample.yml"

    r7 = client.get("/api/pipelines/pipelines%2Fsample.yml/runs")
    assert r7.status_code == 200
    assert r7.json()[0]["pipeline"] == "pipelines/sample.yml"

    r8 = client.get("/api/pipelines/pipelines%2Fsample.yml/validations")
    assert r8.status_code == 200
    assert r8.json()[0]["pipeline"] == "pipelines/sample.yml"

    r9 = client.get("/api/datasets")
    assert r9.status_code == 200
    assert r9.json()[0]["dataset_id"] == "serve.demo"

    r10 = client.get("/api/datasets/serve.demo")
    assert r10.status_code == 200
    assert r10.json()["dataset_id"] == "serve.demo"


def test_web_api_project_filters(monkeypatch):
    pytest.importorskip("fastapi", exc_type=ImportError)
    import etl.web_api as web_api
    from fastapi.testclient import TestClient

    called = {}

    def _fetch_runs(limit=50, status=None, executor=None, q=None, project_id=None):
        called["project_id"] = project_id
        return []

    monkeypatch.setattr(web_api, "fetch_runs", _fetch_runs)
    client = TestClient(web_api.app)
    r = client.get("/api/runs", params={"project_id": "Land Core"})
    assert r.status_code == 200
    assert called["project_id"] == "land_core"


def test_web_api_denies_cross_project_access(monkeypatch):
    pytest.importorskip("fastapi", exc_type=ImportError)
    import etl.web_api as web_api
    from fastapi.testclient import TestClient

    monkeypatch.setattr(web_api, "fetch_runs", lambda **kwargs: [])
    client = TestClient(web_api.app)
    r = client.get("/api/runs", params={"project_id": "gee_lee", "as_user": "land-core"})
    assert r.status_code == 403


def test_web_api_admin_reads_multiple_projects(monkeypatch):
    pytest.importorskip("fastapi", exc_type=ImportError)
    import etl.web_api as web_api
    from fastapi.testclient import TestClient

    def _fetch_runs(limit=50, status=None, executor=None, q=None, project_id=None):
        return [{"run_id": f"r-{project_id}", "project_id": project_id, "started_at": "2026-02-11T00:00:00Z"}]

    monkeypatch.setattr(web_api, "fetch_runs", _fetch_runs)
    client = TestClient(web_api.app)
    r = client.get("/api/runs", params={"as_user": "admin"})
    assert r.status_code == 200
    payload = r.json()
    assert {x["project_id"] for x in payload} == {"land_core", "gee_lee"}


def test_web_api_builder_source_and_validate(tmp_path: Path):
    pytest.importorskip("fastapi", exc_type=ImportError)
    import etl.web_api as web_api
    from fastapi.testclient import TestClient

    p = tmp_path / "draft.yml"
    p.write_text(
        "dirs:\n  workdir: .runs/work\n  logdir: .runs/log\nsteps:\n  - name: s1\n    script: echo.py\n",
        encoding="utf-8",
    )
    client = TestClient(web_api.app)

    s = client.get("/api/builder/source", params={"pipeline": str(p)})
    assert s.status_code == 200
    assert "script: echo.py" in s.json()["yaml_text"]

    v = client.post("/api/builder/validate", json={"yaml_text": p.read_text(encoding="utf-8")})
    assert v.status_code == 200
    assert v.json()["valid"] is True
    assert v.json()["step_count"] == 1


def test_web_api_builder_validate_can_skip_dir_contract() -> None:
    pytest.importorskip("fastapi", exc_type=ImportError)
    import etl.web_api as web_api
    from fastapi.testclient import TestClient

    client = TestClient(web_api.app)
    yaml_text = "steps:\n  - name: s1\n    script: echo.py\n"
    r = client.post("/api/builder/validate", json={"yaml_text": yaml_text, "require_dir_contract": False})
    assert r.status_code == 200
    payload = r.json()
    assert payload["valid"] is True
    assert payload["step_count"] == 1


def test_web_api_builder_validate_rejects_unresolved_step_inputs() -> None:
    pytest.importorskip("fastapi", exc_type=ImportError)
    import etl.web_api as web_api
    from fastapi.testclient import TestClient

    client = TestClient(web_api.app)
    yaml_text = "\n".join(
        [
            "dirs:",
            "  workdir: .out/work",
            "  logdir: .out/log",
            "steps:",
            "  - name: s1",
            "    plugin: echo.py",
            "    args:",
            "      src: \"{env.datadir}/x\"",
        ]
    ) + "\n"
    r = client.post("/api/builder/validate", json={"yaml_text": yaml_text})
    assert r.status_code == 400
    detail = r.json().get("detail")
    assert isinstance(detail, dict)
    assert "Unresolved step inputs detected" in str(detail.get("message"))
    assert isinstance(detail.get("unresolved_inputs"), list)


def test_web_api_builder_validate_can_allow_unresolved_step_inputs() -> None:
    pytest.importorskip("fastapi", exc_type=ImportError)
    import etl.web_api as web_api
    from fastapi.testclient import TestClient

    client = TestClient(web_api.app)
    yaml_text = "\n".join(
        [
            "dirs:",
            "  workdir: .out/work",
            "  logdir: .out/log",
            "steps:",
            "  - name: s1",
            "    plugin: echo.py",
            "    args:",
            "      src: \"{env.datadir}/x\"",
        ]
    ) + "\n"
    r = client.post(
        "/api/builder/validate",
        json={"yaml_text": yaml_text, "require_resolved_inputs": False},
    )
    assert r.status_code == 200
    payload = r.json()
    assert payload["valid"] is True
    unresolved = payload.get("unresolved_inputs") or []
    assert len(unresolved) == 1
    assert unresolved[0]["field"] == "args.src"


def test_web_api_builder_validate_allows_sys_placeholders_in_step_inputs() -> None:
    pytest.importorskip("fastapi", exc_type=ImportError)
    import etl.web_api as web_api
    from fastapi.testclient import TestClient

    client = TestClient(web_api.app)
    yaml_text = "\n".join(
        [
            "dirs:",
            "  workdir: .out/work",
            "  logdir: .out/log",
            "steps:",
            "  - name: s1",
            "    plugin: echo.py",
            "    args:",
            "      run_tag: \"{sys.now.yymmdd}-{sys.run.short_id}\"",
        ]
    ) + "\n"
    r = client.post("/api/builder/validate", json={"yaml_text": yaml_text})
    assert r.status_code == 200
    payload = r.json()
    assert payload["valid"] is True


def test_web_api_builder_validate_allows_prior_step_output_var_tokens() -> None:
    pytest.importorskip("fastapi", exc_type=ImportError)
    import etl.web_api as web_api
    from fastapi.testclient import TestClient

    client = TestClient(web_api.app)
    yaml_text = "\n".join(
        [
            "dirs:",
            "  workdir: .out/work",
            "  logdir: .out/log",
            "steps:",
            "  - name: s1",
            "    plugin: gdrive_download.py",
            "    args:",
            "      out: .out/work/raw",
            "    output_var: staged_raw",
            "  - name: s2",
            "    plugin: archive_extract.py",
            "    args:",
            "      archive: \"{staged_raw.output_dir}/ReleaseData.7z\"",
            "      out: .out/work/unzip",
        ]
    ) + "\n"
    r = client.post("/api/builder/validate", json={"yaml_text": yaml_text})
    assert r.status_code == 200
    payload = r.json()
    assert payload["valid"] is True


def test_web_api_builder_resolve_text_supports_output_var_placeholders() -> None:
    pytest.importorskip("fastapi", exc_type=ImportError)
    import etl.web_api as web_api
    from fastapi.testclient import TestClient

    client = TestClient(web_api.app)
    yaml_text = "\n".join(
        [
            "dirs:",
            "  workdir: .out/work",
            "  logdir: .out/log",
            "steps:",
            "  - name: s1",
            "    plugin: gdrive_download.py",
            "    args:",
            "      out: .out/work/raw",
            "    output_var: staged_raw",
        ]
    ) + "\n"
    r = client.post(
        "/api/builder/resolve-text",
        json={"yaml_text": yaml_text, "value": "{staged_raw.output_dir}/ReleaseData.7z"},
    )
    assert r.status_code == 200
    payload = r.json()
    assert payload["resolved"] == ".out/work/raw/ReleaseData.7z"


def test_web_api_builder_resolve_text() -> None:
    pytest.importorskip("fastapi", exc_type=ImportError)
    import etl.web_api as web_api
    from fastapi.testclient import TestClient

    client = TestClient(web_api.app)
    yaml_text = "\n".join(
        [
            "vars:",
            "  name: demo",
            "dirs:",
            "  workdir: .out/work",
            "steps:",
            "  - plugin: echo.py",
        ]
    ) + "\n"
    r = client.post("/api/builder/resolve-text", json={"yaml_text": yaml_text, "value": "{dirs.workdir}/{vars.name}"})
    assert r.status_code == 200
    payload = r.json()
    assert payload["resolved"] == ".out/work/demo"
    assert payload["changed"] is True


def test_web_api_builder_environments_lists_envs(monkeypatch, tmp_path: Path) -> None:
    pytest.importorskip("fastapi", exc_type=ImportError)
    import etl.web_api as web_api
    from fastapi.testclient import TestClient

    cfg = tmp_path / "config"
    cfg.mkdir(parents=True, exist_ok=True)
    env_cfg = cfg / "environments.yml"
    env_cfg.write_text(
        "\n".join(
            [
                "environments:",
                "  local:",
                "    executor: local",
                "    datadir: .out/work",
                "  hpcc_alpha:",
                "    executor: slurm",
                "    datadir: /scratch/work",
            ]
        )
        + "\n",
        encoding="utf-8",
    )
    client = TestClient(web_api.app)
    r = client.get("/api/builder/environments", params={"environments_config": str(env_cfg)})
    assert r.status_code == 200
    payload = r.json()
    assert payload["environments"] == ["hpcc_alpha", "local"]


def test_web_api_builder_resolve_text_with_env_context(tmp_path: Path) -> None:
    pytest.importorskip("fastapi", exc_type=ImportError)
    import etl.web_api as web_api
    from fastapi.testclient import TestClient

    cfg = tmp_path / "config"
    cfg.mkdir(parents=True, exist_ok=True)
    env_cfg = cfg / "environments.yml"
    env_cfg.write_text(
        "\n".join(
            [
                "environments:",
                "  local:",
                "    executor: local",
                "    datadir: .out/work",
            ]
        )
        + "\n",
        encoding="utf-8",
    )
    client = TestClient(web_api.app)
    yaml_text = "\n".join(
        [
            "vars:",
            "  pipe:",
            "    name: yanroy",
            "dirs:",
            "  raw_cache: \"{env.datadir}/{pipe.name}/cache/raw\"",
            "steps:",
            "  - plugin: echo.py",
        ]
    ) + "\n"
    r = client.post(
        "/api/builder/resolve-text",
        json={
            "yaml_text": yaml_text,
            "value": "{dirs.raw_cache}",
            "environments_config": str(env_cfg),
            "env": "local",
        },
    )
    assert r.status_code == 200
    payload = r.json()
    assert payload["resolved"] == ".out/work/yanroy/cache/raw"


def test_web_api_builder_resolve_text_with_sys_placeholder() -> None:
    pytest.importorskip("fastapi", exc_type=ImportError)
    import etl.web_api as web_api
    from fastapi.testclient import TestClient

    client = TestClient(web_api.app)
    yaml_text = "\n".join(
        [
            "dirs:",
            "  workdir: .out/work",
            "  logdir: .out/log",
            "steps:",
            "  - plugin: echo.py",
        ]
    ) + "\n"
    r = client.post(
        "/api/builder/resolve-text",
        json={"yaml_text": yaml_text, "value": "{sys.run.short_id}"},
    )
    assert r.status_code == 200
    payload = r.json()
    assert payload["resolved"] == "abcd0123"


def test_web_api_builder_namespace_endpoint(tmp_path: Path) -> None:
    pytest.importorskip("fastapi", exc_type=ImportError)
    import etl.web_api as web_api
    from fastapi.testclient import TestClient

    cfg = tmp_path / "config"
    cfg.mkdir(parents=True, exist_ok=True)
    env_cfg = cfg / "environments.yml"
    env_cfg.write_text(
        "\n".join(
            [
                "environments:",
                "  local:",
                "    executor: local",
                "    datadir: .out/work",
            ]
        )
        + "\n",
        encoding="utf-8",
    )
    client = TestClient(web_api.app)
    yaml_text = "\n".join(
        [
            "vars:",
            "  pipe:",
            "    name: yanroy",
            "dirs:",
            "  raw_cache: \"{env.datadir}/{pipe.name}/cache/raw\"",
            "steps:",
            "  - plugin: echo.py",
        ]
    ) + "\n"
    r = client.post(
        "/api/builder/namespace",
        json={
            "yaml_text": yaml_text,
            "environments_config": str(env_cfg),
            "env": "local",
        },
    )
    assert r.status_code == 200
    payload = r.json()
    ns = payload["namespace"]
    assert "sys" in ns
    assert "now" in ns["sys"]
    assert "yymmdd_hhmmss" in ns["sys"]["now"]
    assert "run" in ns["sys"]
    assert "id" in ns["sys"]["run"]
    assert ns["sys"]["run"]["short_id"] == "abcd0123"
    assert ns["dirs"]["raw_cache"] == ".out/work/yanroy/cache/raw"
    assert ns["raw_cache"] == ".out/work/yanroy/cache/raw"
    assert ns["env"]["datadir"] == ".out/work"
    assert 1 <= int(ns["resolution"]["max_passes"]) <= 100
    assert payload["counts"]["sys"] >= 1


def test_web_api_builder_namespace_precedence_global_env_vars(tmp_path: Path) -> None:
    pytest.importorskip("fastapi", exc_type=ImportError)
    import etl.web_api as web_api
    from fastapi.testclient import TestClient

    cfg = tmp_path / "config"
    cfg.mkdir(parents=True, exist_ok=True)
    global_cfg = cfg / "global.yml"
    global_cfg.write_text(
        "\n".join(
            [
                "workdir: /g/work",
            ]
        )
        + "\n",
        encoding="utf-8",
    )
    env_cfg = cfg / "environments.yml"
    env_cfg.write_text(
        "\n".join(
            [
                "environments:",
                "  local:",
                "    executor: local",
                "    workdir: /e/work",
            ]
        )
        + "\n",
        encoding="utf-8",
    )
    client = TestClient(web_api.app)
    yaml_text = "\n".join(
        [
            "vars:",
            "  workdir: \"{env.workdir}/pipe\"",
            "dirs:",
            "  stage: \"{workdir}/stage\"",
            "steps:",
            "  - plugin: echo.py",
        ]
    ) + "\n"
    r = client.post(
        "/api/builder/namespace",
        json={
            "yaml_text": yaml_text,
            "global_config": str(global_cfg),
            "environments_config": str(env_cfg),
            "env": "local",
        },
    )
    assert r.status_code == 200
    ns = r.json()["namespace"]
    assert ns["global"]["workdir"] == "/g/work"
    assert ns["env"]["workdir"] == "/e/work"
    assert ns["vars"]["workdir"] == "/e/work/pipe"
    assert ns["workdir"] == "/e/work/pipe"
    assert ns["dirs"]["stage"] == "/e/work/pipe/stage"


def test_web_api_builder_namespace_uses_configured_resolve_max_passes(tmp_path: Path) -> None:
    pytest.importorskip("fastapi", exc_type=ImportError)
    import etl.web_api as web_api
    from fastapi.testclient import TestClient

    cfg = tmp_path / "config"
    cfg.mkdir(parents=True, exist_ok=True)
    global_cfg = cfg / "global.yml"
    global_cfg.write_text("resolve_max_passes: 9\n", encoding="utf-8")

    client = TestClient(web_api.app)
    yaml_text = "\n".join(
        [
            "vars:",
            "  a: \"{b}\"",
            "  b: \"{c}\"",
            "  c: done",
            "steps:",
            "  - plugin: echo.py",
        ]
    ) + "\n"
    r = client.post(
        "/api/builder/namespace",
        json={
            "yaml_text": yaml_text,
            "global_config": str(global_cfg),
        },
    )
    assert r.status_code == 200
    ns = r.json()["namespace"]
    assert ns["resolution"]["max_passes"] == 9
    assert ns["resolution"]["passes_used"] >= 1


def test_web_api_builder_namespace_resolves_sys_tokens_in_flat_preview() -> None:
    pytest.importorskip("fastapi", exc_type=ImportError)
    import etl.web_api as web_api
    from fastapi.testclient import TestClient

    client = TestClient(web_api.app)
    yaml_text = "\n".join(
        [
            "vars:",
            "  name: demo",
            "dirs:",
            "  workdir: \"{env.workdir}/{name}/{sys.now.yymmdd}/{sys.now.hhmmss}-{sys.run.short_id}\"",
            "  logdir: \"{workdir}/logs\"",
            "steps:",
            "  - plugin: echo.py",
        ]
    ) + "\n"
    r = client.post(
        "/api/builder/namespace",
        json={
            "yaml_text": yaml_text,
            "env": "local",
        },
    )
    assert r.status_code == 200
    ns = r.json()["namespace"]
    assert "abcd0123" in str(ns["workdir"])
    assert "abcd0123" in str(ns["logdir"])
    assert "{sys.now.yymmdd}" not in str(ns["workdir"])
    assert "{sys.now.hhmmss}" not in str(ns["workdir"])


def test_web_api_builder_namespace_preview_avoids_self_recursive_growth() -> None:
    pytest.importorskip("fastapi", exc_type=ImportError)
    import etl.web_api as web_api
    from fastapi.testclient import TestClient

    client = TestClient(web_api.app)
    yaml_text = "\n".join(
        [
            "dirs:",
            "  workdir: \"{workdir}/{pipe.name}/{run_id}\"",
            "  logdir: \"{workdir}/logs\"",
            "steps:",
            "  - plugin: echo.py",
        ]
    ) + "\n"
    r = client.post("/api/builder/namespace", json={"yaml_text": yaml_text})
    assert r.status_code == 200
    ns = r.json()["namespace"]
    # Should not explode via recursive self-substitution.
    assert len(str(ns["workdir"])) < 200
    assert str(ns["workdir"]).count("{pipe.name}") <= 1


def test_web_api_builder_source_parses_plugin_args_model(tmp_path: Path):
    pytest.importorskip("fastapi", exc_type=ImportError)
    import etl.web_api as web_api
    from fastapi.testclient import TestClient

    p = tmp_path / "draft.yml"
    p.write_text(
        "\n".join(
            [
                "steps:",
                "  - plugin: gdrive_download.py",
                "    args:",
                "      src: Data/Field_Boundaries",
                "      recursive: true",
                "      max_files: 20",
            ]
        )
        + "\n",
        encoding="utf-8",
    )
    client = TestClient(web_api.app)
    s = client.get("/api/builder/source", params={"pipeline": str(p)})
    assert s.status_code == 200
    model = s.json()["model"]
    assert model["steps"][0]["plugin"] == "gdrive_download.py"
    assert model["steps"][0]["params"]["src"] == "Data/Field_Boundaries"
    assert model["steps"][0]["params"]["recursive"] is True
    assert model["steps"][0]["params"]["max_files"] == 20


def test_web_api_builder_source_parses_foreach_glob_model(tmp_path: Path) -> None:
    pytest.importorskip("fastapi", exc_type=ImportError)
    import etl.web_api as web_api
    from fastapi.testclient import TestClient

    p = tmp_path / "draft_foreach.yml"
    p.write_text(
        "\n".join(
            [
                "steps:",
                "  - plugin: echo.py",
                "    foreach_glob: \"/tmp/data/*\"",
                "    foreach_kind: dirs",
                "    args:",
                "      msg: \"{item_name}\"",
            ]
        )
        + "\n",
        encoding="utf-8",
    )
    client = TestClient(web_api.app)
    s = client.get("/api/builder/source", params={"pipeline": str(p)})
    assert s.status_code == 200
    model = s.json()["model"]
    step = model["steps"][0]
    assert step["type"] == "foreach"
    assert step["foreach_mode"] == "glob"
    assert step["foreach_glob"] == "/tmp/data/*"
    assert step["foreach_kind"] == "dirs"


def test_web_api_builder_validate_allows_foreach_glob_item_tokens() -> None:
    pytest.importorskip("fastapi", exc_type=ImportError)
    import etl.web_api as web_api
    from fastapi.testclient import TestClient

    client = TestClient(web_api.app)
    yaml_text = "\n".join(
        [
            "dirs:",
            "  workdir: .out/work",
            "  logdir: .out/log",
            "steps:",
            "  - name: s1",
            "    plugin: gdrive_download.py",
            "    args:",
            "      out: .out/work/raw",
            "    output_var: staged_raw",
            "  - name: s2",
            "    plugin: echo.py",
            "    foreach_glob: \"{staged_raw.output_dir}/*\"",
            "    foreach_kind: dirs",
            "    args:",
            "      msg: \"{item_name}-{item_index}\"",
        ]
    ) + "\n"
    r = client.post("/api/builder/validate", json={"yaml_text": yaml_text})
    assert r.status_code == 200
    payload = r.json()
    assert payload["valid"] is True


def test_web_api_plugins_stats_recommendations(monkeypatch, tmp_path: Path):
    pytest.importorskip("fastapi", exc_type=ImportError)
    import etl.web_api as web_api
    from fastapi.testclient import TestClient

    plugins_dir = tmp_path / "plugins"
    plugins_dir.mkdir(parents=True, exist_ok=True)
    (plugins_dir / "demo.py").write_text(
        "\n".join(
            [
                "meta = {'name': 'demo', 'version': '1.0.0', 'description': 'd', 'resources': {'cpu_cores': 2}}",
                "def run(args, ctx):",
                "    return {'ok': True}",
            ]
        )
        + "\n",
        encoding="utf-8",
    )
    monkeypatch.setattr(web_api, "_resolve_builder_plugins_dir", lambda **_k: plugins_dir)
    monkeypatch.setattr(
        web_api,
        "fetch_plugin_resource_stats",
        lambda **_k: {
            "samples": 3,
            "cpu_cores_mean": 2.0,
            "cpu_cores_std": 1.0,
            "cpu_cores_samples": 3,
            "memory_gb_mean": 8.0,
            "memory_gb_std": 2.0,
            "memory_gb_samples": 3,
            "wall_minutes_mean": 20.0,
            "wall_minutes_std": 4.0,
            "wall_minutes_samples": 3,
        },
    )
    monkeypatch.setattr(web_api, "_resolve_builder_env_vars", lambda **_k: {})
    client = TestClient(web_api.app)
    r = client.get("/api/plugins/stats")
    assert r.status_code == 200
    p = r.json()["plugins"][0]
    rec = p["recommendation"]
    assert p["path"] == "demo.py"
    assert rec["samples"] == 3
    assert rec["cpu_cores"] == 3.0
    assert rec["memory_gb"] == 12.0
    assert rec["wall_minutes"] == 30.0


def test_web_api_plugins_stats_clamps_to_env_caps(monkeypatch, tmp_path: Path):
    pytest.importorskip("fastapi", exc_type=ImportError)
    import etl.web_api as web_api
    from fastapi.testclient import TestClient

    plugins_dir = tmp_path / "plugins"
    plugins_dir.mkdir(parents=True, exist_ok=True)
    (plugins_dir / "demo.py").write_text(
        "meta = {'name': 'demo', 'version': '1.0.0', 'description': 'd'}\n"
        "def run(args, ctx):\n"
        "    return {'ok': True}\n",
        encoding="utf-8",
    )
    monkeypatch.setattr(web_api, "_resolve_builder_plugins_dir", lambda **_k: plugins_dir)
    monkeypatch.setattr(
        web_api,
        "fetch_plugin_resource_stats",
        lambda **_k: {
            "samples": 10,
            "cpu_cores_mean": 10.0,
            "cpu_cores_std": 3.0,
            "cpu_cores_samples": 10,
            "memory_gb_mean": 20.0,
            "memory_gb_std": 5.0,
            "memory_gb_samples": 10,
            "wall_minutes_mean": 120.0,
            "wall_minutes_std": 20.0,
            "wall_minutes_samples": 10,
        },
    )
    monkeypatch.setattr(
        web_api,
        "_resolve_builder_env_vars",
        lambda **_k: {"max_cpus_per_task": 16, "max_mem": "32G", "max_time": "04:00:00"},
    )
    client = TestClient(web_api.app)
    r = client.get("/api/plugins/stats", params={"env": "msu"})
    assert r.status_code == 200
    rec = r.json()["plugins"][0]["recommendation"]
    assert rec["cpu_cores"] == 16.0
    assert rec["memory_gb"] == 32.0
    assert rec["wall_minutes"] == 180.0


def test_web_api_builder_source_resolves_bare_filename_under_pipelines(monkeypatch, tmp_path: Path):
    pytest.importorskip("fastapi", exc_type=ImportError)
    import etl.web_api as web_api
    from fastapi.testclient import TestClient

    monkeypatch.chdir(tmp_path)
    p = tmp_path / "pipelines" / "yanroy" / "download.yml"
    p.parent.mkdir(parents=True, exist_ok=True)
    p.write_text("steps:\n  - name: s1\n    script: echo.py\n", encoding="utf-8")
    client = TestClient(web_api.app)

    s = client.get("/api/builder/source", params={"pipeline": "download.yml"})
    assert s.status_code == 200
    assert Path(s.json()["pipeline"]).resolve() == p.resolve()


def test_web_api_builder_source_bare_filename_ambiguous(monkeypatch, tmp_path: Path):
    pytest.importorskip("fastapi", exc_type=ImportError)
    import etl.web_api as web_api
    from fastapi.testclient import TestClient

    monkeypatch.chdir(tmp_path)
    p1 = tmp_path / "pipelines" / "yanroy" / "download.yml"
    p2 = tmp_path / "pipelines" / "other" / "download.yml"
    p1.parent.mkdir(parents=True, exist_ok=True)
    p2.parent.mkdir(parents=True, exist_ok=True)
    p1.write_text("steps:\n  - script: echo.py\n", encoding="utf-8")
    p2.write_text("steps:\n  - script: echo.py\n", encoding="utf-8")
    client = TestClient(web_api.app)

    s = client.get("/api/builder/source", params={"pipeline": "download.yml"})
    assert s.status_code == 409
    assert "Ambiguous pipeline filename" in s.json()["detail"]


def test_web_api_builder_files_lists_pipeline_folder(monkeypatch, tmp_path: Path):
    pytest.importorskip("fastapi", exc_type=ImportError)
    import etl.web_api as web_api
    from fastapi.testclient import TestClient

    monkeypatch.chdir(tmp_path)
    pdir = tmp_path / "pipelines" / "sub"
    pdir.mkdir(parents=True, exist_ok=True)
    (tmp_path / "pipelines" / "a.yml").write_text("steps:\n  - script: echo.py\n", encoding="utf-8")
    (pdir / "b.yml").write_text("steps:\n  - script: echo.py\n", encoding="utf-8")
    client = TestClient(web_api.app)
    r = client.get("/api/builder/files")
    assert r.status_code == 200
    files = r.json()["files"]
    assert "a.yml" in files
    assert "sub/b.yml" in files


def test_web_api_builder_generate(monkeypatch):
    pytest.importorskip("fastapi", exc_type=ImportError)
    import etl.web_api as web_api
    from fastapi.testclient import TestClient

    monkeypatch.setattr(
        web_api,
        "generate_pipeline_draft",
        lambda intent, constraints=None, existing_yaml=None, model=None: "steps:\n  - name: s1\n    script: echo.py\n",
    )
    client = TestClient(web_api.app)
    r = client.post("/api/builder/generate", json={"intent": "download and clean data"})
    assert r.status_code == 200
    payload = r.json()
    assert payload["valid"] is True
    assert payload["attempts"] == 1
    assert payload["repaired"] is False
    assert payload["step_count"] == 1


def test_web_api_builder_generate_auto_repair(monkeypatch):
    pytest.importorskip("fastapi", exc_type=ImportError)
    import etl.web_api as web_api
    from fastapi.testclient import TestClient

    drafts = iter(
        [
            "steps:\n  - name: broken\n",  # invalid: missing script
            "steps:\n  - name: repaired\n    script: echo.py\n",  # valid
        ]
    )
    monkeypatch.setattr(
        web_api,
        "generate_pipeline_draft",
        lambda intent, constraints=None, existing_yaml=None, model=None: next(drafts),
    )
    client = TestClient(web_api.app)
    r = client.post("/api/builder/generate", json={"intent": "build a simple ETL"})
    assert r.status_code == 200
    payload = r.json()
    assert payload["valid"] is True
    assert payload["repaired"] is True
    assert payload["attempts"] == 2
    assert payload["step_names"] == ["repaired"]


def test_web_api_pipeline_save_create_and_update(monkeypatch, tmp_path: Path):
    pytest.importorskip("fastapi", exc_type=ImportError)
    import etl.web_api as web_api
    from fastapi.testclient import TestClient
    from urllib.parse import quote

    monkeypatch.chdir(tmp_path)
    client = TestClient(web_api.app)
    yaml_text = "steps:\n  - name: s1\n    script: echo.py\n"
    (tmp_path / "pipelines").mkdir(parents=True, exist_ok=True)
    create = client.post("/api/pipelines", json={"pipeline": "new_draft", "yaml_text": yaml_text})
    assert create.status_code == 200
    created_path = Path(create.json()["pipeline"])
    assert created_path.exists()
    assert created_path.name == "new_draft.yml"
    assert "echo.py" in created_path.read_text(encoding="utf-8")

    update_text = "steps:\n  - name: s1\n    script: echo.py\n  - name: s2\n    script: echo.py\n"
    pid = quote("new_draft", safe="")
    update = client.put(f"/api/pipelines/{pid}", json={"yaml_text": update_text})
    assert update.status_code == 200
    assert "s2" in created_path.read_text(encoding="utf-8")


def test_web_api_pipeline_save_project_path_admin_allowed(monkeypatch, tmp_path: Path):
    pytest.importorskip("fastapi", exc_type=ImportError)
    import etl.web_api as web_api
    from fastapi.testclient import TestClient
    from urllib.parse import quote

    monkeypatch.chdir(tmp_path)
    client = TestClient(web_api.app)
    pipeline = tmp_path / "pipelines" / "yanroy" / "download.yml"
    pipeline.parent.mkdir(parents=True, exist_ok=True)
    pipeline.write_text("steps:\n  - name: s1\n    script: echo.py\n", encoding="utf-8")
    update_text = "steps:\n  - name: s1\n    script: echo.py\n  - name: s2\n    script: echo.py\n"
    pipeline_id = quote("pipelines/yanroy/download.yml", safe="")
    r = client.put(f"/api/pipelines/{pipeline_id}", params={"as_user": "admin"}, json={"yaml_text": update_text})
    assert r.status_code == 200
    assert "s2" in pipeline.read_text(encoding="utf-8")


def test_web_api_builder_test_step(monkeypatch):
    pytest.importorskip("fastapi", exc_type=ImportError)
    import etl.web_api as web_api
    from etl.pipeline import Pipeline, Step
    from etl.runner import RunResult, StepResult
    from fastapi.testclient import TestClient

    monkeypatch.setattr(
        web_api,
        "_parse_pipeline_from_yaml_text",
        lambda yaml_text, global_config_path=None, environments_config_path=None, env_name=None: Pipeline(
            dirs={"workdir": ".runs/work", "logdir": ".runs/log"},
            steps=[Step(name="s1", script="echo.py")],
        ),
    )
    monkeypatch.setattr(
        web_api,
        "run_pipeline",
        lambda *a, **k: RunResult(
            run_id="builder_run_1",
            steps=[StepResult(step=Step(name="s1", script="echo.py"), success=True, outputs={"ok": True})],
            artifact_dir=".runs/builder/x",
        ),
    )
    client = TestClient(web_api.app)
    r = client.post("/api/builder/test-step", json={"yaml_text": "steps: []", "step_name": "s1", "dry_run": True})
    assert r.status_code == 200
    payload = r.json()
    assert payload["run_id"] == "builder_run_1"
    assert payload["step_name"] == "s1"
    assert payload["step_index"] == 0
    assert payload["success"] is True


def test_web_api_builder_test_step_with_step_index(monkeypatch):
    pytest.importorskip("fastapi", exc_type=ImportError)
    import etl.web_api as web_api
    from etl.pipeline import Pipeline, Step
    from etl.runner import RunResult, StepResult
    from fastapi.testclient import TestClient

    monkeypatch.setattr(
        web_api,
        "_parse_pipeline_from_yaml_text",
        lambda yaml_text, global_config_path=None, environments_config_path=None, env_name=None: Pipeline(
            dirs={"workdir": ".runs/work", "logdir": ".runs/log"},
            steps=[
                Step(name="s1", script="echo.py"),
                Step(name="s2", script="echo.py"),
            ]
        ),
    )
    monkeypatch.setattr(
        web_api,
        "run_pipeline",
        lambda mini, **k: RunResult(
            run_id="builder_run_2",
            steps=[StepResult(step=mini.steps[0], success=True, outputs={"ok": True})],
            artifact_dir=".runs/builder/y",
        ),
    )
    client = TestClient(web_api.app)
    r = client.post("/api/builder/test-step", json={"yaml_text": "steps: []", "step_index": 1})
    assert r.status_code == 200
    payload = r.json()
    assert payload["run_id"] == "builder_run_2"
    assert payload["step_name"] == "s2"
    assert payload["step_index"] == 1
    assert payload["success"] is True


def test_web_api_builder_test_step_uses_work_dir_from_dirs(monkeypatch):
    pytest.importorskip("fastapi", exc_type=ImportError)
    import etl.web_api as web_api
    from etl.pipeline import Pipeline, Step
    from etl.runner import RunResult, StepResult
    from fastapi.testclient import TestClient

    called = {}

    monkeypatch.setattr(
        web_api,
        "_parse_pipeline_from_yaml_text",
        lambda yaml_text, global_config_path=None, environments_config_path=None, env_name=None: Pipeline(
            dirs={"workdir": ".runs/from_dirs", "logdir": ".runs/log"},
            steps=[Step(name="s1", script="echo.py")],
        ),
    )

    def _fake_run_pipeline(*args, **kwargs):
        called["workdir"] = kwargs.get("workdir")
        return RunResult(
            run_id="builder_run_3",
            steps=[StepResult(step=Step(name="s1", script="echo.py"), success=True)],
            artifact_dir=".runs/builder/z",
        )

    monkeypatch.setattr(web_api, "run_pipeline", _fake_run_pipeline)
    client = TestClient(web_api.app)
    r = client.post("/api/builder/test-step", json={"yaml_text": "steps: []"})
    assert r.status_code == 200
    assert str(called["workdir"]).replace("\\", "/").endswith(".runs/from_dirs")


def test_web_api_builder_test_step_resolves_template_workdir(monkeypatch):
    pytest.importorskip("fastapi", exc_type=ImportError)
    import etl.web_api as web_api
    from etl.pipeline import Pipeline, Step
    from etl.runner import RunResult, StepResult
    from fastapi.testclient import TestClient

    called = {}
    monkeypatch.setattr(
        web_api,
        "_parse_pipeline_from_yaml_text",
        lambda yaml_text, global_config_path=None, environments_config_path=None, env_name=None: Pipeline(
            vars={"name": "yanroy"},
            dirs={"workdir": "{env.workdir}/{name}/{sys.now.yymmdd}/{sys.now.hhmmss}-{sys.run.short_id}"},
            steps=[Step(name="s1", script="echo.py")],
        ),
    )
    monkeypatch.setattr(web_api, "_resolve_global_vars", lambda *_a, **_k: {})
    monkeypatch.setattr(web_api, "_resolve_builder_env_vars", lambda **_k: {"workdir": ".out/work"})
    monkeypatch.setattr(web_api, "_raw_vars_dirs_from_yaml_text", lambda _text: ({"name": "yanroy"}, {}))

    def _fake_run_pipeline(*args, **kwargs):
        called["workdir"] = str(kwargs.get("workdir") or "")
        return RunResult(
            run_id="builder_run_4",
            steps=[StepResult(step=Step(name="s1", script="echo.py"), success=True)],
            artifact_dir=".runs/builder/w",
        )

    monkeypatch.setattr(web_api, "run_pipeline", _fake_run_pipeline)
    client = TestClient(web_api.app)
    r = client.post(
        "/api/builder/test-step",
        json={
            "yaml_text": "steps: []",
            "env": "local",
            "run_id": "12345678deadbeef12345678deadbeef",
            "run_started_at": "2026-02-15T08:31:19Z",
        },
    )
    assert r.status_code == 200
    assert "{sys." not in called["workdir"]
    assert "{env." not in called["workdir"]
    assert "{name}" not in called["workdir"]
    assert called["workdir"].replace("\\", "/").startswith(".out/work/yanroy/")
    assert "/260215/083119-12345678" in called["workdir"].replace("\\", "/")


def test_web_api_builder_test_step_falls_back_when_workdir_unresolved(monkeypatch):
    pytest.importorskip("fastapi", exc_type=ImportError)
    import etl.web_api as web_api
    from etl.pipeline import Pipeline, Step
    from etl.runner import RunResult, StepResult
    from fastapi.testclient import TestClient

    called = {}
    monkeypatch.setattr(
        web_api,
        "_parse_pipeline_from_yaml_text",
        lambda yaml_text, global_config_path=None, environments_config_path=None, env_name=None: Pipeline(
            vars={"name": "yanroy"},
            dirs={"workdir": "{env.workdir}/{name}"},
            steps=[Step(name="s1", script="echo.py")],
        ),
    )
    monkeypatch.setattr(web_api, "_resolve_global_vars", lambda *_a, **_k: {})
    monkeypatch.setattr(web_api, "_resolve_builder_env_vars", lambda **_k: {})
    monkeypatch.setattr(web_api, "_raw_vars_dirs_from_yaml_text", lambda _text: ({"name": "yanroy"}, {}))

    def _fake_run_pipeline(*args, **kwargs):
        called["workdir"] = str(kwargs.get("workdir") or "")
        return RunResult(
            run_id="builder_run_5",
            steps=[StepResult(step=Step(name="s1", script="echo.py"), success=True)],
            artifact_dir=".runs/builder/q",
        )

    monkeypatch.setattr(web_api, "run_pipeline", _fake_run_pipeline)
    client = TestClient(web_api.app)
    r = client.post("/api/builder/test-step", json={"yaml_text": "steps: []"})
    assert r.status_code == 200
    assert called["workdir"].replace("\\", "/").endswith(".runs/builder")


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
    from etl.pipeline import Pipeline, Step
    from fastapi.testclient import TestClient

    pipeline_path = tmp_path / "p.yml"
    pipeline_path.write_text("steps: []", encoding="utf-8")
    monkeypatch.setattr(web_api, "parse_pipeline", lambda *a, **k: Pipeline(steps=[Step(name="s1", script="echo.py")]))
    monkeypatch.setattr(web_api, "collect_run_provenance", lambda **k: {"git_commit_sha": "abc"})
    web_api._ACTIVE_LOCAL_RUN_KEYS.clear()
    web_api._LOCAL_RUN_SNAPSHOT.clear()
    called = {}

    class _FakeLocalExecutor:
        def __init__(self, *a, **k):
            pass

    monkeypatch.setattr(web_api, "LocalExecutor", _FakeLocalExecutor)
    monkeypatch.setattr(
        web_api,
        "_submit_local_run_async",
        lambda **kwargs: called.update(kwargs),
    )

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
    assert payload["executor"] == "local"
    assert payload["state"] == "queued"
    assert payload["message"].startswith("Run accepted")
    assert called["run_id"] == payload["run_id"]


def test_web_api_run_action_ignores_unresolved_workdir_payload(monkeypatch, tmp_path: Path):
    pytest.importorskip("fastapi", exc_type=ImportError)
    import etl.web_api as web_api
    from etl.pipeline import Pipeline, Step
    from fastapi.testclient import TestClient

    pipeline_path = tmp_path / "p.yml"
    pipeline_path.write_text("steps: []", encoding="utf-8")
    monkeypatch.setattr(web_api, "parse_pipeline", lambda *a, **k: Pipeline(steps=[Step(name="s1", script="echo.py")]))
    monkeypatch.setattr(web_api, "collect_run_provenance", lambda **k: {"git_commit_sha": "abc"})
    monkeypatch.setattr(
        web_api,
        "_resolve_execution_env",
        lambda *a, **k: ({"workdir": ".out/work"}, None, "local"),
    )
    web_api._ACTIVE_LOCAL_RUN_KEYS.clear()
    web_api._LOCAL_RUN_SNAPSHOT.clear()

    seen = {}

    class _FakeLocalExecutor:
        def __init__(self, *a, **k):
            seen["workdir"] = str(k.get("workdir") or "")

    monkeypatch.setattr(web_api, "LocalExecutor", _FakeLocalExecutor)
    monkeypatch.setattr(web_api, "_submit_local_run_async", lambda **kwargs: None)

    client = TestClient(web_api.app)
    r = client.post(
        "/api/actions/run",
        json={
            "pipeline": str(pipeline_path),
            "executor": "local",
            "plugins_dir": "plugins",
            "workdir": "{env.workdir}/{name}",
            "dry_run": True,
        },
    )
    assert r.status_code == 200
    assert seen["workdir"].replace("\\", "/").endswith(".out/work")


def test_web_api_pipeline_scoped_actions(monkeypatch, tmp_path: Path):
    pytest.importorskip("fastapi", exc_type=ImportError)
    import etl.web_api as web_api
    from etl.pipeline import Pipeline, Step
    from fastapi.testclient import TestClient
    from urllib.parse import quote

    pipeline_path = tmp_path / "p.yml"
    pipeline_path.write_text("steps: []", encoding="utf-8")
    monkeypatch.setattr(web_api, "parse_pipeline", lambda *a, **k: Pipeline(steps=[Step(name="s1", script="echo.py")]))
    monkeypatch.setattr(web_api, "collect_run_provenance", lambda **k: {"git_commit_sha": "abc"})
    web_api._ACTIVE_LOCAL_RUN_KEYS.clear()
    web_api._LOCAL_RUN_SNAPSHOT.clear()

    class _FakeLocalExecutor:
        def __init__(self, *a, **k):
            pass

    monkeypatch.setattr(web_api, "LocalExecutor", _FakeLocalExecutor)
    monkeypatch.setattr(web_api, "_submit_local_run_async", lambda **kwargs: None)

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
    assert r.json()["state"] == "queued"


def test_web_api_run_action_local_dedupes_active_submission(monkeypatch, tmp_path: Path):
    pytest.importorskip("fastapi", exc_type=ImportError)
    import etl.web_api as web_api
    from etl.pipeline import Pipeline, Step
    from fastapi.testclient import TestClient

    pipeline_path = tmp_path / "p.yml"
    pipeline_path.write_text("steps: []", encoding="utf-8")
    monkeypatch.setattr(web_api, "parse_pipeline", lambda *a, **k: Pipeline(steps=[Step(name="s1", script="echo.py")]))
    monkeypatch.setattr(web_api, "collect_run_provenance", lambda **k: {"git_commit_sha": "abc"})
    web_api._ACTIVE_LOCAL_RUN_KEYS.clear()
    web_api._LOCAL_RUN_SNAPSHOT.clear()

    class _FakeLocalExecutor:
        def __init__(self, *a, **k):
            pass

    monkeypatch.setattr(web_api, "LocalExecutor", _FakeLocalExecutor)
    monkeypatch.setattr(web_api, "_submit_local_run_async", lambda **kwargs: None)

    client = TestClient(web_api.app)
    first = client.post("/api/actions/run", json={"pipeline": str(pipeline_path), "executor": "local", "dry_run": True})
    assert first.status_code == 200
    first_run_id = first.json()["run_id"]
    second = client.post("/api/actions/run", json={"pipeline": str(pipeline_path), "executor": "local", "dry_run": True})
    assert second.status_code == 409
    detail = second.json()["detail"]
    assert detail["run_id"] == first_run_id


def test_web_api_stop_local_queued_run(monkeypatch):
    pytest.importorskip("fastapi", exc_type=ImportError)
    import etl.web_api as web_api
    from fastapi.testclient import TestClient

    web_api._ACTIVE_LOCAL_RUN_KEYS.clear()
    web_api._LOCAL_RUN_SNAPSHOT.clear()
    web_api._LOCAL_RUN_KEY_BY_RUN_ID.clear()
    web_api._LOCAL_RUN_FUTURES.clear()
    web_api._LOCAL_RUN_CANCEL_REQUESTED.clear()
    web_api._LOCAL_RUN_SNAPSHOT["r_stop_q"] = {"state": "queued"}

    class _FakeFuture:
        def cancel(self):
            return True

    web_api._LOCAL_RUN_FUTURES["r_stop_q"] = _FakeFuture()
    web_api._LOCAL_RUN_KEY_BY_RUN_ID["r_stop_q"] = "k1"
    web_api._ACTIVE_LOCAL_RUN_KEYS["k1"] = "r_stop_q"
    monkeypatch.setattr(
        web_api,
        "fetch_run_header",
        lambda run_id: {"run_id": run_id, "pipeline": "pipelines/sample.yml", "executor": "local", "status": "queued"},
    )
    monkeypatch.setattr(web_api, "upsert_run_status", lambda **kwargs: None)

    client = TestClient(web_api.app)
    r = client.post("/api/runs/r_stop_q/stop")
    assert r.status_code == 200
    payload = r.json()
    assert payload["state"] == "cancelled"
    assert "r_stop_q" not in web_api._ACTIVE_LOCAL_RUN_KEYS.values()


def test_web_api_stop_local_running_marks_cancel_requested(monkeypatch):
    pytest.importorskip("fastapi", exc_type=ImportError)
    import etl.web_api as web_api
    from fastapi.testclient import TestClient

    web_api._ACTIVE_LOCAL_RUN_KEYS.clear()
    web_api._LOCAL_RUN_SNAPSHOT.clear()
    web_api._LOCAL_RUN_KEY_BY_RUN_ID.clear()
    web_api._LOCAL_RUN_FUTURES.clear()
    web_api._LOCAL_RUN_CANCEL_REQUESTED.clear()
    web_api._LOCAL_RUN_SNAPSHOT["r_stop_r"] = {"state": "running"}

    monkeypatch.setattr(
        web_api,
        "fetch_run_header",
        lambda run_id: {"run_id": run_id, "pipeline": "pipelines/sample.yml", "executor": "local", "status": "running"},
    )
    monkeypatch.setattr(web_api, "upsert_run_status", lambda **kwargs: None)

    client = TestClient(web_api.app)
    r = client.post("/api/runs/r_stop_r/stop")
    assert r.status_code == 200
    payload = r.json()
    assert payload["state"] == "cancel_requested"
    assert "r_stop_r" in web_api._LOCAL_RUN_CANCEL_REQUESTED


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


def test_web_api_run_live_payload(monkeypatch):
    pytest.importorskip("fastapi", exc_type=ImportError)
    import etl.web_api as web_api
    from fastapi.testclient import TestClient

    monkeypatch.setattr(
        web_api,
        "fetch_run_detail",
        lambda run_id: {
            "run_id": run_id,
            "pipeline": "pipelines/sample.yml",
            "status": "running",
            "success": False,
            "executor": "slurm",
            "started_at": "2026-02-09T10:00:00Z",
            "ended_at": None,
            "steps": [
                {"step_name": "s1", "success": True, "skipped": False},
                {"step_name": "s2", "success": False, "skipped": False},
                {"step_name": "s3", "success": True, "skipped": True},
            ],
            "attempts": [
                {"step_name": "s2", "success": False, "skipped": False, "ended_at": None},
                {"step_name": "s1", "success": True, "skipped": False, "ended_at": "2026-02-09T10:01:00Z"},
            ],
            "events": [
                {"event_id": 1, "event_type": "run_started"},
                {"event_id": 2, "event_type": "batch_started"},
            ],
            "provenance": {"git_commit_sha": "abc"},
        },
    )
    client = TestClient(web_api.app)
    r = client.get("/api/runs/r_live/live")
    assert r.status_code == 200
    payload = r.json()
    assert payload["status"] == "running"
    assert payload["active_attempt_count"] == 1
    assert payload["completed_step_count"] == 1
    assert payload["failed_step_count"] == 1
    assert payload["skipped_step_count"] == 1
    assert payload["latest_event"]["event_type"] == "batch_started"


def test_web_api_live_log_from_local_snapshot(monkeypatch):
    pytest.importorskip("fastapi", exc_type=ImportError)
    import etl.web_api as web_api
    from fastapi.testclient import TestClient

    web_api._LOCAL_RUN_SNAPSHOT.clear()
    web_api._LOCAL_RUN_LOG_RING.clear()
    monkeypatch.setattr(web_api, "fetch_run_header", lambda run_id: None)
    web_api._LOCAL_RUN_SNAPSHOT["r_live_local"] = {"state": "running", "project_id": "land_core"}
    web_api._LOCAL_RUN_LOG_RING["r_live_local"] = ["line1", "line2"]

    client = TestClient(web_api.app)
    r = client.get("/api/runs/r_live_local/live-log")
    assert r.status_code == 200
    payload = r.json()
    assert payload["run_id"] == "r_live_local"
    assert payload["state"] == "running"
    assert payload["lines"][-1] == "line2"


def test_web_api_live_log_uses_header_scope(monkeypatch):
    pytest.importorskip("fastapi", exc_type=ImportError)
    import etl.web_api as web_api
    from fastapi.testclient import TestClient

    monkeypatch.setattr(
        web_api,
        "fetch_run_header",
        lambda run_id: {"run_id": run_id, "project_id": "land_core", "executor": "local", "pipeline": "pipelines/sample.yml"},
    )
    client = TestClient(web_api.app)
    r = client.get("/api/runs/r_hdr/live-log", params={"as_user": "land-core"})
    assert r.status_code == 200
    payload = r.json()
    assert payload["run_id"] == "r_hdr"

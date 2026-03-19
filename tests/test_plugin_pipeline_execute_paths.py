from __future__ import annotations

import importlib.util
from pathlib import Path

from etl.plugins.base import PluginContext


def _load_module():
    path = Path("plugins/pipeline_execute.py").resolve()
    spec = importlib.util.spec_from_file_location("pipeline_execute", path)
    assert spec is not None
    assert spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def _ctx(tmp_path: Path) -> PluginContext:
    return PluginContext(run_id="r1", workdir=tmp_path, log=lambda *a, **k: None)


def test_pipeline_execute_resolve_path_prefers_etl_repo_root(monkeypatch, tmp_path: Path) -> None:
    pipeline_execute = _load_module()
    repo_root = tmp_path / "repo"
    child = repo_root / "pipelines" / "prism" / "child.yml"
    child.parent.mkdir(parents=True, exist_ok=True)
    child.write_text("steps: []\n", encoding="utf-8")
    monkeypatch.setenv("ETL_REPO_ROOT", str(repo_root))

    resolved = pipeline_execute._resolve_path("pipelines/prism/child.yml", _ctx(tmp_path / "work"))

    assert resolved == child.resolve()

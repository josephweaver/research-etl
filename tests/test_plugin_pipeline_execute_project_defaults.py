from __future__ import annotations

import importlib.util
from pathlib import Path


def _load_module():
    path = Path("plugins/pipeline_execute.py").resolve()
    spec = importlib.util.spec_from_file_location("pipeline_execute", path)
    assert spec is not None
    assert spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def test_pipeline_execute_build_cmd_uses_project_id_from_env(monkeypatch, tmp_path: Path) -> None:
    pipeline_execute = _load_module()
    repo_root = tmp_path / "repo"
    (repo_root / "config").mkdir(parents=True, exist_ok=True)
    for name in ["global.yml", "projects.yml", "environments.yml"]:
        (repo_root / "config" / name).write_text("", encoding="utf-8")
    monkeypatch.setenv("ETL_REPO_ROOT", str(repo_root))
    monkeypatch.setenv("ETL_PROJECT_ID", "crop_insurance")

    cmd = pipeline_execute._build_cmd({}, repo_root / "pipelines" / "child.yml")

    assert "--project-id" in cmd
    assert "crop_insurance" in cmd

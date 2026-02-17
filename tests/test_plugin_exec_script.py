from __future__ import annotations

import os
from pathlib import Path

from etl.plugins.base import PluginContext, load_plugin


def _ctx(tmp_path: Path) -> PluginContext:
    return PluginContext(run_id="r1", workdir=tmp_path, log=lambda *a, **k: None)


def test_exec_script_runs_python_and_returns_output(tmp_path: Path) -> None:
    plugin = load_plugin(Path("plugins/exec_script.py"))
    script = tmp_path / "hello.py"
    script.write_text("print('hello from script')\n", encoding="utf-8")

    out = plugin.run({"script": str(script)}, _ctx(tmp_path))
    assert out["return_code"] == 0
    assert "hello from script" in out["stdout"]
    assert out["script"].endswith("/hello.py")


def test_exec_script_fails_when_script_returns_nonzero(tmp_path: Path) -> None:
    plugin = load_plugin(Path("plugins/exec_script.py"))
    script = tmp_path / "boom.py"
    script.write_text("import sys\nprint('bad')\nsys.exit(2)\n", encoding="utf-8")

    try:
        plugin.run({"script": str(script)}, _ctx(tmp_path))
        assert False, "expected failure"
    except RuntimeError as exc:
        assert "script failed rc=2" in str(exc)


def test_exec_script_resolves_relative_script_from_etl_repo_root(tmp_path: Path, monkeypatch) -> None:
    plugin = load_plugin(Path("plugins/exec_script.py"))
    repo_root = tmp_path / "repo"
    step_dir = tmp_path / "work" / "step"
    script = repo_root / "scripts" / "yanroy" / "hello.py"
    script.parent.mkdir(parents=True, exist_ok=True)
    step_dir.mkdir(parents=True, exist_ok=True)
    script.write_text("print('ok from repo root')\n", encoding="utf-8")

    monkeypatch.setenv("ETL_REPO_ROOT", str(repo_root))
    cwd_before = Path.cwd()
    os.chdir(step_dir)
    try:
        out = plugin.run({"script": "scripts/yanroy/hello.py"}, _ctx(step_dir))
    finally:
        os.chdir(cwd_before)

    assert out["return_code"] == 0
    assert "ok from repo root" in out["stdout"]
    assert out["script"].endswith("/scripts/yanroy/hello.py")

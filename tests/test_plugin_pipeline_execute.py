from __future__ import annotations

import types
from pathlib import Path

import pytest

from etl.plugins.base import PluginContext, load_plugin


def _ctx(tmp_path: Path) -> PluginContext:
    return PluginContext(run_id="r1", workdir=tmp_path, log=lambda *a, **k: None)


def test_pipeline_execute_synchronized_builds_cli_command_and_parses_child_run_id(tmp_path: Path, monkeypatch) -> None:
    plugin = load_plugin(Path("plugins/pipeline_execute.py"))
    child_pipeline = tmp_path / "child.yml"
    child_pipeline.write_text("steps:\n  - name: s1\n    script: echo.py\n", encoding="utf-8")
    seen = {}

    def _fake_run(cmd, capture_output, text, check):
        seen["cmd"] = list(cmd)
        return types.SimpleNamespace(returncode=0, stdout="Run abcdef12 -> queued [x]\n", stderr="")

    monkeypatch.setattr("subprocess.run", _fake_run)

    outputs = plugin.run(
        {
            "pipeline_path": str(child_pipeline),
            "mode": "synchronized",
            "executor": "local",
            "plugins_dir": "plugins",
            "workdir": str(tmp_path / ".runs"),
            "vars_json": '{"year":"2025","nested":{"day":"20250101"}}',
            "vars_kv": "region=conus;bucket=test",
            "allow_dirty_git": True,
            "dry_run": True,
        },
        _ctx(tmp_path),
    )

    assert seen["cmd"][2] == "cli"
    assert seen["cmd"][3] == "run"
    assert "--var" in seen["cmd"]
    assert "year=2025" in seen["cmd"]
    assert "nested.day=20250101" in seen["cmd"]
    assert "region=conus" in seen["cmd"]
    assert "bucket=test" in seen["cmd"]
    assert outputs["mode"] == "synchronized"
    assert outputs["child_run_id"] == "abcdef12"
    assert outputs["status"] == "succeeded"
    assert outputs["return_code"] == 0


def test_pipeline_execute_synchronized_raises_on_nonzero_exit(tmp_path: Path, monkeypatch) -> None:
    plugin = load_plugin(Path("plugins/pipeline_execute.py"))
    child_pipeline = tmp_path / "child.yml"
    child_pipeline.write_text("steps:\n  - name: s1\n    script: echo.py\n", encoding="utf-8")

    def _fake_run(cmd, capture_output, text, check):
        return types.SimpleNamespace(returncode=1, stdout="", stderr="boom")

    monkeypatch.setattr("subprocess.run", _fake_run)

    with pytest.raises(RuntimeError, match="child pipeline execution failed"):
        plugin.run(
            {
                "pipeline_path": str(child_pipeline),
                "mode": "synchronized",
            },
            _ctx(tmp_path),
        )


def test_pipeline_execute_fire_and_forget_returns_pid_and_log_path(tmp_path: Path, monkeypatch) -> None:
    plugin = load_plugin(Path("plugins/pipeline_execute.py"))
    child_pipeline = tmp_path / "child.yml"
    child_pipeline.write_text("steps:\n  - name: s1\n    script: echo.py\n", encoding="utf-8")
    seen = {}

    class _FakePopen:
        def __init__(self, cmd, stdout, stderr, text):
            seen["cmd"] = list(cmd)
            seen["stdout_name"] = getattr(stdout, "name", "")
            self.pid = 43210

    monkeypatch.setattr("subprocess.Popen", _FakePopen)

    outputs = plugin.run(
        {
            "pipeline_path": str(child_pipeline),
            "mode": "fire_and_forget",
            "plugins_dir": "plugins",
        },
        _ctx(tmp_path),
    )

    assert outputs["mode"] == "fire_and_forget"
    assert outputs["status"] == "submitted"
    assert outputs["pid"] == 43210
    assert outputs["child_log_path"].endswith("child_pipeline_fire_and_forget.log")
    assert "child_pipeline_fire_and_forget.log" in seen["stdout_name"]

# research-etl
# Copyright (c) 2026 Joseph Weaver
# This file is part of the research-etl project and is licensed under the MIT License.
# You may not use this file except in compliance with the License.
# See https://github.com/josephweaver/research-etl for details.

from __future__ import annotations

from pathlib import Path

import cli
from etl.config import artifact_tracking_enabled, resolve_global_config_path, run_context_snapshots_enabled
from etl.executors.base import SubmissionResult
from etl.pipeline import Pipeline, Step


class _Status:
    state = "succeeded"


def test_resolve_global_config_prefers_global_yml(tmp_path: Path, monkeypatch) -> None:
    cfg = tmp_path / "config"
    cfg.mkdir(parents=True, exist_ok=True)
    (cfg / "global.yml").write_text("basedir: ./out\n", encoding="utf-8")
    (cfg / "globals.yml").write_text("basedir: ./out2\n", encoding="utf-8")
    monkeypatch.chdir(tmp_path)
    resolved = resolve_global_config_path(None)
    assert resolved is not None
    assert resolved.as_posix().endswith("config/global.yml")


def test_resolve_global_config_falls_back_to_globals_yml(tmp_path: Path, monkeypatch) -> None:
    cfg = tmp_path / "config"
    cfg.mkdir(parents=True, exist_ok=True)
    (cfg / "globals.yml").write_text("basedir: ./out2\n", encoding="utf-8")
    monkeypatch.chdir(tmp_path)
    resolved = resolve_global_config_path(None)
    assert resolved is not None
    assert resolved.as_posix().endswith("config/globals.yml")


def test_resolve_global_config_returns_none_without_defaults(tmp_path: Path, monkeypatch) -> None:
    (tmp_path / "config").mkdir(parents=True, exist_ok=True)
    monkeypatch.chdir(tmp_path)
    assert resolve_global_config_path(None) is None


def test_cmd_run_auto_loads_default_global_config(monkeypatch, tmp_path: Path) -> None:
    cfg = tmp_path / "config"
    cfg.mkdir(parents=True, exist_ok=True)
    (cfg / "global.yml").write_text("basedir: ./out\n", encoding="utf-8")
    pipeline_path = (tmp_path / "pipeline.yml").resolve()
    pipeline_path.write_text("steps:\n  - name: s\n    script: echo.py\n", encoding="utf-8")
    monkeypatch.chdir(tmp_path)

    parser = cli.build_parser()
    args = parser.parse_args(["run", str(pipeline_path), "--workdir", str(tmp_path / ".runs")])
    args._raw_argv = ["run", str(pipeline_path), "--workdir", str(tmp_path / ".runs")]

    seen_global_vars = []

    def _fake_parse(path, global_vars=None, env_vars=None, context_vars=None):
        seen_global_vars.append(dict(global_vars or {}))
        return Pipeline(steps=[Step(name="s", script="echo.py")])

    class _Exec:
        def __init__(self, **kwargs):
            pass

        def submit(self, pipeline_path, context):
            return SubmissionResult(run_id="run1")

        def status(self, run_id):
            return _Status()

    monkeypatch.setattr(cli, "parse_pipeline", _fake_parse)
    monkeypatch.setattr(cli, "collect_run_provenance", lambda **k: {"git_commit_sha": "abc"})
    monkeypatch.setattr(cli, "load_runs", lambda store: [])
    monkeypatch.setattr(cli, "LocalExecutor", _Exec)

    rc = cli.cmd_run(args)
    assert rc == 0
    assert seen_global_vars
    assert seen_global_vars[0].get("basedir") == "./out"


def test_run_context_snapshots_enabled_defaults_true() -> None:
    assert run_context_snapshots_enabled({}) is True


def test_run_context_snapshots_enabled_respects_enable_flag() -> None:
    assert run_context_snapshots_enabled({"enable_run_context_snapshots": False}) is False
    assert run_context_snapshots_enabled({"enable_run_context_snapshots": "false"}) is False
    assert run_context_snapshots_enabled({"enable_run_context_snapshots": "true"}) is True


def test_run_context_snapshots_enabled_respects_disable_flag() -> None:
    assert run_context_snapshots_enabled({"disable_run_context_snapshots": True}) is False
    assert run_context_snapshots_enabled({"disable_run_context_snapshots": "true"}) is False
    assert run_context_snapshots_enabled({"disable_run_context_snapshots": "false"}) is True


def test_artifact_tracking_enabled_defaults_true() -> None:
    assert artifact_tracking_enabled({}) is True


def test_artifact_tracking_enabled_respects_enable_flag() -> None:
    assert artifact_tracking_enabled({"enable_artifact_tracking": False}) is False
    assert artifact_tracking_enabled({"enable_artifact_tracking": "false"}) is False
    assert artifact_tracking_enabled({"enable_artifact_tracking": "true"}) is True


def test_artifact_tracking_enabled_respects_disable_flag() -> None:
    assert artifact_tracking_enabled({"disable_artifact_tracking": True}) is False
    assert artifact_tracking_enabled({"disable_artifact_tracking": "true"}) is False
    assert artifact_tracking_enabled({"disable_artifact_tracking": "false"}) is True


# research-etl
# Copyright (c) 2026 Joseph Weaver
# This file is part of the research-etl project and is licensed under the MIT License.
# You may not use this file except in compliance with the License.
# See https://github.com/josephweaver/research-etl for details.

from __future__ import annotations

import os
import json
from pathlib import Path

import cli
from etl.diagnostics import write_error_report, find_latest_error_report
from etl.pipeline import Pipeline


def test_write_error_report_includes_traceback_and_frames(tmp_path: Path) -> None:
    try:
        raise RuntimeError("boom")
    except RuntimeError as exc:
        report = write_error_report(
            exc=exc,
            command="etl",
            argv=["run", "pipelines/sample.yml"],
            workdir=tmp_path / ".runs",
            repo_root=Path(".").resolve(),
        )

    payload = json.loads(report.read_text(encoding="utf-8"))
    assert payload["exception_type"] == "RuntimeError"
    assert payload["exception_message"] == "boom"
    assert payload["command"] == "etl"
    assert payload["argv"] == ["run", "pipelines/sample.yml"]
    assert isinstance(payload["frames"], list)
    assert payload["frames"]


def test_cmd_run_failure_emits_diagnostic_path(monkeypatch, capsys, tmp_path: Path) -> None:
    parser = cli.build_parser()
    args = parser.parse_args(["run", "pipelines/sample.yml"])
    args._raw_argv = ["run", "pipelines/sample.yml"]

    class _FailingExecutor:
        def __init__(self, *a, **k):
            pass

        def submit(self, *a, **k):
            raise RuntimeError("synthetic failure")

    monkeypatch.setattr(cli, "LocalExecutor", _FailingExecutor)
    monkeypatch.setattr(cli, "parse_pipeline", lambda *a, **k: Pipeline(steps=[]))
    monkeypatch.setattr(cli, "collect_run_provenance", lambda **k: {})

    fake_report = tmp_path / ".runs" / "error_reports" / "x.json"
    fake_report.parent.mkdir(parents=True, exist_ok=True)
    fake_report.write_text("{}", encoding="utf-8")
    monkeypatch.setattr(cli, "write_error_report", lambda **k: fake_report)

    rc = cli.cmd_run(args)
    captured = capsys.readouterr()
    assert rc == 1
    assert "Run failed before submission/execution: synthetic failure" in captured.err
    assert f"Diagnostic report saved: {fake_report}" in captured.err


def test_find_latest_error_report_returns_newest(tmp_path: Path) -> None:
    report_dir = tmp_path / ".runs" / "error_reports"
    report_dir.mkdir(parents=True, exist_ok=True)
    old = report_dir / "old.json"
    new = report_dir / "new.json"
    old.write_text("{}", encoding="utf-8")
    new.write_text("{}", encoding="utf-8")
    os.utime(old, (1, 1))
    os.utime(new, (2, 2))
    newest = find_latest_error_report(tmp_path / ".runs")
    assert newest is not None
    assert newest.name == "new.json"


def test_cmd_diagnostics_latest_prints_path(monkeypatch, capsys, tmp_path: Path) -> None:
    report = tmp_path / ".runs" / "error_reports" / "diag.json"
    report.parent.mkdir(parents=True, exist_ok=True)
    report.write_text("{\"ok\": true}", encoding="utf-8")
    monkeypatch.setattr(cli, "find_latest_error_report", lambda *_: report)
    args = cli.build_parser().parse_args(["diagnostics", "latest", "--workdir", str(tmp_path / ".runs")])
    rc = cli.cmd_diagnostics_latest(args)
    captured = capsys.readouterr()
    assert rc == 0
    assert str(report) in captured.out


def test_cmd_diagnostics_latest_show_prints_json(monkeypatch, capsys, tmp_path: Path) -> None:
    report = tmp_path / ".runs" / "error_reports" / "diag.json"
    report.parent.mkdir(parents=True, exist_ok=True)
    report.write_text("{\"ok\": true}", encoding="utf-8")
    monkeypatch.setattr(cli, "find_latest_error_report", lambda *_: report)
    args = cli.build_parser().parse_args(["diagnostics", "latest", "--show"])
    rc = cli.cmd_diagnostics_latest(args)
    captured = capsys.readouterr()
    assert rc == 0
    assert str(report) in captured.out
    assert "{\"ok\": true}" in captured.out

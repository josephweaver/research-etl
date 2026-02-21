# research-etl
# Copyright (c) 2026 Joseph Weaver
# This file is part of the research-etl project and is licensed under the MIT License.
# You may not use this file except in compliance with the License.
# See https://github.com/josephweaver/research-etl for details.

from __future__ import annotations

from dataclasses import dataclass

import cli
from etl.executors.base import SubmissionResult
from etl.pipeline import Pipeline, Step


@dataclass
class _Status:
    state: str


def test_cmd_run_passes_provenance_to_local_executor(monkeypatch) -> None:
    parser = cli.build_parser()
    args = parser.parse_args(["run", "pipelines/sample.yml", "--resume-run-id", "prev_run"])
    args._raw_argv = ["run", "pipelines/sample.yml", "--resume-run-id", "prev_run"]

    provenance = {"git_commit_sha": "abc123", "pipeline_checksum": "chk1"}
    monkeypatch.setattr(cli, "parse_pipeline", lambda *a, **k: Pipeline(steps=[Step(name="s1", script="echo.py")]))
    monkeypatch.setattr(cli, "collect_run_provenance", lambda **k: provenance)

    captured_context = {}

    class _LocalExecutor:
        def __init__(self, **kwargs):
            pass

        def submit(self, pipeline_path, context):
            captured_context.update(context)
            return SubmissionResult(run_id="run_local")

        def status(self, run_id):
            return _Status(state="succeeded")

    monkeypatch.setattr(cli, "LocalExecutor", _LocalExecutor)

    rc = cli.cmd_run(args)
    assert rc == 0
    assert captured_context.get("provenance") == provenance
    assert captured_context.get("resume_run_id") == "prev_run"


def test_cmd_run_passes_provenance_to_slurm_executor(monkeypatch) -> None:
    parser = cli.build_parser()
    args = parser.parse_args(["run", "pipelines/sample.yml", "--executor", "slurm"])
    args._raw_argv = ["run", "pipelines/sample.yml", "--executor", "slurm"]

    provenance = {"git_commit_sha": "abc123", "pipeline_checksum": "chk1"}
    monkeypatch.setattr(cli, "parse_pipeline", lambda *a, **k: Pipeline(steps=[Step(name="s1", script="echo.py")]))
    monkeypatch.setattr(cli, "collect_run_provenance", lambda **k: provenance)

    captured_context = {}

    class _SlurmExecutor:
        def __init__(self, **kwargs):
            pass

        def submit(self, pipeline_path, context):
            captured_context.update(context)
            return SubmissionResult(run_id="run_slurm")

        def status(self, run_id):
            return _Status(state="queued")

    monkeypatch.setattr(cli, "SlurmExecutor", _SlurmExecutor)

    rc = cli.cmd_run(args)
    assert rc == 0
    assert captured_context.get("provenance") == provenance

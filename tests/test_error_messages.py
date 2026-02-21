# research-etl
# Copyright (c) 2026 Joseph Weaver
# This file is part of the research-etl project and is licensed under the MIT License.
# You may not use this file except in compliance with the License.
# See https://github.com/josephweaver/research-etl for details.

from __future__ import annotations

import argparse
from pathlib import Path

import pytest

import cli
from etl.db import DatabaseError
from etl.executors.slurm import SlurmSubmitError
from etl.execution_config import ExecutionConfigError, apply_execution_env_overrides
from etl.executors.base import SubmissionResult
from etl.pipeline import Pipeline, Step


def test_format_run_submission_error_for_resume_without_db() -> None:
    msg = cli._format_run_submission_error(  # type: ignore[attr-defined]
        RuntimeError("ETL_DATABASE_URL is required for --resume-run-id because step state is loaded from DB."),
        argparse.Namespace(),
    )
    assert "--resume-run-id requires ETL_DATABASE_URL" in msg


def test_format_run_submission_error_for_missing_sbatch() -> None:
    msg = cli._format_run_submission_error(  # type: ignore[attr-defined]
        FileNotFoundError(2, "No such file or directory", "sbatch"),
        argparse.Namespace(),
    )
    assert "Required command 'sbatch' was not found." in msg


def test_format_run_submission_error_for_slurm_secret_failure() -> None:
    msg = cli._format_run_submission_error(  # type: ignore[attr-defined]
        SlurmSubmitError("Could not initialize remote DB secret."),
        argparse.Namespace(),
    )
    assert "preparing remote DB secret" in msg


def test_format_database_init_error_for_invalid_url() -> None:
    msg = cli._format_database_init_error(  # type: ignore[attr-defined]
        DatabaseError("ETL_DATABASE_URL is set but is not a valid URL.")
    )
    assert "ETL_DATABASE_URL is set but invalid" in msg


def test_cmd_run_rejects_negative_max_retries(capsys) -> None:
    parser = cli.build_parser()
    args = parser.parse_args(["run", "pipelines/sample.yml", "--max-retries", "-1"])
    args._raw_argv = ["run", "pipelines/sample.yml", "--max-retries", "-1"]

    rc = cli.cmd_run(args)
    captured = capsys.readouterr()
    assert rc == 1
    assert "Invalid --max-retries: must be >= 0." in captured.err


def test_apply_execution_env_overrides_rejects_bad_integer(monkeypatch) -> None:
    monkeypatch.setenv("ETL_MAX_PARALLEL", "not-int")
    with pytest.raises(ExecutionConfigError):
        apply_execution_env_overrides({})


def test_cmd_run_rejects_invalid_var_syntax(capsys) -> None:
    parser = cli.build_parser()
    args = parser.parse_args(["run", "pipelines/sample.yml", "--var", "missing_equals"])
    args._raw_argv = ["run", "pipelines/sample.yml", "--var", "missing_equals"]

    rc = cli.cmd_run(args)
    captured = capsys.readouterr()
    assert rc == 1
    assert "expected KEY=VALUE" in captured.err


def test_cli_main_warns_and_ignores_unknown_args(monkeypatch, capsys) -> None:
    monkeypatch.setattr(cli, "ensure_database_schema", lambda *_a, **_k: None)
    monkeypatch.setattr(cli, "_auto_apply_local_db_queue", lambda *_a, **_k: None)

    rc = cli.main(["plugins", "list", "--future-flag", "x"])
    captured = capsys.readouterr()
    assert rc in (0, 1)
    assert "[etl][WARN] ignoring unknown arguments:" in captured.out
    assert "--future-flag x" in captured.out


def test_cli_parser_accepts_hpcc_direct_executor() -> None:
    parser = cli.build_parser()
    args = parser.parse_args(["run", "pipelines/sample.yml", "--executor", "hpcc_direct"])
    assert args.executor == "hpcc_direct"


def test_cmd_run_uses_env_executor_when_executor_not_provided(monkeypatch) -> None:
    parser = cli.build_parser()
    args = parser.parse_args(["run", "pipelines/sample.yml", "--env", "hpcc"])
    args._raw_argv = ["run", "pipelines/sample.yml", "--env", "hpcc"]
    monkeypatch.setattr(cli, "resolve_execution_config_path", lambda _p: Path("config/environments.yml"))
    monkeypatch.setattr(cli, "load_execution_config", lambda _p: {"hpcc": {"executor": "slurm"}})
    monkeypatch.setattr(cli, "apply_execution_env_overrides", lambda env: env)
    monkeypatch.setattr(cli, "resolve_execution_env_templates", lambda env, global_vars=None: env)
    monkeypatch.setattr(cli, "parse_pipeline", lambda *a, **k: Pipeline(steps=[Step(name="s1", script="echo.py")]))
    monkeypatch.setattr(cli, "collect_run_provenance", lambda **k: {"git_commit_sha": "abc"})

    class _SlurmExecutor:
        def __init__(self, **kwargs):
            pass

        def submit(self, pipeline_path, context):
            return SubmissionResult(run_id="run_slurm")

        def status(self, run_id):
            return argparse.Namespace(state="queued", message="")

    monkeypatch.setattr(cli, "SlurmExecutor", _SlurmExecutor)
    rc = cli.cmd_run(args)
    assert rc == 0

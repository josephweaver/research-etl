from __future__ import annotations

import argparse

import pytest

import cli
from etl.db import DatabaseError
from etl.executors.slurm import SlurmSubmitError
from etl.execution_config import ExecutionConfigError, apply_execution_env_overrides


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

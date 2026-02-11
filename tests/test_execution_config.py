from __future__ import annotations

from pathlib import Path

import pytest

from etl.execution_config import (
    ExecutionConfigError,
    resolve_execution_config_path,
    validate_environment_executor,
)


def test_resolve_execution_config_prefers_environments(tmp_path: Path, monkeypatch) -> None:
    cfg = tmp_path / "config"
    cfg.mkdir(parents=True, exist_ok=True)
    (cfg / "environments.yml").write_text("environments: {}\n", encoding="utf-8")
    monkeypatch.chdir(tmp_path)
    resolved = resolve_execution_config_path(None)
    assert resolved is not None
    assert resolved.as_posix().endswith("config/environments.yml")


def test_resolve_execution_config_returns_none_without_environments(tmp_path: Path, monkeypatch) -> None:
    cfg = tmp_path / "config"
    cfg.mkdir(parents=True, exist_ok=True)
    monkeypatch.chdir(tmp_path)
    resolved = resolve_execution_config_path(None)
    assert resolved is None


def test_validate_environment_executor_rejects_mismatch() -> None:
    with pytest.raises(ExecutionConfigError, match="not 'local'"):
        validate_environment_executor("hpcc", {"executor": "slurm"}, executor="local")

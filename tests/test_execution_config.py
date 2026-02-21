# research-etl
# Copyright (c) 2026 Joseph Weaver
# This file is part of the research-etl project and is licensed under the MIT License.
# You may not use this file except in compliance with the License.
# See https://github.com/josephweaver/research-etl for details.

from __future__ import annotations

from pathlib import Path

import pytest

from etl.execution_config import (
    ExecutionConfigError,
    resolve_execution_env_templates,
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


def test_resolve_execution_env_templates_resolves_sibling_refs() -> None:
    env = {
        "basedir": "/scratch/user",
        "workdir": "{basedir}/jobs/work",
        "logdir": "{workdir}/logs",
        "venv": "{basedir}/jobs/etl/.venv",
    }
    resolved = resolve_execution_env_templates(env)
    assert resolved["workdir"] == "/scratch/user/jobs/work"
    assert resolved["logdir"] == "/scratch/user/jobs/work/logs"
    assert resolved["venv"] == "/scratch/user/jobs/etl/.venv"


def test_resolve_execution_env_templates_resolves_global_refs() -> None:
    env = {
        "basedir": "{global.basedir}",
        "workdir": "{basedir}/work",
    }
    resolved = resolve_execution_env_templates(env, global_vars={"basedir": "/mnt/gs21/scratch/weave151"})
    assert resolved["basedir"] == "/mnt/gs21/scratch/weave151"
    assert resolved["workdir"] == "/mnt/gs21/scratch/weave151/work"

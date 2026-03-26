# research-etl
# Copyright (c) 2026 Joseph Weaver
# This file is part of the research-etl project and is licensed under the MIT License.
# You may not use this file except in compliance with the License.
# See https://github.com/josephweaver/research-etl for details.

from __future__ import annotations

from pathlib import Path

import etl.execution_config as ec
from etl.execution_config import resolve_execution_config_path


def test_resolve_execution_config_path_uses_etl_repo_root_for_relative_path(tmp_path: Path, monkeypatch) -> None:
    repo_root = tmp_path / "repo"
    cfg = repo_root / "config" / "environments.yml"
    cfg.parent.mkdir(parents=True)
    cfg.write_text("environments: {}\n", encoding="utf-8")
    monkeypatch.chdir(tmp_path)
    monkeypatch.setenv("ETL_REPO_ROOT", str(repo_root))
    resolved = resolve_execution_config_path(Path("config/environments.yml"))
    assert resolved == cfg.resolve()


def test_resolve_execution_config_path_uses_module_repo_root(tmp_path: Path, monkeypatch) -> None:
    repo_root = tmp_path / "repo"
    cfg = repo_root / "config" / "environments.yml"
    cfg.parent.mkdir(parents=True)
    cfg.write_text("environments: {}\n", encoding="utf-8")
    monkeypatch.chdir(tmp_path)
    monkeypatch.delenv("ETL_REPO_ROOT", raising=False)
    monkeypatch.setattr(ec, "_MODULE_REPO_ROOT", repo_root)
    resolved = resolve_execution_config_path(Path("config/environments.yml"))
    assert resolved == cfg.resolve()

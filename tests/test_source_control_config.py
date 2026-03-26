# research-etl
# Copyright (c) 2026 Joseph Weaver
# This file is part of the research-etl project and is licensed under the MIT License.
# You may not use this file except in compliance with the License.
# See https://github.com/josephweaver/research-etl for details.

from __future__ import annotations

import etl.source_control.config as scc
from etl.source_control.config import resolve_repo_config, resolve_source_control_config_path


def test_resolve_repo_config_reads_named_repository(tmp_path) -> None:
    cfg = tmp_path / "source_control.yml"
    cfg.write_text(
        "\n".join(
            [
                "repositories:",
                "  LC_DUCKDB:",
                "    provider: git",
                "    local_path: ../landcore-duckdb",
                "    default_branch: main",
            ]
        ),
        encoding="utf-8",
    )
    out = resolve_repo_config(repo_alias="LC_DUCKDB", config_path=cfg)
    assert out["repo_alias"] == "LC_DUCKDB"
    assert out["provider"] == "git"
    assert out["local_path"] == "../landcore-duckdb"


def test_resolve_source_control_config_path_uses_etl_repo_root_for_relative_path(tmp_path, monkeypatch) -> None:
    repo_root = tmp_path / "repo"
    cfg = repo_root / "config" / "source_control.yml"
    cfg.parent.mkdir(parents=True)
    cfg.write_text("repositories: {}\n", encoding="utf-8")
    monkeypatch.chdir(tmp_path)
    monkeypatch.setenv("ETL_REPO_ROOT", str(repo_root))
    resolved = resolve_source_control_config_path("config/source_control.yml")
    assert resolved == cfg.resolve()


def test_resolve_source_control_config_path_uses_module_repo_root(tmp_path, monkeypatch) -> None:
    repo_root = tmp_path / "repo"
    cfg = repo_root / "config" / "source_control.yml"
    cfg.parent.mkdir(parents=True)
    cfg.write_text("repositories: {}\n", encoding="utf-8")
    monkeypatch.chdir(tmp_path)
    monkeypatch.delenv("ETL_REPO_ROOT", raising=False)
    monkeypatch.setattr(scc, "_MODULE_REPO_ROOT", repo_root)
    resolved = resolve_source_control_config_path("config/source_control.yml")
    assert resolved == cfg.resolve()

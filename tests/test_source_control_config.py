# research-etl
# Copyright (c) 2026 Joseph Weaver
# This file is part of the research-etl project and is licensed under the MIT License.
# You may not use this file except in compliance with the License.
# See https://github.com/josephweaver/research-etl for details.

from __future__ import annotations

from etl.source_control.config import resolve_repo_config


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

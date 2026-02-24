# research-etl
# Copyright (c) 2026 Joseph Weaver
# This file is part of the research-etl project and is licensed under the MIT License.
# You may not use this file except in compliance with the License.
# See https://github.com/josephweaver/research-etl for details.

from __future__ import annotations

from pathlib import Path

import pytest

from etl.plugins.base import PluginContext, load_plugin


def _ctx(tmp_path: Path) -> PluginContext:
    return PluginContext(run_id="r1", workdir=tmp_path / "work", log=lambda *a, **k: None)


def test_duckdb_load_replace_then_append(tmp_path: Path) -> None:
    duckdb = pytest.importorskip("duckdb")
    plugin = load_plugin(Path("plugins/duckdb_load.py"))
    db_path = tmp_path / "rel.duckdb"
    src1 = tmp_path / "in1.csv"
    src2 = tmp_path / "in2.csv"
    src1.write_text("id,val\n1,a\n2,b\n", encoding="utf-8")
    src2.write_text("id,val\n3,c\n", encoding="utf-8")

    out1 = plugin.run(
        {
            "db_path": str(db_path),
            "table": "rel_tile_field_mukey",
            "source": str(src1),
            "format": "csv",
            "mode": "replace",
        },
        _ctx(tmp_path),
    )
    assert out1["rows_loaded"] == 2
    assert out1["rows_after"] == 2

    out2 = plugin.run(
        {
            "db_path": str(db_path),
            "table": "rel_tile_field_mukey",
            "source": str(src2),
            "format": "csv",
            "mode": "append",
        },
        _ctx(tmp_path),
    )
    assert out2["rows_before"] == 2
    assert out2["rows_loaded"] == 1
    assert out2["rows_after"] == 3

    con = duckdb.connect(str(db_path))
    try:
        count = con.execute("SELECT COUNT(*) FROM rel_tile_field_mukey").fetchone()[0]
        assert int(count) == 3
    finally:
        con.close()


def test_duckdb_load_create_if_missing(tmp_path: Path) -> None:
    pytest.importorskip("duckdb")
    plugin = load_plugin(Path("plugins/duckdb_load.py"))
    db_path = tmp_path / "rel2.duckdb"
    src = tmp_path / "in.csv"
    src.write_text("id,val\n1,a\n", encoding="utf-8")

    out = plugin.run(
        {
            "db_path": str(db_path),
            "table": "rel_any",
            "source": str(src),
            "format": "csv",
            "mode": "create_if_missing",
        },
        _ctx(tmp_path),
    )
    assert out["rows_loaded"] == 1
    assert out["rows_after"] == 1

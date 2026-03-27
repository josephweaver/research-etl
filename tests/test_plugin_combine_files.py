# research-etl
# Copyright (c) 2026 Joseph Weaver
# This file is part of the research-etl project and is licensed under the MIT License.
# You may not use this file except in compliance with the License.
# See https://github.com/josephweaver/research-etl for details.

from __future__ import annotations

import json
from pathlib import Path

from etl.plugins.base import PluginContext, load_plugin


def _ctx(tmp_path: Path) -> PluginContext:
    return PluginContext(run_id="r1", workdir=tmp_path, log=lambda *a, **k: None)


def test_combine_files_csv_merges_rows(tmp_path: Path) -> None:
    plugin = load_plugin(Path("plugins/combine_files.py"))
    d = tmp_path / "in"
    d.mkdir(parents=True, exist_ok=True)
    (d / "a.csv").write_text("id,val\n1,a\n", encoding="utf-8")
    (d / "b.csv").write_text("id,val\n2,b\n", encoding="utf-8")
    out = tmp_path / "out.csv"
    res = plugin.run({"input_glob": f"{d.as_posix()}/*.csv", "output_file": str(out), "format": "csv"}, _ctx(tmp_path))
    text = out.read_text(encoding="utf-8")
    assert "id,val" in text
    assert "1,a" in text
    assert "2,b" in text
    assert res["combined_count"] == 2


def test_combine_files_csv_merges_mismatched_headers_without_buffering_rows(tmp_path: Path) -> None:
    plugin = load_plugin(Path("plugins/combine_files.py"))
    d = tmp_path / "in_mixed"
    d.mkdir(parents=True, exist_ok=True)
    (d / "a.csv").write_text("id,val\n1,a\n", encoding="utf-8")
    (d / "b.csv").write_text("id,extra\n2,z\n", encoding="utf-8")
    out = tmp_path / "out_mixed.csv"

    plugin.run({"input_glob": f"{d.as_posix()}/*.csv", "output_file": str(out), "format": "csv"}, _ctx(tmp_path))

    lines = out.read_text(encoding="utf-8").splitlines()
    assert lines[0] == "id,val,extra"
    assert "1,a," in lines
    assert "2,,z" in lines


def test_combine_files_json_merges_arrays(tmp_path: Path) -> None:
    plugin = load_plugin(Path("plugins/combine_files.py"))
    d = tmp_path / "j"
    d.mkdir(parents=True, exist_ok=True)
    (d / "a.json").write_text(json.dumps([{"x": 1}]), encoding="utf-8")
    (d / "b.json").write_text(json.dumps([{"x": 2}]), encoding="utf-8")
    out = tmp_path / "out.json"
    plugin.run({"input_glob": f"{d.as_posix()}/*.json", "output_file": str(out), "format": "json"}, _ctx(tmp_path))
    payload = json.loads(out.read_text(encoding="utf-8"))
    assert payload == [{"x": 1}, {"x": 2}]


def test_combine_files_text_concat(tmp_path: Path) -> None:
    plugin = load_plugin(Path("plugins/combine_files.py"))
    d = tmp_path / "t"
    d.mkdir(parents=True, exist_ok=True)
    (d / "a.txt").write_text("A", encoding="utf-8")
    (d / "b.txt").write_text("B", encoding="utf-8")
    out = tmp_path / "out.txt"
    plugin.run(
        {"input_glob": f"{d.as_posix()}/*.txt", "output_file": str(out), "format": "text", "text_separator": "|"},
        _ctx(tmp_path),
    )
    assert out.read_text(encoding="utf-8") == "A|B"


def test_combine_files_xml_wraps_docs(tmp_path: Path) -> None:
    plugin = load_plugin(Path("plugins/combine_files.py"))
    d = tmp_path / "x"
    d.mkdir(parents=True, exist_ok=True)
    (d / "a.xml").write_text("<root><v>1</v></root>", encoding="utf-8")
    (d / "b.xml").write_text("<root><v>2</v></root>", encoding="utf-8")
    out = tmp_path / "out.xml"
    plugin.run({"input_glob": f"{d.as_posix()}/*.xml", "output_file": str(out), "format": "xml"}, _ctx(tmp_path))
    text = out.read_text(encoding="utf-8")
    assert "<combined>" in text
    assert "<v>1</v>" in text
    assert "<v>2</v>" in text

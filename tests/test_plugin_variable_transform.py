from __future__ import annotations

import json
from pathlib import Path

from etl.plugins.base import PluginContext, load_plugin


def _ctx(tmp_path: Path) -> PluginContext:
    return PluginContext(run_id="r1", workdir=tmp_path, log=lambda *a, **k: None)


def test_variable_transform_regex_extract(tmp_path: Path) -> None:
    plugin = load_plugin(Path("plugins/variable_transform.py"))
    out = plugin.run(
        {
            "value": "WELD_h12v04_2010_field_segments.gpkg",
            "op": "regex_extract",
            "pattern": "(h\\d{2}v\\d{2})",
            "group": "1",
        },
        _ctx(tmp_path),
    )
    assert out["value"] == "h12v04"


def test_variable_transform_uuid5_from_parts_json(tmp_path: Path) -> None:
    plugin = load_plugin(Path("plugins/variable_transform.py"))
    out = plugin.run(
        {
            "op": "uuid5",
            "namespace": "9e6c1b5a-84d9-49c6-89df-cf8f1ca0af31",
            "parts": json.dumps(["yanroy", "h12v04", "101"]),
        },
        _ctx(tmp_path),
    )
    assert isinstance(out["value"], str)
    assert len(out["value"]) == 36


def test_variable_transform_list_files_and_join(tmp_path: Path) -> None:
    plugin = load_plugin(Path("plugins/variable_transform.py"))
    d = tmp_path / "in"
    d.mkdir(parents=True, exist_ok=True)
    (d / "a.txt").write_text("a", encoding="utf-8")
    (d / "b.txt").write_text("b", encoding="utf-8")
    ops = json.dumps(
        [
            {"op": "list_files", "pattern": str((d / "*.txt").as_posix())},
            {"op": "join", "separator": "|"},
        ]
    )
    out = plugin.run({"operations": ops}, _ctx(tmp_path))
    assert "|" in out["value"]
    assert "a.txt" in out["value"]


def test_variable_transform_read_json_returns_mapping(tmp_path: Path) -> None:
    plugin = load_plugin(Path("plugins/variable_transform.py"))
    p = tmp_path / "payload.json"
    p.write_text(json.dumps({"tile_coord": "h12v04", "field": 101}), encoding="utf-8")
    out = plugin.run({"value": str(p), "op": "read_json"}, _ctx(tmp_path))
    assert out["value_type"] == "dict"
    assert out["tile_coord"] == "h12v04"

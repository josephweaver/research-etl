# research-etl
# Copyright (c) 2026 Joseph Weaver
# This file is part of the research-etl project and is licensed under the MIT License.
# You may not use this file except in compliance with the License.
# See https://github.com/josephweaver/research-etl for details.

from __future__ import annotations

import csv
from pathlib import Path

from etl.plugins.base import PluginContext, load_plugin


def _ctx(tmp_path: Path) -> PluginContext:
    return PluginContext(run_id="r1", workdir=tmp_path, log=lambda *a, **k: None)


def test_yanroy_scan_raw_metadata_writes_expected_outputs(tmp_path: Path) -> None:
    plugin = load_plugin(Path("plugins/yanroy/scan_raw_metadata.py"))
    in_dir = tmp_path / "fields"
    (in_dir / "h03v09").mkdir(parents=True, exist_ok=True)
    (in_dir / "h03v09" / "WELD_h03v09_2010_field_segments").write_text("raster", encoding="utf-8")
    (in_dir / "h03v09" / "WELD_h03v09_2010_field_segments.hdr").write_text("hdr", encoding="utf-8")
    (in_dir / "h03v09" / "WELD_h03v09_2010_field_segments_ancillary_data").write_text("anc", encoding="utf-8")
    (in_dir / "misc.txt").write_text("x", encoding="utf-8")

    out_dir = tmp_path / "meta"
    outputs = plugin.run(
        {
            "input_dir": str(in_dir),
            "output_dir": str(out_dir),
        },
        _ctx(tmp_path),
    )

    assert outputs["file_count"] == 4
    assert outputs["tile_count"] == 1
    assert outputs["tiles"] == ["h03v09"]
    assert (out_dir / "yanroy_raw_metadata.csv").exists()
    assert (out_dir / "tiles.of.interest.csv").exists()
    assert (out_dir / "summary.json").exists()

    with (out_dir / "tiles.of.interest.csv").open("r", encoding="utf-8", newline="") as f:
        rows = list(csv.DictReader(f))
    assert len(rows) == 1
    assert rows[0]["tile_id"] == "h03v09"
    assert rows[0]["has_raster_data"] == "1"
    assert rows[0]["has_header"] == "1"
    assert rows[0]["has_ancillary"] == "1"


def test_yanroy_scan_raw_metadata_requires_input_dir(tmp_path: Path) -> None:
    plugin = load_plugin(Path("plugins/yanroy/scan_raw_metadata.py"))
    try:
        plugin.run({"output_dir": str(tmp_path / "meta")}, _ctx(tmp_path))
        assert False, "expected plugin to fail"
    except ValueError as exc:
        assert "input_dir is required" in str(exc)


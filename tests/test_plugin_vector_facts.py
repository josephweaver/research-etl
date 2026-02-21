# research-etl
# Copyright (c) 2026 Joseph Weaver
# This file is part of the research-etl project and is licensed under the MIT License.
# You may not use this file except in compliance with the License.
# See https://github.com/josephweaver/research-etl for details.

from __future__ import annotations

import csv
import struct
from pathlib import Path

from etl.plugins.base import PluginContext, load_plugin


def _ctx(tmp_path: Path) -> PluginContext:
    return PluginContext(run_id="r1", workdir=tmp_path, log=lambda *a, **k: None)


def _write_fake_shp(path: Path, *, shape_type: int = 5, bbox=(1.0, 2.0, 3.0, 4.0)) -> None:
    # 100-byte shapefile header:
    # - file code 9994 (big-endian int32)
    # - file length (16-bit words, big-endian int32)
    # - version 1000 (little-endian int32)
    # - shape type (little-endian int32)
    # - bbox xmin, ymin, xmax, ymax (little-endian float64)
    header = bytearray(100)
    struct.pack_into(">i", header, 0, 9994)
    struct.pack_into(">i", header, 24, 50)  # 100 bytes / 2 words
    struct.pack_into("<i", header, 28, 1000)
    struct.pack_into("<i", header, 32, shape_type)
    struct.pack_into("<4d", header, 36, *bbox)
    path.write_bytes(bytes(header))


def _write_fake_dbf(path: Path, *, record_count: int) -> None:
    header = bytearray(32)
    struct.pack_into("<I", header, 4, record_count)
    path.write_bytes(bytes(header))


def test_vector_facts_collects_shapefile_basics(tmp_path: Path) -> None:
    plugin = load_plugin(Path("plugins/vector_facts.py"))
    data = tmp_path / "extract"
    data.mkdir(parents=True, exist_ok=True)

    shp = data / "tl_2025_us_state.shp"
    _write_fake_shp(shp, shape_type=5, bbox=(10.0, 20.0, 30.0, 40.0))
    _write_fake_dbf(data / "tl_2025_us_state.dbf", record_count=56)
    (data / "tl_2025_us_state.shx").write_bytes(b"x")
    (data / "tl_2025_us_state.prj").write_text("GEOGCS[\"GCS_North_American_1983\"]", encoding="utf-8")

    out = tmp_path / "facts"
    outputs = plugin.run({"input_dir": str(data), "output_dir": str(out)}, _ctx(tmp_path))
    assert outputs["vector_file_count"] == 1
    assert (out / "vector_facts.csv").exists()
    assert (out / "file_manifest.csv").exists()

    with (out / "vector_facts.csv").open("r", encoding="utf-8", newline="") as f:
        rows = list(csv.DictReader(f))
    assert len(rows) == 1
    assert rows[0]["geometry_type"] == "Polygon"
    assert rows[0]["feature_count"] == "56"
    assert rows[0]["has_dbf"] == "1"
    assert rows[0]["has_prj"] == "1"
    assert rows[0]["bbox_xmin"] == "10.0"
    assert rows[0]["bbox_ymax"] == "40.0"


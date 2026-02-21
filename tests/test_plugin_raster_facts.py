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


class _FakeArray:
    def __init__(self, value):
        self._value = value

    def tolist(self):
        return self._value


class _FakeMaskedChunk:
    def __init__(self, data, mask):
        self.data = _FakeArray(data)
        self.mask = _FakeArray(mask)


class _FakeCRS:
    def to_string(self):
        return "EPSG:5070"


class _FakeBounds:
    left = 1.0
    bottom = 2.0
    right = 3.0
    top = 4.0


class _FakeDataset:
    width = 2
    height = 2
    count = 1
    dtypes = ["int16"]
    nodatavals = [-9999]
    crs = _FakeCRS()
    bounds = _FakeBounds()

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        return False

    def block_windows(self, _band_index):
        yield (0, 0), None

    def read(self, _band_index, window=None, masked=True):
        assert masked is True
        return _FakeMaskedChunk(data=[[0, 1], [2, 3]], mask=[[False, False], [False, False]])


class _FakeRasterio:
    def open(self, path):
        return _FakeDataset()


def test_raster_facts_manifest_created_when_rasterio_missing(tmp_path: Path, monkeypatch) -> None:
    plugin = load_plugin(Path("plugins/raster_facts.py"))
    assert plugin.module is not None
    monkeypatch.setattr(plugin.module, "rasterio", None)

    data = tmp_path / "data"
    data.mkdir(parents=True, exist_ok=True)
    (data / "a.txt").write_text("x", encoding="utf-8")
    (data / "b.bin").write_bytes(b"\x01\x02")

    out = tmp_path / "out"
    outputs = plugin.run({"input_dir": str(data), "output_dir": str(out)}, _ctx(tmp_path))
    assert outputs["file_count"] == 2
    assert outputs["raster_file_count"] == 0
    assert outputs["band_fact_count"] == 0
    assert (out / "file_manifest.csv").exists()
    assert (out / "raster_facts.csv").exists()


def test_raster_facts_collects_basic_stats(tmp_path: Path, monkeypatch) -> None:
    plugin = load_plugin(Path("plugins/raster_facts.py"))
    assert plugin.module is not None
    monkeypatch.setattr(plugin.module, "rasterio", _FakeRasterio())

    data = tmp_path / "data"
    data.mkdir(parents=True, exist_ok=True)
    (data / "tile_01").write_bytes(b"fake")

    out = tmp_path / "out"
    outputs = plugin.run(
        {
            "input_dir": str(data),
            "output_dir": str(out),
            "probe_unknown": True,
        },
        _ctx(tmp_path),
    )
    assert outputs["raster_file_count"] == 1
    assert outputs["band_fact_count"] == 1

    with (out / "raster_facts.csv").open("r", encoding="utf-8", newline="") as f:
        rows = list(csv.DictReader(f))
    assert len(rows) == 1
    assert rows[0]["pixel_type"] == "int16"
    assert rows[0]["width"] == "2"
    assert rows[0]["height"] == "2"
    assert rows[0]["crs"] == "EPSG:5070"
    assert rows[0]["total_count"] == "4"
    assert rows[0]["zero_count"] == "1"
    assert rows[0]["min_value"] == "0.0"
    assert rows[0]["max_value"] == "3.0"


# research-etl
# Copyright (c) 2026 Joseph Weaver
# This file is part of the research-etl project and is licensed under the MIT License.
# You may not use this file except in compliance with the License.
# See https://github.com/josephweaver/research-etl for details.

from __future__ import annotations

import csv
from pathlib import Path

import pytest

from etl.plugins.base import PluginContext, load_plugin


gpd = pytest.importorskip("geopandas")
shapely_geometry = pytest.importorskip("shapely.geometry")


def _ctx(tmp_path: Path) -> PluginContext:
    return PluginContext(run_id="r1", workdir=tmp_path, log=lambda *a, **k: None)


class _FakeCRS:
    def __init__(self, text: str):
        self._text = text

    def to_string(self):
        return self._text


class _FakeBounds:
    def __init__(self, left: float, bottom: float, right: float, top: float):
        self.left = left
        self.bottom = bottom
        self.right = right
        self.top = top


class _FakeDataset:
    def __init__(self, crs_text: str, bounds: tuple[float, float, float, float]):
        self.crs = _FakeCRS(crs_text) if crs_text else None
        self.bounds = _FakeBounds(*bounds)

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        return False


class _FakeRasterio:
    def open(self, path):
        name = Path(path).name.lower()
        if name == "tile_in.tif":
            return _FakeDataset("EPSG:4326", (-101.0, 39.0, -99.0, 41.0))
        if name == "tile_out.tif":
            return _FakeDataset("EPSG:4326", (-120.0, 10.0, -118.0, 12.0))
        if name == "tile_pair":
            return _FakeDataset("EPSG:4326", (-101.0, 39.0, -99.0, 41.0))
        raise FileNotFoundError(name)


def _write_selector(path: Path) -> None:
    poly = shapely_geometry.box(-100.5, 39.5, -99.5, 40.5)
    gdf = gpd.GeoDataFrame({"name": ["aoi"]}, geometry=[poly], crs="EPSG:4326")
    path.parent.mkdir(parents=True, exist_ok=True)
    gdf.to_file(path)


def test_geo_filter_rasters_by_polygon_smoke(tmp_path: Path, monkeypatch) -> None:
    plugin = load_plugin(Path("plugins/geo/geo_filter_rasters_by_polygon.py"))
    assert plugin.module is not None
    monkeypatch.setattr(plugin.module, "rasterio", _FakeRasterio())

    raster_dir = tmp_path / "rasters"
    raster_dir.mkdir(parents=True, exist_ok=True)
    (raster_dir / "tile_in.tif").write_bytes(b"fake")
    (raster_dir / "tile_out.tif").write_bytes(b"fake")

    selector = tmp_path / "selector.gpkg"
    _write_selector(selector)

    output_dir = tmp_path / "filtered"
    out_csv = tmp_path / "meta" / "filtered.csv"
    outputs = plugin.run(
        {
            "raster_dir": str(raster_dir),
            "selector_path": str(selector),
            "copy_selected": True,
            "copy_output_dir": str(output_dir),
            "selected_rasters_csv": str(out_csv),
        },
        _ctx(tmp_path),
    )

    assert outputs["candidate_raster_count"] == 2
    assert outputs["inspected_raster_count"] == 2
    assert outputs["selected_raster_count"] == 1
    assert outputs["copied_raster_count"] >= 1
    assert Path(outputs["selected_rasters_csv"]).exists()
    assert outputs["selected_footprints_path"] == ""
    assert (output_dir / "tile_in.tif").exists()

    with Path(outputs["selected_rasters_csv"]).open("r", encoding="utf-8", newline="") as f:
        rows = list(csv.DictReader(f))
    assert len(rows) == 1
    assert rows[0]["relative_path"] == "tile_in.tif"

def test_geo_filter_rasters_by_polygon_hdr_uses_paired_data_file(tmp_path: Path, monkeypatch) -> None:
    plugin = load_plugin(Path("plugins/geo/geo_filter_rasters_by_polygon.py"))
    assert plugin.module is not None
    monkeypatch.setattr(plugin.module, "rasterio", _FakeRasterio())

    raster_dir = tmp_path / "rasters"
    raster_dir.mkdir(parents=True, exist_ok=True)
    (raster_dir / "tile_pair").write_bytes(b"fake")
    (raster_dir / "tile_pair.hdr").write_text("ENVI", encoding="utf-8")

    selector = tmp_path / "selector.gpkg"
    _write_selector(selector)

    output_dir = tmp_path / "out_hdr"
    out_csv = tmp_path / "meta" / "filtered_hdr.csv"
    outputs = plugin.run(
        {
            "raster_dir": str(raster_dir),
            "selector_path": str(selector),
            "copy_selected": True,
            "copy_output_dir": str(output_dir),
            "selected_rasters_csv": str(out_csv),
            "raster_extensions": ".hdr",
        },
        _ctx(tmp_path),
    )

    assert outputs["candidate_raster_count"] == 1
    assert outputs["inspected_raster_count"] == 1
    assert outputs["selected_raster_count"] == 1
    with Path(outputs["selected_rasters_csv"]).open("r", encoding="utf-8", newline="") as f:
        rows = list(csv.DictReader(f))
    assert len(rows) == 1
    assert rows[0]["relative_path"] == "tile_pair"
    assert (output_dir / "tile_pair").exists()
    assert (output_dir / "tile_pair.hdr").exists()


def test_geo_filter_rasters_by_polygon_skips_writes_when_not_configured(tmp_path: Path, monkeypatch) -> None:
    plugin = load_plugin(Path("plugins/geo/geo_filter_rasters_by_polygon.py"))
    assert plugin.module is not None
    monkeypatch.setattr(plugin.module, "rasterio", _FakeRasterio())

    raster_dir = tmp_path / "rasters"
    raster_dir.mkdir(parents=True, exist_ok=True)
    (raster_dir / "tile_in.tif").write_bytes(b"fake")

    selector = tmp_path / "selector.gpkg"
    _write_selector(selector)

    outputs = plugin.run(
        {
            "raster_dir": str(raster_dir),
            "selector_path": str(selector),
        },
        _ctx(tmp_path),
    )

    assert outputs["selected_raster_count"] == 1
    assert outputs["copied_raster_count"] == 0
    assert outputs["selected_rasters_csv"] == ""
    assert outputs["selected_footprints_path"] == ""

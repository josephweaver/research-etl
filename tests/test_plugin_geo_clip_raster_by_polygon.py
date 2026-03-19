from __future__ import annotations

from pathlib import Path

import numpy as np
import pytest

from etl.plugins.base import PluginContext, load_plugin


gpd = pytest.importorskip("geopandas")
rasterio = pytest.importorskip("rasterio")
shapely_geometry = pytest.importorskip("shapely.geometry")
from rasterio.transform import from_origin  # type: ignore


def _ctx(tmp_path: Path) -> PluginContext:
    return PluginContext(run_id="r1", workdir=tmp_path, log=lambda *a, **k: None)


def _write_raster(path: Path) -> None:
    data = np.arange(1, 17, dtype=np.uint8).reshape(1, 4, 4)
    with rasterio.open(
        path,
        "w",
        driver="GTiff",
        height=4,
        width=4,
        count=1,
        dtype=data.dtype,
        crs="EPSG:4326",
        transform=from_origin(-100.0, 50.0, 1.0, 1.0),
    ) as dst:
        dst.write(data)


def _write_states(path: Path) -> None:
    gdf = gpd.GeoDataFrame(
        {
            "STUSPS": ["MI", "WI"],
        },
        geometry=[
            shapely_geometry.box(-100.0, 47.0, -98.0, 50.0),
            shapely_geometry.box(-98.0, 47.0, -96.0, 50.0),
        ],
        crs="EPSG:4326",
    )
    path.parent.mkdir(parents=True, exist_ok=True)
    gdf.to_file(path)


def test_geo_clip_raster_by_polygon_writes_clipped_raster(tmp_path: Path) -> None:
    plugin = load_plugin(Path("plugins/geo/geo_clip_raster_by_polygon.py"))
    raster_path = tmp_path / "input.tif"
    state_path = tmp_path / "states.gpkg"
    output_path = tmp_path / "out" / "mi.tif"
    _write_raster(raster_path)
    _write_states(state_path)

    outputs = plugin.run(
        {
            "raster_path": str(raster_path),
            "selector_path": str(state_path),
            "output_path": str(output_path),
            "key": "STUSPS",
            "value": "MI",
            "overwrite": True,
        },
        _ctx(tmp_path),
    )

    assert outputs["feature_count"] == 1
    assert outputs["filter_key"] == "STUSPS"
    assert outputs["filter_value"] == "MI"
    assert output_path.exists()
    with rasterio.open(output_path) as ds:
        assert ds.width == 2
        assert ds.height == 3
        arr = ds.read(1)
    assert arr.shape == (3, 2)


def test_geo_clip_raster_by_polygon_supports_where_filter(tmp_path: Path) -> None:
    plugin = load_plugin(Path("plugins/geo/geo_clip_raster_by_polygon.py"))
    raster_path = tmp_path / "input.tif"
    state_path = tmp_path / "states.gpkg"
    output_path = tmp_path / "out" / "multi.tif"
    _write_raster(raster_path)
    _write_states(state_path)

    outputs = plugin.run(
        {
            "raster_path": str(raster_path),
            "selector_path": str(state_path),
            "output_path": str(output_path),
            "where": "STUSPS in (MI, WI)",
            "overwrite": True,
        },
        _ctx(tmp_path),
    )

    assert outputs["feature_count"] == 2
    with rasterio.open(output_path) as ds:
        assert ds.width == 4
        assert ds.height == 3


def test_geo_clip_raster_by_polygon_supports_directory_mode(tmp_path: Path) -> None:
    plugin = load_plugin(Path("plugins/geo/geo_clip_raster_by_polygon.py"))
    input_dir = tmp_path / "inputs" / "2000"
    state_path = tmp_path / "states.gpkg"
    output_dir = tmp_path / "out" / "2000"
    input_dir.mkdir(parents=True, exist_ok=True)
    _write_raster(input_dir / "ppt_20000101.tif")
    _write_raster(input_dir / "ppt_20000102.tif")
    _write_states(state_path)

    outputs = plugin.run(
        {
            "input_dir": str(input_dir),
            "output_dir": str(output_dir),
            "filename_glob": "*.tif",
            "selector_path": str(state_path),
            "key": "STUSPS",
            "value": "MI",
            "overwrite": True,
        },
        _ctx(tmp_path),
    )

    assert outputs["input_count"] == 2
    assert outputs["generated_count"] == 2
    assert outputs["skipped_count"] == 0
    assert output_dir.exists()
    assert (output_dir / "ppt_20000101.tif").exists()
    assert (output_dir / "ppt_20000102.tif").exists()

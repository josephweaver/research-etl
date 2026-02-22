# research-etl
# Copyright (c) 2026 Joseph Weaver
# This file is part of the research-etl project and is licensed under the MIT License.
# You may not use this file except in compliance with the License.
# See https://github.com/josephweaver/research-etl for details.

from __future__ import annotations

from pathlib import Path

import pytest

from etl.plugins.base import PluginContext, load_plugin


gpd = pytest.importorskip("geopandas")
np = pytest.importorskip("numpy")
rasterio = pytest.importorskip("rasterio")
shapely_geometry = pytest.importorskip("shapely.geometry")
from rasterio.transform import from_origin  # type: ignore  # noqa: E402


def _ctx(tmp_path: Path) -> PluginContext:
    return PluginContext(run_id="r1", workdir=tmp_path, log=lambda *a, **k: None)


def _write_raster(path: Path) -> None:
    arr = np.array(
        [
            [0, 1, 1],
            [0, 1, 2],
            [0, 2, 2],
        ],
        dtype=np.int16,
    )
    transform = from_origin(-100.0, 40.0, 1.0, 1.0)
    with rasterio.open(
        path,
        "w",
        driver="GTiff",
        height=arr.shape[0],
        width=arr.shape[1],
        count=1,
        dtype=arr.dtype,
        crs="EPSG:4326",
        transform=transform,
    ) as ds:
        ds.write(arr, 1)


def test_geo_raster_polygonize_excludes_na_value(tmp_path: Path) -> None:
    plugin = load_plugin(Path("plugins/geo/geo_raster_polygonize.py"))
    src = tmp_path / "field_id.tif"
    out = tmp_path / "fields.gpkg"
    _write_raster(src)

    outputs = plugin.run(
        {
            "input_raster": str(src),
            "output_path": str(out),
            "value_field": "field_id",
            "exclude_values": "0",
        },
        _ctx(tmp_path),
    )

    assert outputs["output_feature_count"] > 0
    gdf = gpd.read_file(out)
    assert "field_id" in gdf.columns
    values = sorted(set(int(v) for v in gdf["field_id"].tolist()))
    assert values == [1, 2]


def test_geo_raster_polygonize_applies_selector_mask(tmp_path: Path) -> None:
    plugin = load_plugin(Path("plugins/geo/geo_raster_polygonize.py"))
    src = tmp_path / "field_id2.tif"
    out = tmp_path / "fields_masked.gpkg"
    selector = tmp_path / "selector.gpkg"
    _write_raster(src)

    # Selector intersects only the top-right part (value=1 region).
    poly = shapely_geometry.box(-99.2, 38.8, -97.0, 40.1)
    sgdf = gpd.GeoDataFrame({"name": ["aoi"]}, geometry=[poly], crs="EPSG:4326")
    sgdf.to_file(selector, driver="GPKG")

    outputs = plugin.run(
        {
            "input_raster": str(src),
            "output_path": str(out),
            "value_field": "field_id",
            "exclude_values": "0",
            "selector_path": str(selector),
        },
        _ctx(tmp_path),
    )

    assert outputs["selector_count"] == 1
    gdf = gpd.read_file(out)
    values = set(int(v) for v in gdf["field_id"].tolist())
    assert values == {1}

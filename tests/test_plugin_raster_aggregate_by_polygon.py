# research-etl
# Copyright (c) 2026 Joseph Weaver
# This file is part of the research-etl project and is licensed under the MIT License.
# You may not use this file except in compliance with the License.
# See https://github.com/josephweaver/research-etl for details.

from __future__ import annotations

import csv
from pathlib import Path

import numpy as np
import pytest

from etl.plugins.base import PluginContext, load_plugin


gpd = pytest.importorskip("geopandas")
rasterio = pytest.importorskip("rasterio")
shapely_geometry = pytest.importorskip("shapely.geometry")


def _ctx(tmp_path: Path) -> PluginContext:
    return PluginContext(run_id="r1", workdir=tmp_path, log=lambda *a, **k: None)


def _write_raster(path: Path) -> None:
    data = np.array(
        [
            [1, 2, 3, 4],
            [5, 6, 7, 8],
            [9, 10, 11, 12],
            [13, 14, 15, 16],
        ],
        dtype="float32",
    )
    transform = rasterio.transform.from_origin(0, 4, 1, 1)
    with rasterio.open(
        path,
        "w",
        driver="GTiff",
        height=4,
        width=4,
        count=1,
        dtype="float32",
        crs="EPSG:4326",
        transform=transform,
        nodata=-9999.0,
    ) as ds:
        ds.write(data, 1)


def _write_raster_with_nan(path: Path) -> None:
    data = np.array(
        [
            [1, np.nan, 3, 4],
            [5, 6, 7, 8],
            [9, 10, 11, np.nan],
            [13, 14, 15, 16],
        ],
        dtype="float32",
    )
    transform = rasterio.transform.from_origin(0, 4, 1, 1)
    with rasterio.open(
        path,
        "w",
        driver="GTiff",
        height=4,
        width=4,
        count=1,
        dtype="float32",
        crs="EPSG:4326",
        transform=transform,
        nodata=-9999.0,
    ) as ds:
        ds.write(data, 1)


def _write_raster_with_empty_intersection(path: Path) -> None:
    data = np.array(
        [
            [np.nan, np.nan, 3, 4],
            [np.nan, np.nan, 7, 8],
            [9, 10, 11, 12],
            [13, 14, 15, 16],
        ],
        dtype="float32",
    )
    transform = rasterio.transform.from_origin(0, 4, 1, 1)
    with rasterio.open(
        path,
        "w",
        driver="GTiff",
        height=4,
        width=4,
        count=1,
        dtype="float32",
        crs="EPSG:4326",
        transform=transform,
        nodata=-9999.0,
    ) as ds:
        ds.write(data, 1)


def _write_polygons(path: Path) -> None:
    geoms = [
        shapely_geometry.box(0, 2, 2, 4),
        shapely_geometry.box(2, 0, 4, 2),
    ]
    gdf = gpd.GeoDataFrame(
        {
            "GEOID": ["001", "002"],
            "NAME": ["A", "B"],
            "tile_field_id": ["h18v05_001", "h18v05_002"],
        },
        geometry=geoms,
        crs="EPSG:4326",
    )
    gdf.to_file(path)


def _write_polygons_with_outside(path: Path) -> None:
    geoms = [
        shapely_geometry.box(0, 2, 2, 4),
        shapely_geometry.box(2, 0, 4, 2),
        shapely_geometry.box(10, 10, 12, 12),
    ]
    gdf = gpd.GeoDataFrame(
        {
            "GEOID": ["001", "002", "999"],
            "NAME": ["A", "B", "Outside"],
        },
        geometry=geoms,
        crs="EPSG:4326",
    )
    gdf.to_file(path)


def test_raster_aggregate_by_polygon_basic(tmp_path: Path) -> None:
    plugin = load_plugin(Path("plugins/geo/raster_aggregate_by_polygon.py"))

    raster_path = tmp_path / "PRISM_ppt_stable_4kmD2_20260115_bil.tif"
    polygon_path = tmp_path / "polygons.gpkg"
    output_path = tmp_path / "polygon_daily.csv"
    _write_raster(raster_path)
    _write_polygons(polygon_path)

    outputs = plugin.run(
        {
            "raster_path": str(raster_path),
            "polygon_path": str(polygon_path),
            "output_path": str(output_path),
            "polygon_id_field": "GEOID",
            "polygon_name_field": "NAME",
            "aggregations": "sum,mean,count,max",
            "value_prefix": "ppt",
            "day_from_filename_regex": r"(\d{8})",
        },
        _ctx(tmp_path),
    )

    assert outputs["row_count"] == 2
    assert outputs["day"] == "20260115"

    with output_path.open("r", encoding="utf-8", newline="") as f:
        rows = list(csv.DictReader(f))

    by_id = {r["polygon_id"]: r for r in rows}
    assert by_id["001"]["polygon_name"] == "A"
    assert float(by_id["001"]["ppt_sum"]) == 14.0
    assert float(by_id["001"]["ppt_mean"]) == 3.5
    assert int(float(by_id["001"]["ppt_count"])) == 4
    assert float(by_id["001"]["ppt_max"]) == 6.0
    assert by_id["002"]["polygon_name"] == "B"
    assert float(by_id["002"]["ppt_sum"]) == 54.0
    assert float(by_id["002"]["ppt_mean"]) == 13.5
    assert int(float(by_id["002"]["ppt_count"])) == 4
    assert float(by_id["002"]["ppt_max"]) == 16.0


def test_raster_aggregate_by_polygon_custom_columns(tmp_path: Path) -> None:
    plugin = load_plugin(Path("plugins/geo/raster_aggregate_by_polygon.py"))

    raster_path = tmp_path / "corn_2016.tif"
    polygon_path = tmp_path / "polygons.gpkg"
    output_path = tmp_path / "polygon_custom.csv"
    _write_raster(raster_path)
    _write_polygons(polygon_path)

    plugin.run(
        {
            "raster_path": str(raster_path),
            "polygon_path": str(polygon_path),
            "output_path": str(output_path),
            "polygon_id_field": "GEOID",
            "polygon_name_field": "NAME",
            "columns": """
- source: polygon.tile_field_id
  name: tile_field_id
  type: str
- source: literal
  value: "2016"
  name: year
  type: int
  format: "04d"
- source: raster.mean
  name: corn_yield_mean
  type: double
  format: ".2f"
- source: raster.count
  name: pixel_count
  type: int
""",
        },
        _ctx(tmp_path),
    )

    with output_path.open("r", encoding="utf-8", newline="") as f:
        rows = list(csv.DictReader(f))

    assert rows[0].keys() == {"tile_field_id", "year", "corn_yield_mean", "pixel_count"}
    by_id = {r["tile_field_id"]: r for r in rows}
    assert by_id["h18v05_001"]["year"] == "2016"
    assert by_id["h18v05_001"]["corn_yield_mean"] == "3.50"
    assert int(by_id["h18v05_001"]["pixel_count"]) == 4
    assert by_id["h18v05_002"]["year"] == "2016"
    assert by_id["h18v05_002"]["corn_yield_mean"] == "13.50"
    assert int(by_id["h18v05_002"]["pixel_count"]) == 4


def test_raster_aggregate_by_polygon_rejects_bad_agg(tmp_path: Path) -> None:
    plugin = load_plugin(Path("plugins/geo/raster_aggregate_by_polygon.py"))

    raster_path = tmp_path / "x.tif"
    polygon_path = tmp_path / "polygons.gpkg"
    _write_raster(raster_path)
    _write_polygons(polygon_path)

    with pytest.raises(ValueError, match="Unsupported aggregation"):
        plugin.run(
            {"raster_path": str(raster_path), "polygon_path": str(polygon_path), "aggregations": "sum,p97"},
            _ctx(tmp_path),
        )


def test_raster_aggregate_by_polygon_quantile_aliases(tmp_path: Path) -> None:
    plugin = load_plugin(Path("plugins/geo/raster_aggregate_by_polygon.py"))

    raster_path = tmp_path / "PRISM_ppt_stable_4kmD2_20260115_bil.tif"
    polygon_path = tmp_path / "polygons.gpkg"
    output_path = tmp_path / "polygon_quantiles.csv"
    _write_raster(raster_path)
    _write_polygons(polygon_path)

    outputs = plugin.run(
        {
            "raster_path": str(raster_path),
            "polygon_path": str(polygon_path),
            "output_path": str(output_path),
            "polygon_id_field": "GEOID",
            "polygon_name_field": "NAME",
            "aggregations": "min,p5,q1,med,avg,q3,p95,max",
            "value_prefix": "ppt",
            "day_from_filename_regex": r"(\d{8})",
        },
        _ctx(tmp_path),
    )

    assert outputs["row_count"] == 2
    with output_path.open("r", encoding="utf-8", newline="") as f:
        rows = list(csv.DictReader(f))
    expected_cols = {"ppt_min", "ppt_p5", "ppt_q1", "ppt_med", "ppt_avg", "ppt_q3", "ppt_p95", "ppt_max"}
    assert expected_cols.issubset(set(rows[0].keys()))


def test_raster_aggregate_by_polygon_ignores_nan_pixels(tmp_path: Path) -> None:
    plugin = load_plugin(Path("plugins/geo/raster_aggregate_by_polygon.py"))

    raster_path = tmp_path / "corn_2016.tif"
    polygon_path = tmp_path / "polygons.gpkg"
    output_path = tmp_path / "polygon_nan.csv"
    _write_raster_with_nan(raster_path)
    _write_polygons(polygon_path)

    plugin.run(
        {
            "raster_path": str(raster_path),
            "polygon_path": str(polygon_path),
            "output_path": str(output_path),
            "polygon_id_field": "GEOID",
            "polygon_name_field": "NAME",
            "aggregations": "sum,mean,count,max",
            "value_prefix": "ppt",
        },
        _ctx(tmp_path),
    )

    with output_path.open("r", encoding="utf-8", newline="") as f:
        rows = list(csv.DictReader(f))

    by_id = {r["polygon_id"]: r for r in rows}
    assert float(by_id["001"]["ppt_sum"]) == 12.0
    assert float(by_id["001"]["ppt_mean"]) == 4.0
    assert int(float(by_id["001"]["ppt_count"])) == 3
    assert float(by_id["001"]["ppt_max"]) == 6.0
    assert float(by_id["002"]["ppt_sum"]) == 42.0
    assert float(by_id["002"]["ppt_mean"]) == 14.0
    assert int(float(by_id["002"]["ppt_count"])) == 3
    assert float(by_id["002"]["ppt_max"]) == 16.0


def test_raster_aggregate_by_polygon_keeps_empty_polygons_by_default(tmp_path: Path) -> None:
    plugin = load_plugin(Path("plugins/geo/raster_aggregate_by_polygon.py"))

    raster_path = tmp_path / "corn_2016.tif"
    polygon_path = tmp_path / "polygons_with_outside.gpkg"
    output_path = tmp_path / "polygon_default.csv"
    _write_raster_with_empty_intersection(raster_path)
    _write_polygons_with_outside(polygon_path)

    plugin.run(
        {
            "raster_path": str(raster_path),
            "polygon_path": str(polygon_path),
            "output_path": str(output_path),
            "polygon_id_field": "GEOID",
            "polygon_name_field": "NAME",
            "aggregations": "mean,count",
            "value_prefix": "ppt",
        },
        _ctx(tmp_path),
    )

    with output_path.open("r", encoding="utf-8", newline="") as f:
        rows = list(csv.DictReader(f))

    by_id = {r["polygon_id"]: r for r in rows}
    assert set(by_id) == {"001", "002", "999"}
    assert by_id["001"]["ppt_mean"] == ""
    assert int(float(by_id["001"]["ppt_count"])) == 0
    assert by_id["999"]["ppt_mean"] == ""
    assert int(float(by_id["999"]["ppt_count"])) == 0


def test_raster_aggregate_by_polygon_can_drop_empty_polygons(tmp_path: Path) -> None:
    plugin = load_plugin(Path("plugins/geo/raster_aggregate_by_polygon.py"))

    raster_path = tmp_path / "corn_2016.tif"
    polygon_path = tmp_path / "polygons_with_outside.gpkg"
    output_path = tmp_path / "polygon_no_empty.csv"
    _write_raster_with_empty_intersection(raster_path)
    _write_polygons_with_outside(polygon_path)

    plugin.run(
        {
            "raster_path": str(raster_path),
            "polygon_path": str(polygon_path),
            "output_path": str(output_path),
            "polygon_id_field": "GEOID",
            "polygon_name_field": "NAME",
            "aggregations": "mean,count",
            "value_prefix": "ppt",
            "include_empty_polygons": False,
        },
        _ctx(tmp_path),
    )

    with output_path.open("r", encoding="utf-8", newline="") as f:
        rows = list(csv.DictReader(f))

    by_id = {r["polygon_id"]: r for r in rows}
    assert set(by_id) == {"002"}
    assert float(by_id["002"]["ppt_mean"]) == 13.5
    assert int(float(by_id["002"]["ppt_count"])) == 4

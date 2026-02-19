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


def _write_counties(path: Path) -> None:
    geoms = [
        shapely_geometry.box(0, 2, 2, 4),
        shapely_geometry.box(2, 0, 4, 2),
    ]
    gdf = gpd.GeoDataFrame(
        {
            "GEOID": ["001", "002"],
            "NAME": ["A", "B"],
        },
        geometry=geoms,
        crs="EPSG:4326",
    )
    gdf.to_file(path)


def test_geo_county_raster_aggregate_basic(tmp_path: Path) -> None:
    plugin = load_plugin(Path("plugins/geo/geo_county_raster_aggregate.py"))

    raster_path = tmp_path / "PRISM_ppt_stable_4kmD2_20260115_bil.tif"
    county_path = tmp_path / "counties.gpkg"
    output_path = tmp_path / "county_daily.csv"
    _write_raster(raster_path)
    _write_counties(county_path)

    outputs = plugin.run(
        {
            "raster_path": str(raster_path),
            "county_path": str(county_path),
            "output_path": str(output_path),
            "county_id_field": "GEOID",
            "county_name_field": "NAME",
            "aggregations": "sum,mean,count,max",
            "value_prefix": "ppt",
            "day_from_filename_regex": r"(\d{8})",
        },
        _ctx(tmp_path),
    )

    assert outputs["row_count"] == 2
    assert outputs["day"] == "20260115"
    assert Path(outputs["output_path"]).exists()

    with output_path.open("r", encoding="utf-8", newline="") as f:
        rows = list(csv.DictReader(f))

    assert len(rows) == 2
    by_id = {r["county_id"]: r for r in rows}

    assert by_id["001"]["county_name"] == "A"
    assert float(by_id["001"]["ppt_sum"]) == 14.0
    assert float(by_id["001"]["ppt_mean"]) == 3.5
    assert int(float(by_id["001"]["ppt_count"])) == 4
    assert float(by_id["001"]["ppt_max"]) == 6.0

    assert by_id["002"]["county_name"] == "B"
    assert float(by_id["002"]["ppt_sum"]) == 54.0
    assert float(by_id["002"]["ppt_mean"]) == 13.5
    assert int(float(by_id["002"]["ppt_count"])) == 4
    assert float(by_id["002"]["ppt_max"]) == 16.0


def test_geo_county_raster_aggregate_rejects_bad_agg(tmp_path: Path) -> None:
    plugin = load_plugin(Path("plugins/geo/geo_county_raster_aggregate.py"))

    raster_path = tmp_path / "x.tif"
    county_path = tmp_path / "counties.gpkg"
    _write_raster(raster_path)
    _write_counties(county_path)

    with pytest.raises(ValueError, match="Unsupported aggregation"):
        plugin.run(
            {
                "raster_path": str(raster_path),
                "county_path": str(county_path),
                "aggregations": "sum,p97",
            },
            _ctx(tmp_path),
        )


def test_geo_county_raster_aggregate_quantile_aliases(tmp_path: Path) -> None:
    plugin = load_plugin(Path("plugins/geo/geo_county_raster_aggregate.py"))

    raster_path = tmp_path / "PRISM_ppt_stable_4kmD2_20260115_bil.tif"
    county_path = tmp_path / "counties.gpkg"
    output_path = tmp_path / "county_quantiles.csv"
    _write_raster(raster_path)
    _write_counties(county_path)

    outputs = plugin.run(
        {
            "raster_path": str(raster_path),
            "county_path": str(county_path),
            "output_path": str(output_path),
            "county_id_field": "GEOID",
            "county_name_field": "NAME",
            "aggregations": "min,p5,q1,med,avg,q3,p95,max",
            "value_prefix": "ppt",
            "day_from_filename_regex": r"(\d{8})",
        },
        _ctx(tmp_path),
    )

    assert outputs["row_count"] == 2
    with output_path.open("r", encoding="utf-8", newline="") as f:
        rows = list(csv.DictReader(f))
    assert len(rows) == 2
    expected_cols = {"ppt_min", "ppt_p5", "ppt_q1", "ppt_med", "ppt_avg", "ppt_q3", "ppt_p95", "ppt_max"}
    assert expected_cols.issubset(set(rows[0].keys()))


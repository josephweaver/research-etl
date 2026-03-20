from __future__ import annotations

from pathlib import Path

import pytest

gpd = pytest.importorskip("geopandas")
np = pytest.importorskip("numpy")
rasterio = pytest.importorskip("rasterio")
from rasterio.transform import from_origin  # type: ignore
from shapely.geometry import box  # type: ignore

from etl.plugins.base import PluginContext, load_plugin


def _ctx(tmp_path: Path) -> PluginContext:
    return PluginContext(run_id="r1", workdir=tmp_path, log=lambda *a, **k: None)


def _write_raster(path: Path, data) -> None:
    with rasterio.open(
        path,
        "w",
        driver="GTiff",
        height=int(data.shape[0]),
        width=int(data.shape[1]),
        count=1,
        dtype="float32",
        crs="EPSG:4326",
        transform=from_origin(0, int(data.shape[0]), 1, 1),
        nodata=-9999,
    ) as dst:
        dst.write(data.astype("float32"), 1)


def test_geo_county_weighted_raster_aggregate_computes_weighted_mean(tmp_path: Path) -> None:
    plugin = load_plugin(Path("plugins/geo/geo_county_weighted_raster_aggregate.py"))
    num = tmp_path / "num.tif"
    den = tmp_path / "den.tif"
    _write_raster(num, np.array([[5.0, 0.0], [3.0, 2.0]], dtype="float32"))
    _write_raster(den, np.array([[0.5, 0.0], [0.5, 1.0]], dtype="float32"))
    county = tmp_path / "county.gpkg"
    gdf = gpd.GeoDataFrame({"GEOID": ["26001"], "NAME": ["A"]}, geometry=[box(0, 0, 2, 2)], crs="EPSG:4326")
    gdf.to_file(county, driver="GPKG")
    out_csv = tmp_path / "out.csv"
    out = plugin.run(
        {
            "input_raster": str(num),
            "weight_raster": str(den),
            "county_path": str(county),
            "output_path": str(out_csv),
            "value_prefix": "ppt",
        },
        _ctx(tmp_path),
    )
    assert out["row_count"] == 1
    text = out_csv.read_text(encoding="utf-8")
    assert "ppt_weighted_mean" in text
    assert ",4.0," in text

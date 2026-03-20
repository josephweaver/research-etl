from __future__ import annotations

from pathlib import Path

import pytest

np = pytest.importorskip("numpy")
rasterio = pytest.importorskip("rasterio")
from rasterio.transform import from_origin  # type: ignore

from etl.plugins.base import PluginContext, load_plugin


def _ctx(tmp_path: Path) -> PluginContext:
    return PluginContext(run_id="r1", workdir=tmp_path, log=lambda *a, **k: None)


def test_geo_raster_value_mask_matches_explicit_values_and_ranges(tmp_path: Path) -> None:
    plugin = load_plugin(Path("plugins/geo/geo_raster_value_mask.py"))

    input_path = tmp_path / "input.tif"
    arr = np.array(
        [
            [1, 2, 5],
            [7, 8, 9],
            [0, 255, 3],
        ],
        dtype="uint16",
    )
    with rasterio.open(
        input_path,
        "w",
        driver="GTiff",
        height=3,
        width=3,
        count=1,
        dtype="uint16",
        crs="EPSG:4326",
        transform=from_origin(0, 3, 1, 1),
        nodata=255,
    ) as dst:
        dst.write(arr, 1)

    output_path = tmp_path / "mask.tif"
    outputs = plugin.run(
        {
            "input_raster": str(input_path),
            "output_path": str(output_path),
            "include_values": "1,5",
            "include_ranges": "7-8",
        },
        _ctx(tmp_path),
    )

    with rasterio.open(output_path) as ds:
        data = ds.read(1)
        assert ds.nodata == 255
        assert data.tolist() == [
            [1, 0, 1],
            [1, 1, 0],
            [0, 255, 0],
        ]
    assert outputs["matched_pixel_count"] == 4
    assert outputs["valid_pixel_count"] == 8


def test_geo_raster_value_mask_supports_invert_and_literal_range_list(tmp_path: Path) -> None:
    plugin = load_plugin(Path("plugins/geo/geo_raster_value_mask.py"))

    input_path = tmp_path / "input2.tif"
    arr = np.array([[1, 2], [3, 4]], dtype="uint8")
    with rasterio.open(
        input_path,
        "w",
        driver="GTiff",
        height=2,
        width=2,
        count=1,
        dtype="uint8",
        crs="EPSG:4326",
        transform=from_origin(0, 2, 1, 1),
        nodata=0,
    ) as dst:
        dst.write(arr, 1)

    output_path = tmp_path / "mask2.tif"
    plugin.run(
        {
            "input_raster": str(input_path),
            "output_path": str(output_path),
            "include_ranges": "[[2,3]]",
            "invert": True,
            "mask_nodata_value": 9,
        },
        _ctx(tmp_path),
    )

    with rasterio.open(output_path) as ds:
        data = ds.read(1)
        assert data.tolist() == [
            [1, 0],
            [0, 1],
        ]

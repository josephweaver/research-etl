from __future__ import annotations

import csv
from pathlib import Path

import numpy as np
import pytest

from etl.plugins.base import PluginContext, load_plugin


rasterio = pytest.importorskip("rasterio")
from rasterio.transform import from_origin  # type: ignore  # noqa: E402


def _ctx(tmp_path: Path) -> PluginContext:
    return PluginContext(run_id="r1", workdir=tmp_path, log=lambda *a, **k: None)


def _write_daily_raster(path: Path, value: float) -> None:
    data = np.full((1, 2, 2), value, dtype=np.float32)
    path.parent.mkdir(parents=True, exist_ok=True)
    with rasterio.open(
        path,
        "w",
        driver="GTiff",
        height=2,
        width=2,
        count=1,
        dtype=data.dtype,
        crs="EPSG:4326",
        transform=from_origin(-85.0, 48.0, 1.0, 1.0),
        nodata=-9999.0,
    ) as dst:
        dst.write(data)


def test_geo_raster_spi_writes_zscore_outputs(tmp_path: Path) -> None:
    plugin = load_plugin(Path("plugins/geo/geo_raster_spi.py"))
    input_dir = tmp_path / "inputs"
    output_dir = tmp_path / "outputs"
    for day, value in (
        ("20240101", 1.0),
        ("20240102", 2.0),
        ("20240103", 3.0),
    ):
        _write_daily_raster(input_dir / "2004" / f"ppt_{day}.tif", value)

    outputs = plugin.run(
        {
            "input_glob": str(input_dir / "*" / "*.tif"),
            "output_dir": str(output_dir),
            "method": "zscore",
            "overwrite": True,
        },
        _ctx(tmp_path),
    )

    assert outputs["input_count"] == 3
    assert outputs["generated_count"] == 3
    out_day1 = output_dir / "2024" / "ppt_20240101.tif"
    out_day2 = output_dir / "2024" / "ppt_20240102.tif"
    out_day3 = output_dir / "2024" / "ppt_20240103.tif"
    assert out_day1.exists()
    assert out_day2.exists()
    assert out_day3.exists()

    with rasterio.open(out_day1) as ds:
        arr1 = ds.read(1)
    with rasterio.open(out_day2) as ds:
        arr2 = ds.read(1)
    with rasterio.open(out_day3) as ds:
        arr3 = ds.read(1)

    assert np.allclose(arr1, np.full((2, 2), -1.0, dtype=np.float32))
    assert np.allclose(arr2, np.full((2, 2), 0.0, dtype=np.float32))
    assert np.allclose(arr3, np.full((2, 2), 1.0, dtype=np.float32))

    manifest_path = Path(outputs["manifest_path"])
    with manifest_path.open("r", encoding="utf-8", newline="") as f:
        rows = list(csv.DictReader(f))
    assert [row["day"] for row in rows] == ["20240101", "20240102", "20240103"]


def test_geo_raster_spi_rejects_unknown_method(tmp_path: Path) -> None:
    plugin = load_plugin(Path("plugins/geo/geo_raster_spi.py"))
    _write_daily_raster(tmp_path / "inputs" / "2004" / "ppt_20240101.tif", 1.0)
    with pytest.raises(ValueError, match="Unsupported method"):
        plugin.run(
            {
                "input_glob": str(tmp_path / "inputs" / "*" / "*.tif"),
                "output_dir": str(tmp_path / "outputs"),
                "method": "gamma",
            },
            _ctx(tmp_path),
        )

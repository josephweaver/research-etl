from __future__ import annotations

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


def test_geo_raster_running_window_writes_trailing_sum_outputs(tmp_path: Path) -> None:
    plugin = load_plugin(Path("plugins/geo/geo_raster_running_window.py"))
    input_dir = tmp_path / "inputs"
    output_dir = tmp_path / "outputs"
    for day, value in (
        ("20240101", 1.0),
        ("20240102", 2.0),
        ("20240103", 3.0),
        ("20240104", 4.0),
    ):
        _write_daily_raster(input_dir / f"ppt_{day}.tif", value)

    outputs = plugin.run(
        {
            "input_glob": str(input_dir / "*.tif"),
            "output_dir": str(output_dir),
            "metric": "sum",
            "windows": "3",
            "overwrite": True,
        },
        _ctx(tmp_path),
    )

    assert outputs["input_count"] == 4
    assert outputs["generated_count"] == 2
    assert outputs["metric"] == "sum"
    assert outputs["window"] == 3

    out_day3 = output_dir / "ppt_20240103.tif"
    out_day4 = output_dir / "ppt_20240104.tif"
    assert out_day3.exists()
    assert out_day4.exists()

    with rasterio.open(out_day3) as ds:
        arr_day3 = ds.read(1)
    with rasterio.open(out_day4) as ds:
        arr_day4 = ds.read(1)

    assert np.allclose(arr_day3, np.full((2, 2), 6.0, dtype=np.float32))
    assert np.allclose(arr_day4, np.full((2, 2), 9.0, dtype=np.float32))


def test_geo_raster_running_window_rejects_unknown_metric(tmp_path: Path) -> None:
    plugin = load_plugin(Path("plugins/geo/geo_raster_running_window.py"))
    _write_daily_raster(tmp_path / "inputs" / "ppt_20240101.tif", 1.0)

    with pytest.raises(ValueError, match="Unsupported metric"):
        plugin.run(
            {
                "input_glob": str(tmp_path / "inputs" / "*.tif"),
                "output_dir": str(tmp_path / "outputs"),
                "metric": "mean",
                "windows": "3",
            },
            _ctx(tmp_path),
        )


def test_geo_raster_running_window_supports_output_day_filter(tmp_path: Path) -> None:
    plugin = load_plugin(Path("plugins/geo/geo_raster_running_window.py"))
    input_dir = tmp_path / "inputs"
    output_dir = tmp_path / "outputs"
    for day, value in (
        ("20231230", 10.0),
        ("20231231", 20.0),
        ("20240101", 30.0),
        ("20240102", 40.0),
    ):
        _write_daily_raster(input_dir / f"ppt_{day}.tif", value)

    outputs = plugin.run(
        {
            "input_glob": str(input_dir / "*.tif"),
            "output_dir": str(output_dir),
            "metric": "sum",
            "windows": "3",
            "output_start_day": "20240101",
            "output_end_day": "20240131",
            "overwrite": True,
        },
        _ctx(tmp_path),
    )

    assert outputs["generated_count"] == 2
    assert not (output_dir / "ppt_20231231.tif").exists()
    out_day1 = output_dir / "ppt_20240101.tif"
    out_day2 = output_dir / "ppt_20240102.tif"
    assert out_day1.exists()
    assert out_day2.exists()
    with rasterio.open(out_day1) as ds:
        arr_day1 = ds.read(1)
    with rasterio.open(out_day2) as ds:
        arr_day2 = ds.read(1)
    assert np.allclose(arr_day1, np.full((2, 2), 60.0, dtype=np.float32))
    assert np.allclose(arr_day2, np.full((2, 2), 90.0, dtype=np.float32))


def test_geo_raster_running_window_supports_input_day_filter(tmp_path: Path) -> None:
    plugin = load_plugin(Path("plugins/geo/geo_raster_running_window.py"))
    input_dir = tmp_path / "inputs"
    output_dir = tmp_path / "outputs"
    for day, value in (
        ("20231229", 5.0),
        ("20231230", 10.0),
        ("20231231", 20.0),
        ("20240101", 30.0),
        ("20240102", 40.0),
    ):
        _write_daily_raster(input_dir / f"ppt_{day}.tif", value)

    outputs = plugin.run(
        {
            "input_glob": str(input_dir / "*.tif"),
            "output_dir": str(output_dir),
            "metric": "sum",
            "windows": "3",
            "input_start_day": "20231230",
            "input_end_day": "20240102",
            "output_start_day": "20240101",
            "output_end_day": "20240131",
            "overwrite": True,
        },
        _ctx(tmp_path),
    )

    assert outputs["input_count"] == 4
    assert not (output_dir / "ppt_20231229.tif").exists()
    out_day1 = output_dir / "ppt_20240101.tif"
    assert out_day1.exists()
    with rasterio.open(out_day1) as ds:
        arr_day1 = ds.read(1)
    assert np.allclose(arr_day1, np.full((2, 2), 60.0, dtype=np.float32))


def test_geo_raster_running_window_can_derive_windows_from_target_year(tmp_path: Path) -> None:
    plugin = load_plugin(Path("plugins/geo/geo_raster_running_window.py"))
    input_dir = tmp_path / "inputs"
    output_dir = tmp_path / "outputs"
    for day, value in (
        ("20011230", 10.0),
        ("20011231", 20.0),
        ("20020101", 30.0),
        ("20020102", 40.0),
    ):
        _write_daily_raster(input_dir / f"ppt_{day}.tif", value)

    outputs = plugin.run(
        {
            "input_glob": str(input_dir / "*.tif"),
            "output_dir": str(output_dir),
            "metric": "sum",
            "windows": "3",
            "target_year": 2002,
            "overwrite": True,
        },
        _ctx(tmp_path),
    )

    assert outputs["input_count"] == 4
    out_day1 = output_dir / "ppt_20020101.tif"
    out_day2 = output_dir / "ppt_20020102.tif"
    assert out_day1.exists()
    assert out_day2.exists()
    assert not (output_dir / "ppt_20011231.tif").exists()

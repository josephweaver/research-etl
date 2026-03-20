from __future__ import annotations

from pathlib import Path

import pytest

np = pytest.importorskip("numpy")
rasterio = pytest.importorskip("rasterio")
from rasterio.transform import from_origin  # type: ignore

from etl.plugins.base import PluginContext, load_plugin


def _ctx(tmp_path: Path) -> PluginContext:
    return PluginContext(run_id="r1", workdir=tmp_path, log=lambda *a, **k: None)


def _write_raster(path: Path, data, *, dtype: str = "float32") -> None:
    with rasterio.open(
        path,
        "w",
        driver="GTiff",
        height=int(data.shape[0]),
        width=int(data.shape[1]),
        count=1,
        dtype=dtype,
        crs="EPSG:4326",
        transform=from_origin(0, int(data.shape[0]), 1, 1),
        nodata=-9999,
    ) as dst:
        dst.write(data.astype(dtype), 1)


def test_geo_raster_multiply_directory_mode(tmp_path: Path) -> None:
    plugin = load_plugin(Path("plugins/geo/geo_raster_multiply.py"))
    in_dir = tmp_path / "in"
    in_dir.mkdir()
    _write_raster(in_dir / "a.tif", np.array([[2, 4], [6, 8]], dtype="float32"))
    weight = tmp_path / "w.tif"
    _write_raster(weight, np.array([[0.5, 0.25], [1.0, 0.0]], dtype="float32"))
    out_dir = tmp_path / "out"
    out = plugin.run(
        {"input_dir": str(in_dir), "weight_raster": str(weight), "output_dir": str(out_dir), "overwrite": True},
        _ctx(tmp_path),
    )
    assert out["generated_count"] == 1
    with rasterio.open(out_dir / "a.tif") as ds:
        data = ds.read(1)
        assert data.tolist() == [[1.0, 1.0], [6.0, 0.0]]

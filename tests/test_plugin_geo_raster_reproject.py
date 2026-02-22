# research-etl
# Copyright (c) 2026 Joseph Weaver
# This file is part of the research-etl project and is licensed under the MIT License.
# You may not use this file except in compliance with the License.
# See https://github.com/josephweaver/research-etl for details.

from __future__ import annotations

from pathlib import Path

import pytest

from etl.plugins.base import PluginContext, load_plugin


rasterio = pytest.importorskip("rasterio")
np = pytest.importorskip("numpy")
from rasterio.transform import from_origin  # type: ignore  # noqa: E402


def _ctx(tmp_path: Path) -> PluginContext:
    return PluginContext(run_id="r1", workdir=tmp_path, log=lambda *a, **k: None)


def test_geo_raster_reproject_reprojects_to_target_crs(tmp_path: Path) -> None:
    plugin = load_plugin(Path("plugins/geo/geo_raster_reproject.py"))

    src_path = tmp_path / "src.tif"
    out_path = tmp_path / "out_5070.tif"

    data = np.array([[1, 2], [3, 4]], dtype=np.uint16)
    transform = from_origin(-90.0, 45.0, 0.1, 0.1)
    with rasterio.open(
        src_path,
        "w",
        driver="GTiff",
        height=2,
        width=2,
        count=1,
        dtype=data.dtype,
        crs="EPSG:4326",
        transform=transform,
    ) as ds:
        ds.write(data, 1)

    outputs = plugin.run(
        {
            "input_path": str(src_path),
            "output_path": str(out_path),
            "target_crs": "EPSG:5070",
            "resampling": "nearest",
        },
        _ctx(tmp_path),
    )

    assert Path(outputs["output_path"]).exists()
    assert outputs["band_count"] == 1
    with rasterio.open(out_path) as out:
        assert out.crs is not None
        assert "5070" in str(out.crs)
        assert out.count == 1


def test_geo_raster_reproject_requires_target_crs(tmp_path: Path) -> None:
    plugin = load_plugin(Path("plugins/geo/geo_raster_reproject.py"))
    with pytest.raises(ValueError, match="target_crs"):
        plugin.run({"input_path": "x.tif", "output_path": "y.tif"}, _ctx(tmp_path))

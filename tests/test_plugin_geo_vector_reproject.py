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
shapely_geometry = pytest.importorskip("shapely.geometry")


def _ctx(tmp_path: Path) -> PluginContext:
    return PluginContext(run_id="r1", workdir=tmp_path, log=lambda *a, **k: None)


def test_geo_vector_reproject_reprojects_to_target_crs(tmp_path: Path) -> None:
    plugin = load_plugin(Path("plugins/geo/geo_vector_reproject.py"))

    src_path = tmp_path / "src.gpkg"
    out_path = tmp_path / "out.gpkg"

    gdf = gpd.GeoDataFrame(
        {"id": [1], "name": ["a"]},
        geometry=[shapely_geometry.Point(-84.5, 42.7)],
        crs="EPSG:4326",
    )
    gdf.to_file(src_path, driver="GPKG")

    outputs = plugin.run(
        {
            "input_path": str(src_path),
            "output_path": str(out_path),
            "target_crs": "EPSG:5070",
        },
        _ctx(tmp_path),
    )

    assert outputs["input_feature_count"] == 1
    assert outputs["output_feature_count"] == 1
    reproj = gpd.read_file(out_path)
    assert reproj.crs is not None
    assert "5070" in str(reproj.crs)


def test_geo_vector_reproject_can_subset_fields(tmp_path: Path) -> None:
    plugin = load_plugin(Path("plugins/geo/geo_vector_reproject.py"))

    src_path = tmp_path / "src2.gpkg"
    out_path = tmp_path / "out2.gpkg"

    gdf = gpd.GeoDataFrame(
        {"id": [1], "name": ["a"], "extra": ["z"]},
        geometry=[shapely_geometry.Point(-90.0, 40.0)],
        crs="EPSG:4326",
    )
    gdf.to_file(src_path, driver="GPKG")

    plugin.run(
        {
            "input_path": str(src_path),
            "output_path": str(out_path),
            "target_crs": "EPSG:5070",
            "keep_fields": "id,name",
        },
        _ctx(tmp_path),
    )

    out = gpd.read_file(out_path)
    assert "id" in out.columns
    assert "name" in out.columns
    assert "extra" not in out.columns
    assert "geometry" in out.columns

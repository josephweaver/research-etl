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


def test_geo_vector_combine_merges_inputs_with_source_name(tmp_path: Path) -> None:
    plugin = load_plugin(Path("plugins/geo/geo_vector_combine.py"))

    src_dir = tmp_path / "in"
    src_dir.mkdir(parents=True, exist_ok=True)
    a = src_dir / "a.gpkg"
    b = src_dir / "b.gpkg"
    out = tmp_path / "combined.gpkg"

    gdf_a = gpd.GeoDataFrame(
        {"field_id": [1]},
        geometry=[shapely_geometry.Point(-90.0, 40.0)],
        crs="EPSG:4326",
    )
    gdf_b = gpd.GeoDataFrame(
        {"field_id": [2]},
        geometry=[shapely_geometry.Point(-89.0, 41.0)],
        crs="EPSG:4326",
    )
    gdf_a.to_file(a, driver="GPKG")
    gdf_b.to_file(b, driver="GPKG")

    outputs = plugin.run(
        {
            "input_glob": f"{src_dir.as_posix()}/*.gpkg",
            "output_path": str(out),
            "target_crs": "EPSG:5070",
            "include_source_name": True,
            "source_name_field": "source_name",
        },
        _ctx(tmp_path),
    )

    assert outputs["input_count"] == 2
    assert outputs["row_count"] == 2
    merged = gpd.read_file(out)
    assert "source_name" in merged.columns
    assert set(merged["source_name"].tolist()) == {"a.gpkg", "b.gpkg"}
    assert "5070" in str(merged.crs)

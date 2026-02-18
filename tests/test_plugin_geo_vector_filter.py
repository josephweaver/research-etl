from __future__ import annotations

from pathlib import Path

import pytest

from etl.plugins.base import PluginContext, load_plugin


gpd = pytest.importorskip("geopandas")
shapely_geometry = pytest.importorskip("shapely.geometry")


def _ctx(tmp_path: Path) -> PluginContext:
    return PluginContext(run_id="r1", workdir=tmp_path, log=lambda *a, **k: None)


def _write_states(input_path: Path) -> None:
    geoms = [
        shapely_geometry.Point(-84.5, 43.0),
        shapely_geometry.Point(-89.6, 44.6),
        shapely_geometry.Point(-86.1, 40.1),
        shapely_geometry.Point(-93.5, 42.0),
    ]
    gdf = gpd.GeoDataFrame(
        {
            "STUSPS": ["MI", "WI", "IN", "IA"],
            "STATEFP": ["26", "55", "18", "19"],
        },
        geometry=geoms,
        crs="EPSG:4326",
    )
    input_path.parent.mkdir(parents=True, exist_ok=True)
    gdf.to_file(input_path)


def test_geo_vector_filter_in_list(tmp_path: Path) -> None:
    plugin = load_plugin(Path("plugins/geo_vector_filter.py"))
    src = tmp_path / "state.gpkg"
    _write_states(src)
    out = tmp_path / "filtered.gpkg"

    outputs = plugin.run(
        {
            "input_path": str(src),
            "output_path": str(out),
            "key": "STUSPS",
            "op": "in",
            "values": "MI,WI,IN",
        },
        _ctx(tmp_path),
    )

    assert outputs["input_feature_count"] == 4
    assert outputs["output_feature_count"] == 3
    filtered = gpd.read_file(out)
    assert sorted(filtered["STUSPS"].tolist()) == ["IN", "MI", "WI"]


def test_geo_vector_filter_where_expression(tmp_path: Path) -> None:
    plugin = load_plugin(Path("plugins/geo_vector_filter.py"))
    src = tmp_path / "state.gpkg"
    _write_states(src)
    out = tmp_path / "filtered_where.gpkg"

    outputs = plugin.run(
        {
            "input_path": str(src),
            "output_path": str(out),
            "where": "STUSPS in (MI, WI)",
        },
        _ctx(tmp_path),
    )

    assert outputs["filter_key"] == "STUSPS"
    assert outputs["filter_op"] == "in"
    assert outputs["output_feature_count"] == 2
    filtered = gpd.read_file(out)
    assert sorted(filtered["STUSPS"].tolist()) == ["MI", "WI"]


def test_geo_vector_filter_fails_on_missing_key(tmp_path: Path) -> None:
    plugin = load_plugin(Path("plugins/geo_vector_filter.py"))
    src = tmp_path / "state.gpkg"
    _write_states(src)
    out = tmp_path / "filtered_fail.gpkg"

    with pytest.raises(ValueError, match="not found"):
        plugin.run(
            {
                "input_path": str(src),
                "output_path": str(out),
                "key": "ABBR",
                "op": "eq",
                "value": "MI",
            },
            _ctx(tmp_path),
        )

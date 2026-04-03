# research-etl
# Copyright (c) 2026 Joseph Weaver
# This file is part of the research-etl project and is licensed under the MIT License.
# You may not use this file except in compliance with the License.
# See https://github.com/josephweaver/research-etl for details.

from __future__ import annotations

import importlib.util
from pathlib import Path

import pytest


ASSET_REPO = Path("../landcore-etl-pipelines")


def _load_module(name: str, path: Path):
    spec = importlib.util.spec_from_file_location(name, path)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)  # type: ignore[call-arg]
    return module


def test_read_tiles_of_interest_dedupes(tmp_path: Path) -> None:
    mod = _load_module("build_google_maps_field_keys", ASSET_REPO / "scripts/yanroy/build_google_maps_field_keys.py")
    csv_path = tmp_path / "tiles.of.interest.csv"
    csv_path.write_text(
        "\n".join(
            [
                "statefp,stusps,tile_id,h,v",
                "17,IL,h10v04,10,4",
                "17,IL,h10v04,10,4",
                "26,MI,h11v05,11,5",
            ]
        )
        + "\n",
        encoding="utf-8",
    )

    rows = mod._read_tiles_of_interest(csv_path)
    assert rows == ["h10v04", "h11v05"]


def test_candidate_rasters_skips_sidecars(tmp_path: Path) -> None:
    mod = _load_module("build_google_maps_field_keys", ASSET_REPO / "scripts/yanroy/build_google_maps_field_keys.py")
    tile_dir = tmp_path / "h10v04"
    tile_dir.mkdir(parents=True, exist_ok=True)
    (tile_dir / "WELD_h10v04_2010_field_segments.hdr").write_text("hdr", encoding="utf-8")
    (tile_dir / "WELD_h10v04_2010_field_segments").write_text("raster", encoding="utf-8")

    candidates = mod._candidate_rasters(tile_dir)
    assert len(candidates) == 1
    assert candidates[0].name == "WELD_h10v04_2010_field_segments"


def test_tippecanoe_missing_binary_raises(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    mod = _load_module(
        "build_vector_tiles_with_tippecanoe", ASSET_REPO / "scripts/yanroy/build_vector_tiles_with_tippecanoe.py"
    )
    in_file = tmp_path / "in.ndjson"
    in_file.write_text('{"type":"FeatureCollection","features":[]}\n', encoding="utf-8")

    monkeypatch.setattr(mod.shutil, "which", lambda _: None)

    with pytest.raises(FileNotFoundError):
        mod.build_vector_tiles_with_tippecanoe(
            input_ndjson=in_file,
            output_mbtiles=tmp_path / "out.mbtiles",
            layer_name="yanroy_fields",
        )


def test_tippecanoe_defaults_to_uncompressed_tiles(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    mod = _load_module(
        "build_vector_tiles_with_tippecanoe", ASSET_REPO / "scripts/yanroy/build_vector_tiles_with_tippecanoe.py"
    )
    in_file = tmp_path / "in.ndjson"
    out_dir = tmp_path / "tiles"
    in_file.write_text('{"type":"FeatureCollection","features":[]}\n', encoding="utf-8")

    monkeypatch.setattr(mod.shutil, "which", lambda _: "tippecanoe")

    captured: list[list[str]] = []

    def _fake_run(cmd: list[str]) -> tuple[str, str]:
        captured.append(list(cmd))
        out_dir.mkdir(parents=True, exist_ok=True)
        return "", ""

    monkeypatch.setattr(mod, "_run_tippecanoe", _fake_run)

    result = mod.build_vector_tiles_with_tippecanoe(
        input_ndjson=in_file,
        output_mbtiles=None,
        output_tiles_dir=out_dir,
        layer_name="yanroy_fields",
    )

    assert len(captured) == 1
    assert "--no-tile-compression" in captured[0]
    assert result["output_tiles_dir"] == out_dir.resolve().as_posix()


def test_export_tile_polygon_geojson_normalizes_integer_like_field_ids() -> None:
    mod = _load_module(
        "export_tile_polygon_geojson", ASSET_REPO / "scripts/yanroy/export_tile_polygon_geojson.py"
    )

    assert mod._normalize_field_id_text(1673.0) == "1673"
    assert mod._normalize_field_id_text("1673.0") == "1673"
    assert mod._normalize_field_id_text("1673") == "1673"


def test_export_tile_polygon_geojson_allows_empty_inputs(tmp_path: Path) -> None:
    geopandas = pytest.importorskip("geopandas")
    pandas = pytest.importorskip("pandas")

    mod = _load_module(
        "export_tile_polygon_geojson", ASSET_REPO / "scripts/yanroy/export_tile_polygon_geojson.py"
    )

    input_vector = tmp_path / "WELD_h17v12_2010_field_segments.gpkg"
    output_dir = tmp_path / "geojson"
    summary_json = tmp_path / "summary.json"

    empty_gdf = geopandas.GeoDataFrame(
        {"field_id": pandas.Series(dtype="object")},
        geometry=geopandas.GeoSeries([], crs="EPSG:5070"),
        crs="EPSG:5070",
    )
    empty_gdf.to_file(input_vector, driver="GPKG")

    result = mod.export_tile_polygon_geojson(
        input_vector=input_vector,
        output_dir=output_dir,
        summary_json=summary_json,
    )

    assert result["tile_id"] == "h17v12"
    assert result["row_count"] == 0
    assert result["empty_input"] is True
    assert (output_dir / "h17v12.geojson").exists()
    assert summary_json.exists()


def test_normalize_polygon_field_ids_coerces_integer_like_values() -> None:
    mod = _load_module(
        "normalize_polygon_field_ids", ASSET_REPO / "scripts/yanroy/normalize_polygon_field_ids.py"
    )

    assert mod._normalize_field_id(1673.0) == 1673
    assert mod._normalize_field_id("1673.0") == 1673
    assert mod._normalize_field_id("1673") == 1673


def test_normalize_polygon_field_ids_allows_empty_inputs(tmp_path: Path) -> None:
    geopandas = pytest.importorskip("geopandas")
    pandas = pytest.importorskip("pandas")

    mod = _load_module(
        "normalize_polygon_field_ids", ASSET_REPO / "scripts/yanroy/normalize_polygon_field_ids.py"
    )

    input_vector = tmp_path / "input.gpkg"
    output_vector = tmp_path / "output.gpkg"
    summary_json = tmp_path / "summary.json"

    empty_gdf = geopandas.GeoDataFrame(
        {"field_id": pandas.array([], dtype="Int64")},
        geometry=geopandas.GeoSeries([], crs="EPSG:5070"),
        crs="EPSG:5070",
    )
    empty_gdf.to_file(input_vector, driver="GPKG")

    result = mod.normalize_polygon_field_ids(
        input_vector=input_vector,
        output_vector=output_vector,
        summary_json=summary_json,
        overwrite=True,
    )

    assert result["row_count"] == 0
    assert result["changed_rows"] == 0
    assert result["output_type"] == "integer"
    assert result["empty_input"] is True
    assert output_vector.exists()
    assert summary_json.exists()


def test_assign_polygon_fips_extracts_tile_id_from_filename() -> None:
    mod = _load_module(
        "assign_polygon_fips", ASSET_REPO / "scripts/yanroy/assign_polygon_fips.py"
    )

    assert mod._extract_tile_id("WELD_h12v05_2010_field_segments.gpkg") == "h12v05"


def test_assign_polygon_fips_allows_empty_inputs(tmp_path: Path) -> None:
    geopandas = pytest.importorskip("geopandas")
    pandas = pytest.importorskip("pandas")

    mod = _load_module(
        "assign_polygon_fips", ASSET_REPO / "scripts/yanroy/assign_polygon_fips.py"
    )

    input_vector = tmp_path / "WELD_h17v12_2010_field_segments.gpkg"
    county_path = tmp_path / "county.gpkg"
    output_vector = tmp_path / "output.gpkg"
    summary_json = tmp_path / "summary.json"

    empty_fields = geopandas.GeoDataFrame(
        {"field_id": pandas.Series(dtype="object")},
        geometry=geopandas.GeoSeries([], crs="EPSG:5070"),
        crs="EPSG:5070",
    )
    empty_fields.to_file(input_vector, driver="GPKG")

    county_gdf = geopandas.GeoDataFrame(
        {
            "GEOID": ["17001"],
            "NAME": ["Adams"],
            "NAMELSAD": ["Adams County"],
            "STATEFP": ["17"],
            "COUNTYFP": ["001"],
        },
        geometry=geopandas.GeoSeries.from_wkt(["POLYGON ((0 0, 0 1, 1 1, 1 0, 0 0))"], crs="EPSG:5070"),
        crs="EPSG:5070",
    )
    county_gdf.to_file(county_path, driver="GPKG")

    result = mod.assign_polygon_fips(
        input_vector=input_vector,
        county_path=county_path,
        output_vector=output_vector,
        summary_json=summary_json,
        tile="h17v12",
        overwrite=True,
    )

    assert result["tile"] == "h17v12"
    assert result["input_row_count"] == 0
    assert result["output_row_count"] == 0
    assert result["empty_input"] is True
    assert output_vector.exists()
    assert summary_json.exists()


def test_simplify_geojson_geometry_rejects_negative_tolerance(tmp_path: Path) -> None:
    mod = _load_module(
        "simplify_geojson_geometry", ASSET_REPO / "scripts/yanroy/simplify_geojson_geometry.py"
    )

    input_vector = tmp_path / "input.geojson"
    input_vector.write_text('{"type":"FeatureCollection","features":[]}\n', encoding="utf-8")

    with pytest.raises(ValueError):
        mod.simplify_geojson_geometry(
            input_vector=input_vector,
            output_vector=tmp_path / "output.geojson",
            tolerance=-1.0,
        )


def test_build_google_maps_raster_tiles_tile_bounds_cover_world() -> None:
    mod = _load_module(
        "build_google_maps_raster_tiles", ASSET_REPO / "scripts/yanroy/build_google_maps_raster_tiles.py"
    )

    minx, miny, maxx, maxy = mod._tile_bounds_3857(0, 0, 0)

    assert round(minx) == -20037508
    assert round(miny) == -20037508
    assert round(maxx) == 20037508
    assert round(maxy) == 20037508

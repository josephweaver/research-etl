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


def test_normalize_polygon_field_ids_coerces_integer_like_values() -> None:
    mod = _load_module(
        "normalize_polygon_field_ids", ASSET_REPO / "scripts/yanroy/normalize_polygon_field_ids.py"
    )

    assert mod._normalize_field_id(1673.0) == 1673
    assert mod._normalize_field_id("1673.0") == 1673
    assert mod._normalize_field_id("1673") == 1673


def test_assign_polygon_fips_extracts_tile_id_from_filename() -> None:
    mod = _load_module(
        "assign_polygon_fips", ASSET_REPO / "scripts/yanroy/assign_polygon_fips.py"
    )

    assert mod._extract_tile_id("WELD_h12v05_2010_field_segments.gpkg") == "h12v05"

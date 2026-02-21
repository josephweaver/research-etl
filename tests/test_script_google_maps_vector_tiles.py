# research-etl
# Copyright (c) 2026 Joseph Weaver
# This file is part of the research-etl project and is licensed under the MIT License.
# You may not use this file except in compliance with the License.
# See https://github.com/josephweaver/research-etl for details.

from __future__ import annotations

import importlib.util
from pathlib import Path

import pytest


def _load_module(name: str, path: Path):
    spec = importlib.util.spec_from_file_location(name, path)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)  # type: ignore[call-arg]
    return module


def test_read_tiles_of_interest_dedupes(tmp_path: Path) -> None:
    mod = _load_module("build_field_polygons_ndjson", Path("scripts/yanroy/build_field_polygons_ndjson.py"))
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
    mod = _load_module("build_field_polygons_ndjson", Path("scripts/yanroy/build_field_polygons_ndjson.py"))
    tile_dir = tmp_path / "h10v04"
    tile_dir.mkdir(parents=True, exist_ok=True)
    (tile_dir / "WELD_h10v04_2010_field_segments.hdr").write_text("hdr", encoding="utf-8")
    (tile_dir / "WELD_h10v04_2010_field_segments").write_text("raster", encoding="utf-8")

    candidates = mod._candidate_rasters(tile_dir)
    assert len(candidates) == 1
    assert candidates[0].name == "WELD_h10v04_2010_field_segments"


def test_tippecanoe_missing_binary_raises(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    mod = _load_module(
        "build_vector_tiles_with_tippecanoe", Path("scripts/yanroy/build_vector_tiles_with_tippecanoe.py")
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

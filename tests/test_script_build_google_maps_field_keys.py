from __future__ import annotations

import importlib.util
from pathlib import Path


def _load_module(path: Path):
    spec = importlib.util.spec_from_file_location("build_google_maps_field_keys", path)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)  # type: ignore[call-arg]
    return module


def test_read_tiles_of_interest_dedupes_and_orders(tmp_path: Path) -> None:
    mod = _load_module(Path("scripts/yanroy/build_google_maps_field_keys.py"))
    tiles_csv = tmp_path / "tiles.of.interest.csv"
    tiles_csv.write_text(
        "\n".join(
            [
                "statefp,stusps,tile_id,h,v",
                "17,IL,h10v04,10,4",
                "17,IL,h10v04,10,4",
                "26,MI,h11v04,11,4",
            ]
        )
        + "\n",
        encoding="utf-8",
    )

    tiles = mod._read_tiles_of_interest(tiles_csv)
    assert tiles == ["h10v04", "h11v04"]


def test_candidate_rasters_ignores_sidecars(tmp_path: Path) -> None:
    mod = _load_module(Path("scripts/yanroy/build_google_maps_field_keys.py"))
    tile_dir = tmp_path / "h10v04"
    tile_dir.mkdir(parents=True, exist_ok=True)
    (tile_dir / "WELD_h10v04_2010_field_segments.hdr").write_text("hdr", encoding="utf-8")
    (tile_dir / "WELD_h10v04_2010_field_segments").write_text("raster", encoding="utf-8")

    candidates = mod._candidate_rasters(tile_dir)
    assert len(candidates) == 1
    assert candidates[0].name == "WELD_h10v04_2010_field_segments"

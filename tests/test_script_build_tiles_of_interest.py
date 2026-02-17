from __future__ import annotations

import importlib.util
from pathlib import Path


def _load_module(path: Path):
    spec = importlib.util.spec_from_file_location("build_tiles_of_interest", path)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)  # type: ignore[call-arg]
    return module


def test_read_state_codes_parses_fips_and_abbr(tmp_path: Path) -> None:
    mod = _load_module(Path("scripts/yanroy/build_tiles_of_interest.py"))
    csv_path = tmp_path / "states.of.interest.csv"
    csv_path.write_text(
        "\n".join(
            [
                "statefp,stusps",
                "26,MI",
                "06,CA",
            ]
        )
        + "\n",
        encoding="utf-8",
    )
    fips, abbr = mod._read_state_codes(csv_path)
    assert fips == {"26", "06"}
    assert abbr == {"MI", "CA"}


def test_read_state_codes_no_header_parses_abbr_lines(tmp_path: Path) -> None:
    mod = _load_module(Path("scripts/yanroy/build_tiles_of_interest_from_facts.py"))
    csv_path = tmp_path / "states.of.interest.csv"
    csv_path.write_text("IL\nMN\nWI\n", encoding="utf-8")

    fips, abbr = mod._read_state_codes(csv_path)
    assert fips == set()
    assert abbr == {"IL", "MN", "WI"}


def test_read_tiles_from_raster_facts_extracts_unique_tiles(tmp_path: Path) -> None:
    mod = _load_module(Path("scripts/yanroy/build_tiles_of_interest_from_facts.py"))
    facts_csv = tmp_path / "raster_facts.csv"
    facts_csv.write_text(
        "\n".join(
            [
                "relative_path,band_index",
                "WELD_h01v08_2010_field_segments,1",
                "WELD_h01v08_2011_field_segments,1",
                "folder/WELD_h12v04_2010_field_segments,1",
            ]
        )
        + "\n",
        encoding="utf-8",
    )

    tiles = mod._read_tiles_from_raster_facts(facts_csv)
    assert tiles == {"h01v08", "h12v04"}

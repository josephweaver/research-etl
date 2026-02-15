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

from __future__ import annotations

from pathlib import Path

import pytest

from etl.datasets.locations import (
    DataLocationConfigError,
    load_data_locations,
    resolve_data_location_alias,
)


def test_load_data_locations_and_resolve_alias(tmp_path: Path) -> None:
    cfg = tmp_path / "data_locations.yml"
    cfg.write_text(
        "\n".join(
            [
                "locations:",
                "  LC_GDrive:",
                "    location_type: gdrive",
                "    transport: rclone",
                "    target_uri: gdrive://LandCore/ETL",
            ]
        ),
        encoding="utf-8",
    )
    data = load_data_locations(cfg)
    spec = resolve_data_location_alias("LC_GDrive", config_data=data)
    assert spec["alias"] == "LC_GDrive"
    assert spec["location_type"] == "gdrive"
    assert spec["transport"] == "rclone"


def test_resolve_data_location_alias_raises_on_unknown(tmp_path: Path) -> None:
    cfg = tmp_path / "data_locations.yml"
    cfg.write_text("locations: {}\n", encoding="utf-8")
    data = load_data_locations(cfg)
    with pytest.raises(DataLocationConfigError, match="Unknown data location alias"):
        resolve_data_location_alias("LC_Missing", config_data=data)

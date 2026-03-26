# research-etl
# Copyright (c) 2026 Joseph Weaver
# This file is part of the research-etl project and is licensed under the MIT License.
# You may not use this file except in compliance with the License.
# See https://github.com/josephweaver/research-etl for details.

from __future__ import annotations

from pathlib import Path

import pytest

from etl.datasets.locations import (
    DataLocationConfigError,
    load_data_locations,
    resolve_data_location_alias,
    resolve_data_locations_config_path,
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


def test_load_data_locations_and_resolve_gcs_alias(tmp_path: Path) -> None:
    cfg = tmp_path / "data_locations.yml"
    cfg.write_text(
        "\n".join(
            [
                "locations:",
                "  LC_GCS:",
                "    location_type: gcs",
                "    transport: gcs",
                "    target_uri: gcs://demo-bucket/lake",
            ]
        ),
        encoding="utf-8",
    )
    data = load_data_locations(cfg)
    spec = resolve_data_location_alias("LC_GCS", config_data=data)
    assert spec["alias"] == "LC_GCS"
    assert spec["location_type"] == "gcs"
    assert spec["transport"] == "gcs"


def test_resolve_data_location_alias_raises_on_unknown(tmp_path: Path) -> None:
    cfg = tmp_path / "data_locations.yml"
    cfg.write_text("locations: {}\n", encoding="utf-8")
    data = load_data_locations(cfg)
    with pytest.raises(DataLocationConfigError, match="Unknown data location alias"):
        resolve_data_location_alias("LC_Missing", config_data=data)


def test_resolve_data_locations_config_path_uses_etl_repo_root_for_relative_path(tmp_path: Path, monkeypatch) -> None:
    repo_root = tmp_path / "repo"
    cfg = repo_root / "config" / "data_locations.yml"
    cfg.parent.mkdir(parents=True)
    cfg.write_text("locations: {}\n", encoding="utf-8")
    monkeypatch.chdir(tmp_path)
    monkeypatch.setenv("ETL_REPO_ROOT", str(repo_root))
    resolved = resolve_data_locations_config_path(Path("config/data_locations.yml"))
    assert resolved == cfg.resolve()

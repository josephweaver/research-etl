# research-etl
# Copyright (c) 2026 Joseph Weaver
# This file is part of the research-etl project and is licensed under the MIT License.
# You may not use this file except in compliance with the License.
# See https://github.com/josephweaver/research-etl for details.

from __future__ import annotations

from pathlib import Path

import etl.datasets as datasets_module
from etl.plugins.base import PluginContext, load_plugin


def _ctx(tmp_path: Path) -> PluginContext:
    return PluginContext(run_id="r1", workdir=tmp_path, log=lambda *a, **k: None)


def test_dataset_store_plugin_calls_service(monkeypatch, tmp_path: Path) -> None:
    captured = {}

    def _fake_store_data(**kwargs):
        captured.update(kwargs)
        return {
            "dataset_id": kwargs["dataset_id"],
            "version_label": kwargs.get("version_label") or "v1",
            "stage": kwargs["stage"],
            "environment": kwargs.get("environment"),
            "location_type": kwargs.get("location_type") or "local_cache",
            "target_uri": kwargs.get("target_uri") or "C:/tmp/cache/file.txt",
            "transport": kwargs.get("transport") or "local_fs",
            "checksum": "abc",
            "size_bytes": 12,
            "dry_run": kwargs.get("dry_run", False),
        }

    monkeypatch.setattr(datasets_module, "store_data", _fake_store_data)
    plugin = load_plugin(Path("plugins/dataset_store.py"))
    source = tmp_path / "payload.txt"
    source.write_text("payload", encoding="utf-8")

    out = plugin.run(
        {
            "dataset_id": "serve.demo",
            "path": str(source),
            "stage": "staging",
            "runtime_context": "local",
            "dry_run": False,
        },
        _ctx(tmp_path),
    )

    assert captured["dataset_id"] == "serve.demo"
    assert captured["source_path"] == str(source)
    assert captured["stage"] == "staging"
    assert out["dataset_id"] == "serve.demo"
    assert out["transport"] == "local_fs"


def test_dataset_store_plugin_requires_inputs(tmp_path: Path) -> None:
    plugin = load_plugin(Path("plugins/dataset_store.py"))
    try:
        plugin.run({"path": "x"}, _ctx(tmp_path))
        assert False, "expected missing dataset_id"
    except ValueError as exc:
        assert "dataset_id is required" in str(exc)


def test_dataset_store_plugin_passes_location_alias(monkeypatch, tmp_path: Path) -> None:
    captured = {}

    def _fake_store_data(**kwargs):
        captured.update(kwargs)
        return {
            "dataset_id": kwargs["dataset_id"],
            "version_label": "v1",
            "stage": kwargs["stage"],
            "environment": kwargs.get("environment"),
            "location_type": kwargs.get("location_type") or "gdrive",
            "target_uri": kwargs.get("target_uri") or "gdrive://LandCore/ETL",
            "transport": kwargs.get("transport") or "rclone",
            "checksum": "abc",
            "size_bytes": 12,
            "dry_run": kwargs.get("dry_run", False),
            "location_alias": kwargs.get("location_alias"),
        }

    monkeypatch.setattr(datasets_module, "store_data", _fake_store_data)
    plugin = load_plugin(Path("plugins/dataset_store.py"))
    source = tmp_path / "payload.txt"
    source.write_text("payload", encoding="utf-8")

    out = plugin.run(
        {
            "dataset_id": "serve.demo",
            "path": str(source),
            "location_alias": "LC_GDrive",
            "locations_config": "config/data_locations.yml",
        },
        _ctx(tmp_path),
    )
    assert captured["location_alias"] == "LC_GDrive"
    assert captured["locations_config_path"] == "config/data_locations.yml"
    assert out["location_alias"] == "LC_GDrive"

from __future__ import annotations

from pathlib import Path

import etl.datasets as datasets_module
from etl.plugins.base import PluginContext, load_plugin


def _ctx(tmp_path: Path) -> PluginContext:
    return PluginContext(run_id="r1", workdir=tmp_path, log=lambda *a, **k: None)


def test_dataset_get_plugin_calls_service(monkeypatch, tmp_path: Path) -> None:
    captured = {}

    def _fake_get_data(**kwargs):
        captured.update(kwargs)
        return {
            "dataset_id": kwargs["dataset_id"],
            "version_label": "v2",
            "environment": "local",
            "location_type": "local_cache",
            "source_uri": "C:/tmp/cache/file.txt",
            "local_path": "C:/tmp/cache/file.txt",
            "transport": "none",
            "fetched": False,
            "checksum": "abc",
            "size_bytes": 12,
            "dry_run": kwargs.get("dry_run", False),
        }

    monkeypatch.setattr(datasets_module, "get_data", _fake_get_data)
    plugin = load_plugin(Path("plugins/dataset_get.py"))
    out = plugin.run(
        {
            "dataset_id": "serve.demo",
            "version": "latest",
            "runtime_context": "local",
        },
        _ctx(tmp_path),
    )

    assert captured["dataset_id"] == "serve.demo"
    assert captured["version"] == "latest"
    assert out["dataset_id"] == "serve.demo"
    assert out["version_label"] == "v2"


def test_dataset_get_plugin_requires_dataset_id(tmp_path: Path) -> None:
    plugin = load_plugin(Path("plugins/dataset_get.py"))
    try:
        plugin.run({}, _ctx(tmp_path))
        assert False, "expected missing dataset_id"
    except ValueError as exc:
        assert "dataset_id is required" in str(exc)


def test_dataset_get_plugin_passes_location_alias(monkeypatch, tmp_path: Path) -> None:
    captured = {}

    def _fake_get_data(**kwargs):
        captured.update(kwargs)
        return {
            "dataset_id": kwargs["dataset_id"],
            "version_label": "v2",
            "environment": "local",
            "location_type": "gdrive",
            "source_uri": "gdrive://LandCore/ETL/demo",
            "local_path": "C:/tmp/cache/file.txt",
            "transport": "rclone",
            "fetched": True,
            "checksum": "abc",
            "size_bytes": 12,
            "dry_run": kwargs.get("dry_run", False),
            "location_alias": kwargs.get("location_alias"),
        }

    monkeypatch.setattr(datasets_module, "get_data", _fake_get_data)
    plugin = load_plugin(Path("plugins/dataset_get.py"))
    out = plugin.run(
        {
            "dataset_id": "serve.demo",
            "location_alias": "LC_GDrive",
            "locations_config": "config/data_locations.yml",
        },
        _ctx(tmp_path),
    )
    assert captured["location_alias"] == "LC_GDrive"
    assert captured["locations_config_path"] == "config/data_locations.yml"
    assert out["location_alias"] == "LC_GDrive"

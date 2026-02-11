from __future__ import annotations

import json
from pathlib import Path

from etl.plugins.base import PluginContext, load_plugin


def _ctx(tmp_path: Path) -> PluginContext:
    return PluginContext(run_id="r1", workdir=tmp_path, log=lambda *a, **k: None)


def test_catalog_resolve_uri_from_catalog_json_entry(tmp_path: Path) -> None:
    plugin = load_plugin(Path("plugins/catalog_resolve_uri.py"))
    catalog = tmp_path / ".runs" / "catalog" / "catalog.json"
    catalog.parent.mkdir(parents=True, exist_ok=True)
    catalog.write_text(
        json.dumps(
            {
                "datasets": {
                    "raw.yanroy_download_v1": {
                        "artifact_uri": "gdrive://landcore/yanroy/raw",
                        "data_class": "RAW",
                    }
                }
            }
        ),
        encoding="utf-8",
    )
    outputs = plugin.run(
        {
            "dataset_id": "raw.yanroy_download_v1",
            "catalog_json": str(catalog),
            "prefer_yaml": False,
        },
        _ctx(tmp_path),
    )
    assert outputs["dataset_id"] == "raw.yanroy_download_v1"
    assert outputs["input_uri"] == "gdrive://landcore/yanroy/raw"
    assert outputs["source"] == "catalog_json"


def test_catalog_resolve_uri_from_catalog_yaml(tmp_path: Path) -> None:
    plugin = load_plugin(Path("plugins/catalog_resolve_uri.py"))
    repo = tmp_path / "landcore-data-catalog"
    yml_path = repo / "datasets" / "raw" / "raw.yanroy_download_v1.yml"
    yml_path.parent.mkdir(parents=True, exist_ok=True)
    yml_path.write_text(
        "\n".join(
            [
                "dataset_id: raw.yanroy_download_v1",
                "data_class: RAW",
                "representations:",
                "  - kind: file",
                "    uri: gdrive://landcore/yanroy/raw",
            ]
        ),
        encoding="utf-8",
    )
    outputs = plugin.run(
        {
            "dataset_id": "raw.yanroy_download_v1",
            "catalog_repo": str(repo),
            "catalog_json": str(tmp_path / ".runs" / "catalog" / "catalog.json"),
            "prefer_yaml": True,
        },
        _ctx(tmp_path),
    )
    assert outputs["input_uri"] == "gdrive://landcore/yanroy/raw"
    assert outputs["source"] == "catalog_yaml"

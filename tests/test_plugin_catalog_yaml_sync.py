from __future__ import annotations

import json
from pathlib import Path

import yaml

from etl.plugins.base import PluginContext, load_plugin


def _ctx(tmp_path: Path) -> PluginContext:
    return PluginContext(run_id="r1", workdir=tmp_path, log=lambda *a, **k: None)


def test_catalog_yaml_sync_creates_dataset_yaml(tmp_path: Path) -> None:
    plugin = load_plugin(Path("plugins/catalog_yaml_sync.py"))
    repo = tmp_path / "landcore-data-catalog"
    (repo / "datasets" / "serve").mkdir(parents=True, exist_ok=True)
    catalog_json = tmp_path / "catalog.json"
    catalog_json.write_text(
        json.dumps(
            {
                "schema_version": 1,
                "datasets": {
                    "serve.demo_v1": {
                        "dataset_id": "serve.demo_v1",
                        "auto": {
                            "title": "Demo",
                            "description": "Desc",
                            "how_to_use_notes": "Filter by id.",
                            "tags": ["demo"],
                            "lineage_upstream": ["model_out.demo_v1"],
                            "quality_validation": ["no dupes"],
                            "quality_known_issues": ["sparse in 2012"],
                            "assumptions": ["values are annual"],
                        },
                    }
                },
            }
        ),
        encoding="utf-8",
    )

    outputs = plugin.run(
        {
            "catalog_json": str(catalog_json),
            "catalog_repo": str(repo),
        },
        _ctx(tmp_path),
    )
    assert outputs["updated_count"] == 1
    out_file = repo / "datasets" / "serve" / "serve.demo_v1.yml"
    assert out_file.exists()
    doc = yaml.safe_load(out_file.read_text(encoding="utf-8"))
    assert doc["dataset_id"] == "serve.demo_v1"
    assert doc["title"] == "Demo"
    assert doc["how_to_use"]["notes"] == "Filter by id."
    assert doc["lineage"]["upstream"] == ["model_out.demo_v1"]


def test_catalog_yaml_sync_preserves_manual_fields_when_not_overwriting(tmp_path: Path) -> None:
    plugin = load_plugin(Path("plugins/catalog_yaml_sync.py"))
    repo = tmp_path / "landcore-data-catalog"
    target = repo / "datasets" / "serve" / "serve.demo_v1.yml"
    target.parent.mkdir(parents=True, exist_ok=True)
    target.write_text(
        yaml.safe_dump(
            {
                "dataset_id": "serve.demo_v1",
                "title": "Manual Title",
                "description": "Manual desc",
                "data_class": "SERVE",
                "owner": "ManualOwner",
                "status": "active",
                "how_to_use": {"notes": "manual usage"},
            },
            sort_keys=False,
        ),
        encoding="utf-8",
    )
    catalog_json = tmp_path / "catalog.json"
    catalog_json.write_text(
        json.dumps(
            {
                "schema_version": 1,
                "datasets": {
                    "serve.demo_v1": {
                        "auto": {
                            "title": "AI Title",
                            "description": "AI desc",
                            "how_to_use_notes": "AI usage",
                        }
                    }
                },
            }
        ),
        encoding="utf-8",
    )

    _ = plugin.run(
        {
            "catalog_json": str(catalog_json),
            "catalog_repo": str(repo),
            "overwrite_managed_fields": False,
        },
        _ctx(tmp_path),
    )
    doc = yaml.safe_load(target.read_text(encoding="utf-8"))
    assert doc["title"] == "Manual Title"
    assert doc["description"] == "Manual desc"
    assert doc["how_to_use"]["notes"] == "manual usage"


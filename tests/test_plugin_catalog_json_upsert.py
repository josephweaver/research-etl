from __future__ import annotations

import json
from pathlib import Path

from etl.plugins.base import PluginContext, load_plugin


def _ctx(tmp_path: Path) -> PluginContext:
    return PluginContext(run_id="r1", workdir=tmp_path, log=lambda *a, **k: None)


def test_catalog_json_upsert_creates_catalog_from_research_file(tmp_path: Path) -> None:
    plugin = load_plugin(Path("plugins/catalog_json_upsert.py"))
    research = tmp_path / "serve.demo_v1.research.json"
    research.write_text(
        json.dumps(
            {
                "title": "Demo",
                "description": "Desc",
                "how_to_use_notes": "Use it",
                "tags": ["demo"],
                "quality_validation": ["no dupes"],
                "quality_known_issues": [],
                "assumptions": ["assume annual"],
                "lineage_upstream": ["model_out.demo_v1"],
            }
        ),
        encoding="utf-8",
    )
    outputs = plugin.run(
        {
            "research_file": str(research),
            "catalog_json": str(tmp_path / "catalog.json"),
            "project_id": "proj_a",
        },
        _ctx(tmp_path),
    )
    catalog = json.loads(Path(outputs["catalog_json"]).read_text(encoding="utf-8"))
    entry = catalog["datasets"]["serve.demo_v1"]
    assert entry["project_id"] == "proj_a"
    assert entry["auto"]["title"] == "Demo"
    assert entry["auto"]["lineage_upstream"] == ["model_out.demo_v1"]


def test_catalog_json_upsert_glob_updates_multiple(tmp_path: Path) -> None:
    plugin = load_plugin(Path("plugins/catalog_json_upsert.py"))
    out_dir = tmp_path / "ai"
    out_dir.mkdir(parents=True, exist_ok=True)
    for dsid in ("serve.a_v1", "serve.b_v1"):
        (out_dir / f"{dsid}.research.json").write_text(
            json.dumps({"title": dsid, "description": "d"}),
            encoding="utf-8",
        )
    outputs = plugin.run(
        {
            "research_glob": "ai/*.research.json",
            "catalog_json": "catalog.json",
        },
        _ctx(tmp_path),
    )
    assert outputs["updated_count"] == 2
    catalog = json.loads(Path(outputs["catalog_json"]).read_text(encoding="utf-8"))
    assert "serve.a_v1" in catalog["datasets"]
    assert "serve.b_v1" in catalog["datasets"]

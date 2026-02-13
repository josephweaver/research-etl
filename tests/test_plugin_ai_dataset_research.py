from __future__ import annotations

import json
from pathlib import Path

from etl.plugins.base import PluginContext, load_plugin


def _ctx(tmp_path: Path) -> PluginContext:
    return PluginContext(run_id="r1", workdir=tmp_path, log=lambda *a, **k: None)


def test_ai_dataset_research_plugin_writes_output_and_artifact(tmp_path: Path) -> None:
    plugin = load_plugin(Path("plugins/ai_dataset_research.py"))
    assert plugin.module is not None

    def _fake_generate(**kwargs):
        return {
            "title": "Demo Dataset",
            "description": "Demo description.",
            "how_to_use_notes": "Filter by county.",
            "tags": ["demo"],
            "quality_validation": ["no dupes"],
            "quality_known_issues": [],
            "assumptions": [],
            "lineage_upstream": [],
        }

    setattr(plugin.module, "generate_dataset_research", _fake_generate)

    outputs = plugin.run(
        {
            "dataset_id": "serve.demo_v1",
            "output_dir": str(tmp_path / "research"),
        },
        _ctx(tmp_path),
    )
    out_file = Path(outputs["output_file"])
    assert out_file.exists()
    payload = json.loads(out_file.read_text(encoding="utf-8"))
    assert payload["title"] == "Demo Dataset"
    assert outputs["_artifacts"][0]["metadata"]["dataset_id"] == "serve.demo_v1"


def test_ai_dataset_research_plugin_uses_specs_file(tmp_path: Path) -> None:
    plugin = load_plugin(Path("plugins/ai_dataset_research.py"))
    assert plugin.module is not None
    specs = tmp_path / "dataset_specs.yml"
    specs.write_text(
        "\n".join(
            [
                "datasets:",
                "  serve.demo_v1:",
                "    data_class: SERVE",
                "    title: Demo",
                "    notes: notes from spec",
                "    supplemental_urls:",
                "      - https://example.com/spec-a",
                "      - https://example.com/spec-b",
            ]
        ),
        encoding="utf-8",
    )
    captured = {}

    def _fake_generate(**kwargs):
        captured.update(kwargs)
        return {
            "title": "Demo",
            "description": "desc",
            "how_to_use_notes": "how",
            "tags": [],
            "quality_validation": [],
            "quality_known_issues": [],
            "assumptions": [],
            "lineage_upstream": [],
        }

    setattr(plugin.module, "generate_dataset_research", _fake_generate)

    _ = plugin.run(
        {
            "dataset_id": "serve.demo_v1",
            "specs_file": str(specs),
        },
        _ctx(tmp_path),
    )
    assert captured["data_class"] == "SERVE"
    assert captured["title"] == "Demo"
    assert captured["notes"] == "notes from spec"
    assert captured["supplemental_urls"] == ["https://example.com/spec-a", "https://example.com/spec-b"]


def test_ai_dataset_research_plugin_reads_notes_file(tmp_path: Path) -> None:
    plugin = load_plugin(Path("plugins/ai_dataset_research.py"))
    assert plugin.module is not None
    notes_file = tmp_path / "notes.txt"
    notes_file.write_text("line a\nline b\n", encoding="utf-8")
    captured = {}

    def _fake_generate(**kwargs):
        captured.update(kwargs)
        return {
            "title": "Demo",
            "description": "desc",
            "how_to_use_notes": "how",
            "tags": [],
            "quality_validation": [],
            "quality_known_issues": [],
            "assumptions": [],
            "lineage_upstream": [],
        }

    setattr(plugin.module, "generate_dataset_research", _fake_generate)

    _ = plugin.run(
        {
            "dataset_id": "serve.demo_v1",
            "notes": "base notes",
            "notes_file": str(notes_file),
        },
        _ctx(tmp_path),
    )
    assert captured["notes"] == "base notes\n\nline a\nline b"


def test_ai_dataset_research_plugin_default_output_dir_uses_ctx_workdir(tmp_path: Path) -> None:
    plugin = load_plugin(Path("plugins/ai_dataset_research.py"))
    assert plugin.module is not None

    def _fake_generate(**kwargs):
        return {
            "title": "Demo Dataset",
            "description": "Demo description.",
            "how_to_use_notes": "Filter by county.",
            "tags": ["demo"],
            "quality_validation": ["no dupes"],
            "quality_known_issues": [],
            "assumptions": [],
            "lineage_upstream": [],
        }

    setattr(plugin.module, "generate_dataset_research", _fake_generate)

    ctx = _ctx(tmp_path / "step_work")
    outputs = plugin.run(
        {
            "dataset_id": "serve.demo_v1",
        },
        ctx,
    )
    out_file = Path(outputs["output_file"])
    assert out_file.exists()
    expected_root = (ctx.workdir / "ai_research").resolve()
    assert str(out_file.resolve()).startswith(str(expected_root))

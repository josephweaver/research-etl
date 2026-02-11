from __future__ import annotations

import json
from pathlib import Path

import cli


def test_cmd_ai_research_writes_output(monkeypatch, tmp_path: Path, capsys):
    sample = tmp_path / "sample.txt"
    sample.write_text("sample rows", encoding="utf-8")
    output = tmp_path / "out" / "research.json"

    monkeypatch.setattr(
        cli,
        "generate_dataset_research",
        lambda **kwargs: {
            "title": "x",
            "description": "d",
            "how_to_use_notes": "h",
            "tags": ["a"],
            "quality_validation": [],
            "quality_known_issues": [],
            "assumptions": [],
            "lineage_upstream": [],
        },
    )

    rc = cli.main(
        [
            "ai",
            "research",
            "--dataset-id",
            "serve.demo_v1",
            "--sample-file",
            str(sample),
            "--output",
            str(output),
        ]
    )
    assert rc == 0
    payload = json.loads(output.read_text(encoding="utf-8"))
    assert payload["title"] == "x"
    captured = capsys.readouterr()
    assert "Wrote AI research output" in captured.out


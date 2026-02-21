# research-etl
# Copyright (c) 2026 Joseph Weaver
# This file is part of the research-etl project and is licensed under the MIT License.
# You may not use this file except in compliance with the License.
# See https://github.com/josephweaver/research-etl for details.

from __future__ import annotations

import json
from pathlib import Path

import cli


def test_cmd_ai_research_writes_output(monkeypatch, tmp_path: Path, capsys):
    sample = tmp_path / "sample.txt"
    sample.write_text("sample rows", encoding="utf-8")
    urls_file = tmp_path / "urls.txt"
    urls_file.write_text("# comment\nhttps://example.com/a\n", encoding="utf-8")
    output = tmp_path / "out" / "research.json"
    captured_kwargs = {}

    monkeypatch.setattr(
        cli,
        "generate_dataset_research",
        lambda **kwargs: (
            captured_kwargs.update(kwargs)
            or {
                "title": "x",
                "description": "d",
                "how_to_use_notes": "h",
                "tags": ["a"],
                "quality_validation": [],
                "quality_known_issues": [],
                "assumptions": [],
                "lineage_upstream": [],
            }
        ),
    )

    rc = cli.main(
        [
            "ai",
            "research",
            "--dataset-id",
            "serve.demo_v1",
            "--sample-file",
            str(sample),
            "--supplemental-url",
            "https://example.com/b",
            "--supplemental-urls-file",
            str(urls_file),
            "--output",
            str(output),
        ]
    )
    assert rc == 0
    payload = json.loads(output.read_text(encoding="utf-8"))
    assert payload["title"] == "x"
    assert captured_kwargs["supplemental_urls"] == ["https://example.com/b", "https://example.com/a"]
    captured = capsys.readouterr()
    assert "Wrote AI research output" in captured.out

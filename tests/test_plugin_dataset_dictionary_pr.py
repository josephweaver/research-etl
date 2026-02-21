# research-etl
# Copyright (c) 2026 Joseph Weaver
# This file is part of the research-etl project and is licensed under the MIT License.
# You may not use this file except in compliance with the License.
# See https://github.com/josephweaver/research-etl for details.

from __future__ import annotations

from pathlib import Path

import etl.dictionary_pr as dictionary_pr_module
from etl.plugins.base import PluginContext, load_plugin


def _ctx(tmp_path: Path) -> PluginContext:
    return PluginContext(run_id="r1", workdir=tmp_path, log=lambda *a, **k: None)


def test_dataset_dictionary_pr_plugin_calls_service(monkeypatch, tmp_path: Path) -> None:
    captured = {}
    src = tmp_path / "entry.yml"
    src.write_text("dataset_id: serve.demo\n", encoding="utf-8")

    def _fake_create_dictionary_pr(**kwargs):
        captured.update(kwargs)
        return {
            "dataset_id": kwargs["dataset_id"],
            "repo_key": kwargs["repo_key"],
            "provider": "github",
            "repo": "landcore/landcore-data-catalog",
            "local_repo_path": "C:/repos/landcore-data-catalog",
            "file_path": "datasets/serve.demo.yml",
            "base_branch": "main",
            "branch_name": "bot/dict-serve.demo-1",
            "has_changes": True,
            "commit_sha": "abc123",
            "pr_url": "https://github.com/landcore/landcore-data-catalog/pull/7",
            "pr_number": 7,
            "review_status": "open",
            "dry_run": False,
            "operation_log": ["start", "done"],
        }

    monkeypatch.setattr(dictionary_pr_module, "create_dictionary_pr", _fake_create_dictionary_pr)
    plugin = load_plugin(Path("plugins/dataset_dictionary_pr.py"))
    out = plugin.run(
        {
            "dataset_id": "serve.demo",
            "repo_key": "catalog",
            "source_file": str(src),
            "no_pr": False,
            "no_github_api": False,
            "dry_run": False,
        },
        _ctx(tmp_path),
    )

    assert captured["dataset_id"] == "serve.demo"
    assert captured["repo_key"] == "catalog"
    assert captured["source_file"] == str(src)
    assert out["pr_number"] == 7
    assert out["review_status"] == "open"


def test_dataset_dictionary_pr_plugin_requires_args(tmp_path: Path) -> None:
    plugin = load_plugin(Path("plugins/dataset_dictionary_pr.py"))
    try:
        plugin.run({}, _ctx(tmp_path))
        assert False, "expected validation error"
    except ValueError as exc:
        assert "dataset_id is required" in str(exc)

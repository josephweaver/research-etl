# research-etl
# Copyright (c) 2026 Joseph Weaver
# This file is part of the research-etl project and is licensed under the MIT License.
# You may not use this file except in compliance with the License.
# See https://github.com/josephweaver/research-etl for details.

from __future__ import annotations

from pathlib import Path

from etl.pipeline import Pipeline, Step
from etl.provenance import collect_run_provenance


def test_collect_run_provenance_returns_expected_keys(tmp_path: Path) -> None:
    plugin_dir = tmp_path / "plugins"
    plugin_dir.mkdir(parents=True, exist_ok=True)
    plugin_file = plugin_dir / "echo.py"
    plugin_file.write_text(
        "\n".join(
            [
                "meta = {'name': 'echo', 'version': '0.1.0', 'description': 'test'}",
                "def run(args, ctx):",
                "    return {'ok': True}",
            ]
        ),
        encoding="utf-8",
    )
    pipeline_path = tmp_path / "sample.yml"
    pipeline_path.write_text("steps: []", encoding="utf-8")

    pipeline = Pipeline(steps=[Step(name="echo", script="echo.py")])
    prov = collect_run_provenance(
        repo_root=Path(".").resolve(),
        pipeline_path=pipeline_path,
        global_config_path=None,
        environments_config_path=None,
        plugin_dir=plugin_dir,
        pipeline=pipeline,
        cli_command="python cli.py run pipelines/sample.yml",
    )

    expected_keys = {
        "source_provider",
        "source_revision",
        "source_origin_url",
        "source_repo_name",
        "source_is_dirty",
        "git_commit_sha",
        "git_branch",
        "git_tag",
        "git_origin_url",
        "git_repo_name",
        "git_is_dirty",
        "cli_command",
        "pipeline_checksum",
        "global_config_checksum",
        "execution_config_checksum",
        "plugin_checksums_json",
    }
    assert expected_keys.issubset(set(prov.keys()))
    assert prov["pipeline_checksum"] is not None
    assert isinstance(prov["plugin_checksums_json"], dict)
    assert str(plugin_file.as_posix()) in prov["plugin_checksums_json"]

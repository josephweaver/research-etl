# research-etl
# Copyright (c) 2026 Joseph Weaver
# This file is part of the research-etl project and is licensed under the MIT License.
# You may not use this file except in compliance with the License.
# See https://github.com/josephweaver/research-etl for details.

from __future__ import annotations

from pathlib import Path

from etl.pipeline import parse_pipeline
from etl.projects import (
    infer_project_id_from_pipeline_path,
    normalize_project_id,
    resolve_project_id,
    load_project_vars,
    resolve_projects_config_path,
)


def test_normalize_project_id() -> None:
    assert normalize_project_id("Land Core") == "land_core"
    assert normalize_project_id("crop-insurance") == "crop-insurance"
    assert normalize_project_id("") is None


def test_infer_project_id_from_pipeline_path() -> None:
    assert infer_project_id_from_pipeline_path("pipelines/land_core/prism.yml") == "land_core"
    assert infer_project_id_from_pipeline_path(Path("pipelines/shared/prism.yml")) == "shared"
    assert infer_project_id_from_pipeline_path("pipelines/sample.yml") is None


def test_resolve_project_id_precedence() -> None:
    assert (
        resolve_project_id(
            explicit_project_id="Land Core",
            pipeline_project_id="crop_insurance",
            pipeline_path="pipelines/shared/p.yml",
        )
        == "land_core"
    )
    assert resolve_project_id(pipeline_project_id="crop_insurance", pipeline_path="pipelines/shared/p.yml") == "crop_insurance"
    assert resolve_project_id(pipeline_path="pipelines/shared/p.yml") == "shared"


def test_parse_pipeline_reads_project_metadata(tmp_path: Path) -> None:
    p = tmp_path / "p.yml"
    p.write_text(
        "\n".join(
            [
                "project_id: land_core",
                "shared_with_projects:",
                "  - crop_insurance",
                "steps:",
                "  - name: s1",
                "    script: echo.py",
            ]
        ),
        encoding="utf-8",
    )
    parsed = parse_pipeline(p)
    assert parsed.project_id == "land_core"
    assert parsed.shared_with_projects == ["crop_insurance"]


def test_load_project_vars_from_projects_config(tmp_path: Path) -> None:
    cfg = tmp_path / "projects.yml"
    cfg.write_text(
        "\n".join(
            [
                "projects:",
                "  default:",
                "    vars:",
                "      basedir: /data/default",
                "      owner: core",
                "  land_core:",
                "    vars:",
                "      basedir: /data/land",
            ]
        ),
        encoding="utf-8",
    )
    vars_map = load_project_vars(project_id="land_core", projects_config_path=cfg)
    assert vars_map["basedir"] == "/data/land"
    assert vars_map["owner"] == "core"


def test_parse_pipeline_supports_project_namespace(tmp_path: Path) -> None:
    p = tmp_path / "p.yml"
    p.write_text(
        "\n".join(
            [
                "vars:",
                "  name: demo",
                "dirs:",
                "  workdir: \"{project.basedir}/{pipe.name}\"",
                "steps:",
                "  - name: s1",
                "    script: 'echo.py message=\"{project.owner}\"'",
            ]
        ),
        encoding="utf-8",
    )
    parsed = parse_pipeline(
        p,
        global_vars={},
        env_vars={},
        project_vars={"basedir": "/data/land", "owner": "land-core"},
    )
    assert parsed.dirs["workdir"] == "/data/land/demo"
    assert "land-core" in parsed.steps[0].script


def test_resolve_projects_config_path_default(tmp_path: Path, monkeypatch) -> None:
    cfg_dir = tmp_path / "config"
    cfg_dir.mkdir(parents=True, exist_ok=True)
    cfg = cfg_dir / "projects.yml"
    cfg.write_text("projects: {}\n", encoding="utf-8")
    monkeypatch.chdir(tmp_path)
    resolved = resolve_projects_config_path(None)
    assert resolved == cfg


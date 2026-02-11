from __future__ import annotations

from pathlib import Path

from etl.pipeline import parse_pipeline
from etl.projects import infer_project_id_from_pipeline_path, normalize_project_id, resolve_project_id


def test_normalize_project_id() -> None:
    assert normalize_project_id("Land Core") == "land_core"
    assert normalize_project_id("GEE-LEE") == "gee-lee"
    assert normalize_project_id("") is None


def test_infer_project_id_from_pipeline_path() -> None:
    assert infer_project_id_from_pipeline_path("pipelines/land_core/prism.yml") == "land_core"
    assert infer_project_id_from_pipeline_path(Path("pipelines/shared/prism.yml")) == "shared"
    assert infer_project_id_from_pipeline_path("pipelines/sample.yml") is None


def test_resolve_project_id_precedence() -> None:
    assert (
        resolve_project_id(
            explicit_project_id="Land Core",
            pipeline_project_id="gee_lee",
            pipeline_path="pipelines/shared/p.yml",
        )
        == "land_core"
    )
    assert resolve_project_id(pipeline_project_id="gee_lee", pipeline_path="pipelines/shared/p.yml") == "gee_lee"
    assert resolve_project_id(pipeline_path="pipelines/shared/p.yml") == "shared"


def test_parse_pipeline_reads_project_metadata(tmp_path: Path) -> None:
    p = tmp_path / "p.yml"
    p.write_text(
        "\n".join(
            [
                "project_id: land_core",
                "shared_with_projects:",
                "  - gee_lee",
                "steps:",
                "  - name: s1",
                "    script: echo.py",
            ]
        ),
        encoding="utf-8",
    )
    parsed = parse_pipeline(p)
    assert parsed.project_id == "land_core"
    assert parsed.shared_with_projects == ["gee_lee"]

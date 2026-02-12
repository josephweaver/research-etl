from __future__ import annotations

import json
import zipfile
from pathlib import Path

from etl.plugins.base import PluginContext, load_plugin


def _ctx(tmp_path: Path) -> PluginContext:
    return PluginContext(run_id="r1", workdir=tmp_path, log=lambda *a, **k: None)


def test_evidence_bundle_scans_nested_tile_dirs_and_builds_outputs(tmp_path: Path) -> None:
    plugin = load_plugin(Path("plugins/ai_dataset_evidence_bundle.py"))
    data_root = tmp_path / "yanroy"
    (data_root / "h00v00").mkdir(parents=True, exist_ok=True)
    (data_root / "h00v01").mkdir(parents=True, exist_ok=True)
    (data_root / "h00v00" / "field_id.tif").write_bytes(b"fake-raster")
    (data_root / "h00v00" / "field_id.tfw").write_text("30\n0\n0\n-30\n255000\n4980000\n", encoding="utf-8")
    (data_root / "h00v00" / "field_id.prj").write_text("PROJCS[\"NAD83 / Conus Albers\"]", encoding="utf-8")
    (data_root / "h00v00" / "attrib.csv").write_text("id,name\n1,a\n2,b\n", encoding="utf-8")
    (data_root / "h00v01" / "notes.txt").write_text("tile-level notes", encoding="utf-8")
    (data_root / "README.md").write_text("YanRoy dataset docs", encoding="utf-8")

    pipe_file = tmp_path / "pipelines" / "yanroy.yml"
    pipe_file.parent.mkdir(parents=True, exist_ok=True)
    pipe_file.write_text("steps:\n- script: ai_dataset_research.py dataset_id=\"serve.yanroy_v1\"\n", encoding="utf-8")

    outputs = plugin.run(
        {
            "dataset_id": "serve.yanroy_v1",
            "input_path": str(data_root),
            "pipeline_glob": str(pipe_file),
            "output_dir": str(tmp_path / "out"),
            "supplemental_urls": "https://example.com/yanroy",
            "notes": "30m field identifiers for midwest",
        },
        _ctx(tmp_path),
    )

    assert Path(outputs["manifest_file"]).exists()
    assert Path(outputs["schema_file"]).exists()
    assert Path(outputs["sample_file"]).exists()
    assert Path(outputs["notes_file"]).exists()
    assert Path(outputs["specs_fragment_file"]).exists()

    manifest = json.loads(Path(outputs["manifest_file"]).read_text(encoding="utf-8"))
    assert "h00v00" in manifest["tile_segments"]
    assert manifest["counts"]["raster_file_count"] >= 1
    assert manifest["suffix_counts"][".tif"] >= 1
    assert manifest["schema_candidates"][0]["schema"]["fields"][0]["name"] == "id"
    assert manifest["raster_details"][0]["has_world_file_sidecar"] is True
    assert manifest["raster_details"][0]["has_prj_sidecar"] is True

    notes_text = Path(outputs["notes_file"]).read_text(encoding="utf-8")
    assert "supplemental_urls" in notes_text
    assert "h00v00" in notes_text


def test_evidence_bundle_reads_zip_member_inventory(tmp_path: Path) -> None:
    plugin = load_plugin(Path("plugins/ai_dataset_evidence_bundle.py"))
    archive = tmp_path / "yanroy_tiles.zip"
    with zipfile.ZipFile(archive, mode="w") as zf:
        zf.writestr("h00v00/field_id.tif", "x")
        zf.writestr("h00v00/field_id.tfw", "30\n0\n0\n-30\n255000\n4980000\n")
        zf.writestr("h00v00/meta.txt", "y")
        zf.writestr("h00v00/attrib.csv", "id,value\n1,10\n2,11\n")
        zf.writestr("README.md", "YanRoy zip package")

    outputs = plugin.run(
        {
            "dataset_id": "serve.yanroy_v1",
            "input_path": str(archive),
            "output_dir": str(tmp_path / "out"),
        },
        _ctx(tmp_path),
    )

    manifest = json.loads(Path(outputs["manifest_file"]).read_text(encoding="utf-8"))
    assert manifest["counts"]["archive_count"] == 1
    assert manifest["counts"]["file_records"] == 5
    assert "h00v00" in manifest["tile_segments"]
    assert any(item["path"].endswith("README.md") for item in manifest["readme_excerpts"])
    assert any(item["path"].endswith("attrib.csv") for item in manifest["schema_candidates"])
    assert any(item["source"] == "zip_member" for item in manifest["raster_details"])

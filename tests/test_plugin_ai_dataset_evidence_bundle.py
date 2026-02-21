# research-etl
# Copyright (c) 2026 Joseph Weaver
# This file is part of the research-etl project and is licensed under the MIT License.
# You may not use this file except in compliance with the License.
# See https://github.com/josephweaver/research-etl for details.

from __future__ import annotations

import json
import struct
import types
import zipfile
from pathlib import Path

from etl.plugins.base import PluginContext, load_plugin


def _ctx(tmp_path: Path) -> PluginContext:
    return PluginContext(run_id="r1", workdir=tmp_path, log=lambda *a, **k: None)


def _write_dbf_with_fields(path: Path) -> None:
    # dBASE III-style file with one record and three fields.
    # Fields: GEOID (C,5), NAME (C,20), ALAND (N,14,0)
    fields = [
        ("GEOID", b"C", 5, 0),
        ("NAME", b"C", 20, 0),
        ("ALAND", b"N", 14, 0),
    ]
    nrec = 1
    header_len = 32 + (32 * len(fields)) + 1
    rec_len = 1 + sum(f[2] for f in fields)

    header = bytearray(32)
    header[0] = 0x03
    struct.pack_into("<I", header, 4, nrec)
    struct.pack_into("<H", header, 8, header_len)
    struct.pack_into("<H", header, 10, rec_len)

    body = bytearray()
    for name, ftype, flen, dec in fields:
        desc = bytearray(32)
        name_bytes = name.encode("ascii")[:11]
        desc[0 : len(name_bytes)] = name_bytes
        desc[11] = ftype[0]
        desc[16] = flen
        desc[17] = dec
        body.extend(desc)
    body.append(0x0D)

    record = bytearray(rec_len)
    record[0] = 0x20  # active row
    cursor = 1
    values = ["01001", "Autauga", "1539637834"]
    for (_, _, flen, _), raw_val in zip(fields, values):
        text = str(raw_val)
        if len(text) > flen:
            text = text[:flen]
        if text.isdigit():
            text = text.rjust(flen)
        else:
            text = text.ljust(flen)
        record[cursor : cursor + flen] = text.encode("ascii")
        cursor += flen

    path.write_bytes(bytes(header) + bytes(body) + bytes(record) + b"\x1A")


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


def test_evidence_bundle_reads_7z_member_inventory_with_py7zr(tmp_path: Path, monkeypatch) -> None:
    plugin = load_plugin(Path("plugins/ai_dataset_evidence_bundle.py"))
    assert plugin.module is not None

    archive = tmp_path / "yanroy_tiles.7z"
    archive.write_bytes(b"fake-7z")

    archive_store = {
        archive.resolve().as_posix(): {
            "h00v00/field_id.tif": b"x",
            "h00v00/field_id.tfw": b"30\n0\n0\n-30\n255000\n4980000\n",
            "h00v00/attrib.csv": b"id,value\n1,10\n2,11\n",
            "README.md": b"YanRoy 7z package",
        }
    }

    class _Fake7zFile:
        def __init__(self, path, mode="r"):
            self.path = Path(path).resolve().as_posix()

        def __enter__(self):
            return self

        def __exit__(self, exc_type, exc, tb):
            return False

        def getnames(self):
            return list(archive_store.get(self.path, {}).keys())

        def read(self, targets=None, **_kwargs):
            targets = list(targets or [])
            payload = archive_store.get(self.path, {})
            out = {}
            for t in targets:
                if t in payload:
                    out[t] = payload[t]
            return out

    monkeypatch.setattr(plugin.module, "py7zr", types.SimpleNamespace(SevenZipFile=_Fake7zFile))

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
    assert manifest["counts"]["file_records"] == 4
    assert "h00v00" in manifest["tile_segments"]
    assert any(item["path"].endswith("README.md") for item in manifest["readme_excerpts"])
    assert any(item["path"].endswith("attrib.csv") for item in manifest["schema_candidates"])
    assert any(item["source"] == "archive_member" for item in manifest["raster_details"])


def test_evidence_bundle_extracts_dbf_schema_and_sample_rows(tmp_path: Path) -> None:
    plugin = load_plugin(Path("plugins/ai_dataset_evidence_bundle.py"))
    data_root = tmp_path / "county"
    data_root.mkdir(parents=True, exist_ok=True)
    _write_dbf_with_fields(data_root / "tl_2025_us_county.dbf")

    outputs = plugin.run(
        {
            "dataset_id": "raw.tiger_county_download_v1",
            "input_path": str(data_root),
            "output_dir": str(tmp_path / "out"),
        },
        _ctx(tmp_path),
    )

    manifest = json.loads(Path(outputs["manifest_file"]).read_text(encoding="utf-8"))
    dbf_entries = [
        item for item in (manifest.get("schema_candidates") or [])
        if str(((item or {}).get("schema") or {}).get("format") or "") == "dbf"
    ]
    assert dbf_entries
    fields = dbf_entries[0]["schema"]["fields"]
    assert any(str(f.get("name")) == "GEOID" for f in fields)
    assert any(str(f.get("name")) == "NAME" for f in fields)
    assert any(str(f.get("name")) == "ALAND" for f in fields)
    rows = dbf_entries[0]["schema"]["sample_rows"]
    assert rows and rows[0]["GEOID"] == "01001"

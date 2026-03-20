# research-etl
# Copyright (c) 2026 Joseph Weaver
# This file is part of the research-etl project and is licensed under the MIT License.
# You may not use this file except in compliance with the License.
# See https://github.com/josephweaver/research-etl for details.

from __future__ import annotations

import gzip
from pathlib import Path

from etl.plugins.base import PluginContext, load_plugin


def _ctx(tmp_path: Path) -> PluginContext:
    return PluginContext(run_id="r1", workdir=tmp_path, log=lambda *a, **k: None)


def test_gzip_extract_single_file(tmp_path: Path) -> None:
    plugin = load_plugin(Path("plugins/gzip_extract.py"))
    archive = tmp_path / "sample.txt.gz"
    with gzip.open(archive, mode="wb") as fh:
        fh.write(b"hello")

    outdir = tmp_path / "out"
    outputs = plugin.run({"archive": str(archive), "out": str(outdir)}, _ctx(tmp_path))

    assert outputs["output_dir"] == outdir.resolve().as_posix()
    assert outputs["extracted_count"] == 1
    assert (outdir / "sample.txt").read_text(encoding="utf-8") == "hello"


def test_gzip_extract_archive_glob_accepts_exact_file_path(tmp_path: Path) -> None:
    plugin = load_plugin(Path("plugins/gzip_extract.py"))
    archive = tmp_path / "qs.crops.txt.gz"
    with gzip.open(archive, mode="wb") as fh:
        fh.write(b"crop data")

    outdir = tmp_path / "out2"
    outputs = plugin.run({"archive_glob": str(archive), "out": str(outdir)}, _ctx(tmp_path))

    assert outputs["extracted_count"] == 1
    assert (outdir / "qs.crops.txt").read_text(encoding="utf-8") == "crop data"


def test_gzip_extract_keeps_dot_out_paths_repo_relative(tmp_path: Path, monkeypatch) -> None:
    plugin = load_plugin(Path("plugins/gzip_extract.py"))
    monkeypatch.chdir(tmp_path)

    archive = tmp_path / ".out" / "data" / "qs" / "raw" / "qs.census2022.txt.gz"
    archive.parent.mkdir(parents=True, exist_ok=True)
    with gzip.open(archive, mode="wb") as fh:
        fh.write(b"census")

    ctx = PluginContext(run_id="r1", workdir=tmp_path / "step_1", log=lambda *a, **k: None)
    outputs = plugin.run(
        {"archive": ".out/data/qs/raw/qs.census2022.txt.gz", "out": ".out/data/qs/extract"},
        ctx,
    )

    assert outputs["output_dir"].endswith("/.out/data/qs/extract")
    assert (tmp_path / ".out" / "data" / "qs" / "extract" / "qs.census2022.txt").exists()
    assert not (ctx.workdir / ".out" / "data" / "qs" / "extract").exists()

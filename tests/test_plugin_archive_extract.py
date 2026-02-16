from __future__ import annotations

import types
import zipfile
from pathlib import Path

from etl.plugins.base import PluginContext, load_plugin


def _ctx(tmp_path: Path) -> PluginContext:
    return PluginContext(run_id="r1", workdir=tmp_path, log=lambda *a, **k: None)


def test_archive_extract_zip_returns_extracted_files(tmp_path: Path) -> None:
    plugin = load_plugin(Path("plugins/archive_extract.py"))
    archive = tmp_path / "sample.zip"
    with zipfile.ZipFile(archive, mode="w") as zf:
        zf.writestr("a.txt", "a")
        zf.writestr("nested/b.txt", "b")

    outdir = tmp_path / "out"
    outputs = plugin.run({"archive": str(archive), "out": str(outdir)}, _ctx(tmp_path))

    assert outputs["output_dir"] == outdir.resolve().as_posix()
    assert outputs["extracted_count"] == 2
    assert (outdir / "a.txt").exists()
    assert (outdir / "nested" / "b.txt").exists()
    assert any(p.endswith("/a.txt") for p in outputs["extracted_files"])
    assert any(p.endswith("/nested/b.txt") for p in outputs["extracted_files"])


def test_archive_extract_zip_include_glob_filters_members(tmp_path: Path) -> None:
    plugin = load_plugin(Path("plugins/archive_extract.py"))
    archive = tmp_path / "sample.zip"
    with zipfile.ZipFile(archive, mode="w") as zf:
        zf.writestr("a.tif", "a")
        zf.writestr("nested/b.xml", "b")

    outdir = tmp_path / "outf"
    outputs = plugin.run(
        {"archive": str(archive), "out": str(outdir), "include_glob": "*.tif"},
        _ctx(tmp_path),
    )
    assert outputs["extracted_count"] == 1
    assert (outdir / "a.tif").exists()
    assert not (outdir / "nested" / "b.xml").exists()


def test_archive_extract_7z_falls_back_to_binary_when_py7zr_missing(tmp_path: Path, monkeypatch) -> None:
    plugin = load_plugin(Path("plugins/archive_extract.py"))
    assert plugin.module is not None

    archive = tmp_path / "sample.7z"
    archive.write_bytes(b"fake")
    outdir = tmp_path / "out7z"
    seen = {}

    def _fake_run(cmd, capture_output, text, check):
        seen["cmd"] = cmd
        (outdir / "x.txt").parent.mkdir(parents=True, exist_ok=True)
        (outdir / "x.txt").write_text("ok", encoding="utf-8")
        return types.SimpleNamespace(returncode=0, stdout="", stderr="")

    monkeypatch.setattr(plugin.module, "py7zr", None)
    monkeypatch.setattr("subprocess.run", _fake_run)

    outputs = plugin.run(
        {
            "archive": str(archive),
            "out": str(outdir),
            "seven_zip_bin": "bin/7z",
            "include_glob": "*.txt",
        },
        _ctx(tmp_path),
    )

    assert seen["cmd"][0] == "bin/7z"
    assert seen["cmd"][1] == "x"
    assert any(str(c).startswith("-ir!") for c in seen["cmd"])
    assert outputs["extracted_count"] == 1
    assert outputs["extracted_files"][0].endswith("/x.txt")


def test_archive_extract_7z_retries_without_include_filters_on_fail(tmp_path: Path, monkeypatch) -> None:
    plugin = load_plugin(Path("plugins/archive_extract.py"))
    assert plugin.module is not None

    archive = tmp_path / "sample.7z"
    archive.write_bytes(b"fake")
    outdir = tmp_path / "out7z_retry"
    calls = []

    def _fake_run(cmd, capture_output, text, check):
        calls.append(list(cmd))
        if len(calls) == 1:
            return types.SimpleNamespace(returncode=2, stdout="Archives with Errors: 1", stderr="ERROR: E_FAIL")
        (outdir / "x.txt").parent.mkdir(parents=True, exist_ok=True)
        (outdir / "x.txt").write_text("ok", encoding="utf-8")
        (outdir / "drop.bin").write_text("nope", encoding="utf-8")
        return types.SimpleNamespace(returncode=0, stdout="", stderr="")

    monkeypatch.setattr(plugin.module, "py7zr", None)
    monkeypatch.setattr("subprocess.run", _fake_run)

    outputs = plugin.run(
        {
            "archive": str(archive),
            "out": str(outdir),
            "seven_zip_bin": "bin/7z",
            "include_glob": "*.txt",
        },
        _ctx(tmp_path),
    )

    assert len(calls) == 2
    assert any(str(c).startswith("-ir!") for c in calls[0])
    assert not any(str(c).startswith("-ir!") for c in calls[1])
    assert outputs["extracted_count"] == 1
    assert outputs["extracted_files"][0].endswith("/x.txt")
    assert (outdir / "x.txt").exists()
    assert not (outdir / "drop.bin").exists()


def test_archive_extract_requires_archive_or_glob(tmp_path: Path) -> None:
    plugin = load_plugin(Path("plugins/archive_extract.py"))
    try:
        plugin.run({"out": str(tmp_path / "out")}, _ctx(tmp_path))
        assert False, "expected plugin to fail"
    except ValueError as exc:
        assert "archive or archive_glob is required" in str(exc)


def test_archive_extract_archive_glob_accepts_exact_file_path(tmp_path: Path) -> None:
    plugin = load_plugin(Path("plugins/archive_extract.py"))
    archive = tmp_path / "sample.zip"
    with zipfile.ZipFile(archive, mode="w") as zf:
        zf.writestr("a.txt", "a")

    outdir = tmp_path / "out"
    outputs = plugin.run({"archive_glob": str(archive), "out": str(outdir)}, _ctx(tmp_path))
    assert outputs["extracted_count"] == 1
    assert (outdir / "a.txt").exists()


def test_archive_extract_archive_glob_resolves_relative_to_ctx_workdir(tmp_path: Path) -> None:
    plugin = load_plugin(Path("plugins/archive_extract.py"))
    data_dir = tmp_path / "data"
    data_dir.mkdir(parents=True, exist_ok=True)
    archive = data_dir / "sample.zip"
    with zipfile.ZipFile(archive, mode="w") as zf:
        zf.writestr("b.txt", "b")

    outdir = tmp_path / "out2"
    outputs = plugin.run({"archive_glob": "data/*.zip", "out": str(outdir)}, _ctx(tmp_path))
    assert outputs["extracted_count"] == 1
    assert (outdir / "b.txt").exists()


def test_archive_extract_keeps_dot_out_paths_repo_relative(tmp_path: Path, monkeypatch) -> None:
    plugin = load_plugin(Path("plugins/archive_extract.py"))
    monkeypatch.chdir(tmp_path)

    archive = tmp_path / ".out" / "data" / "yanroy" / "raw" / "sample.zip"
    archive.parent.mkdir(parents=True, exist_ok=True)
    with zipfile.ZipFile(archive, mode="w") as zf:
        zf.writestr("c.txt", "c")

    ctx = PluginContext(run_id="r1", workdir=tmp_path / "step_1", log=lambda *a, **k: None)
    outputs = plugin.run(
        {"archive": ".out/data/yanroy/raw/sample.zip", "out": ".out/data/yanroy/unzip"},
        ctx,
    )
    assert outputs["output_dir"].endswith("/.out/data/yanroy/unzip")
    assert (tmp_path / ".out" / "data" / "yanroy" / "unzip" / "c.txt").exists()
    assert not (ctx.workdir / ".out" / "data" / "yanroy" / "unzip").exists()

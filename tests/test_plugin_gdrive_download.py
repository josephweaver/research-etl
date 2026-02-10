from __future__ import annotations

import types
from pathlib import Path

from etl.plugins.base import PluginContext, load_plugin


def _ctx(tmp_path: Path) -> PluginContext:
    return PluginContext(run_id="r1", workdir=tmp_path, log=lambda *a, **k: None)


def test_gdrive_download_plugin_runs_and_returns_file_list(tmp_path: Path, monkeypatch) -> None:
    plugin = load_plugin(Path("plugins/gdrive_download.py"))
    outdir = tmp_path / "cache"

    def _fake_run(cmd, capture_output, text, check):
        (outdir / "a.txt").parent.mkdir(parents=True, exist_ok=True)
        (outdir / "a.txt").write_text("ok", encoding="utf-8")
        return types.SimpleNamespace(returncode=0, stdout="done", stderr="")

    monkeypatch.setattr("subprocess.run", _fake_run)

    outputs = plugin.run(
        {
            "rscript_path": "tools/gdrv/download.R",
            "src": "Data",
            "out": str(outdir),
            "glob": "*.txt",
            "recursive": True,
        },
        _ctx(tmp_path),
    )
    assert outputs["output_dir"] == outdir.as_posix()
    assert outputs["downloaded_count"] == 1
    assert outputs["downloaded_files"][0].endswith("/a.txt")


def test_gdrive_download_plugin_raises_on_failure(tmp_path: Path, monkeypatch) -> None:
    plugin = load_plugin(Path("plugins/gdrive_download.py"))

    def _fake_run(cmd, capture_output, text, check):
        return types.SimpleNamespace(returncode=1, stdout="", stderr="boom")

    monkeypatch.setattr("subprocess.run", _fake_run)

    try:
        plugin.run({"out": str(tmp_path / "cache")}, _ctx(tmp_path))
        assert False, "expected plugin to fail"
    except RuntimeError as exc:
        assert "gdrive download failed" in str(exc)

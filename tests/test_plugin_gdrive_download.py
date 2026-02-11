from __future__ import annotations

import types
from pathlib import Path

from etl.plugins.base import PluginContext, load_plugin


def _ctx(tmp_path: Path) -> PluginContext:
    return PluginContext(run_id="r1", workdir=tmp_path, log=lambda *a, **k: None)


def test_gdrive_download_plugin_runs_and_returns_file_list(tmp_path: Path, monkeypatch) -> None:
    plugin = load_plugin(Path("plugins/gdrive_download.py"))
    outdir = tmp_path / "cache"
    seen = {}

    def _fake_run(cmd, capture_output, text, check):
        seen["cmd"] = cmd
        (outdir / "a.txt").parent.mkdir(parents=True, exist_ok=True)
        (outdir / "a.txt").write_text("ok", encoding="utf-8")
        return types.SimpleNamespace(returncode=0, stdout="done", stderr="")

    monkeypatch.setattr("subprocess.run", _fake_run)

    outputs = plugin.run(
        {
            "rclone_bin": "bin/rclone",
            "remote": "gdrive",
            "src": "Data",
            "out": str(outdir),
            "glob": "*.txt",
            "recursive": True,
            "shared_drive_id": "team123",
        },
        _ctx(tmp_path),
    )
    assert seen["cmd"][0] == "bin/rclone"
    assert "copy" in seen["cmd"]
    assert "--include" in seen["cmd"]
    assert "--drive-team-drive" in seen["cmd"]
    assert outputs["output_dir"] == outdir.as_posix()
    assert outputs["downloaded_count"] == 1
    assert outputs["downloaded_files"][0].endswith("/a.txt")
    assert outputs["remote_spec"].startswith("gdrive:")


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


def test_gdrive_download_plugin_builds_remote_from_shared_drive_name(tmp_path: Path, monkeypatch) -> None:
    plugin = load_plugin(Path("plugins/gdrive_download.py"))
    outdir = tmp_path / "cache"
    seen = {}

    def _fake_run(cmd, capture_output, text, check):
        seen["cmd"] = cmd
        (outdir / "x.txt").parent.mkdir(parents=True, exist_ok=True)
        (outdir / "x.txt").write_text("ok", encoding="utf-8")
        return types.SimpleNamespace(returncode=0, stdout="done", stderr="")

    monkeypatch.setattr("subprocess.run", _fake_run)
    outputs = plugin.run(
        {
            "remote": "gdrive",
            "src": "Data/yanroy/raw",
            "shared_drive": "Land Core Risk Model Development",
            "out": str(outdir),
        },
        _ctx(tmp_path),
    )
    assert outputs["remote_spec"] == "gdrive:Land Core Risk Model Development/Data/yanroy/raw"
    assert seen["cmd"][2] == outputs["remote_spec"]


def test_gdrive_download_plugin_uses_env_var_when_arg_missing(tmp_path: Path, monkeypatch) -> None:
    plugin = load_plugin(Path("plugins/gdrive_download.py"))
    outdir = tmp_path / "cache"
    seen = {}

    def _fake_run(cmd, capture_output, text, check):
        seen["cmd"] = cmd
        (outdir / "z.txt").parent.mkdir(parents=True, exist_ok=True)
        (outdir / "z.txt").write_text("ok", encoding="utf-8")
        return types.SimpleNamespace(returncode=0, stdout="done", stderr="")

    monkeypatch.setattr("subprocess.run", _fake_run)
    monkeypatch.setenv("ETL_RCLONE_BIN", "bin/rclone-from-env")
    outputs = plugin.run(
        {
            "src": "Data/yanroy/raw",
            "out": str(outdir),
        },
        _ctx(tmp_path),
    )
    assert seen["cmd"][0] == "bin/rclone-from-env"
    assert outputs["downloaded_count"] == 1

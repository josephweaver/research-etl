from __future__ import annotations

import types
from pathlib import Path

from etl.plugins.base import PluginContext, load_plugin


def _ctx(tmp_path: Path) -> PluginContext:
    return PluginContext(run_id="r1", workdir=tmp_path, log=lambda *a, **k: None)


def test_gdrive_upload_uploads_single_file(tmp_path: Path, monkeypatch) -> None:
    plugin = load_plugin(Path("plugins/gdrive_upload.py"))
    src = tmp_path / "tiles.of.interest.csv"
    src.write_text("tile_id\nh10v04\n", encoding="utf-8")
    seen = {}

    def _fake_run(cmd, capture_output, text, check):
        seen["cmd"] = cmd
        return types.SimpleNamespace(returncode=0, stdout="ok", stderr="")

    monkeypatch.setattr("subprocess.run", _fake_run)
    out = plugin.run(
        {
            "rclone_bin": "bin/rclone",
            "input_path": str(src),
            "dst": "Data/ETL/Meta",
            "shared_drive_id": "team123",
        },
        _ctx(tmp_path),
    )
    assert seen["cmd"][0] == "bin/rclone"
    assert seen["cmd"][1] == "copyto"
    assert "--drive-team-drive" in seen["cmd"]
    assert out["uploaded_count"] == 1
    assert out["output_path"].endswith("/tiles.of.interest.csv")


def test_gdrive_upload_raises_on_failure(tmp_path: Path, monkeypatch) -> None:
    plugin = load_plugin(Path("plugins/gdrive_upload.py"))
    src = tmp_path / "x.txt"
    src.write_text("x", encoding="utf-8")

    def _fake_run(cmd, capture_output, text, check):
        return types.SimpleNamespace(returncode=1, stdout="", stderr="boom")

    monkeypatch.setattr("subprocess.run", _fake_run)
    try:
        plugin.run({"input_path": str(src)}, _ctx(tmp_path))
        assert False, "expected failure"
    except RuntimeError as exc:
        assert "gdrive upload failed" in str(exc)

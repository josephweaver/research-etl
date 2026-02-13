from __future__ import annotations

from pathlib import Path

from etl.plugins.base import PluginContext, load_plugin


def _ctx(tmp_path: Path) -> PluginContext:
    return PluginContext(run_id="r1", workdir=tmp_path / "work", log=lambda *a, **k: None)


def test_file_move_regex_moves_matching_files_and_preserves_tree(tmp_path: Path) -> None:
    plugin = load_plugin(Path("plugins/file_move_regex.py"))
    src = tmp_path / "src"
    dst = tmp_path / "dst"
    (src / "h00v00").mkdir(parents=True, exist_ok=True)
    (src / "h00v01").mkdir(parents=True, exist_ok=True)
    (src / "h00v00" / "a.tif").write_text("a", encoding="utf-8")
    (src / "h00v00" / "a.xml").write_text("a", encoding="utf-8")
    (src / "h00v01" / "b.tif").write_text("b", encoding="utf-8")

    out = plugin.run(
        {
            "src": str(src),
            "dst": str(dst),
            "pattern": r"\.tif$",
            "match_on": "relative_path",
            "flags": "",
        },
        _ctx(tmp_path),
    )
    assert out["matched_count"] == 2
    assert out["moved_count"] == 2
    assert (dst / "h00v00" / "a.tif").exists()
    assert (dst / "h00v01" / "b.tif").exists()
    assert not (src / "h00v00" / "a.tif").exists()
    assert (src / "h00v00" / "a.xml").exists()


def test_file_move_regex_skip_existing_when_overwrite_false(tmp_path: Path) -> None:
    plugin = load_plugin(Path("plugins/file_move_regex.py"))
    src = tmp_path / "src2"
    dst = tmp_path / "dst2"
    (src / "nested").mkdir(parents=True, exist_ok=True)
    (dst / "nested").mkdir(parents=True, exist_ok=True)
    (src / "nested" / "x.txt").write_text("new", encoding="utf-8")
    (dst / "nested" / "x.txt").write_text("old", encoding="utf-8")

    out = plugin.run(
        {
            "src": str(src),
            "dst": str(dst),
            "pattern": r"x\.txt$",
            "overwrite": False,
            "flags": "",
        },
        _ctx(tmp_path),
    )
    assert out["matched_count"] == 1
    assert out["moved_count"] == 0
    assert out["skipped_existing_count"] == 1
    assert (src / "nested" / "x.txt").exists()
    assert (dst / "nested" / "x.txt").read_text(encoding="utf-8") == "old"


def test_file_move_regex_match_on_filename_and_dry_run(tmp_path: Path) -> None:
    plugin = load_plugin(Path("plugins/file_move_regex.py"))
    src = tmp_path / "src3"
    dst = tmp_path / "dst3"
    (src / "tile").mkdir(parents=True, exist_ok=True)
    (src / "tile" / "ReleaseData.7z").write_text("z", encoding="utf-8")

    out = plugin.run(
        {
            "src": str(src),
            "dst": str(dst),
            "pattern": r"releasedata\.7z$",
            "match_on": "filename",
            "flags": "i",
            "dry_run": True,
        },
        _ctx(tmp_path),
    )
    assert out["matched_count"] == 1
    assert out["moved_count"] == 1
    assert (src / "tile" / "ReleaseData.7z").exists()
    assert not (dst / "tile" / "ReleaseData.7z").exists()


from __future__ import annotations

from pathlib import Path

from etl.plugins.base import PluginContext, load_plugin


def _ctx(tmp_path: Path) -> PluginContext:
    return PluginContext(run_id="r1", workdir=tmp_path / "work", log=lambda *a, **k: None)


def test_file_delete_regex_deletes_matching_and_prunes_empty_dirs(tmp_path: Path) -> None:
    plugin = load_plugin(Path("plugins/file_delete_regex.py"))
    src = tmp_path / "src"
    (src / "a" / "b").mkdir(parents=True, exist_ok=True)
    (src / "a" / "b" / "x.tif").write_text("x", encoding="utf-8")
    (src / "a" / "b" / "y.xml").write_text("y", encoding="utf-8")

    out = plugin.run(
        {
            "src": str(src),
            "pattern": r"\.tif$",
            "match_on": "relative_path",
            "flags": "",
            "remove_empty_dirs": True,
        },
        _ctx(tmp_path),
    )
    assert out["matched_count"] == 1
    assert out["deleted_count"] == 1
    assert not (src / "a" / "b" / "x.tif").exists()
    assert (src / "a" / "b" / "y.xml").exists()


def test_file_delete_regex_match_on_filename_case_insensitive(tmp_path: Path) -> None:
    plugin = load_plugin(Path("plugins/file_delete_regex.py"))
    src = tmp_path / "src2"
    src.mkdir(parents=True, exist_ok=True)
    (src / "ReleaseData.7z").write_text("z", encoding="utf-8")

    out = plugin.run(
        {
            "src": str(src),
            "pattern": r"releasedata\.7z$",
            "match_on": "filename",
            "flags": "i",
        },
        _ctx(tmp_path),
    )
    assert out["deleted_count"] == 1
    assert not (src / "ReleaseData.7z").exists()


def test_file_delete_regex_dry_run_keeps_files(tmp_path: Path) -> None:
    plugin = load_plugin(Path("plugins/file_delete_regex.py"))
    src = tmp_path / "src3"
    src.mkdir(parents=True, exist_ok=True)
    (src / "to_delete.txt").write_text("x", encoding="utf-8")

    out = plugin.run(
        {
            "src": str(src),
            "pattern": r"\.txt$",
            "dry_run": True,
            "flags": "",
        },
        _ctx(tmp_path),
    )
    assert out["matched_count"] == 1
    assert out["deleted_count"] == 1
    assert (src / "to_delete.txt").exists()


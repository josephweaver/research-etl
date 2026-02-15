from __future__ import annotations

from pathlib import Path

from etl.plugins.base import PluginContext, load_plugin


def _ctx(tmp_path: Path) -> PluginContext:
    return PluginContext(run_id="r1", workdir=tmp_path, log=lambda *a, **k: None)


class _FakeResponse:
    def __init__(self, payload: bytes):
        self._payload = payload
        self._offset = 0

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        return False

    def read(self, size: int = -1):
        if self._offset >= len(self._payload):
            return b""
        if size is None or int(size) < 0:
            chunk = self._payload[self._offset :]
            self._offset = len(self._payload)
            return chunk
        end = min(len(self._payload), self._offset + int(size))
        chunk = self._payload[self._offset : end]
        self._offset = end
        return chunk


def test_web_download_list_downloads_from_urls_file(tmp_path: Path, monkeypatch) -> None:
    plugin = load_plugin(Path("plugins/web_download_list.py"))
    assert plugin.module is not None
    urls_file = tmp_path / "urls.txt"
    urls_file.write_text(
        "\n".join(
            [
                "# tiger",
                "https://example.com/state.zip",
                "https://example.com/county.zip",
            ]
        ),
        encoding="utf-8",
    )

    def _fake_urlopen(req, timeout=0):  # noqa: ANN001
        url = getattr(req, "full_url", str(req))
        if str(url).endswith("state.zip"):
            return _FakeResponse(b"STATE")
        return _FakeResponse(b"COUNTY")

    monkeypatch.setattr(plugin.module.request, "urlopen", _fake_urlopen)

    out_dir = tmp_path / "out"
    outputs = plugin.run({"urls_file": str(urls_file), "out": str(out_dir)}, _ctx(tmp_path))
    assert outputs["downloaded_count"] == 2
    assert outputs["failed_count"] == 0
    assert (out_dir / "state.zip").exists()
    assert (out_dir / "county.zip").exists()
    assert (out_dir / "download_manifest.json").exists()


def test_web_download_list_skips_existing_when_no_overwrite(tmp_path: Path, monkeypatch) -> None:
    plugin = load_plugin(Path("plugins/web_download_list.py"))
    assert plugin.module is not None
    out_dir = tmp_path / "out2"
    out_dir.mkdir(parents=True, exist_ok=True)
    existing = out_dir / "state.zip"
    existing.write_bytes(b"OLD")

    def _fake_urlopen(req, timeout=0):  # noqa: ANN001
        raise AssertionError("should not download existing file")

    monkeypatch.setattr(plugin.module.request, "urlopen", _fake_urlopen)
    outputs = plugin.run(
        {
            "urls": "https://example.com/state.zip",
            "out": str(out_dir),
            "overwrite": False,
        },
        _ctx(tmp_path),
    )
    assert outputs["downloaded_count"] == 0
    assert outputs["skipped_count"] == 1
    assert existing.read_bytes() == b"OLD"


def test_web_download_list_resolves_repo_relative_urls_file_with_env_root(tmp_path: Path, monkeypatch) -> None:
    plugin = load_plugin(Path("plugins/web_download_list.py"))
    assert plugin.module is not None

    repo_root = tmp_path / "repo"
    urls_file = repo_root / "pipelines" / "tiger" / "state_urls.txt"
    urls_file.parent.mkdir(parents=True, exist_ok=True)
    urls_file.write_text("https://example.com/state.zip\n", encoding="utf-8")

    workdir = tmp_path / "work"
    workdir.mkdir(parents=True, exist_ok=True)
    monkeypatch.chdir(workdir)
    monkeypatch.setenv("ETL_REPO_ROOT", str(repo_root))

    def _fake_urlopen(req, timeout=0):  # noqa: ANN001
        return _FakeResponse(b"STATE")

    monkeypatch.setattr(plugin.module.request, "urlopen", _fake_urlopen)

    out_dir = tmp_path / "out3"
    outputs = plugin.run({"urls_file": "pipelines/tiger/state_urls.txt", "out": str(out_dir)}, _ctx(workdir))
    assert outputs["downloaded_count"] == 1
    assert outputs["failed_count"] == 0
    assert (out_dir / "state.zip").exists()

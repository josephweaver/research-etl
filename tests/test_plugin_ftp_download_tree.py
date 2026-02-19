from __future__ import annotations

from pathlib import Path

from etl.plugins.base import PluginContext, load_plugin


def _ctx(tmp_path: Path) -> PluginContext:
    return PluginContext(run_id="r1", workdir=tmp_path, log=lambda *a, **k: None)


def _norm(path_text: str) -> str:
    text = str(path_text or "").replace("\\", "/")
    parts = [p for p in text.split("/") if p]
    return "/" + "/".join(parts) if parts else "/"


class _FakeFTP:
    def __init__(self, tree: dict[str, object], payloads: dict[str, bytes]):
        self._tree = {_norm(k): v for k, v in tree.items()}
        self._payloads = {_norm(k): v for k, v in payloads.items()}
        self._cwd = "/"
        self.connected = None
        self.logged_in = None
        self.passive = None
        self.quit_called = False

    def connect(self, host: str, port: int, timeout: int) -> None:
        self.connected = (host, port, timeout)

    def login(self, user: str, passwd: str) -> None:
        self.logged_in = (user, passwd)

    def set_pasv(self, passive: bool) -> None:
        self.passive = passive

    def pwd(self) -> str:
        return self._cwd

    def cwd(self, path: str) -> None:
        p = _norm(path)
        node = self._tree.get(p)
        if not isinstance(node, dict):
            raise RuntimeError(f"not a directory: {p}")
        self._cwd = p

    def mlsd(self, path: str):
        p = _norm(path)
        node = self._tree.get(p)
        if not isinstance(node, dict):
            raise RuntimeError(f"not a directory: {p}")
        for name, kind in sorted(node.items()):
            yield name, {"type": kind}

    def retrbinary(self, cmd: str, callback, blocksize: int = 8192):  # noqa: ANN001
        remote = _norm(str(cmd).replace("RETR ", "", 1).strip())
        payload = self._payloads.get(remote)
        if payload is None:
            raise RuntimeError(f"missing file payload: {remote}")
        callback(payload)

    def quit(self) -> None:
        self.quit_called = True


def test_ftp_download_tree_recursive_with_filename_glob(tmp_path: Path, monkeypatch) -> None:
    plugin = load_plugin(Path("plugins/ftp_download_tree.py"))
    assert plugin.module is not None

    tree = {
        "/": {"root": "dir"},
        "/root": {"top.txt": "file", "top.csv": "file", "sub": "dir"},
        "/root/sub": {"nested.txt": "file", "nested.bin": "file"},
    }
    payloads = {
        "/root/top.txt": b"TOP",
        "/root/top.csv": b"CSV",
        "/root/sub/nested.txt": b"NESTED",
        "/root/sub/nested.bin": b"BIN",
    }
    fake = _FakeFTP(tree=tree, payloads=payloads)
    monkeypatch.setattr(plugin.module.ftplib, "FTP", lambda: fake)

    out = tmp_path / "out"
    outputs = plugin.run(
        {
            "url": "ftp://prism.oregonstate.edu/root",
            "out": str(out),
            "recursive": True,
            "filename_glob": "*.txt",
        },
        _ctx(tmp_path),
    )

    assert outputs["downloaded_count"] == 2
    assert outputs["failed_count"] == 0
    assert (out / "top.txt").read_bytes() == b"TOP"
    assert (out / "sub" / "nested.txt").read_bytes() == b"NESTED"
    assert not (out / "top.csv").exists()
    assert fake.connected == ("prism.oregonstate.edu", 21, 120)
    assert fake.logged_in == ("anonymous", "anonymous@")
    assert fake.quit_called is True


def test_ftp_download_tree_skips_existing_when_no_overwrite(tmp_path: Path, monkeypatch) -> None:
    plugin = load_plugin(Path("plugins/ftp_download_tree.py"))
    assert plugin.module is not None

    tree = {
        "/": {"root": "dir"},
        "/root": {"top.txt": "file"},
    }
    payloads = {
        "/root/top.txt": b"NEW",
    }
    fake = _FakeFTP(tree=tree, payloads=payloads)
    monkeypatch.setattr(plugin.module.ftplib, "FTP", lambda: fake)

    out = tmp_path / "out"
    out.mkdir(parents=True, exist_ok=True)
    (out / "top.txt").write_bytes(b"OLD")

    outputs = plugin.run(
        {
            "url": "ftp://example.org/root",
            "out": str(out),
            "overwrite": False,
        },
        _ctx(tmp_path),
    )

    assert outputs["downloaded_count"] == 0
    assert outputs["skipped_count"] == 1
    assert (out / "top.txt").read_bytes() == b"OLD"

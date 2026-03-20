# research-etl
# Copyright (c) 2026 Joseph Weaver
# This file is part of the research-etl project and is licensed under the MIT License.
# You may not use this file except in compliance with the License.
# See https://github.com/josephweaver/research-etl for details.

from __future__ import annotations

import json
from pathlib import Path

from etl.plugins.base import PluginContext, load_plugin


def _ctx(tmp_path: Path) -> PluginContext:
    return PluginContext(run_id="r1", workdir=tmp_path, log=lambda *a, **k: None)


class _FakeResponse:
    def __init__(self, payload: bytes, headers: dict | None = None):
        self._payload = payload
        self._offset = 0
        self.headers = dict(headers or {})

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


def test_stac_asset_download_searches_signs_and_downloads(tmp_path: Path, monkeypatch) -> None:
    plugin = load_plugin(Path("plugins/stac_asset_download.py"))
    assert plugin.module is not None

    feature = {
        "id": "S2_TEST_ITEM",
        "assets": {
            "B04": {"href": "https://example.blob.core.windows.net/container/B04.tif"},
            "SCL": {"href": "https://example.blob.core.windows.net/container/SCL.tif"},
        },
    }
    search_body = {"type": "FeatureCollection", "features": [feature], "links": []}
    seen_urls: list[str] = []

    def _fake_urlopen(req, timeout=0):  # noqa: ANN001
        url = getattr(req, "full_url", str(req))
        seen_urls.append(str(url))
        if str(url).endswith("/token/sentinel-2-l2a"):
            return _FakeResponse(json.dumps({"token": "sv=1&sig=abc"}).encode("utf-8"))
        if str(url).endswith("/search"):
            return _FakeResponse(json.dumps(search_body).encode("utf-8"))
        if "B04.tif" in str(url):
            assert "sig=abc" in str(url)
            return _FakeResponse(b"B04DATA")
        if "SCL.tif" in str(url):
            assert "sig=abc" in str(url)
            return _FakeResponse(b"SCLDATA")
        raise AssertionError(f"unexpected url: {url}")

    monkeypatch.setattr(plugin.module.request, "urlopen", _fake_urlopen)

    out_dir = tmp_path / "out"
    outputs = plugin.run(
        {
            "api_url": "https://planetarycomputer.microsoft.com/api/stac/v1/search",
            "collection": "sentinel-2-l2a",
            "datetime": "2025-07-01/2025-07-03",
            "bbox": "-90.42,41.69,-82.12,48.31",
            "asset_keys": "B04,SCL",
            "sign_api_url": "https://planetarycomputer.microsoft.com/api/sas/v1/token/sentinel-2-l2a",
            "out": str(out_dir),
        },
        _ctx(tmp_path),
    )

    item_dir = out_dir / "S2_TEST_ITEM"
    assert outputs["item_count"] == 1
    assert outputs["asset_count"] == 2
    assert (item_dir / "item.json").exists()
    assert (item_dir / "B04.tif").read_bytes() == b"B04DATA"
    assert (item_dir / "SCL.tif").read_bytes() == b"SCLDATA"
    assert (out_dir / "download_manifest.json").exists()
    assert any(u.endswith("/search") for u in seen_urls)


def test_stac_asset_download_supports_next_link_paging(tmp_path: Path, monkeypatch) -> None:
    plugin = load_plugin(Path("plugins/stac_asset_download.py"))
    assert plugin.module is not None

    search_page_1 = {
        "type": "FeatureCollection",
        "features": [{"id": "ITEM1", "assets": {"B04": {"href": "https://example.com/1.tif"}}}],
        "links": [{"rel": "next", "href": "https://planetarycomputer.microsoft.com/api/stac/v1/search", "method": "POST", "body": {"token": "next1"}}],
    }
    search_page_2 = {
        "type": "FeatureCollection",
        "features": [{"id": "ITEM2", "assets": {"B04": {"href": "https://example.com/2.tif"}}}],
        "links": [],
    }
    calls = {"search": 0}

    def _fake_urlopen(req, timeout=0):  # noqa: ANN001
        url = getattr(req, "full_url", str(req))
        if str(url).endswith("/search"):
            calls["search"] += 1
            if calls["search"] == 1:
                return _FakeResponse(json.dumps(search_page_1).encode("utf-8"))
            return _FakeResponse(json.dumps(search_page_2).encode("utf-8"))
        if str(url).endswith("/1.tif"):
            return _FakeResponse(b"1")
        if str(url).endswith("/2.tif"):
            return _FakeResponse(b"2")
        raise AssertionError(f"unexpected url: {url}")

    monkeypatch.setattr(plugin.module.request, "urlopen", _fake_urlopen)
    outputs = plugin.run(
        {
            "api_url": "https://planetarycomputer.microsoft.com/api/stac/v1/search",
            "collection": "sentinel-2-l2a",
            "datetime": "2025-07-01/2025-07-03",
            "bbox": [-90.42, 41.69, -82.12, 48.31],
            "asset_keys": ["B04"],
            "out": str(tmp_path / "out"),
        },
        _ctx(tmp_path),
    )

    assert outputs["item_count"] == 2
    assert calls["search"] == 2

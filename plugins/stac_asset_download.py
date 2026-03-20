# research-etl
# Copyright (c) 2026 Joseph Weaver
# This file is part of the research-etl project and is licensed under the MIT License.
# You may not use this file except in compliance with the License.
# See https://github.com/josephweaver/research-etl for details.

from __future__ import annotations

import ast
import json
import os
from pathlib import Path
from typing import Any, Dict, Iterable, List, Tuple
from urllib import parse, request


meta = {
    "name": "stac_asset_download",
    "version": "0.1.0",
    "description": "Search a STAC API, optionally sign asset URLs, and download item assets.",
    "inputs": [],
    "outputs": [
        "output_dir",
        "manifest_file",
        "downloaded_files",
        "downloaded_count",
        "item_count",
        "asset_count",
    ],
    "params": {
        "api_url": {"type": "str", "default": ""},
        "collection": {"type": "str", "default": ""},
        "datetime": {"type": "str", "default": ""},
        "bbox": {"type": "str", "default": ""},
        "query_json": {"type": "str", "default": ""},
        "asset_keys": {"type": "str", "default": ""},
        "page_size": {"type": "int", "default": 100},
        "max_items": {"type": "int", "default": 0},
        "out": {"type": "str", "default": ".runs/cache/stac_assets"},
        "overwrite": {"type": "bool", "default": False},
        "sign_api_url": {"type": "str", "default": ""},
        "sign_token_json_key": {"type": "str", "default": "token"},
        "save_item_json": {"type": "bool", "default": True},
        "timeout_seconds": {"type": "int", "default": 300},
        "verbose": {"type": "bool", "default": False},
    },
    "idempotent": True,
}


def _resolve_path(path_text: str, ctx) -> Path:
    p = Path(str(path_text or "")).expanduser()
    if p.is_absolute():
        return p
    repo_root = str(os.environ.get("ETL_REPO_ROOT", "") or "").strip()
    if repo_root:
        repo_candidate = (Path(repo_root).expanduser() / p).resolve()
        if repo_candidate.exists():
            return repo_candidate
    repo_rel = (Path(".").resolve() / p).resolve()
    if repo_rel.exists():
        return repo_rel
    text = str(path_text or "").replace("\\", "/")
    if text.startswith(".") or "/" in text:
        return repo_rel
    return (ctx.workdir / p).resolve()


def _http_json(url: str, *, method: str = "GET", payload: Dict[str, Any] | None = None, timeout_seconds: int = 300) -> Dict[str, Any]:
    data = json.dumps(payload).encode("utf-8") if payload is not None else None
    headers = {"User-Agent": "research-etl/0.1"}
    if payload is not None:
        headers["Content-Type"] = "application/json"
    req = request.Request(url, data=data, headers=headers, method=method.upper())
    with request.urlopen(req, timeout=timeout_seconds) as resp:  # noqa: S310
        body = json.loads(resp.read().decode("utf-8"))
    if not isinstance(body, dict):
        raise RuntimeError(f"Expected JSON object from {url}")
    return body


def _download(url: str, target: Path, *, timeout_seconds: int) -> int:
    req = request.Request(url, headers={"User-Agent": "research-etl/0.1"})
    total = 0
    with request.urlopen(req, timeout=timeout_seconds) as resp:  # noqa: S310
        with target.open("wb") as out:
            while True:
                chunk = resp.read(1024 * 1024)
                if not chunk:
                    break
                out.write(chunk)
                total += len(chunk)
    return total


def _parse_bbox(raw: Any) -> List[float]:
    if isinstance(raw, (list, tuple)):
        vals = [float(x) for x in raw]
    else:
        text = str(raw or "").strip()
        if not text:
            raise ValueError("bbox is required")
        if text.startswith("["):
            try:
                parsed = json.loads(text)
            except Exception:
                parsed = ast.literal_eval(text)
            vals = [float(x) for x in parsed]
        else:
            vals = [float(x.strip()) for x in text.split(",") if x.strip()]
    if len(vals) != 4:
        raise ValueError("bbox must contain exactly 4 numbers: minx,miny,maxx,maxy")
    return vals


def _parse_query_json(raw: Any) -> Dict[str, Any]:
    text = str(raw or "").strip()
    if not text:
        return {}
    try:
        parsed = json.loads(text)
    except Exception:
        parsed = ast.literal_eval(text)
    if not isinstance(parsed, dict):
        raise ValueError("query_json must decode to a JSON object")
    return parsed


def _parse_asset_keys(raw: Any) -> List[str]:
    if raw is None:
        return []
    if isinstance(raw, (list, tuple)):
        return [str(x).strip() for x in raw if str(x).strip()]
    text = str(raw).strip()
    if not text:
        return []
    if text.startswith("["):
        try:
            parsed = json.loads(text)
        except Exception:
            parsed = ast.literal_eval(text)
        if not isinstance(parsed, list):
            raise ValueError("asset_keys JSON form must be a list")
        return [str(x).strip() for x in parsed if str(x).strip()]
    return [part.strip() for part in text.replace("\n", ",").split(",") if part.strip()]


def _iter_search(api_url: str, payload: Dict[str, Any], *, timeout_seconds: int) -> Iterable[Dict[str, Any]]:
    next_url = api_url
    next_method = "POST"
    next_payload: Dict[str, Any] | None = payload
    while next_url:
        body = _http_json(next_url, method=next_method, payload=next_payload, timeout_seconds=timeout_seconds)
        features = body.get("features") or []
        if not isinstance(features, list):
            raise RuntimeError("STAC search returned non-list features")
        for feature in features:
            if isinstance(feature, dict):
                yield feature
        links = body.get("links") or []
        next_link = None
        if isinstance(links, list):
            next_link = next((x for x in links if isinstance(x, dict) and str(x.get("rel") or "") == "next"), None)
        if not next_link:
            return
        next_url = str(next_link.get("href") or "").strip()
        next_method = str(next_link.get("method") or "GET").strip().upper() or "GET"
        next_payload = next_link.get("body") if isinstance(next_link.get("body"), dict) else None


def _sign_url(url: str, token: str) -> str:
    token_text = str(token or "").strip()
    if not token_text:
        return url
    parts = list(parse.urlsplit(url))
    query = dict(parse.parse_qsl(parts[3], keep_blank_values=True))
    query.update(dict(parse.parse_qsl(token_text, keep_blank_values=True)))
    parts[3] = parse.urlencode(query)
    return parse.urlunsplit(parts)


def _safe_name(text: str, fallback: str) -> str:
    cleaned = "".join(ch if ch.isalnum() or ch in {"-", "_", "."} else "_" for ch in str(text or "").strip())
    return cleaned.strip("._") or fallback


def _asset_target(item_dir: Path, asset_key: str, href: str) -> Path:
    parsed = parse.urlparse(href)
    base = Path(parse.unquote(parsed.path)).name
    if not base:
        base = _safe_name(asset_key, "asset.bin")
    if base == "download":
        base = _safe_name(asset_key, "asset.bin")
    return item_dir / base


def run(args, ctx):
    api_url = str(args.get("api_url") or "").strip()
    collection = str(args.get("collection") or "").strip()
    datetime_text = str(args.get("datetime") or "").strip()
    if not api_url:
        raise ValueError("api_url is required")
    if not collection:
        raise ValueError("collection is required")
    if not datetime_text:
        raise ValueError("datetime is required")

    bbox = _parse_bbox(args.get("bbox"))
    query_json = _parse_query_json(args.get("query_json"))
    asset_keys = _parse_asset_keys(args.get("asset_keys"))
    page_size = max(1, int(args.get("page_size") or 100))
    max_items = max(0, int(args.get("max_items") or 0))
    overwrite = bool(args.get("overwrite", False))
    timeout_seconds = max(1, int(args.get("timeout_seconds") or 300))
    save_item_json = bool(args.get("save_item_json", True))
    verbose = bool(args.get("verbose", False))

    out_dir = _resolve_path(str(args.get("out") or ".runs/cache/stac_assets"), ctx)
    out_dir.mkdir(parents=True, exist_ok=True)

    sign_api_url = str(args.get("sign_api_url") or "").strip()
    sign_token_json_key = str(args.get("sign_token_json_key") or "token").strip() or "token"
    sign_token = ""
    if sign_api_url:
        sign_body = _http_json(sign_api_url, method="GET", timeout_seconds=timeout_seconds)
        sign_token = str(sign_body.get(sign_token_json_key) or "").strip()
        if not sign_token:
            raise RuntimeError(f"sign_api_url response missing token key: {sign_token_json_key}")

    payload: Dict[str, Any] = {
        "collections": [collection],
        "bbox": bbox,
        "datetime": datetime_text,
        "limit": page_size,
    }
    if query_json:
        payload["query"] = query_json

    ctx.log(
        f"[stac_asset_download] start collection={collection} api={api_url} out={out_dir.resolve().as_posix()} "
        f"page_size={page_size} max_items={max_items or 'all'}"
    )
    downloaded_files: List[str] = []
    manifest: List[Dict[str, Any]] = []
    item_count = 0
    asset_count = 0

    for feature in _iter_search(api_url, payload, timeout_seconds=timeout_seconds):
        if max_items and item_count >= max_items:
            break
        item_id = str(feature.get("id") or "").strip() or f"item_{item_count + 1:04d}"
        item_dir = out_dir / _safe_name(item_id, f"item_{item_count + 1:04d}")
        item_dir.mkdir(parents=True, exist_ok=True)
        assets = feature.get("assets") or {}
        if not isinstance(assets, dict):
            assets = {}
        chosen_assets: List[Tuple[str, Dict[str, Any]]] = []
        for key, value in sorted(assets.items()):
            if not isinstance(value, dict):
                continue
            if asset_keys and key not in asset_keys:
                continue
            href = str(value.get("href") or "").strip()
            if not href:
                continue
            chosen_assets.append((str(key), value))

        if save_item_json:
            item_json = item_dir / "item.json"
            item_json.write_text(json.dumps(feature, indent=2, ensure_ascii=True) + "\n", encoding="utf-8")
            downloaded_files.append(item_json.resolve().as_posix())

        if verbose:
            ctx.log(f"[stac_asset_download] item={item_id} assets={len(chosen_assets)}")

        for asset_key, asset in chosen_assets:
            href = str(asset.get("href") or "").strip()
            signed_href = _sign_url(href, sign_token)
            target = _asset_target(item_dir, asset_key, href)
            if target.exists() and not overwrite:
                manifest.append(
                    {
                        "item_id": item_id,
                        "asset_key": asset_key,
                        "href": href,
                        "path": target.resolve().as_posix(),
                        "status": "skipped_existing",
                    }
                )
                continue
            size = _download(signed_href, target, timeout_seconds=timeout_seconds)
            downloaded_files.append(target.resolve().as_posix())
            asset_count += 1
            manifest.append(
                {
                    "item_id": item_id,
                    "asset_key": asset_key,
                    "href": href,
                    "path": target.resolve().as_posix(),
                    "status": "downloaded",
                    "size_bytes": size,
                }
            )
        item_count += 1

    manifest_file = out_dir / "download_manifest.json"
    manifest_file.write_text(json.dumps(manifest, indent=2, ensure_ascii=True) + "\n", encoding="utf-8")
    ctx.log(
        f"[stac_asset_download] done items={item_count} assets_downloaded={asset_count} "
        f"files_written={len(downloaded_files)}"
    )
    return {
        "output_dir": out_dir.resolve().as_posix(),
        "manifest_file": manifest_file.resolve().as_posix(),
        "downloaded_files": sorted(set(downloaded_files)),
        "downloaded_count": len(set(downloaded_files)),
        "item_count": item_count,
        "asset_count": asset_count,
    }

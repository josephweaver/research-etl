from __future__ import annotations

import json
from pathlib import Path
from typing import List
from urllib import parse, request


meta = {
    "name": "web_download_list",
    "version": "0.1.0",
    "description": "Download HTTP/HTTPS files from an inline list or a urls file.",
    "inputs": [],
    "outputs": [
        "output_dir",
        "manifest_file",
        "downloaded_files",
        "downloaded_count",
        "skipped_count",
        "failed_count",
        "failed_urls",
    ],
    "params": {
        "urls": {"type": "str", "default": ""},
        "urls_file": {"type": "str", "default": ""},
        "out": {"type": "str", "default": ".runs/cache/web_downloads"},
        "overwrite": {"type": "bool", "default": False},
        "timeout_seconds": {"type": "int", "default": 120},
        "fail_on_error": {"type": "bool", "default": True},
    },
    "idempotent": True,
}


def _resolve_path(path_text: str, ctx) -> Path:
    p = Path(str(path_text or "")).expanduser()
    if p.is_absolute():
        return p
    repo_rel = (Path(".").resolve() / p).resolve()
    if repo_rel.exists():
        return repo_rel
    text = str(path_text or "").replace("\\", "/")
    if text.startswith(".") or "/" in text:
        return repo_rel
    return (ctx.workdir / p).resolve()


def _split_urls(raw: str) -> List[str]:
    text = str(raw or "").strip()
    if not text:
        return []
    out: List[str] = []
    for line in text.replace(",", "\n").splitlines():
        item = line.strip()
        if not item or item.startswith("#"):
            continue
        out.append(item)
    return out


def _read_urls_file(path_text: str, ctx) -> List[str]:
    raw = str(path_text or "").strip()
    if not raw:
        return []
    p = _resolve_path(raw, ctx)
    if not p.exists():
        raise FileNotFoundError(f"urls_file not found: {p}")
    return _split_urls(p.read_text(encoding="utf-8", errors="replace"))


def _filename_from_url(url: str, index: int) -> str:
    parsed = parse.urlparse(url)
    path_name = Path(parse.unquote(parsed.path)).name
    if path_name:
        return path_name
    return f"download_{index:04d}.bin"


def _download(url: str, target: Path, timeout_seconds: int) -> int:
    req = request.Request(url, headers={"User-Agent": "research-etl/0.1"})
    with request.urlopen(req, timeout=timeout_seconds) as resp:  # noqa: S310
        data = resp.read()
    target.write_bytes(data)
    return len(data)


def run(args, ctx):
    urls_inline = _split_urls(str(args.get("urls") or ""))
    urls_file = _read_urls_file(str(args.get("urls_file") or ""), ctx)
    urls = []
    seen = set()
    for url in [*urls_inline, *urls_file]:
        if url in seen:
            continue
        seen.add(url)
        urls.append(url)
    if not urls:
        raise ValueError("Provide at least one URL via urls or urls_file")

    out_dir = _resolve_path(str(args.get("out") or ".runs/cache/web_downloads"), ctx)
    out_dir.mkdir(parents=True, exist_ok=True)
    overwrite = bool(args.get("overwrite", False))
    timeout_seconds = max(1, int(args.get("timeout_seconds", 120)))
    fail_on_error = bool(args.get("fail_on_error", True))

    downloaded_files: List[str] = []
    failed_urls: List[str] = []
    skipped_count = 0
    manifest = []

    for idx, url in enumerate(urls, start=1):
        parsed = parse.urlparse(url)
        if parsed.scheme.lower() not in {"http", "https"}:
            raise ValueError(f"Only http/https URLs are supported: {url}")
        filename = _filename_from_url(url, idx)
        target = out_dir / filename
        if target.exists() and not overwrite:
            skipped_count += 1
            manifest.append({"url": url, "path": target.resolve().as_posix(), "status": "skipped_existing"})
            continue
        try:
            size = _download(url, target, timeout_seconds=timeout_seconds)
            downloaded_files.append(target.resolve().as_posix())
            manifest.append({"url": url, "path": target.resolve().as_posix(), "status": "downloaded", "size_bytes": size})
        except Exception as exc:  # noqa: BLE001
            failed_urls.append(url)
            manifest.append({"url": url, "path": target.resolve().as_posix(), "status": "failed", "error": str(exc)})
            if fail_on_error:
                raise RuntimeError(f"download failed for {url}: {exc}") from exc

    manifest_file = out_dir / "download_manifest.json"
    manifest_file.write_text(json.dumps(manifest, indent=2, ensure_ascii=True) + "\n", encoding="utf-8")
    ctx.log(
        f"[web_download_list] urls={len(urls)} downloaded={len(downloaded_files)} "
        f"skipped={skipped_count} failed={len(failed_urls)}"
    )
    return {
        "output_dir": out_dir.resolve().as_posix(),
        "manifest_file": manifest_file.resolve().as_posix(),
        "downloaded_files": downloaded_files,
        "downloaded_count": len(downloaded_files),
        "skipped_count": skipped_count,
        "failed_count": len(failed_urls),
        "failed_urls": failed_urls,
    }


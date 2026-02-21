# research-etl
# Copyright (c) 2026 Joseph Weaver
# This file is part of the research-etl project and is licensed under the MIT License.
# You may not use this file except in compliance with the License.
# See https://github.com/josephweaver/research-etl for details.

from __future__ import annotations

import csv
import json
import re
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, List, Set


meta = {
    "name": "yanroy_scan_raw_metadata",
    "version": "0.1.0",
    "description": "Scan YanRoy extracted files and emit per-file metadata + tile summary facts.",
    "inputs": [],
    "outputs": [
        "input_dir",
        "output_dir",
        "metadata_csv",
        "tiles_of_interest_csv",
        "summary_json",
        "file_count",
        "tile_count",
        "tiles",
    ],
    "params": {
        "input_dir": {"type": "str", "default": ""},
        "output_dir": {"type": "str", "default": ".runs/yanroy/meta"},
        "tile_regex": {"type": "str", "default": r"(?i)h\d{2}v\d{2}"},
        "verbose": {"type": "bool", "default": False},
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


def _detect_role(name: str, suffix: str) -> str:
    low = str(name or "").lower()
    if low.endswith("_ancillary_data"):
        return "ancillary"
    if suffix.lower() == ".hdr":
        return "header"
    if "_field_segments" in low:
        return "raster_data"
    return "other"


def _tile_key(path: Path, tile_re: re.Pattern[str]) -> str:
    rel_parts = [x.lower() for x in path.parts]
    for part in rel_parts:
        if tile_re.fullmatch(part):
            return part
    match = tile_re.search(path.name.lower())
    if match:
        return match.group(0).lower()
    return ""


def _utc_iso_from_ts(ts: float) -> str:
    return datetime.fromtimestamp(ts, tz=timezone.utc).isoformat().replace("+00:00", "Z")


def run(args, ctx):
    input_dir_text = str(args.get("input_dir") or "").strip()
    if not input_dir_text:
        raise ValueError("input_dir is required")
    input_dir = _resolve_path(input_dir_text, ctx)
    if not input_dir.exists() or not input_dir.is_dir():
        raise FileNotFoundError(f"input_dir not found: {input_dir}")

    output_dir = _resolve_path(str(args.get("output_dir") or ".runs/yanroy/meta"), ctx)
    output_dir.mkdir(parents=True, exist_ok=True)

    tile_regex = str(args.get("tile_regex") or r"(?i)h\d{2}v\d{2}").strip()
    tile_re = re.compile(tile_regex)
    verbose = bool(args.get("verbose", False))
    ctx.log(
        f"[yanroy_scan_raw_metadata] start input={input_dir.resolve().as_posix()} "
        f"output={output_dir.resolve().as_posix()} tile_regex={tile_regex}"
    )

    metadata_rows: List[Dict[str, str]] = []
    tile_agg: Dict[str, Dict[str, int | Set[str]]] = {}

    files = sorted(p for p in input_dir.rglob("*") if p.is_file())
    for file_path in files:
        rel = file_path.relative_to(input_dir)
        stat = file_path.stat()
        suffix = file_path.suffix or ""
        role = _detect_role(file_path.name, suffix)
        tile = _tile_key(rel, tile_re)
        metadata_rows.append(
            {
                "tile_id": tile,
                "file_name": file_path.name,
                "relative_path": rel.as_posix(),
                "relative_dir": rel.parent.as_posix() if str(rel.parent) != "." else "",
                "extension": suffix.lower(),
                "data_role": role,
                "size_bytes": str(int(stat.st_size)),
                "modified_utc": _utc_iso_from_ts(stat.st_mtime),
            }
        )
        if tile:
            state = tile_agg.setdefault(
                tile,
                {
                    "file_count": 0,
                    "has_raster_data": 0,
                    "has_header": 0,
                    "has_ancillary": 0,
                    "roles": set(),
                },
            )
            state["file_count"] = int(state["file_count"]) + 1
            if role == "raster_data":
                state["has_raster_data"] = 1
            if role == "header":
                state["has_header"] = 1
            if role == "ancillary":
                state["has_ancillary"] = 1
            cast_roles = state["roles"]
            if isinstance(cast_roles, set):
                cast_roles.add(role)

    metadata_csv = output_dir / "yanroy_raw_metadata.csv"
    with metadata_csv.open("w", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(
            f,
            fieldnames=[
                "tile_id",
                "file_name",
                "relative_path",
                "relative_dir",
                "extension",
                "data_role",
                "size_bytes",
                "modified_utc",
            ],
        )
        writer.writeheader()
        writer.writerows(metadata_rows)

    tiles_rows: List[Dict[str, str]] = []
    for tile in sorted(tile_agg.keys()):
        row = tile_agg[tile]
        roles = row.get("roles", set())
        role_text = ",".join(sorted(roles)) if isinstance(roles, set) else ""
        tiles_rows.append(
            {
                "tile_id": tile,
                "file_count": str(int(row.get("file_count", 0))),
                "has_raster_data": str(int(row.get("has_raster_data", 0))),
                "has_header": str(int(row.get("has_header", 0))),
                "has_ancillary": str(int(row.get("has_ancillary", 0))),
                "roles": role_text,
            }
        )

    tiles_csv = output_dir / "tiles.of.interest.csv"
    with tiles_csv.open("w", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(
            f,
            fieldnames=[
                "tile_id",
                "file_count",
                "has_raster_data",
                "has_header",
                "has_ancillary",
                "roles",
            ],
        )
        writer.writeheader()
        writer.writerows(tiles_rows)

    summary = {
        "file_count": len(metadata_rows),
        "tile_count": len(tiles_rows),
        "tiles": [r["tile_id"] for r in tiles_rows],
    }
    summary_json = output_dir / "summary.json"
    summary_json.write_text(json.dumps(summary, indent=2), encoding="utf-8")

    ctx.log(
        f"[yanroy_scan_raw_metadata] scanned_files={summary['file_count']} "
        f"tiles={summary['tile_count']} output={output_dir.as_posix()}"
    )
    if verbose and summary["tiles"]:
        ctx.log(f"[yanroy_scan_raw_metadata] tile_preview={summary['tiles'][:20]}")
    return {
        "input_dir": input_dir.resolve().as_posix(),
        "output_dir": output_dir.resolve().as_posix(),
        "metadata_csv": metadata_csv.resolve().as_posix(),
        "tiles_of_interest_csv": tiles_csv.resolve().as_posix(),
        "summary_json": summary_json.resolve().as_posix(),
        "file_count": summary["file_count"],
        "tile_count": summary["tile_count"],
        "tiles": summary["tiles"],
    }

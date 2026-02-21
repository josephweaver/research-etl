# research-etl
# Copyright (c) 2026 Joseph Weaver
# This file is part of the research-etl project and is licensed under the MIT License.
# You may not use this file except in compliance with the License.
# See https://github.com/josephweaver/research-etl for details.

from __future__ import annotations

import csv
import glob
import json
from copy import deepcopy
from pathlib import Path
from typing import Any, Dict, List
import xml.etree.ElementTree as ET

import yaml


meta = {
    "name": "combine_files",
    "version": "0.1.0",
    "description": "Combine multiple files into one output (csv/json/yaml/xml/text).",
    "inputs": [],
    "outputs": ["output_file", "combined_count", "input_files", "format"],
    "params": {
        "input_glob": {"type": "str", "default": ""},
        "output_file": {"type": "str", "default": ""},
        "format": {"type": "str", "default": "auto"},
        "text_separator": {"type": "str", "default": "\n"},
        "xml_root": {"type": "str", "default": "combined"},
        "allow_empty": {"type": "bool", "default": False},
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


def _detect_format(fmt: str, out: Path) -> str:
    v = str(fmt or "auto").strip().lower()
    if v and v != "auto":
        return v
    ext = out.suffix.lower()
    if ext == ".csv":
        return "csv"
    if ext == ".json":
        return "json"
    if ext in {".yaml", ".yml"}:
        return "yaml"
    if ext == ".xml":
        return "xml"
    return "text"


def _combine_csv(paths: List[Path], out: Path) -> None:
    rows: List[Dict[str, Any]] = []
    headers: List[str] = []
    seen = set()
    for p in paths:
        with p.open("r", encoding="utf-8-sig", newline="") as f:
            rdr = csv.DictReader(f)
            if not rdr.fieldnames:
                continue
            for h in rdr.fieldnames:
                if h not in seen:
                    seen.add(h)
                    headers.append(h)
            for row in rdr:
                rows.append(dict(row))
    out.parent.mkdir(parents=True, exist_ok=True)
    with out.open("w", encoding="utf-8", newline="") as f:
        w = csv.DictWriter(f, fieldnames=headers)
        w.writeheader()
        for row in rows:
            w.writerow({h: row.get(h, "") for h in headers})


def _combine_json(paths: List[Path], out: Path) -> None:
    merged: List[Any] = []
    for p in paths:
        with p.open("r", encoding="utf-8") as f:
            payload = json.load(f)
        if isinstance(payload, list):
            merged.extend(payload)
        else:
            merged.append(payload)
    out.parent.mkdir(parents=True, exist_ok=True)
    out.write_text(json.dumps(merged, indent=2, ensure_ascii=True) + "\n", encoding="utf-8")


def _combine_yaml(paths: List[Path], out: Path) -> None:
    merged: List[Any] = []
    for p in paths:
        with p.open("r", encoding="utf-8") as f:
            payload = yaml.safe_load(f)  # type: ignore[no-untyped-call]
        if isinstance(payload, list):
            merged.extend(payload)
        else:
            merged.append(payload)
    out.parent.mkdir(parents=True, exist_ok=True)
    out.write_text(yaml.safe_dump(merged, sort_keys=False), encoding="utf-8")  # type: ignore[no-untyped-call]


def _combine_xml(paths: List[Path], out: Path, root_tag: str) -> None:
    root = ET.Element(root_tag or "combined")
    for p in paths:
        tree = ET.parse(p)
        src_root = tree.getroot()
        src_copy = ET.fromstring(ET.tostring(src_root))
        root.append(src_copy)
    out.parent.mkdir(parents=True, exist_ok=True)
    ET.ElementTree(root).write(out, encoding="utf-8", xml_declaration=True)


def _combine_text(paths: List[Path], out: Path, sep: str) -> None:
    parts = [p.read_text(encoding="utf-8", errors="replace") for p in paths]
    out.parent.mkdir(parents=True, exist_ok=True)
    out.write_text(str(sep).join(parts), encoding="utf-8")


def run(args, ctx):
    pattern = str(args.get("input_glob") or "").strip()
    if not pattern:
        raise ValueError("input_glob is required")
    output_file_text = str(args.get("output_file") or "").strip()
    if not output_file_text:
        raise ValueError("output_file is required")

    raw_matches = sorted(glob.glob(pattern, recursive=True))
    if not raw_matches:
        raw_matches = sorted(glob.glob(str((ctx.workdir / pattern).as_posix()), recursive=True))
    paths = [Path(p) for p in raw_matches if Path(p).is_file()]
    allow_empty = bool(args.get("allow_empty", False))
    if not paths and not allow_empty:
        raise FileNotFoundError(f"no files matched input_glob: {pattern}")

    out = _resolve_path(output_file_text, ctx)
    fmt = _detect_format(str(args.get("format") or "auto"), out)
    verbose = bool(args.get("verbose", False))

    ctx.log(f"[combine_files] start format={fmt} inputs={len(paths)} output={out.as_posix()}")
    if paths:
        if fmt == "csv":
            _combine_csv(paths, out)
        elif fmt == "json":
            _combine_json(paths, out)
        elif fmt in {"yaml", "yml"}:
            _combine_yaml(paths, out)
        elif fmt == "xml":
            _combine_xml(paths, out, root_tag=str(args.get("xml_root") or "combined"))
        else:
            _combine_text(paths, out, sep=str(args.get("text_separator") or "\n"))
    else:
        out.parent.mkdir(parents=True, exist_ok=True)
        out.write_text("", encoding="utf-8")

    if verbose:
        ctx.log(f"[combine_files] preview_inputs={[p.as_posix() for p in paths[:5]]}")
    return {
        "output_file": out.resolve().as_posix(),
        "combined_count": len(paths),
        "input_files": [p.resolve().as_posix() for p in paths],
        "format": fmt,
    }

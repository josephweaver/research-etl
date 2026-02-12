from __future__ import annotations

import csv
import json
import re
import zipfile
from collections import Counter
from io import StringIO
from pathlib import Path
from typing import Any, Dict, Iterable, List

import yaml
from etl.ai_evidence import extract_raster_metadata, parse_world_file_text

try:
    import py7zr  # type: ignore
except Exception:  # noqa: BLE001
    py7zr = None


meta = {
    "name": "ai_dataset_evidence_bundle",
    "version": "0.1.0",
    "description": "Build evidence bundle files for ai_dataset_research inputs",
    "inputs": [],
    "outputs": [
        "dataset_id",
        "bundle_dir",
        "manifest_file",
        "schema_file",
        "sample_file",
        "notes_file",
        "supplemental_urls_file",
        "specs_fragment_file",
    ],
    "params": {
        "dataset_id": {"type": "str", "default": ""},
        "input_path": {"type": "str", "default": ""},
        "input_glob": {"type": "str", "default": ""},
        "pipeline_glob": {"type": "str", "default": "pipelines/*.yml"},
        "code_glob": {"type": "str", "default": ""},
        "supplemental_urls": {"type": "str", "default": ""},
        "supplemental_urls_file": {"type": "str", "default": ""},
        "notes": {"type": "str", "default": ""},
        "output_dir": {"type": "str", "default": ".runs/ai_context"},
        "overwrite": {"type": "bool", "default": True},
        "max_files": {"type": "int", "default": 5000},
        "sample_rows": {"type": "int", "default": 20},
        "schema_rows_scan": {"type": "int", "default": 2000},
        "max_text_chars_per_file": {"type": "int", "default": 4000},
    },
    "idempotent": True,
}

_TEXT_EXTS = {
    ".txt",
    ".md",
    ".rst",
    ".csv",
    ".tsv",
    ".json",
    ".jsonl",
    ".xml",
    ".yml",
    ".yaml",
    ".log",
}
_READ_ME_NAMES = {"readme", "readme.txt", "readme.md", "readme.rst"}
_RASTER_EXTS = {".tif", ".tiff", ".img", ".vrt", ".asc", ".bil", ".adf", ".jp2"}
_TABULAR_EXTS = {".csv", ".tsv", ".json", ".jsonl"}
_TILE_SEGMENT_RE = re.compile(r"^h\d{2}v\d{2}$", flags=re.IGNORECASE)


def _resolve_input_path(raw: str, ctx) -> Path:
    p = Path(str(raw or "").strip()).expanduser()
    if p.is_absolute():
        return p
    return p


def _split_csvish(raw: str) -> List[str]:
    text = str(raw or "").strip()
    if not text:
        return []
    parts = text.replace("|", ",").split(",")
    return [p.strip() for p in parts if p.strip()]


def _read_lines_file(path_text: str) -> List[str]:
    text = str(path_text or "").strip()
    if not text:
        return []
    p = Path(text).expanduser()
    if not p.exists():
        raise FileNotFoundError(f"input file not found: {p}")
    out: List[str] = []
    for line in p.read_text(encoding="utf-8", errors="replace").splitlines():
        item = line.strip()
        if not item or item.startswith("#"):
            continue
        out.append(item)
    return out


def _dedupe_keep_order(values: Iterable[str]) -> List[str]:
    out: List[str] = []
    seen: set[str] = set()
    for value in values:
        item = str(value or "").strip()
        if not item:
            continue
        key = item.lower()
        if key in seen:
            continue
        seen.add(key)
        out.append(item)
    return out


def _collect_files(path: Path, max_files: int) -> List[Path]:
    if not path.exists():
        return []
    if path.is_file():
        return [path]
    files: List[Path] = []
    for item in path.rglob("*"):
        if not item.is_file():
            continue
        files.append(item)
        if len(files) >= max_files:
            break
    return sorted(files)


def _resolve_glob_files(pattern: str) -> List[Path]:
    pat = str(pattern or "").strip()
    if not pat:
        return []
    p = Path(pat).expanduser()
    if p.is_absolute():
        return [p] if p.exists() and p.is_file() else []
    return [v for v in Path(".").glob(pat) if v.is_file()]


def _read_archive_members(path: Path) -> List[str]:
    suffix = path.suffix.lower()
    if suffix == ".zip":
        with zipfile.ZipFile(path, mode="r") as zf:
            return sorted(
                rel
                for rel in (str(name).replace("\\", "/").strip("/") for name in zf.namelist())
                if rel and not rel.endswith("/")
            )
    if suffix == ".7z" and py7zr is not None:
        with py7zr.SevenZipFile(path, mode="r") as zf:
            return sorted(
                rel
                for rel in (str(name).replace("\\", "/").strip("/") for name in zf.getnames())
                if rel and not rel.endswith("/")
            )
    return []


def _read_zip_member_text(zip_path: Path, member: str, max_chars: int = 200000) -> str:
    with zipfile.ZipFile(zip_path, mode="r") as zf:
        with zf.open(member, mode="r") as f:
            raw = f.read(max_chars)
    return raw.decode("utf-8", errors="replace")


def _suffix(path_like: str) -> str:
    return Path(path_like).suffix.lower()


def _top_segment(path_like: str) -> str:
    parts = [p for p in str(path_like).replace("\\", "/").split("/") if p]
    return parts[0] if parts else ""


def _is_probably_text(path: Path) -> bool:
    if path.suffix.lower() in _TEXT_EXTS:
        return True
    return path.name.lower() in _READ_ME_NAMES


def _read_text_excerpt(path: Path, max_chars: int) -> str:
    try:
        return path.read_text(encoding="utf-8", errors="replace")[:max_chars]
    except Exception:  # noqa: BLE001
        return ""


def _infer_tabular_schema(path: Path, sample_rows: int, scan_rows: int) -> Dict[str, Any]:
    text = path.read_text(encoding="utf-8", errors="replace")
    suffix = path.suffix.lower()
    return _infer_tabular_schema_from_text(text=text, suffix=suffix, sample_rows=sample_rows, scan_rows=scan_rows)


def _infer_tabular_schema_from_text(text: str, suffix: str, sample_rows: int, scan_rows: int) -> Dict[str, Any]:
    suffix = str(suffix or "").lower()
    if suffix == ".csv" or suffix == ".tsv":
        delimiter = "," if suffix == ".csv" else "\t"
        reader = csv.DictReader(StringIO(text), delimiter=delimiter)
        rows: List[Dict[str, Any]] = []
        non_null_counts: Dict[str, int] = {}
        for idx, row in enumerate(reader):
            if idx < sample_rows:
                rows.append({k: row.get(k) for k in (reader.fieldnames or [])})
            if idx >= scan_rows:
                break
            for col in (reader.fieldnames or []):
                value = row.get(col)
                if value is not None and str(value).strip() != "":
                    non_null_counts[col] = non_null_counts.get(col, 0) + 1
        fields = []
        for col in (reader.fieldnames or []):
            fields.append(
                {
                    "name": col,
                    "observed_non_null_rows": int(non_null_counts.get(col, 0)),
                    "inferred_type": "string",
                }
            )
        return {"format": suffix.lstrip("."), "fields": fields, "sample_rows": rows}

    if suffix == ".json":
        payload = json.loads(text)
        rows = payload if isinstance(payload, list) else [payload] if isinstance(payload, dict) else []
        rows = [r for r in rows if isinstance(r, dict)]
        sample = rows[:sample_rows]
        keys: set[str] = set()
        for item in rows[:scan_rows]:
            keys.update(item.keys())
        fields = [{"name": k, "inferred_type": "string"} for k in sorted(keys)]
        return {"format": "json", "fields": fields, "sample_rows": sample}

    if suffix == ".jsonl":
        sample: List[Dict[str, Any]] = []
        keys: set[str] = set()
        for idx, line in enumerate(text.splitlines()):
            raw = line.strip()
            if not raw:
                continue
            try:
                obj = json.loads(raw)
            except json.JSONDecodeError:
                continue
            if not isinstance(obj, dict):
                continue
            if len(sample) < sample_rows:
                sample.append(obj)
            if idx < scan_rows:
                keys.update(obj.keys())
        fields = [{"name": k, "inferred_type": "string"} for k in sorted(keys)]
        return {"format": "jsonl", "fields": fields, "sample_rows": sample}

    return {"format": suffix.lstrip("."), "fields": [], "sample_rows": []}


def _collect_zip_records(path: Path, max_files: int) -> List[Dict[str, Any]]:
    records: List[Dict[str, Any]] = []
    with zipfile.ZipFile(path, mode="r") as zf:
        for info in zf.infolist():
            rel = str(info.filename).replace("\\", "/").strip("/")
            if not rel or rel.endswith("/"):
                continue
            records.append(
                {
                    "path": f"{path.resolve().as_posix()}::{rel}",
                    "rel_path": rel,
                    "size_bytes": int(getattr(info, "file_size", 0)),
                    "suffix": _suffix(rel),
                    "archive_path": path.resolve().as_posix(),
                    "archive_type": "zip",
                }
            )
            if len(records) >= max_files:
                break
    return records


def _extract_zip_raster_details(zip_path: Path, zip_records: List[Dict[str, Any]], max_chars: int) -> List[Dict[str, Any]]:
    out: List[Dict[str, Any]] = []
    member_set = {str(r.get("rel_path") or "") for r in zip_records}
    for rec in zip_records:
        suffix = str(rec.get("suffix") or "").lower()
        if suffix not in _RASTER_EXTS:
            continue
        rel = str(rec.get("rel_path") or "")
        stem = str(Path(rel).with_suffix("")).replace("\\", "/")
        prj_rel = f"{stem}.prj"
        tfw_rel = f"{stem}.tfw"
        detail: Dict[str, Any] = {
            "path": rec["path"],
            "suffix": suffix,
            "source": "zip_member",
            "has_prj_sidecar": prj_rel in member_set,
            "has_world_file_sidecar": tfw_rel in member_set,
        }
        if prj_rel in member_set:
            try:
                detail["crs_wkt_excerpt"] = _read_zip_member_text(zip_path, prj_rel, max_chars=max_chars)[:1200]
            except Exception:  # noqa: BLE001
                pass
        if tfw_rel in member_set:
            try:
                tfw_text = _read_zip_member_text(zip_path, tfw_rel, max_chars=max_chars)
                parsed = parse_world_file_text(tfw_text)
                if parsed:
                    detail["world_file"] = parsed
            except Exception:  # noqa: BLE001
                pass
        out.append(detail)
    return out


def _find_refs(file_paths: List[Path], token: str, max_hits: int = 12) -> List[Dict[str, Any]]:
    if not token:
        return []
    out: List[Dict[str, Any]] = []
    lowered = token.lower()
    for path in file_paths:
        try:
            lines = path.read_text(encoding="utf-8", errors="replace").splitlines()
        except Exception:  # noqa: BLE001
            continue
        for idx, line in enumerate(lines):
            if lowered not in line.lower():
                continue
            out.append({"path": path.as_posix(), "line": idx + 1, "text": line.strip()[:220]})
            if len(out) >= max_hits:
                return out
    return out


def run(args, ctx):
    dataset_id = str(args.get("dataset_id") or "").strip()
    if not dataset_id:
        raise ValueError("dataset_id is required")

    input_path = str(args.get("input_path") or "").strip()
    input_glob = str(args.get("input_glob") or "").strip()
    if not input_path and not input_glob:
        raise ValueError("input_path or input_glob is required")

    max_files = int(args.get("max_files", 5000))
    sample_rows = int(args.get("sample_rows", 20))
    scan_rows = int(args.get("schema_rows_scan", 2000))
    max_text_chars = int(args.get("max_text_chars_per_file", 4000))

    roots: List[Path] = []
    if input_path:
        roots.append(_resolve_input_path(input_path, ctx))
    if input_glob:
        roots.extend(sorted(Path(".").glob(input_glob)))
    roots = [p for p in roots if p.exists()]
    if not roots:
        raise FileNotFoundError("No inputs found for input_path/input_glob")

    out_root = _resolve_input_path(str(args.get("output_dir") or ".runs/ai_context"), ctx)
    safe_id = dataset_id.replace("/", "_").replace("\\", "_")
    bundle_dir = out_root / safe_id
    if bundle_dir.exists() and not bool(args.get("overwrite", True)):
        raise FileExistsError(f"bundle already exists and overwrite=false: {bundle_dir}")
    bundle_dir.mkdir(parents=True, exist_ok=True)

    regular_files: List[Dict[str, str]] = []
    zip_records_all: List[Dict[str, Any]] = []
    archive_members: Dict[str, List[str]] = {}
    for root in roots:
        if root.is_file() and root.suffix.lower() in {".zip", ".7z"}:
            members = _read_archive_members(root)
            archive_members[root.resolve().as_posix()] = members
            if root.suffix.lower() == ".zip":
                zip_records_all.extend(_collect_zip_records(root, max_files=max_files))
            continue
        if root.is_file():
            regular_files.append({"abs_path": root.resolve().as_posix(), "rel_path": root.name})
            continue
        for item in _collect_files(root, max_files=max_files):
            try:
                rel = item.relative_to(root).as_posix()
            except Exception:  # noqa: BLE001
                rel = item.name
            regular_files.append({"abs_path": item.resolve().as_posix(), "rel_path": rel})

    file_records: List[Dict[str, Any]] = []
    for item in regular_files[:max_files]:
        path = Path(item["abs_path"])
        file_records.append(
            {
                "path": item["abs_path"],
                "rel_path": item["rel_path"],
                "size_bytes": int(path.stat().st_size) if path.exists() else 0,
                "suffix": path.suffix.lower(),
            }
        )
    for rec in zip_records_all[:max_files]:
        file_records.append(rec)
    for archive_path, members in archive_members.items():
        if archive_path.lower().endswith(".zip"):
            continue
        for rel in members[:max_files]:
            file_records.append(
                {
                    "path": f"{archive_path}::{rel}",
                    "rel_path": rel,
                    "size_bytes": None,
                    "suffix": _suffix(rel),
                    "archive_path": archive_path,
                    "archive_type": "7z",
                }
            )

    suffix_counts = Counter(record["suffix"] for record in file_records if record["suffix"])
    top_segment_counts = Counter(_top_segment(str(record.get("rel_path") or record["path"])) for record in file_records)

    tile_segments = []
    for segment in top_segment_counts:
        if _TILE_SEGMENT_RE.match(segment):
            tile_segments.append(segment)
    tile_segments = sorted(tile_segments)

    readme_files = [
        Path(record["path"])
        for record in file_records
        if "::" not in str(record["path"])
        and (
            Path(str(record["path"])).name.lower() in _READ_ME_NAMES
            or "readme" in Path(str(record["path"])).name.lower()
        )
    ]
    text_excerpts: List[Dict[str, str]] = []
    for path in readme_files[:8]:
        excerpt = _read_text_excerpt(path, max_text_chars)
        if excerpt.strip():
            text_excerpts.append({"path": path.as_posix(), "excerpt": excerpt})
    zip_readmes = [
        rec
        for rec in file_records
        if str(rec.get("archive_type") or "") == "zip"
        and ("readme" in Path(str(rec.get("rel_path") or "")).name.lower() or Path(str(rec.get("rel_path") or "")).name.lower() in _READ_ME_NAMES)
    ]
    for rec in zip_readmes[:8]:
        archive_path = Path(str(rec.get("archive_path") or ""))
        rel = str(rec.get("rel_path") or "")
        try:
            excerpt = _read_zip_member_text(archive_path, rel, max_chars=max_text_chars)
        except Exception:  # noqa: BLE001
            excerpt = ""
        if excerpt.strip():
            text_excerpts.append({"path": str(rec.get("path") or ""), "excerpt": excerpt[:max_text_chars]})

    tabular_candidates = [
        Path(record["path"])
        for record in file_records
        if "::" not in str(record["path"]) and Path(str(record["path"])).suffix.lower() in _TABULAR_EXTS
    ]
    schema_candidates: List[Dict[str, Any]] = []
    for path in tabular_candidates[:3]:
        try:
            schema_candidates.append(
                {
                    "path": path.as_posix(),
                    "schema": _infer_tabular_schema(path, sample_rows=sample_rows, scan_rows=scan_rows),
                }
            )
        except Exception as exc:  # noqa: BLE001
            schema_candidates.append({"path": path.as_posix(), "error": str(exc)})
    zip_tabular_candidates = [
        rec
        for rec in file_records
        if str(rec.get("archive_type") or "") == "zip" and str(rec.get("suffix") or "").lower() in _TABULAR_EXTS
    ]
    for rec in zip_tabular_candidates[:3]:
        archive_path = Path(str(rec.get("archive_path") or ""))
        rel = str(rec.get("rel_path") or "")
        try:
            text = _read_zip_member_text(archive_path, rel)
            schema_candidates.append(
                {
                    "path": str(rec.get("path") or ""),
                    "schema": _infer_tabular_schema_from_text(
                        text=text,
                        suffix=str(rec.get("suffix") or ""),
                        sample_rows=sample_rows,
                        scan_rows=scan_rows,
                    ),
                }
            )
        except Exception as exc:  # noqa: BLE001
            schema_candidates.append({"path": str(rec.get("path") or ""), "error": str(exc)})

    raster_count = sum(1 for record in file_records if str(record.get("suffix") or "").lower() in _RASTER_EXTS)
    raster_examples = [record["path"] for record in file_records if record["suffix"] in _RASTER_EXTS][:20]
    raster_details: List[Dict[str, Any]] = []
    for rec in file_records:
        if str(rec.get("archive_type") or ""):
            continue
        suffix = str(rec.get("suffix") or "").lower()
        if suffix not in _RASTER_EXTS:
            continue
        path = Path(str(rec.get("path") or ""))
        try:
            raster_details.append(extract_raster_metadata(path))
        except Exception as exc:  # noqa: BLE001
            raster_details.append({"path": path.as_posix(), "source": "filesystem", "error": str(exc)})
    for root in roots:
        if not (root.is_file() and root.suffix.lower() == ".zip"):
            continue
        root_zip_records = [r for r in zip_records_all if str(r.get("archive_path") or "") == root.resolve().as_posix()]
        raster_details.extend(_extract_zip_raster_details(root, root_zip_records, max_chars=max_text_chars))

    pipeline_globs = _split_csvish(str(args.get("pipeline_glob") or ""))
    code_globs = _split_csvish(str(args.get("code_glob") or ""))
    ref_files: List[Path] = []
    for g in pipeline_globs + code_globs:
        ref_files.extend([p for p in _resolve_glob_files(g) if _is_probably_text(p)])
    dataset_refs = _find_refs(sorted(set(ref_files)), dataset_id, max_hits=12)

    supplemental_urls = _dedupe_keep_order(
        [*_split_csvish(str(args.get("supplemental_urls") or "")), *_read_lines_file(str(args.get("supplemental_urls_file") or ""))]
    )

    manifest: Dict[str, Any] = {
        "dataset_id": dataset_id,
        "inputs": [p.resolve().as_posix() for p in roots],
        "counts": {
            "file_records": len(file_records),
            "regular_files_scanned": len(regular_files),
            "archive_count": len(archive_members),
            "raster_file_count": raster_count,
        },
        "suffix_counts": dict(sorted(suffix_counts.items(), key=lambda kv: (-kv[1], kv[0]))),
        "top_level_segment_counts": dict(sorted(top_segment_counts.items(), key=lambda kv: (-kv[1], kv[0]))),
        "tile_segments": tile_segments,
        "archive_members": archive_members,
        "schema_candidates": schema_candidates,
        "readme_excerpts": text_excerpts,
        "dataset_references": dataset_refs,
        "raster_examples": raster_examples,
        "raster_details": raster_details,
    }

    schema_payload = {
        "dataset_id": dataset_id,
        "schema_candidates": schema_candidates,
        "suffix_counts": manifest["suffix_counts"],
        "tile_segments": tile_segments,
        "raster_details": raster_details,
    }
    sample_payload = {
        "dataset_id": dataset_id,
        "raster_examples": raster_examples,
        "readme_excerpts": text_excerpts,
        "dataset_references": dataset_refs,
        "raster_details": raster_details[:20],
    }

    note_lines = [
        f"dataset_id: {dataset_id}",
        f"inputs: {', '.join(manifest['inputs'])}",
        f"file_records: {manifest['counts']['file_records']}",
        f"raster_file_count: {raster_count}",
    ]
    if tile_segments:
        note_lines.append(f"tile_segments_detected: {', '.join(tile_segments)}")
    if supplemental_urls:
        note_lines.append("supplemental_urls:")
        for url in supplemental_urls:
            note_lines.append(f"- {url}")
    free_notes = str(args.get("notes") or "").strip()
    if free_notes:
        note_lines.append("user_notes:")
        note_lines.append(free_notes)
    notes_text = "\n".join(note_lines).strip() + "\n"

    manifest_file = bundle_dir / "manifest.json"
    schema_file = bundle_dir / "schema_summary.json"
    sample_file = bundle_dir / "sample_summary.json"
    notes_file = bundle_dir / "notes_for_ai.txt"
    urls_file = bundle_dir / "supplemental_urls.txt"
    specs_fragment_file = bundle_dir / "catalog_ai_specs.fragment.yml"

    manifest_file.write_text(json.dumps(manifest, indent=2, ensure_ascii=True) + "\n", encoding="utf-8")
    schema_file.write_text(json.dumps(schema_payload, indent=2, ensure_ascii=True) + "\n", encoding="utf-8")
    sample_file.write_text(json.dumps(sample_payload, indent=2, ensure_ascii=True) + "\n", encoding="utf-8")
    notes_file.write_text(notes_text, encoding="utf-8")
    urls_file.write_text("\n".join(supplemental_urls).strip() + ("\n" if supplemental_urls else ""), encoding="utf-8")

    specs_fragment = {
        "datasets": {
            dataset_id: {
                "sample_file": sample_file.resolve().as_posix(),
                "schema_file": schema_file.resolve().as_posix(),
                "notes": notes_text.strip(),
                "supplemental_urls_file": urls_file.resolve().as_posix(),
            }
        }
    }
    specs_fragment_file.write_text(yaml.safe_dump(specs_fragment, sort_keys=False), encoding="utf-8")

    bundle_uri = bundle_dir.resolve().as_posix()
    return {
        "dataset_id": dataset_id,
        "bundle_dir": bundle_uri,
        "manifest_file": manifest_file.resolve().as_posix(),
        "schema_file": schema_file.resolve().as_posix(),
        "sample_file": sample_file.resolve().as_posix(),
        "notes_file": notes_file.resolve().as_posix(),
        "supplemental_urls_file": urls_file.resolve().as_posix(),
        "specs_fragment_file": specs_fragment_file.resolve().as_posix(),
        "_artifacts": [
            {
                "uri": bundle_uri,
                "class": "diagnostic",
                "location_type": "run_artifact",
                "is_canonical": False,
                "metadata": {"kind": "ai_dataset_evidence_bundle", "dataset_id": dataset_id},
            }
        ],
    }

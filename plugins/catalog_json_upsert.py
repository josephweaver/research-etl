from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List


meta = {
    "name": "catalog_json_upsert",
    "version": "0.1.0",
    "description": "Merge AI research JSON artifacts into a local canonical catalog.json",
    "inputs": [],
    "outputs": ["catalog_json", "updated_count", "updated_dataset_ids"],
    "params": {
        "catalog_json": {"type": "str", "default": "catalog.json"},
        "research_file": {"type": "str", "default": ""},
        "research_glob": {"type": "str", "default": ""},
        "project_id": {"type": "str", "default": ""},
        "source_run_id": {"type": "str", "default": ""},
        "source_step": {"type": "str", "default": ""},
        "overwrite_auto": {"type": "bool", "default": True},
    },
    "idempotent": True,
}


def _utc_now() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()


def _load_json(path: Path) -> Dict[str, Any]:
    with path.open("r", encoding="utf-8") as f:
        data = json.load(f)
    if not isinstance(data, dict):
        raise ValueError(f"JSON file must contain an object: {path}")
    return data


def _dataset_id_from_file(path: Path) -> str:
    name = path.name
    if name.endswith(".research.json"):
        return name[: -len(".research.json")]
    if name.endswith(".json"):
        return name[: -len(".json")]
    return path.stem


def _collect_research_files(args: Dict[str, Any], ctx) -> List[Path]:
    files: List[Path] = []
    one = str(args.get("research_file") or "").strip()
    if one:
        p = Path(one).expanduser()
        if not p.is_absolute() and not p.exists():
            p2 = ctx.workdir / p
            if p2.exists():
                p = p2
        if not p.exists():
            raise FileNotFoundError(f"research_file not found: {p}")
        files.append(p)
    pattern = str(args.get("research_glob") or "").strip()
    if pattern:
        matches = sorted(Path(".").glob(pattern))
        if not matches:
            matches = sorted(ctx.workdir.glob(pattern))
        files.extend([m for m in matches if m.is_file() and m.suffix.lower() == ".json"])
    dedup: Dict[str, Path] = {}
    for p in files:
        dedup[p.resolve().as_posix()] = p
    out = list(dedup.values())
    if not out:
        raise ValueError("No research inputs found. Provide research_file and/or research_glob.")
    return out


def _coerce_str_list(value: Any) -> List[str]:
    if not isinstance(value, list):
        return []
    out: List[str] = []
    for item in value:
        text = str(item).strip()
        if text:
            out.append(text)
    return out


def run(args, ctx):
    inputs = _collect_research_files(args, ctx)

    catalog_path = Path(str(args.get("catalog_json") or "catalog.json")).expanduser()
    catalog_path.parent.mkdir(parents=True, exist_ok=True)

    catalog: Dict[str, Any]
    if catalog_path.exists():
        catalog = _load_json(catalog_path)
    else:
        catalog = {"schema_version": 1, "datasets": {}}

    datasets = catalog.get("datasets")
    if not isinstance(datasets, dict):
        datasets = {}
        catalog["datasets"] = datasets

    overwrite_auto = bool(args.get("overwrite_auto", True))
    project_id = str(args.get("project_id") or "").strip() or None
    source_run_id = str(args.get("source_run_id") or "").strip() or None
    source_step = str(args.get("source_step") or "").strip() or None

    updated_ids: List[str] = []
    for research_path in inputs:
        payload = _load_json(research_path)
        dataset_id = str(payload.get("dataset_id") or "").strip() or _dataset_id_from_file(research_path)
        if not dataset_id:
            raise ValueError(f"Could not infer dataset_id from {research_path}")

        entry = datasets.get(dataset_id)
        if not isinstance(entry, dict):
            entry = {"dataset_id": dataset_id}
            datasets[dataset_id] = entry

        if project_id:
            entry["project_id"] = project_id

        auto = entry.get("auto")
        if not isinstance(auto, dict):
            auto = {}
            entry["auto"] = auto

        if overwrite_auto:
            auto["title"] = str(payload.get("title") or auto.get("title") or "").strip()
            auto["description"] = str(payload.get("description") or auto.get("description") or "").strip()
            auto["how_to_use_notes"] = str(payload.get("how_to_use_notes") or auto.get("how_to_use_notes") or "").strip()
            auto["tags"] = _coerce_str_list(payload.get("tags")) or _coerce_str_list(auto.get("tags"))
            auto["quality_validation"] = _coerce_str_list(payload.get("quality_validation")) or _coerce_str_list(auto.get("quality_validation"))
            auto["quality_known_issues"] = _coerce_str_list(payload.get("quality_known_issues")) or _coerce_str_list(auto.get("quality_known_issues"))
            auto["assumptions"] = _coerce_str_list(payload.get("assumptions")) or _coerce_str_list(auto.get("assumptions"))
            auto["lineage_upstream"] = _coerce_str_list(payload.get("lineage_upstream")) or _coerce_str_list(auto.get("lineage_upstream"))
        else:
            auto.setdefault("title", str(payload.get("title") or "").strip())
            auto.setdefault("description", str(payload.get("description") or "").strip())
            auto.setdefault("how_to_use_notes", str(payload.get("how_to_use_notes") or "").strip())
            auto.setdefault("tags", _coerce_str_list(payload.get("tags")))
            auto.setdefault("quality_validation", _coerce_str_list(payload.get("quality_validation")))
            auto.setdefault("quality_known_issues", _coerce_str_list(payload.get("quality_known_issues")))
            auto.setdefault("assumptions", _coerce_str_list(payload.get("assumptions")))
            auto.setdefault("lineage_upstream", _coerce_str_list(payload.get("lineage_upstream")))

        ai_meta = entry.get("ai_meta")
        if not isinstance(ai_meta, dict):
            ai_meta = {}
            entry["ai_meta"] = ai_meta
        ai_meta.update(
            {
                "updated_at": _utc_now(),
                "source_research_file": research_path.resolve().as_posix(),
                "source_run_id": source_run_id,
                "source_step": source_step,
            }
        )
        updated_ids.append(dataset_id)

    with catalog_path.open("w", encoding="utf-8") as f:
        json.dump(catalog, f, indent=2, ensure_ascii=True, sort_keys=True)
        f.write("\n")

    catalog_uri = catalog_path.resolve().as_posix()
    return {
        "catalog_json": catalog_uri,
        "updated_count": len(updated_ids),
        "updated_dataset_ids": updated_ids,
        "_artifacts": [
            {
                "uri": catalog_uri,
                "class": "diagnostic",
                "location_type": "run_artifact",
                "is_canonical": False,
                "metadata": {
                    "kind": "catalog_json",
                    "updated_dataset_ids": updated_ids,
                },
            }
        ],
    }

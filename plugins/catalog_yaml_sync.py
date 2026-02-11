from __future__ import annotations

import json
from pathlib import Path
from typing import Any, Dict, List

import yaml


meta = {
    "name": "catalog_yaml_sync",
    "version": "0.1.0",
    "description": "Sync selected fields from catalog.json into catalog repo dataset YAML files",
    "inputs": [],
    "outputs": ["updated_files", "updated_count", "catalog_repo"],
    "params": {
        "catalog_json": {"type": "str", "default": "catalog.json"},
        "catalog_repo": {"type": "str", "default": "../landcore-data-catalog"},
        "dataset_id": {"type": "str", "default": ""},
        "default_owner": {"type": "str", "default": "LandCore"},
        "default_status": {"type": "str", "default": "draft"},
        "overwrite_managed_fields": {"type": "bool", "default": True},
        "dry_run": {"type": "bool", "default": False},
    },
    "idempotent": True,
}


def _load_json(path: Path) -> Dict[str, Any]:
    with path.open("r", encoding="utf-8") as f:
        data = json.load(f)
    if not isinstance(data, dict):
        raise ValueError(f"catalog_json must contain an object: {path}")
    return data


def _data_class_from_dataset_id(dataset_id: str) -> str:
    prefix = (dataset_id.split(".", 1)[0] if "." in dataset_id else "").strip().lower()
    mapping = {
        "raw": "RAW",
        "ext": "EXT",
        "stage": "STAGE",
        "model_in": "MODEL_IN",
        "model_out": "MODEL_OUT",
        "serve": "SERVE",
        "ref": "REF",
    }
    return mapping.get(prefix, "STAGE")


def _class_dir(data_class: str) -> str:
    return str(data_class or "STAGE").strip().lower()


def _ensure_defaults(doc: Dict[str, Any], *, dataset_id: str, default_owner: str, default_status: str) -> None:
    doc["dataset_id"] = dataset_id
    doc.setdefault("title", "")
    doc.setdefault("data_class", _data_class_from_dataset_id(dataset_id))
    doc.setdefault("status", default_status)
    doc.setdefault("owner", default_owner)
    doc.setdefault("description", "")
    doc.setdefault("tags", [])
    doc.setdefault("geometry_type", "tabular")
    doc.setdefault("spatial_coverage", {"region": "", "bbox": [], "crs": "EPSG:4326"})
    doc.setdefault("temporal_coverage", {"type": "static", "start": "", "end": "", "values": []})
    doc.setdefault("grain", "")
    doc.setdefault("representations", [{"kind": "file", "uri": "", "details": {}}])
    doc.setdefault("how_to_use", {"notes": "", "examples": []})
    doc.setdefault("lineage", {"upstream": [], "transform": {"tool": "etl", "ref": "", "parameters": {}}})
    doc.setdefault("quality", {"known_issues": [], "validation": []})
    doc.setdefault("notes", [])


def _apply_auto_fields(
    doc: Dict[str, Any],
    *,
    dataset_id: str,
    auto: Dict[str, Any],
    default_owner: str,
    default_status: str,
    overwrite: bool,
) -> None:
    _ensure_defaults(doc, dataset_id=dataset_id, default_owner=default_owner, default_status=default_status)

    ai_assumptions = auto.get("assumptions")
    if not isinstance(ai_assumptions, list):
        ai_assumptions = []

    updates = {
        "title": str(auto.get("title") or ""),
        "description": str(auto.get("description") or ""),
        "tags": [str(v).strip() for v in (auto.get("tags") or []) if str(v).strip()],
        "lineage_upstream": [str(v).strip() for v in (auto.get("lineage_upstream") or []) if str(v).strip()],
        "how_to_use_notes": str(auto.get("how_to_use_notes") or ""),
        "quality_validation": [str(v).strip() for v in (auto.get("quality_validation") or []) if str(v).strip()],
        "quality_known_issues": [str(v).strip() for v in (auto.get("quality_known_issues") or []) if str(v).strip()],
        "assumptions": [str(v).strip() for v in ai_assumptions if str(v).strip()],
    }

    data_class = str(doc.get("data_class") or "").strip() or _data_class_from_dataset_id(dataset_id)
    doc["data_class"] = data_class
    doc["status"] = str(doc.get("status") or default_status).strip() or default_status
    doc["owner"] = str(doc.get("owner") or default_owner).strip() or default_owner

    if overwrite or not str(doc.get("title") or "").strip():
        if updates["title"]:
            doc["title"] = updates["title"]
    if overwrite or not str(doc.get("description") or "").strip():
        if updates["description"]:
            doc["description"] = updates["description"]

    if overwrite:
        if updates["tags"]:
            doc["tags"] = updates["tags"]
    elif not isinstance(doc.get("tags"), list):
        doc["tags"] = updates["tags"]

    lineage = doc.get("lineage")
    if not isinstance(lineage, dict):
        lineage = {}
        doc["lineage"] = lineage
    if overwrite or not isinstance(lineage.get("upstream"), list) or not lineage.get("upstream"):
        if updates["lineage_upstream"]:
            lineage["upstream"] = updates["lineage_upstream"]

    how_to_use = doc.get("how_to_use")
    if not isinstance(how_to_use, dict):
        how_to_use = {}
        doc["how_to_use"] = how_to_use
    if overwrite or not str(how_to_use.get("notes") or "").strip():
        if updates["how_to_use_notes"]:
            how_to_use["notes"] = updates["how_to_use_notes"]

    quality = doc.get("quality")
    if not isinstance(quality, dict):
        quality = {}
        doc["quality"] = quality
    if overwrite or not isinstance(quality.get("validation"), list):
        if updates["quality_validation"]:
            quality["validation"] = updates["quality_validation"]
    if overwrite or not isinstance(quality.get("known_issues"), list):
        if updates["quality_known_issues"]:
            quality["known_issues"] = updates["quality_known_issues"]

    if updates["assumptions"]:
        notes = doc.get("notes")
        if not isinstance(notes, list):
            notes = []
            doc["notes"] = notes
        marker = "AI assumptions:"
        if overwrite:
            notes = [n for n in notes if not (isinstance(n, str) and n.startswith(marker))]
            doc["notes"] = notes
        for item in updates["assumptions"]:
            notes.append(f"{marker} {item}")


def _yaml_path_for(repo_root: Path, dataset_id: str, data_class: str) -> Path:
    return repo_root / "datasets" / _class_dir(data_class) / f"{dataset_id}.yml"


def run(args, ctx):
    catalog_path = Path(str(args.get("catalog_json") or "catalog.json")).expanduser()
    if not catalog_path.is_absolute():
        catalog_path = ctx.workdir / catalog_path
    if not catalog_path.exists():
        raise FileNotFoundError(f"catalog_json not found: {catalog_path}")

    repo_root = Path(str(args.get("catalog_repo") or "../landcore-data-catalog")).expanduser()
    if not repo_root.is_absolute():
        repo_root = ctx.workdir / repo_root
    if not repo_root.exists():
        raise FileNotFoundError(f"catalog_repo not found: {repo_root}")

    catalog = _load_json(catalog_path)
    datasets = catalog.get("datasets")
    if not isinstance(datasets, dict):
        raise ValueError("catalog_json missing object field 'datasets'")

    one_dataset_id = str(args.get("dataset_id") or "").strip()
    overwrite = bool(args.get("overwrite_managed_fields", True))
    default_owner = str(args.get("default_owner") or "LandCore").strip() or "LandCore"
    default_status = str(args.get("default_status") or "draft").strip() or "draft"
    dry_run = bool(args.get("dry_run", False))

    updated_files: List[str] = []
    for dataset_id, entry in sorted(datasets.items()):
        if one_dataset_id and one_dataset_id != dataset_id:
            continue
        if not isinstance(entry, dict):
            continue
        auto = entry.get("auto")
        if not isinstance(auto, dict):
            continue

        data_class = str(entry.get("data_class") or auto.get("data_class") or "").strip() or _data_class_from_dataset_id(dataset_id)
        yml_path = _yaml_path_for(repo_root, dataset_id, data_class)
        yml_path.parent.mkdir(parents=True, exist_ok=True)

        doc: Dict[str, Any] = {}
        if yml_path.exists():
            with yml_path.open("r", encoding="utf-8") as f:
                current = yaml.safe_load(f) or {}
            if isinstance(current, dict):
                doc = current
        _apply_auto_fields(
            doc,
            dataset_id=dataset_id,
            auto=auto,
            default_owner=default_owner,
            default_status=default_status,
            overwrite=overwrite,
        )

        if not dry_run:
            text = yaml.safe_dump(doc, sort_keys=False, allow_unicode=False)
            yml_path.write_text(text, encoding="utf-8")
        updated_files.append(yml_path.resolve().as_posix())

    return {
        "catalog_repo": repo_root.resolve().as_posix(),
        "updated_count": len(updated_files),
        "updated_files": updated_files,
        "_artifacts": [
            {
                "uri": repo_root.resolve().as_posix(),
                "class": "diagnostic",
                "location_type": "run_artifact",
                "is_canonical": False,
                "metadata": {
                    "kind": "catalog_yaml_sync",
                    "updated_count": len(updated_files),
                    "dry_run": dry_run,
                },
            }
        ],
    }

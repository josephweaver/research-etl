from __future__ import annotations

import json
from pathlib import Path
from typing import Any, Dict, Optional

import yaml


meta = {
    "name": "catalog_resolve_uri",
    "version": "0.1.0",
    "description": "Resolve a dataset input URI from local catalog JSON and/or catalog YAML repo",
    "inputs": [],
    "outputs": ["dataset_id", "input_uri", "data_class", "source"],
    "params": {
        "dataset_id": {"type": "str", "default": ""},
        "catalog_json": {"type": "str", "default": ".runs/catalog/catalog.json"},
        "catalog_repo": {"type": "str", "default": "../landcore-data-catalog"},
        "representation_kind": {"type": "str", "default": ""},
        "prefer_yaml": {"type": "bool", "default": True},
        "verbose": {"type": "bool", "default": False},
    },
    "idempotent": True,
}


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


def _resolve_path(path_text: str, ctx) -> Path:
    p = Path(path_text).expanduser()
    if p.is_absolute():
        return p
    if p.exists():
        return p
    return (ctx.workdir / p)


def _load_json(path: Path) -> Dict[str, Any]:
    with path.open("r", encoding="utf-8") as f:
        data = json.load(f)
    if not isinstance(data, dict):
        raise ValueError(f"catalog_json must contain an object: {path}")
    return data


def _first_uri_from_representations(
    representations: Any,
    *,
    representation_kind: str,
) -> Optional[str]:
    if not isinstance(representations, list):
        return None
    kind = representation_kind.strip().lower()
    # prefer matching kind first
    if kind:
        for item in representations:
            if not isinstance(item, dict):
                continue
            rep_kind = str(item.get("kind") or "").strip().lower()
            uri = str(item.get("uri") or "").strip()
            if rep_kind == kind and uri:
                return uri
    # fallback first non-empty uri
    for item in representations:
        if not isinstance(item, dict):
            continue
        uri = str(item.get("uri") or "").strip()
        if uri:
            return uri
    return None


def _yaml_path_for(repo_root: Path, dataset_id: str, data_class: str) -> Path:
    return repo_root / "datasets" / _class_dir(data_class) / f"{dataset_id}.yml"


def _resolve_from_yaml(
    *,
    dataset_id: str,
    catalog_repo: Path,
    representation_kind: str,
) -> Optional[Dict[str, Any]]:
    data_class = _data_class_from_dataset_id(dataset_id)
    yml_path = _yaml_path_for(catalog_repo, dataset_id, data_class)
    if not yml_path.exists():
        return None
    with yml_path.open("r", encoding="utf-8") as f:
        data = yaml.safe_load(f) or {}
    if not isinstance(data, dict):
        return None
    uri = _first_uri_from_representations(data.get("representations"), representation_kind=representation_kind)
    if not uri:
        return None
    return {
        "dataset_id": dataset_id,
        "input_uri": uri,
        "data_class": str(data.get("data_class") or data_class),
        "source": "catalog_yaml",
        "source_path": yml_path.resolve().as_posix(),
    }


def _resolve_from_json(
    *,
    dataset_id: str,
    catalog_json: Path,
    representation_kind: str,
) -> Optional[Dict[str, Any]]:
    if not catalog_json.exists():
        return None
    payload = _load_json(catalog_json)
    datasets = payload.get("datasets")
    if not isinstance(datasets, dict):
        return None
    entry = datasets.get(dataset_id)
    if not isinstance(entry, dict):
        return None

    # Direct fields first.
    for key in ("input_uri", "artifact_uri", "uri"):
        uri = str(entry.get(key) or "").strip()
        if uri:
            return {
                "dataset_id": dataset_id,
                "input_uri": uri,
                "data_class": str(entry.get("data_class") or _data_class_from_dataset_id(dataset_id)),
                "source": "catalog_json",
                "source_path": catalog_json.resolve().as_posix(),
            }

    # Structured representations.
    uri = _first_uri_from_representations(entry.get("representations"), representation_kind=representation_kind)
    if uri:
        return {
            "dataset_id": dataset_id,
            "input_uri": uri,
            "data_class": str(entry.get("data_class") or _data_class_from_dataset_id(dataset_id)),
            "source": "catalog_json",
            "source_path": catalog_json.resolve().as_posix(),
        }

    # Fallback to research file pointer from ai_meta.
    ai_meta = entry.get("ai_meta")
    if isinstance(ai_meta, dict):
        research_file = str(ai_meta.get("source_research_file") or "").strip()
        if research_file:
            rp = Path(research_file).expanduser()
            if rp.exists():
                with rp.open("r", encoding="utf-8") as f:
                    research = json.load(f)
                if isinstance(research, dict):
                    uri = str(research.get("artifact_uri") or "").strip()
                    if uri:
                        return {
                            "dataset_id": dataset_id,
                            "input_uri": uri,
                            "data_class": str(entry.get("data_class") or _data_class_from_dataset_id(dataset_id)),
                            "source": "ai_research",
                            "source_path": rp.resolve().as_posix(),
                        }
    return None


def run(args, ctx):
    dataset_id = str(args.get("dataset_id") or "").strip()
    if not dataset_id:
        raise ValueError("dataset_id is required")

    representation_kind = str(args.get("representation_kind") or "").strip()
    prefer_yaml = bool(args.get("prefer_yaml", True))
    verbose = bool(args.get("verbose", False))

    catalog_json = _resolve_path(str(args.get("catalog_json") or ".runs/catalog/catalog.json"), ctx)
    catalog_repo = _resolve_path(str(args.get("catalog_repo") or "../landcore-data-catalog"), ctx)
    ctx.log(
        f"[catalog_resolve_uri] start dataset_id={dataset_id} prefer_yaml={prefer_yaml} "
        f"representation_kind={representation_kind or '*'}"
    )
    if verbose:
        ctx.log(
            f"[catalog_resolve_uri] sources catalog_json={catalog_json.resolve().as_posix()} "
            f"catalog_repo={catalog_repo.resolve().as_posix()}"
        )

    result: Optional[Dict[str, Any]] = None
    if prefer_yaml:
        result = _resolve_from_yaml(
            dataset_id=dataset_id,
            catalog_repo=catalog_repo,
            representation_kind=representation_kind,
        ) or _resolve_from_json(
            dataset_id=dataset_id,
            catalog_json=catalog_json,
            representation_kind=representation_kind,
        )
    else:
        result = _resolve_from_json(
            dataset_id=dataset_id,
            catalog_json=catalog_json,
            representation_kind=representation_kind,
        ) or _resolve_from_yaml(
            dataset_id=dataset_id,
            catalog_repo=catalog_repo,
            representation_kind=representation_kind,
        )

    if not result or not str(result.get("input_uri") or "").strip():
        raise FileNotFoundError(
            "Could not resolve dataset URI from catalog sources. "
            f"dataset_id={dataset_id} catalog_json={catalog_json} catalog_repo={catalog_repo}"
        )
    ctx.log(
        f"[catalog_resolve_uri] resolved source={result['source']} uri={result['input_uri']}"
    )

    return {
        "dataset_id": result["dataset_id"],
        "input_uri": result["input_uri"],
        "data_class": result["data_class"],
        "source": result["source"],
        "source_path": result.get("source_path"),
    }

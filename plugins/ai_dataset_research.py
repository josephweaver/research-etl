from __future__ import annotations

import json
from pathlib import Path
from typing import Any, Dict

import yaml

from etl.ai_research import generate_dataset_research


meta = {
    "name": "ai_dataset_research",
    "version": "0.1.0",
    "description": "Generate AI-assisted dataset documentation research JSON",
    "inputs": [],
    "outputs": ["dataset_id", "output_file", "research_json"],
    "params": {
        "dataset_id": {"type": "str", "default": ""},
        "data_class": {"type": "str", "default": ""},
        "title": {"type": "str", "default": ""},
        "artifact_uri": {"type": "str", "default": ""},
        "sample_file": {"type": "str", "default": ""},
        "schema_file": {"type": "str", "default": ""},
        "notes": {"type": "str", "default": ""},
        "model": {"type": "str", "default": ""},
        "output_dir": {"type": "str", "default": ""},
        "output_file": {"type": "str", "default": ""},
        "overwrite": {"type": "bool", "default": True},
        "specs_file": {"type": "str", "default": ""},
    },
    "idempotent": True,
}


def _read_text_optional(path_text: str) -> str | None:
    raw = (path_text or "").strip()
    if not raw:
        return None
    p = Path(raw).expanduser()
    if not p.exists():
        raise FileNotFoundError(f"input file not found: {p}")
    return p.read_text(encoding="utf-8", errors="replace")


def _load_specs(path_text: str) -> Dict[str, Dict[str, Any]]:
    raw = (path_text or "").strip()
    if not raw:
        return {}
    p = Path(raw).expanduser()
    if not p.exists():
        raise FileNotFoundError(f"specs_file not found: {p}")
    with p.open("r", encoding="utf-8") as f:
        data = yaml.safe_load(f) or {}
    if not isinstance(data, dict):
        raise ValueError("specs_file must contain a mapping at top level")
    if "datasets" in data and isinstance(data.get("datasets"), dict):
        data = data["datasets"]
    out: Dict[str, Dict[str, Any]] = {}
    for key, value in data.items():
        if isinstance(value, dict):
            out[str(key)] = value
    return out


def _resolve_file_path(output_dir: str, output_file: str, dataset_id: str, ctx) -> Path:
    if (output_file or "").strip():
        p = Path(output_file).expanduser()
        return p if p.is_absolute() else (ctx.workdir / p)
    base = Path(output_dir).expanduser() if (output_dir or "").strip() else (ctx.workdir / "ai_research")
    safe_id = dataset_id.replace("/", "_").replace("\\", "_")
    return base / f"{safe_id}.research.json"


def run(args, ctx):
    specs = _load_specs(str(args.get("specs_file") or ""))

    dataset_id = str(args.get("dataset_id") or "").strip()
    if not dataset_id:
        raise ValueError("dataset_id is required")

    spec = specs.get(dataset_id, {})
    if specs and not spec:
        ctx.warn(f"[ai_dataset_research] dataset_id '{dataset_id}' not found in specs_file; using CLI args only")

    data_class = str(args.get("data_class") or spec.get("data_class") or "").strip() or None
    title = str(args.get("title") or spec.get("title") or "").strip() or None
    artifact_uri = str(args.get("artifact_uri") or spec.get("artifact_uri") or "").strip() or None
    notes = str(args.get("notes") or spec.get("notes") or "").strip() or None
    model = str(args.get("model") or spec.get("model") or "").strip() or None

    sample_file = str(args.get("sample_file") or spec.get("sample_file") or "").strip()
    schema_file = str(args.get("schema_file") or spec.get("schema_file") or "").strip()
    sample_text = _read_text_optional(sample_file)
    schema_text = _read_text_optional(schema_file)

    out_path = _resolve_file_path(
        str(args.get("output_dir") or spec.get("output_dir") or ""),
        str(args.get("output_file") or spec.get("output_file") or ""),
        dataset_id,
        ctx,
    )
    out_path.parent.mkdir(parents=True, exist_ok=True)
    if out_path.exists() and not bool(args.get("overwrite", True)):
        raise FileExistsError(f"output file already exists and overwrite=false: {out_path}")

    ctx.log(f"[ai_dataset_research] generating research for {dataset_id}")
    research = generate_dataset_research(
        dataset_id=dataset_id,
        data_class=data_class,
        title=title,
        artifact_uri=artifact_uri,
        sample_text=sample_text,
        schema_text=schema_text,
        notes=notes,
        model=model,
    )
    out_path.write_text(json.dumps(research, indent=2, ensure_ascii=True) + "\n", encoding="utf-8")
    uri = out_path.resolve().as_posix()
    return {
        "dataset_id": dataset_id,
        "output_file": uri,
        "research_json": research,
        "_artifacts": [
            {
                "uri": uri,
                "class": "diagnostic",
                "location_type": "run_artifact",
                "is_canonical": False,
                "metadata": {
                    "kind": "ai_dataset_research",
                    "dataset_id": dataset_id,
                    "data_class": data_class,
                },
            }
        ],
    }

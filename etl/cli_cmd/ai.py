# research-etl
# Copyright (c) 2026 Joseph Weaver
# This file is part of the research-etl project and is licensed under the MIT License.
# You may not use this file except in compliance with the License.
# See https://github.com/josephweaver/research-etl for details.

from __future__ import annotations
"""AI CLI command group.

High-level idea:
- Run AI-assisted dataset research tasks from the CLI.
- Produces structured JSON payloads for downstream catalog/documentation flows.
"""

import argparse
import json
import sys
from pathlib import Path

from etl.ai_research import AIResearchError, generate_dataset_research
from etl.cli_cmd.common import CommandHandler


def register_ai_args(
    ai_sub: argparse._SubParsersAction,
    *,
    cmd_ai_research: CommandHandler,
) -> None:
    # CLI inputs: ai research arguments (dataset metadata/context/model/output).
    # CLI output: binds `ai research` parser to the research handler.
    p_ai_research = ai_sub.add_parser("research", help="Generate dataset explanation draft JSON")
    p_ai_research.add_argument("--dataset-id", required=True, help="Catalog dataset_id (e.g., serve.example_v1)")
    p_ai_research.add_argument("--data-class", default=None, help="Catalog data_class hint (RAW|EXT|STAGE|MODEL_IN|MODEL_OUT|SERVE|REF)")
    p_ai_research.add_argument("--title", default=None, help="Optional dataset title hint")
    p_ai_research.add_argument("--artifact-uri", default=None, help="Optional storage URI/path hint")
    p_ai_research.add_argument("--sample-file", default=None, help="Optional sample data excerpt file")
    p_ai_research.add_argument("--schema-file", default=None, help="Optional schema/column description file")
    p_ai_research.add_argument(
        "--supplemental-url",
        action="append",
        default=[],
        help="Optional URL to fetch and include as supplemental research context (repeatable).",
    )
    p_ai_research.add_argument(
        "--supplemental-urls-file",
        default=None,
        help="Optional text file with one supplemental URL per line (# comments allowed).",
    )
    p_ai_research.add_argument("--notes", default=None, help="Optional analyst notes/instructions")
    p_ai_research.add_argument("--model", default=None, help="Optional OpenAI model override")
    p_ai_research.add_argument("--output", default=None, help="Write JSON output to file path instead of stdout")
    p_ai_research.set_defaults(func=cmd_ai_research)


def cmd_ai_research(args: argparse.Namespace) -> int:
    """Generate structured AI research output for a dataset.

    Inputs:
    - dataset id and optional class/title/artifact URI.
    - optional context files (`sample_file`, `schema_file`, supplemental URLs).
    - optional model and output-file destination.

    Outputs:
    - stdout: formatted JSON payload (or success path message when writing file).
    - writes JSON file when `--output` is provided.
    - stderr: validation/research errors.
    - return code: `0` on success, `1` on error.
    """
    dataset_id = str(args.dataset_id or "").strip()
    if not dataset_id:
        print("AI research error: --dataset-id is required.", file=sys.stderr)
        return 1

    sample_text = None
    if args.sample_file:
        sample_path = Path(args.sample_file)
        if not sample_path.exists():
            print(f"AI research error: sample file not found: {sample_path}", file=sys.stderr)
            return 1
        sample_text = sample_path.read_text(encoding="utf-8", errors="replace")

    schema_text = None
    if args.schema_file:
        schema_path = Path(args.schema_file)
        if not schema_path.exists():
            print(f"AI research error: schema file not found: {schema_path}", file=sys.stderr)
            return 1
        schema_text = schema_path.read_text(encoding="utf-8", errors="replace")

    supplemental_urls: list[str] = []
    for url in list(args.supplemental_url or []):
        text = str(url or "").strip()
        if text:
            supplemental_urls.append(text)
    if args.supplemental_urls_file:
        urls_path = Path(args.supplemental_urls_file)
        if not urls_path.exists():
            print(f"AI research error: supplemental URLs file not found: {urls_path}", file=sys.stderr)
            return 1
        for line in urls_path.read_text(encoding="utf-8", errors="replace").splitlines():
            text = line.strip()
            if not text or text.startswith("#"):
                continue
            supplemental_urls.append(text)

    try:
        payload = generate_dataset_research(
            dataset_id=dataset_id,
            data_class=args.data_class,
            title=args.title,
            artifact_uri=args.artifact_uri,
            sample_text=sample_text,
            schema_text=schema_text,
            notes=args.notes,
            supplemental_urls=supplemental_urls or None,
            model=args.model,
        )
    except AIResearchError as exc:
        print(f"AI research error: {exc}", file=sys.stderr)
        return 1

    out = json.dumps(payload, indent=2, ensure_ascii=True)
    if args.output:
        out_path = Path(args.output)
        out_path.parent.mkdir(parents=True, exist_ok=True)
        out_path.write_text(out + "\n", encoding="utf-8")
        print(f"Wrote AI research output: {out_path}")
        return 0

    print(out)
    return 0

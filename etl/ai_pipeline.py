# research-etl
# Copyright (c) 2026 Joseph Weaver
# This file is part of the research-etl project and is licensed under the MIT License.
# You may not use this file except in compliance with the License.
# See https://github.com/josephweaver/research-etl for details.

from __future__ import annotations

import json
import os
from typing import Any, Optional
from urllib import request, error


class AIPipelineError(RuntimeError):
    """Raised when AI pipeline generation fails."""


def _extract_text(payload: dict[str, Any]) -> str:
    text = payload.get("output_text")
    if isinstance(text, str) and text.strip():
        return text.strip()
    for item in payload.get("output", []) or []:
        if not isinstance(item, dict):
            continue
        for c in item.get("content", []) or []:
            if not isinstance(c, dict):
                continue
            t = c.get("text")
            if isinstance(t, str) and t.strip():
                return t.strip()
    return ""


def _extract_yaml(text: str) -> str:
    raw = (text or "").strip()
    if not raw:
        return ""
    if "```" not in raw:
        return raw
    lines = raw.splitlines()
    in_block = False
    buf: list[str] = []
    for line in lines:
        if line.strip().startswith("```"):
            if not in_block:
                in_block = True
                continue
            break
        if in_block:
            buf.append(line)
    return "\n".join(buf).strip() if buf else raw


def generate_pipeline_draft(
    *,
    intent: str,
    constraints: Optional[str] = None,
    existing_yaml: Optional[str] = None,
    model: Optional[str] = None,
) -> str:
    api_key = (os.environ.get("OPENAI_API_KEY") or "").strip()
    if not api_key:
        raise AIPipelineError("OPENAI_API_KEY is not configured.")
    model_name = (model or os.environ.get("OPENAI_MODEL") or "gpt-4.1-mini").strip()
    prompt = (
        "Generate a valid ETL pipeline YAML for this schema: top-level keys vars, dirs, steps. "
        "Each step needs name and script. Return YAML only."
    )
    user_bits = [f"Intent:\n{intent.strip()}"]
    if constraints and constraints.strip():
        user_bits.append(f"Constraints:\n{constraints.strip()}")
    if existing_yaml and existing_yaml.strip():
        user_bits.append(f"Existing draft to improve:\n{existing_yaml.strip()}")
    body = {
        "model": model_name,
        "input": [
            {"role": "system", "content": [{"type": "input_text", "text": prompt}]},
            {"role": "user", "content": [{"type": "input_text", "text": "\n\n".join(user_bits)}]},
        ],
        "temperature": 0.2,
        "max_output_tokens": 1200,
    }
    req = request.Request(
        "https://api.openai.com/v1/responses",
        data=json.dumps(body).encode("utf-8"),
        headers={
            "Authorization": f"Bearer {api_key}",
            "Content-Type": "application/json",
        },
        method="POST",
    )
    try:
        with request.urlopen(req, timeout=60) as resp:
            payload = json.loads(resp.read().decode("utf-8"))
    except error.HTTPError as exc:
        detail = exc.read().decode("utf-8", errors="replace")
        raise AIPipelineError(f"OpenAI API error ({exc.code}): {detail}") from exc
    except Exception as exc:  # noqa: BLE001
        raise AIPipelineError(f"OpenAI request failed: {exc}") from exc
    out = _extract_yaml(_extract_text(payload))
    if not out:
        raise AIPipelineError("Model returned empty draft.")
    return out


__all__ = ["AIPipelineError", "generate_pipeline_draft"]

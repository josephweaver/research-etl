from __future__ import annotations

import json
import os
from typing import Any, Optional
from urllib import error, request


class AIResearchError(RuntimeError):
    """Raised when AI dataset research generation fails."""


def _extract_text(payload: dict[str, Any]) -> str:
    text = payload.get("output_text")
    if isinstance(text, str) and text.strip():
        return text.strip()
    for item in payload.get("output", []) or []:
        if not isinstance(item, dict):
            continue
        for content in item.get("content", []) or []:
            if not isinstance(content, dict):
                continue
            text = content.get("text")
            if isinstance(text, str) and text.strip():
                return text.strip()
    return ""


def _extract_json(text: str) -> str:
    raw = (text or "").strip()
    if not raw:
        return ""
    if raw.startswith("{") and raw.endswith("}"):
        return raw
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


def _normalize_result(data: dict[str, Any]) -> dict[str, Any]:
    tags = data.get("tags")
    if not isinstance(tags, list):
        tags = []
    validations = data.get("quality_validation")
    if not isinstance(validations, list):
        validations = []
    known_issues = data.get("quality_known_issues")
    if not isinstance(known_issues, list):
        known_issues = []
    assumptions = data.get("assumptions")
    if not isinstance(assumptions, list):
        assumptions = []
    lineage_upstream = data.get("lineage_upstream")
    if not isinstance(lineage_upstream, list):
        lineage_upstream = []
    return {
        "title": str(data.get("title") or "").strip(),
        "description": str(data.get("description") or "").strip(),
        "how_to_use_notes": str(data.get("how_to_use_notes") or "").strip(),
        "tags": [str(v).strip() for v in tags if str(v).strip()],
        "quality_validation": [str(v).strip() for v in validations if str(v).strip()],
        "quality_known_issues": [str(v).strip() for v in known_issues if str(v).strip()],
        "assumptions": [str(v).strip() for v in assumptions if str(v).strip()],
        "lineage_upstream": [str(v).strip() for v in lineage_upstream if str(v).strip()],
    }


def generate_dataset_research(
    *,
    dataset_id: str,
    data_class: Optional[str] = None,
    title: Optional[str] = None,
    artifact_uri: Optional[str] = None,
    sample_text: Optional[str] = None,
    schema_text: Optional[str] = None,
    notes: Optional[str] = None,
    model: Optional[str] = None,
) -> dict[str, Any]:
    api_key = (os.environ.get("OPENAI_API_KEY") or "").strip()
    if not api_key:
        raise AIResearchError("OPENAI_API_KEY is not configured.")
    model_name = (model or os.environ.get("OPENAI_MODEL") or "gpt-4.1-mini").strip()
    if not model_name:
        raise AIResearchError("OpenAI model name is empty.")

    prompt = (
        "You are preparing structured dataset documentation for a data catalog. "
        "Respond with valid JSON only. Avoid markdown. "
        "If uncertain, include assumptions and avoid hallucinating concrete values. "
        "Return keys: title, description, how_to_use_notes, tags, quality_validation, "
        "quality_known_issues, assumptions, lineage_upstream."
    )
    context = {
        "dataset_id": dataset_id,
        "data_class": data_class or "",
        "title": title or "",
        "artifact_uri": artifact_uri or "",
        "notes": notes or "",
        "schema_excerpt": (schema_text or "")[:12000],
        "sample_excerpt": (sample_text or "")[:12000],
    }
    body = {
        "model": model_name,
        "input": [
            {"role": "system", "content": [{"type": "input_text", "text": prompt}]},
            {
                "role": "user",
                "content": [
                    {
                        "type": "input_text",
                        "text": json.dumps(context, ensure_ascii=True, indent=2),
                    }
                ],
            },
        ],
        "temperature": 0.2,
        "max_output_tokens": 1400,
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
        with request.urlopen(req, timeout=90) as resp:
            payload = json.loads(resp.read().decode("utf-8"))
    except error.HTTPError as exc:
        detail = exc.read().decode("utf-8", errors="replace")
        raise AIResearchError(f"OpenAI API error ({exc.code}): {detail}") from exc
    except Exception as exc:  # noqa: BLE001
        raise AIResearchError(f"OpenAI request failed: {exc}") from exc

    raw = _extract_json(_extract_text(payload))
    if not raw:
        raise AIResearchError("Model returned empty research output.")
    try:
        data = json.loads(raw)
    except json.JSONDecodeError as exc:
        raise AIResearchError(f"Model returned non-JSON output: {exc}") from exc
    if not isinstance(data, dict):
        raise AIResearchError("Model returned JSON that is not an object.")
    return _normalize_result(data)


__all__ = ["AIResearchError", "generate_dataset_research"]

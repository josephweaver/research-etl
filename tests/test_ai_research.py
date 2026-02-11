from __future__ import annotations

import io
import json
from urllib import error

import pytest

from etl.ai_research import AIResearchError, generate_dataset_research


class _Resp:
    def __init__(self, payload: dict, headers: dict | None = None):
        self._payload = payload
        self.headers = headers or {"Content-Type": "application/json"}

    def read(self) -> bytes:
        return json.dumps(self._payload).encode("utf-8")

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        return False


class _RawResp:
    def __init__(self, text: str, content_type: str = "text/html"):
        self._text = text
        self.headers = {"Content-Type": content_type}

    def read(self) -> bytes:
        return self._text.encode("utf-8")

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        return False


def test_generate_dataset_research_success(monkeypatch):
    monkeypatch.setenv("OPENAI_API_KEY", "test-key")

    def _fake_urlopen(req, timeout=0):  # noqa: ANN001
        payload = {
            "output_text": json.dumps(
                {
                    "title": "County Yield Summary",
                    "description": "Dataset summary.",
                    "how_to_use_notes": "Filter by county and year.",
                    "tags": ["yield", "county"],
                    "quality_validation": ["no duplicate keys"],
                    "quality_known_issues": [],
                    "assumptions": ["values are annual"],
                    "lineage_upstream": ["model_out.yield_predictions_v3"],
                }
            )
        }
        return _Resp(payload)

    monkeypatch.setattr("etl.ai_research.request.urlopen", _fake_urlopen)
    out = generate_dataset_research(dataset_id="serve.county_yield_v1", data_class="SERVE")
    assert out["title"] == "County Yield Summary"
    assert out["tags"] == ["yield", "county"]
    assert out["lineage_upstream"] == ["model_out.yield_predictions_v3"]


def test_generate_dataset_research_includes_supplemental_urls(monkeypatch):
    monkeypatch.setenv("OPENAI_API_KEY", "test-key")
    captured = {}

    def _fake_urlopen(req, timeout=0):  # noqa: ANN001
        url = getattr(req, "full_url", "")
        if str(url).startswith("https://example.com"):
            return _RawResp("<html><body><h1>Source</h1><p>Supplemental details</p></body></html>", "text/html")
        body = json.loads(req.data.decode("utf-8"))
        captured.update(body)
        return _Resp(
            {
                "output_text": json.dumps(
                    {
                        "title": "T",
                        "description": "D",
                        "how_to_use_notes": "H",
                        "tags": [],
                        "quality_validation": [],
                        "quality_known_issues": [],
                        "assumptions": [],
                        "lineage_upstream": [],
                    }
                )
            }
        )

    monkeypatch.setattr("etl.ai_research.request.urlopen", _fake_urlopen)
    _ = generate_dataset_research(
        dataset_id="serve.county_yield_v1",
        supplemental_urls=["https://example.com/info"],
    )
    user_text = captured["input"][1]["content"][0]["text"]
    assert "supplemental_sources" in user_text
    assert "https://example.com/info" in user_text
    assert "Supplemental details" in user_text


def test_generate_dataset_research_http_error(monkeypatch):
    monkeypatch.setenv("OPENAI_API_KEY", "test-key")

    def _fake_urlopen(req, timeout=0):  # noqa: ANN001
        raise error.HTTPError(
            url="https://api.openai.com/v1/responses",
            code=400,
            msg="bad request",
            hdrs=None,
            fp=io.BytesIO(b'{"error":"bad"}'),
        )

    monkeypatch.setattr("etl.ai_research.request.urlopen", _fake_urlopen)
    with pytest.raises(AIResearchError, match="OpenAI API error"):
        generate_dataset_research(dataset_id="serve.county_yield_v1")

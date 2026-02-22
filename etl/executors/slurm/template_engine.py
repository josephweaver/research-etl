from __future__ import annotations

import re
from pathlib import Path
from typing import Dict

_VAR_PATTERN = re.compile(r"\{\{\s*([a-zA-Z0-9_]+)\s*\}\}")


def render_template_text(template_text: str, values: Dict[str, str]) -> str:
    data = dict(values or {})

    def _replace(match: re.Match[str]) -> str:
        key = str(match.group(1) or "").strip()
        return str(data.get(key, ""))

    return _VAR_PATTERN.sub(_replace, str(template_text or ""))


def render_template_file(template_path: Path, values: Dict[str, str]) -> str:
    text = Path(template_path).read_text(encoding="utf-8")
    return render_template_text(text, values)

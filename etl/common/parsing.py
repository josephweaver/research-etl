# research-etl
# Copyright (c) 2026 Joseph Weaver
# This file is part of the research-etl project and is licensed under the MIT License.
# You may not use this file except in compliance with the License.
# See https://github.com/josephweaver/research-etl for details.

from __future__ import annotations

from typing import Any, Optional


def parse_bool(value: Any, default: bool = False) -> bool:
    if value is None:
        return bool(default)
    if isinstance(value, bool):
        return value
    text = str(value).strip().lower()
    if not text:
        return bool(default)
    if text in {"1", "true", "yes", "on", "y"}:
        return True
    if text in {"0", "false", "no", "off", "n"}:
        return False
    return bool(default)


def parse_str_list(value: Any) -> list[str]:
    if value is None:
        return []
    if isinstance(value, (list, tuple, set)):
        raw_items = [str(x).strip() for x in value]
    else:
        raw_items = [x.strip() for x in str(value).replace(";", ",").split(",")]
    out: list[str] = []
    seen: set[str] = set()
    for item in raw_items:
        if not item or item in seen:
            continue
        seen.add(item)
        out.append(item)
    return out


def last_non_empty_text(value: Any) -> str:
    lines = str(value or "").replace("\r\n", "\n").replace("\r", "\n").split("\n")
    for line in reversed(lines):
        text = str(line or "").strip()
        if text:
            return text
    return ""


def parse_optional_int(value: Any, *, field_name: str) -> Optional[int]:
    if value is None or str(value).strip() == "":
        return None
    try:
        return int(value)
    except (TypeError, ValueError) as exc:
        raise ValueError(f"Invalid {field_name}: must be an integer.") from exc


def parse_optional_float(value: Any, *, field_name: str) -> Optional[float]:
    if value is None or str(value).strip() == "":
        return None
    try:
        return float(value)
    except (TypeError, ValueError) as exc:
        raise ValueError(f"Invalid {field_name}: must be a number.") from exc

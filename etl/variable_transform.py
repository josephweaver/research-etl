from __future__ import annotations

import glob
import hashlib
import json
import os
import re
import uuid
from pathlib import Path
from typing import Any, Mapping, Sequence


def to_scalar(value: Any) -> Any:
    if hasattr(value, "item"):
        try:
            return value.item()
        except Exception:  # noqa: BLE001
            return value
    return value


def normalize_text(value: Any) -> str:
    return str(to_scalar(value) or "").strip()


def regex_extract(value: Any, pattern: str, group: int | str = 1, default: str = "") -> str:
    text = normalize_text(value)
    if not text or not str(pattern or "").strip():
        return str(default or "")
    m = re.search(str(pattern), text)
    if not m:
        return str(default or "")
    group_value: int | str = group
    if isinstance(group_value, str) and group_value.isdigit():
        group_value = int(group_value)
    try:
        return normalize_text(m.group(group_value))
    except Exception:  # noqa: BLE001
        return str(default or "")


def substring(value: Any, start: int | None = None, end: int | None = None) -> str:
    text = normalize_text(value)
    s = int(start) if start is not None else None
    e = int(end) if end is not None else None
    return text[s:e]


def sha256_hex(value: Any) -> str:
    if isinstance(value, bytes):
        payload = value
    else:
        payload = normalize_text(value).encode("utf-8")
    return hashlib.sha256(payload).hexdigest()


def uuid5_from(namespace_text: str, *parts: Any) -> str:
    ns = uuid.UUID(str(namespace_text).strip())
    payload = "|".join(normalize_text(p) for p in parts)
    return str(uuid.uuid5(ns, payload))


def resolve_path(path_text: str, *, workdir: str | Path | None = None) -> Path:
    p = Path(str(path_text or "")).expanduser()
    if p.is_absolute():
        return p.resolve()
    repo_root_env = str(os.environ.get("ETL_REPO_ROOT") or "").strip()
    if repo_root_env:
        repo_rel = (Path(repo_root_env).expanduser().resolve() / p).resolve()
        if repo_rel.exists():
            return repo_rel
    cwd_rel = (Path(".").resolve() / p).resolve()
    if cwd_rel.exists():
        return cwd_rel
    if workdir is not None:
        return (Path(workdir).expanduser().resolve() / p).resolve()
    return cwd_rel


def read_text(path_text: str, *, workdir: str | Path | None = None, encoding: str = "utf-8") -> str:
    return resolve_path(path_text, workdir=workdir).read_text(encoding=encoding)


def read_json(path_text: str, *, workdir: str | Path | None = None, encoding: str = "utf-8") -> Any:
    return json.loads(read_text(path_text, workdir=workdir, encoding=encoding))


def list_files(pattern: str, *, workdir: str | Path | None = None, files_only: bool = True) -> list[str]:
    base_pattern = str(pattern or "").strip()
    if not base_pattern:
        return []
    matches = sorted(glob.glob(base_pattern, recursive=True))
    if not matches and workdir is not None:
        matches = sorted(glob.glob(str((Path(workdir).expanduser().resolve() / base_pattern).as_posix()), recursive=True))
    out: list[str] = []
    for raw in matches:
        p = Path(raw)
        if files_only and not p.is_file():
            continue
        out.append(p.resolve().as_posix())
    return out


def _path_name(value: Any) -> str:
    return Path(normalize_text(value)).name


def _path_stem(value: Any) -> str:
    return Path(normalize_text(value)).stem


def _path_parent(value: Any) -> str:
    return Path(normalize_text(value)).parent.as_posix()


def _path_suffix(value: Any) -> str:
    return Path(normalize_text(value)).suffix


def _as_json_value(text: str) -> Any:
    raw = str(text or "").strip()
    if not raw:
        return ""
    try:
        return json.loads(raw)
    except Exception:
        return raw


def _resolve_parts(parts_value: Any) -> list[Any]:
    if isinstance(parts_value, (list, tuple)):
        return list(parts_value)
    if isinstance(parts_value, str):
        parsed = _as_json_value(parts_value)
        if isinstance(parsed, list):
            return list(parsed)
        return [parsed]
    if parts_value is None:
        return []
    return [parts_value]


def apply_operation(value: Any, spec: Mapping[str, Any], *, workdir: str | Path | None = None) -> Any:
    op = normalize_text(spec.get("op") or spec.get("name")).lower()
    if not op:
        raise ValueError("transform op is required")

    current = spec.get("value", value)

    if op == "literal":
        return current
    if op == "lower":
        return normalize_text(current).lower()
    if op == "upper":
        return normalize_text(current).upper()
    if op == "strip":
        return normalize_text(current)
    if op == "substring":
        return substring(current, spec.get("start"), spec.get("end"))
    if op == "regex_extract":
        return regex_extract(current, str(spec.get("pattern") or ""), spec.get("group", 1), str(spec.get("default") or ""))
    if op == "path_name":
        return _path_name(current)
    if op == "path_stem":
        return _path_stem(current)
    if op == "path_parent":
        return _path_parent(current)
    if op == "path_suffix":
        return _path_suffix(current)
    if op == "read_text":
        return read_text(normalize_text(current), workdir=workdir, encoding=str(spec.get("encoding") or "utf-8"))
    if op == "read_json":
        return read_json(normalize_text(current), workdir=workdir, encoding=str(spec.get("encoding") or "utf-8"))
    if op == "list_files":
        return list_files(str(spec.get("pattern") or current), workdir=workdir, files_only=bool(spec.get("files_only", True)))
    if op == "json_parse":
        return _as_json_value(normalize_text(current))
    if op == "json_dump":
        return json.dumps(current, ensure_ascii=True, sort_keys=bool(spec.get("sort_keys", False)))
    if op == "sha256":
        return sha256_hex(current)
    if op == "uuid5":
        parts = _resolve_parts(spec.get("parts", current))
        return uuid5_from(str(spec.get("namespace") or ""), *parts)
    if op == "join":
        items = current if isinstance(current, Sequence) and not isinstance(current, (str, bytes, bytearray)) else _resolve_parts(current)
        sep = str(spec.get("separator") or "")
        return sep.join(normalize_text(x) for x in items)
    if op == "split":
        text = normalize_text(current)
        sep = spec.get("separator")
        return text.split(str(sep)) if sep is not None else text.split()
    if op == "dict":
        out: dict[str, Any] = {}
        fields = spec.get("fields")
        if not isinstance(fields, Mapping):
            raise ValueError("dict op requires `fields` mapping")
        for key, val in fields.items():
            out[str(key)] = val
        return out
    raise ValueError(f"unsupported transform op: {op}")


def apply_operations(value: Any, operations: Sequence[Mapping[str, Any]], *, workdir: str | Path | None = None) -> Any:
    current = value
    for spec in operations:
        current = apply_operation(current, spec, workdir=workdir)
    return current


__all__ = [
    "apply_operation",
    "apply_operations",
    "list_files",
    "normalize_text",
    "read_json",
    "read_text",
    "regex_extract",
    "resolve_path",
    "sha256_hex",
    "substring",
    "to_scalar",
    "uuid5_from",
]

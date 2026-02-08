from __future__ import annotations

import json
import traceback
import uuid
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional

from . import __version__


def _now_stamp() -> str:
    return datetime.utcnow().strftime("%Y%m%dT%H%M%SZ")


def _read_excerpt(path: Path, lineno: int, context: int = 4) -> Optional[Dict[str, Any]]:
    if lineno <= 0 or not path.exists() or not path.is_file():
        return None
    try:
        lines = path.read_text(encoding="utf-8").splitlines()
    except Exception:
        return None
    start = max(1, lineno - context)
    end = min(len(lines), lineno + context)
    excerpt_lines = []
    for n in range(start, end + 1):
        excerpt_lines.append({"line": n, "text": lines[n - 1]})
    return {"start_line": start, "end_line": end, "lines": excerpt_lines}


def _relative(path: Path, repo_root: Path) -> str:
    try:
        return str(path.resolve().relative_to(repo_root.resolve()))
    except Exception:
        return str(path)


def write_error_report(
    *,
    exc: Exception,
    command: str,
    argv: Optional[List[str]] = None,
    workdir: Path = Path(".runs"),
    repo_root: Path = Path("."),
) -> Path:
    """
    Write a portable JSON diagnostic report with traceback and code excerpts.
    """
    report_dir = workdir / "error_reports"
    report_dir.mkdir(parents=True, exist_ok=True)
    report_path = report_dir / f"{_now_stamp()}-{uuid.uuid4().hex[:8]}.json"

    tb_exc = traceback.TracebackException.from_exception(exc)
    frames: List[Dict[str, Any]] = []
    for fs in tb_exc.stack:
        file_path = Path(fs.filename)
        frames.append(
            {
                "file": _relative(file_path, repo_root),
                "line": fs.lineno,
                "name": fs.name,
                "code": fs.line or "",
                "excerpt": _read_excerpt(file_path, fs.lineno),
            }
        )

    payload = {
        "generated_at": datetime.utcnow().isoformat() + "Z",
        "version": __version__,
        "command": command,
        "argv": argv or [],
        "exception_type": exc.__class__.__name__,
        "exception_message": str(exc),
        "traceback": "".join(tb_exc.format()),
        "frames": frames,
    }
    report_path.write_text(json.dumps(payload, indent=2), encoding="utf-8")
    return report_path


def find_latest_error_report(workdir: Path = Path(".runs")) -> Optional[Path]:
    report_dir = workdir / "error_reports"
    if not report_dir.exists():
        return None
    reports = sorted(
        [p for p in report_dir.glob("*.json") if p.is_file()],
        key=lambda p: p.stat().st_mtime,
        reverse=True,
    )
    return reports[0] if reports else None


__all__ = ["write_error_report", "find_latest_error_report"]

# research-etl
# Copyright (c) 2026 Joseph Weaver
# This file is part of the research-etl project and is licensed under the MIT License.
# You may not use this file except in compliance with the License.
# See https://github.com/josephweaver/research-etl for details.

"""
Offline queue utilities for DB tracking updates.
"""

from __future__ import annotations

import json
import shutil
import uuid
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, Optional

from .tracking import upsert_run_status, upsert_step_attempt


def _now_stamp() -> str:
    return datetime.utcnow().strftime("%Y%m%dT%H%M%S%fZ")


@dataclass
class QueueApplySummary:
    queued: int = 0
    applied: int = 0
    failed: int = 0


def queue_tracking_update(queue_dir: Path, *, operation: str, payload: Dict[str, Any]) -> Path:
    queue_dir.mkdir(parents=True, exist_ok=True)
    path = queue_dir / f"{_now_stamp()}-{uuid.uuid4().hex[:10]}.json"
    blob = {
        "version": 1,
        "queued_at": datetime.utcnow().isoformat() + "Z",
        "operation": str(operation or "").strip(),
        "payload": payload or {},
    }
    path.write_text(json.dumps(blob, ensure_ascii=True), encoding="utf-8")
    return path


def apply_tracking_queue(
    queue_dir: Path,
    *,
    processed_dir: Optional[Path] = None,
) -> QueueApplySummary:
    summary = QueueApplySummary()
    if not queue_dir.exists():
        return summary
    files = sorted([p for p in queue_dir.iterdir() if p.is_file() and p.suffix.lower() == ".json"])
    summary.queued = len(files)
    if not files:
        return summary

    processed = processed_dir or (queue_dir.parent / "processed")
    processed.mkdir(parents=True, exist_ok=True)

    for fpath in files:
        try:
            blob = json.loads(fpath.read_text(encoding="utf-8"))
            op = str(blob.get("operation") or "").strip()
            payload = blob.get("payload") or {}
            if not isinstance(payload, dict):
                raise ValueError("queue payload must be an object")
            if op == "upsert_run_status":
                upsert_run_status(**payload)
            elif op == "upsert_step_attempt":
                upsert_step_attempt(**payload)
            else:
                raise ValueError(f"unsupported queue operation: {op}")
            shutil.move(str(fpath), str(processed / fpath.name))
            summary.applied += 1
        except Exception:
            summary.failed += 1
    return summary


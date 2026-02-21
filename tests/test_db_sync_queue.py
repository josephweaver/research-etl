# research-etl
# Copyright (c) 2026 Joseph Weaver
# This file is part of the research-etl project and is licensed under the MIT License.
# You may not use this file except in compliance with the License.
# See https://github.com/josephweaver/research-etl for details.

from __future__ import annotations

from pathlib import Path

import etl.db_sync_queue as qmod


def test_queue_and_apply_run_status(monkeypatch, tmp_path: Path) -> None:
    queue_dir = tmp_path / "pending"
    processed_dir = tmp_path / "processed"
    seen = []
    monkeypatch.setattr(qmod, "upsert_run_status", lambda **kw: seen.append(kw))

    qmod.queue_tracking_update(
        queue_dir,
        operation="upsert_run_status",
        payload={"run_id": "r1", "pipeline": "p.yml", "status": "running", "success": False},
    )
    summary = qmod.apply_tracking_queue(queue_dir, processed_dir=processed_dir)

    assert summary.queued == 1
    assert summary.applied == 1
    assert summary.failed == 0
    assert len(seen) == 1
    assert seen[0]["run_id"] == "r1"
    assert list(processed_dir.glob("*.json"))


def test_apply_queue_keeps_failed_items(monkeypatch, tmp_path: Path) -> None:
    queue_dir = tmp_path / "pending"
    processed_dir = tmp_path / "processed"
    monkeypatch.setattr(qmod, "upsert_step_attempt", lambda **_kw: (_ for _ in ()).throw(RuntimeError("db down")))

    qmod.queue_tracking_update(
        queue_dir,
        operation="upsert_step_attempt",
        payload={"run_id": "r1", "step_name": "s1", "attempt_no": 1, "script": "echo.py", "success": True},
    )
    summary = qmod.apply_tracking_queue(queue_dir, processed_dir=processed_dir)

    assert summary.queued == 1
    assert summary.applied == 0
    assert summary.failed == 1
    assert list(queue_dir.glob("*.json"))
    assert not list(processed_dir.glob("*.json"))


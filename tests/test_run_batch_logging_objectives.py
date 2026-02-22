# research-etl
# Copyright (c) 2026 Joseph Weaver
# This file is part of the research-etl project and is licensed under the MIT License.
# You may not use this file except in compliance with the License.
# See https://github.com/josephweaver/research-etl for details.

from __future__ import annotations

from pathlib import Path

import etl.run_batch as run_batch
from etl.pipeline import Step
from etl.runner import RunResult, StepResult


def _write_min_pipeline(path: Path) -> None:
    path.write_text(
        "\n".join(
            [
                "steps:",
                "  - name: s0",
                "    script: echo.py",
            ]
        ),
        encoding="utf-8",
    )


def test_run_batch_writes_durable_live_log(monkeypatch, tmp_path: Path) -> None:
    pipeline_path = tmp_path / "pipeline.yml"
    _write_min_pipeline(pipeline_path)
    workdir = tmp_path / ".runs"
    run_id = "runlog1"

    monkeypatch.setattr(run_batch, "upsert_run_status", lambda **_: None)
    monkeypatch.setattr(run_batch, "upsert_step_attempt", lambda **_: None)
    monkeypatch.setattr(run_batch, "collect_run_provenance", lambda **_: {})
    monkeypatch.setattr(
        run_batch,
        "run_pipeline",
        lambda *args, **kwargs: RunResult(
            run_id=run_id,
            steps=[
                StepResult(
                    step=Step(name="s0", script="echo.py"),
                    success=True,
                    attempt_no=1,
                    attempts=[{"attempt_no": 1, "success": True, "skipped": False, "error": None, "outputs": {}}],
                )
            ],
            artifact_dir=str(workdir),
        ),
    )

    rc = run_batch.main(
        [
            str(pipeline_path),
            "--steps",
            "0",
            "--run-id",
            run_id,
            "--workdir",
            str(workdir),
        ]
    )
    assert rc == 0
    live_log = workdir / "_live" / f"{run_id}.log"
    assert live_log.exists()
    text = live_log.read_text(encoding="utf-8")
    assert "run_batch start run_id=runlog1" in text
    assert "Batch execution finished success=True" in text


def test_run_batch_top_level_exception_trap_logs_and_returns_1(monkeypatch, tmp_path: Path) -> None:
    pipeline_path = tmp_path / "pipeline.yml"
    _write_min_pipeline(pipeline_path)
    workdir = tmp_path / ".runs"
    run_id = "runlog2"

    monkeypatch.setattr(run_batch, "upsert_run_status", lambda **_: None)
    monkeypatch.setattr(run_batch, "upsert_step_attempt", lambda **_: None)
    monkeypatch.setattr(run_batch, "collect_run_provenance", lambda **_: {})
    monkeypatch.setattr(run_batch, "parse_pipeline", lambda *args, **kwargs: (_ for _ in ()).throw(RuntimeError("boom")))

    rc = run_batch.main(
        [
            str(pipeline_path),
            "--steps",
            "0",
            "--run-id",
            run_id,
            "--workdir",
            str(workdir),
        ]
    )
    assert rc == 1
    live_log = workdir / "_live" / f"{run_id}.log"
    assert live_log.exists()
    text = live_log.read_text(encoding="utf-8")
    assert "Unhandled run_batch error: boom" in text

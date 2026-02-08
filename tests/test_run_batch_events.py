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
                "  - name: s1",
                "    script: echo.py",
                "  - name: s2",
                "    script: echo.py",
            ]
        ),
        encoding="utf-8",
    )


def test_run_batch_emits_completed_and_run_completed_events(monkeypatch, tmp_path: Path) -> None:
    pipeline_path = tmp_path / "pipeline.yml"
    _write_min_pipeline(pipeline_path)

    status_calls = []
    attempt_calls = []
    provenance = {"git_commit_sha": "abc123", "pipeline_checksum": "pchk"}
    monkeypatch.setattr(run_batch, "upsert_run_status", lambda **kw: status_calls.append(kw))
    monkeypatch.setattr(run_batch, "upsert_step_attempt", lambda **kw: attempt_calls.append(kw))
    monkeypatch.setattr(run_batch, "collect_run_provenance", lambda **kw: provenance)

    result = RunResult(
        run_id="run123",
        steps=[
            StepResult(
                step=Step(name="s1", script="echo.py"),
                success=True,
                attempt_no=2,
                attempts=[
                    {"attempt_no": 1, "success": False, "skipped": False, "error": "boom", "outputs": {}},
                    {"attempt_no": 2, "success": True, "skipped": False, "error": None, "outputs": {"ok": 1}},
                ],
            ),
            StepResult(
                step=Step(name="s2", script="echo.py"),
                success=True,
                attempt_no=1,
                attempts=[
                    {"attempt_no": 1, "success": True, "skipped": False, "error": None, "outputs": {"ok": 2}},
                ],
            ),
        ],
        artifact_dir=str(tmp_path / ".runs"),
    )
    monkeypatch.setattr(run_batch, "run_pipeline", lambda *args, **kwargs: result)

    rc = run_batch.main(
        [
            str(pipeline_path),
            "--steps",
            "1,2",
            "--run-id",
            "run123",
            "--workdir",
            str(tmp_path / ".runs"),
        ]
    )
    assert rc == 0

    event_types = [c["event_type"] for c in status_calls]
    assert event_types == ["batch_started", "batch_completed", "run_completed"]
    assert all(c.get("provenance") == provenance for c in status_calls)
    assert len(attempt_calls) == 3

    completed_payload = status_calls[1]["event_details"]
    assert completed_payload["step_indices"] == [1, 2]
    assert completed_payload["step_attempts"] == [
        {
            "step_name": "s1",
            "attempts": 2,
            "success": True,
            "skipped": False,
            "final_error": None,
        },
        {
            "step_name": "s2",
            "attempts": 1,
            "success": True,
            "skipped": False,
            "final_error": None,
        },
    ]


def test_run_batch_emits_failed_event_with_attempt_summary(monkeypatch, tmp_path: Path) -> None:
    pipeline_path = tmp_path / "pipeline.yml"
    _write_min_pipeline(pipeline_path)

    status_calls = []
    attempt_calls = []
    provenance = {"git_commit_sha": "abc123", "pipeline_checksum": "pchk"}
    monkeypatch.setattr(run_batch, "upsert_run_status", lambda **kw: status_calls.append(kw))
    monkeypatch.setattr(run_batch, "upsert_step_attempt", lambda **kw: attempt_calls.append(kw))
    monkeypatch.setattr(run_batch, "collect_run_provenance", lambda **kw: provenance)

    result = RunResult(
        run_id="run999",
        steps=[
            StepResult(
                step=Step(name="s1", script="echo.py"),
                success=False,
                error="boom-2",
                attempt_no=2,
                attempts=[
                    {"attempt_no": 1, "success": False, "skipped": False, "error": "boom-1", "outputs": {}},
                    {"attempt_no": 2, "success": False, "skipped": False, "error": "boom-2", "outputs": {}},
                ],
            ),
        ],
        artifact_dir=str(tmp_path / ".runs"),
    )
    monkeypatch.setattr(run_batch, "run_pipeline", lambda *args, **kwargs: result)

    rc = run_batch.main(
        [
            str(pipeline_path),
            "--steps",
            "1",
            "--run-id",
            "run999",
            "--workdir",
            str(tmp_path / ".runs"),
        ]
    )
    assert rc == 1

    event_types = [c["event_type"] for c in status_calls]
    assert event_types == ["batch_started", "batch_failed"]
    assert all(c.get("provenance") == provenance for c in status_calls)
    assert len(attempt_calls) == 2

    failed_payload = status_calls[1]["event_details"]
    assert failed_payload["step_indices"] == [1]
    assert failed_payload["step_attempts"] == [
        {
            "step_name": "s1",
            "attempts": 2,
            "success": False,
            "skipped": False,
            "final_error": "boom-2",
        }
    ]

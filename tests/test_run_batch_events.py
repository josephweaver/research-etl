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
            "step_id": None,
            "attempts": 2,
            "success": True,
            "skipped": False,
            "final_error": None,
        },
        {
            "step_name": "s2",
            "step_id": None,
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
            "step_id": None,
            "attempts": 2,
            "success": False,
            "skipped": False,
            "final_error": "boom-2",
        }
    ]


def test_run_batch_prints_failed_step_error_summary(monkeypatch, tmp_path: Path, capsys) -> None:
    pipeline_path = tmp_path / "pipeline.yml"
    _write_min_pipeline(pipeline_path)

    monkeypatch.setattr(run_batch, "upsert_run_status", lambda **_: None)
    monkeypatch.setattr(run_batch, "upsert_step_attempt", lambda **_: None)
    monkeypatch.setattr(run_batch, "collect_run_provenance", lambda **_: {})

    result = RunResult(
        run_id="runerr",
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
            "runerr",
            "--workdir",
            str(tmp_path / ".runs"),
        ]
    )
    assert rc == 1
    out = capsys.readouterr().out
    assert "[run_batch][ERROR] step=s1 attempts=2 final_error=boom-2" in out


def test_run_batch_verbose_prints_progress(monkeypatch, tmp_path: Path, capsys) -> None:
    pipeline_path = tmp_path / "pipeline.yml"
    _write_min_pipeline(pipeline_path)

    monkeypatch.setattr(run_batch, "upsert_run_status", lambda **_: None)
    monkeypatch.setattr(run_batch, "upsert_step_attempt", lambda **_: None)
    monkeypatch.setattr(run_batch, "collect_run_provenance", lambda **_: {})

    result = RunResult(
        run_id="runv1",
        steps=[
            StepResult(
                step=Step(name="s0", script="echo.py"),
                success=True,
                attempt_no=1,
                attempts=[{"attempt_no": 1, "success": True, "skipped": False, "error": None, "outputs": {}}],
            )
        ],
        artifact_dir=str(tmp_path / ".runs"),
    )
    monkeypatch.setattr(run_batch, "run_pipeline", lambda *args, **kwargs: result)

    rc = run_batch.main(
        [
            str(pipeline_path),
            "--steps",
            "0",
            "--run-id",
            "runv1",
            "--workdir",
            str(tmp_path / ".runs"),
            "--verbose",
        ]
    )
    assert rc == 0
    out = capsys.readouterr().out
    assert "[run_batch] starting run_id=runv1" in out
    assert "[run_batch] parsed pipeline with 3 step(s)" in out
    assert "[run_batch] recorded batch_completed tracking event" in out


def test_run_batch_passes_run_started_at_to_runner(monkeypatch, tmp_path: Path) -> None:
    pipeline_path = tmp_path / "pipeline.yml"
    _write_min_pipeline(pipeline_path)

    monkeypatch.setattr(run_batch, "upsert_run_status", lambda **_: None)
    monkeypatch.setattr(run_batch, "upsert_step_attempt", lambda **_: None)
    monkeypatch.setattr(run_batch, "collect_run_provenance", lambda **_: {})

    captured = {}

    def _fake_run_pipeline(*args, **kwargs):
        captured["run_started"] = kwargs.get("run_started")
        return RunResult(
            run_id="runfixed",
            steps=[
                StepResult(
                    step=Step(name="s0", script="echo.py"),
                    success=True,
                    attempt_no=1,
                    attempts=[{"attempt_no": 1, "success": True, "skipped": False, "error": None, "outputs": {}}],
                )
            ],
            artifact_dir=str(tmp_path / ".runs"),
        )

    monkeypatch.setattr(run_batch, "run_pipeline", _fake_run_pipeline)

    rc = run_batch.main(
        [
            str(pipeline_path),
            "--steps",
            "0",
            "--run-id",
            "runfixed",
            "--run-started-at",
            "2026-02-15T23:22:42Z",
            "--workdir",
            str(tmp_path / ".runs"),
        ]
    )
    assert rc == 0
    assert captured["run_started"] is not None
    assert captured["run_started"].strftime("%H%M%S") == "232242"


def test_run_batch_warns_and_ignores_unknown_args(monkeypatch, tmp_path: Path, capsys) -> None:
    pipeline_path = tmp_path / "pipeline.yml"
    _write_min_pipeline(pipeline_path)

    monkeypatch.setattr(run_batch, "upsert_run_status", lambda **_: None)
    monkeypatch.setattr(run_batch, "upsert_step_attempt", lambda **_: None)
    monkeypatch.setattr(run_batch, "collect_run_provenance", lambda **_: {})

    result = RunResult(
        run_id="rununk",
        steps=[
            StepResult(
                step=Step(name="s0", script="echo.py"),
                success=True,
                attempt_no=1,
                attempts=[{"attempt_no": 1, "success": True, "skipped": False, "error": None, "outputs": {}}],
            )
        ],
        artifact_dir=str(tmp_path / ".runs"),
    )
    monkeypatch.setattr(run_batch, "run_pipeline", lambda *args, **kwargs: result)

    rc = run_batch.main(
        [
            str(pipeline_path),
            "--steps",
            "0",
            "--run-id",
            "rununk",
            "--workdir",
            str(tmp_path / ".runs"),
            "--future-flag",
            "x",
        ]
    )
    assert rc == 0
    out = capsys.readouterr().out
    assert "[run_batch][WARN] ignoring unknown arguments:" in out
    assert "--future-flag x" in out


def test_run_batch_merges_context_into_runtime_vars(monkeypatch, tmp_path: Path) -> None:
    pipeline_path = tmp_path / "pipeline.yml"
    _write_min_pipeline(pipeline_path)
    context_path = tmp_path / "context.json"
    context_path.write_text(
        '{"states_interest":{"output_dir":"/tmp/meta"},"project_id":"land_core"}',
        encoding="utf-8",
    )

    monkeypatch.setattr(run_batch, "upsert_run_status", lambda **_: None)
    monkeypatch.setattr(run_batch, "upsert_step_attempt", lambda **_: None)
    monkeypatch.setattr(run_batch, "collect_run_provenance", lambda **_: {})

    captured = {}

    def _fake_run_pipeline(pipeline, *args, **kwargs):
        captured["vars"] = dict(getattr(pipeline, "vars", {}) or {})
        return RunResult(
            run_id="runctx",
            steps=[
                StepResult(
                    step=Step(name="s1", script="echo.py"),
                    success=True,
                    attempt_no=1,
                    attempts=[{"attempt_no": 1, "success": True, "skipped": False, "error": None, "outputs": {}}],
                )
            ],
            artifact_dir=str(tmp_path / ".runs"),
        )

    monkeypatch.setattr(run_batch, "run_pipeline", _fake_run_pipeline)

    rc = run_batch.main(
        [
            str(pipeline_path),
            "--steps",
            "1",
            "--run-id",
            "runctx",
            "--workdir",
            str(tmp_path / ".runs"),
            "--context-file",
            str(context_path),
        ]
    )
    assert rc == 0
    assert captured["vars"]["states_interest"]["output_dir"] == "/tmp/meta"

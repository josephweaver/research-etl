from __future__ import annotations

import os
import uuid
from datetime import datetime
from pathlib import Path

import psycopg
import pytest

from etl.db import ensure_database_schema
from etl.executors.local import LocalExecutor
from etl.executors.slurm import SlurmExecutor
from etl.run_batch import main as run_batch_main
from etl.tracking import upsert_run_status, upsert_step_attempt


pytestmark = pytest.mark.integration


def _db_url() -> str:
    url = os.environ.get("ETL_DATABASE_URL", "").strip()
    if not url:
        pytest.skip("ETL_DATABASE_URL is not set; skipping integration test")
    return url


def test_integration_migrations_apply_and_track_versions() -> None:
    url = _db_url()
    applied = ensure_database_schema()
    # First run may apply scripts, later runs should be idempotent.
    assert isinstance(applied, list)

    with psycopg.connect(url) as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT COUNT(*) FROM etl_schema_versions")
            num_versions = cur.fetchone()[0]
            assert num_versions >= 1
            cur.execute("SELECT latest_version FROM etl_schema_state WHERE singleton_id = 1")
            latest = cur.fetchone()
            assert latest is not None
            assert latest[0].endswith(".sql")


def test_integration_tracking_writes_run_and_attempt_rows() -> None:
    url = _db_url()
    ensure_database_schema()

    run_id = f"it_{uuid.uuid4().hex}"
    now = datetime.utcnow().isoformat() + "Z"
    upsert_run_status(
        run_id=run_id,
        pipeline="pipelines/sample.yml",
        status="running",
        success=False,
        started_at=now,
        ended_at=now,
        message="integration running",
        executor="local",
        artifact_dir=".runs/integration",
        event_type="integration_started",
        event_details={"kind": "integration"},
    )
    upsert_step_attempt(
        run_id=run_id,
        step_name="echo",
        attempt_no=1,
        script="echo.py",
        success=True,
        outputs={"message": "ok"},
        started_at=now,
        ended_at=now,
    )
    upsert_run_status(
        run_id=run_id,
        pipeline="pipelines/sample.yml",
        status="succeeded",
        success=True,
        started_at=now,
        ended_at=now,
        message="integration done",
        executor="local",
        artifact_dir=".runs/integration",
        event_type="integration_completed",
        event_details={"kind": "integration"},
    )

    with psycopg.connect(url) as conn:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT status, success FROM etl_runs WHERE run_id = %s",
                (run_id,),
            )
            row = cur.fetchone()
            assert row == ("succeeded", True)

            cur.execute(
                "SELECT success FROM etl_run_steps WHERE run_id = %s AND step_name = %s",
                (run_id, "echo"),
            )
            step_row = cur.fetchone()
            assert step_row == (True,)

            cur.execute(
                """
                SELECT success
                FROM etl_run_step_attempts
                WHERE run_id = %s AND step_name = %s AND attempt_no = 1
                """,
                (run_id, "echo"),
            )
            att_row = cur.fetchone()
            assert att_row == (True,)

            cur.execute(
                """
                SELECT COUNT(*)
                FROM etl_run_events
                WHERE run_id = %s AND event_type IN ('integration_started', 'integration_completed')
                """,
                (run_id,),
            )
            ev_count = cur.fetchone()[0]
            assert ev_count == 2


def test_integration_slurm_event_transitions_success(tmp_path: Path) -> None:
    url = _db_url()
    ensure_database_schema()

    plugins_dir = tmp_path / "plugins"
    plugins_dir.mkdir(parents=True, exist_ok=True)
    (plugins_dir / "echo.py").write_text(
        "\n".join(
            [
                "meta = {'name': 'echo', 'version': '0.1.0', 'description': 'integration'}",
                "def run(args, ctx):",
                "    return {'ok': True}",
            ]
        ),
        encoding="utf-8",
    )
    pipeline_path = tmp_path / "pipeline_success.yml"
    pipeline_path.write_text(
        "\n".join(
            [
                "steps:",
                "  - name: s1",
                "    script: echo.py",
            ]
        ),
        encoding="utf-8",
    )

    run_id = f"it_slurm_ok_{uuid.uuid4().hex[:12]}"
    ex = SlurmExecutor(
        env_config={"workdir": str(tmp_path / ".runs"), "logdir": str(tmp_path / "logs")},
        repo_root=Path(".").resolve(),
        plugins_dir=plugins_dir,
        workdir=tmp_path / ".runs",
        dry_run=True,
    )
    submit = ex.submit(str(pipeline_path), context={"run_id": run_id, "provenance": {"git_commit_sha": "it"}})
    assert submit.run_id == run_id

    rc = run_batch_main(
        [
            str(pipeline_path),
            "--steps",
            "0",
            "--plugins-dir",
            str(plugins_dir),
            "--workdir",
            str(tmp_path / ".runs"),
            "--run-id",
            run_id,
        ]
    )
    assert rc == 0

    with psycopg.connect(url) as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT status, success FROM etl_runs WHERE run_id = %s", (run_id,))
            row = cur.fetchone()
            assert row == ("succeeded", True)

            cur.execute(
                """
                SELECT event_type
                FROM etl_run_events
                WHERE run_id = %s
                ORDER BY event_id
                """,
                (run_id,),
            )
            events = [r[0] for r in cur.fetchall()]
            assert events == ["run_queued", "batch_started", "batch_completed", "run_completed"]


def test_integration_slurm_event_transitions_failed(tmp_path: Path) -> None:
    url = _db_url()
    ensure_database_schema()

    plugins_dir = tmp_path / "plugins"
    plugins_dir.mkdir(parents=True, exist_ok=True)
    (plugins_dir / "boom.py").write_text(
        "\n".join(
            [
                "meta = {'name': 'boom', 'version': '0.1.0', 'description': 'integration'}",
                "def run(args, ctx):",
                "    raise RuntimeError('boom')",
            ]
        ),
        encoding="utf-8",
    )
    pipeline_path = tmp_path / "pipeline_fail.yml"
    pipeline_path.write_text(
        "\n".join(
            [
                "steps:",
                "  - name: s1",
                "    script: boom.py",
            ]
        ),
        encoding="utf-8",
    )

    run_id = f"it_slurm_fail_{uuid.uuid4().hex[:10]}"
    ex = SlurmExecutor(
        env_config={"workdir": str(tmp_path / ".runs"), "logdir": str(tmp_path / "logs")},
        repo_root=Path(".").resolve(),
        plugins_dir=plugins_dir,
        workdir=tmp_path / ".runs",
        dry_run=True,
    )
    submit = ex.submit(str(pipeline_path), context={"run_id": run_id, "provenance": {"git_commit_sha": "it"}})
    assert submit.run_id == run_id

    rc = run_batch_main(
        [
            str(pipeline_path),
            "--steps",
            "0",
            "--plugins-dir",
            str(plugins_dir),
            "--workdir",
            str(tmp_path / ".runs"),
            "--run-id",
            run_id,
        ]
    )
    assert rc == 1

    with psycopg.connect(url) as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT status, success FROM etl_runs WHERE run_id = %s", (run_id,))
            row = cur.fetchone()
            assert row == ("failed", False)

            cur.execute(
                """
                SELECT event_type
                FROM etl_run_events
                WHERE run_id = %s
                ORDER BY event_id
                """,
                (run_id,),
            )
            events = [r[0] for r in cur.fetchall()]
            assert events == ["run_queued", "batch_started", "batch_failed"]


def test_integration_resume_from_partial_success_local(tmp_path: Path) -> None:
    url = _db_url()
    ensure_database_schema()

    plugins_dir = tmp_path / "plugins"
    plugins_dir.mkdir(parents=True, exist_ok=True)
    (plugins_dir / "ok.py").write_text(
        "\n".join(
            [
                "meta = {'name': 'ok', 'version': '0.1.0', 'description': 'integration'}",
                "def run(args, ctx):",
                "    return {'ok': True}",
            ]
        ),
        encoding="utf-8",
    )
    (plugins_dir / "fail_once.py").write_text(
        "\n".join(
            [
                "from pathlib import Path",
                "meta = {'name': 'fail_once', 'version': '0.1.0', 'description': 'integration'}",
                "def run(args, ctx):",
                "    marker = Path(__file__).with_name('.resume_fail_once_marker')",
                "    if not marker.exists():",
                "        marker.write_text('1', encoding='utf-8')",
                "        raise RuntimeError('fail-once')",
                "    return {'ok': True}",
            ]
        ),
        encoding="utf-8",
    )
    pipeline_path = tmp_path / "pipeline_resume.yml"
    pipeline_path.write_text(
        "\n".join(
            [
                "steps:",
                "  - name: s1",
                "    script: ok.py",
                "  - name: s2",
                "    script: fail_once.py",
            ]
        ),
        encoding="utf-8",
    )

    ex = LocalExecutor(plugin_dir=plugins_dir, workdir=tmp_path / ".runs")
    first = ex.submit(
        str(pipeline_path),
        context={"provenance": {"git_commit_sha": "it"}},
    )
    first_status = ex.status(first.run_id)
    assert str(first_status.state) == "RunState.FAILED"

    second = ex.submit(
        str(pipeline_path),
        context={"resume_run_id": first.run_id, "provenance": {"git_commit_sha": "it"}},
    )
    second_status = ex.status(second.run_id)
    assert str(second_status.state) == "RunState.SUCCEEDED"

    with psycopg.connect(url) as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT step_name, success, skipped
                FROM etl_run_steps
                WHERE run_id = %s
                ORDER BY step_name
                """,
                (second.run_id,),
            )
            rows = cur.fetchall()
            assert rows == [("s1", True, True), ("s2", True, False)]

            cur.execute(
                """
                SELECT COUNT(*)
                FROM etl_run_step_attempts
                WHERE run_id = %s AND step_name = 's1'
                """,
                (second.run_id,),
            )
            s1_attempts = cur.fetchone()[0]
            assert s1_attempts == 0

            cur.execute(
                """
                SELECT COUNT(*)
                FROM etl_run_step_attempts
                WHERE run_id = %s AND step_name = 's2'
                """,
                (second.run_id,),
            )
            s2_attempts = cur.fetchone()[0]
            assert s2_attempts == 1

from __future__ import annotations

import os
import uuid
from datetime import datetime

import psycopg
import pytest

from etl.db import ensure_database_schema
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

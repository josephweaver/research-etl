"""
Run tracking persistence (JSONL-based).
"""

from __future__ import annotations

import json
from dataclasses import asdict, dataclass, field
from datetime import datetime
from pathlib import Path
import shlex
import statistics
from typing import Any, Dict, List, Optional

import psycopg

from .artifacts import register_run_artifacts, register_step_artifacts
from .db import get_database_url
from .runner import RunResult, StepResult


def _now_iso() -> str:
    return datetime.utcnow().isoformat() + "Z"


@dataclass
class RunRecord:
    run_id: str
    pipeline: str
    success: bool
    status: str
    started_at: str
    ended_at: str
    project_id: Optional[str] = None
    message: str = ""
    steps: List[Dict[str, Any]] = field(default_factory=list)


@dataclass
class RunStepState:
    step_name: str
    success: bool
    skipped: bool
    outputs: Dict[str, Any] = field(default_factory=dict)


def _step_to_dict(step_result: StepResult) -> Dict[str, Any]:
    return {
        "name": step_result.step.name,
        "script": step_result.step.script,
        "success": step_result.success,
        "skipped": step_result.skipped,
        "error": step_result.error,
        "outputs": step_result.outputs,
        "attempt_no": step_result.attempt_no,
        "attempts": step_result.attempts,
    }


def _upsert_run_db(
    rec: RunRecord,
    *,
    executor: Optional[str] = None,
    artifact_dir: Optional[str] = None,
    provenance: Optional[Dict[str, Any]] = None,
) -> None:
    db_url = get_database_url()
    if not db_url:
        return

    provenance = provenance or {}
    git_commit_sha = provenance.get("git_commit_sha")
    git_branch = provenance.get("git_branch")
    git_tag = provenance.get("git_tag")
    git_is_dirty = provenance.get("git_is_dirty")
    cli_command = provenance.get("cli_command")
    pipeline_checksum = provenance.get("pipeline_checksum")
    global_config_checksum = provenance.get("global_config_checksum")
    execution_config_checksum = provenance.get("execution_config_checksum")
    plugin_checksums_json = json.dumps(provenance.get("plugin_checksums_json")) if provenance.get("plugin_checksums_json") is not None else None

    with psycopg.connect(db_url) as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO etl_runs (
                    run_id, pipeline, project_id, success, status, started_at, ended_at, message, executor, artifact_dir,
                    git_commit_sha, git_branch, git_tag, git_is_dirty, cli_command, pipeline_checksum,
                    global_config_checksum, execution_config_checksum, plugin_checksums_json
                )
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s::jsonb)
                ON CONFLICT (run_id)
                DO UPDATE SET
                    pipeline = EXCLUDED.pipeline,
                    project_id = COALESCE(EXCLUDED.project_id, etl_runs.project_id),
                    success = EXCLUDED.success,
                    status = EXCLUDED.status,
                    started_at = LEAST(etl_runs.started_at, EXCLUDED.started_at),
                    ended_at = GREATEST(etl_runs.ended_at, EXCLUDED.ended_at),
                    message = EXCLUDED.message,
                    executor = EXCLUDED.executor,
                    artifact_dir = EXCLUDED.artifact_dir,
                    git_commit_sha = COALESCE(EXCLUDED.git_commit_sha, etl_runs.git_commit_sha),
                    git_branch = COALESCE(EXCLUDED.git_branch, etl_runs.git_branch),
                    git_tag = COALESCE(EXCLUDED.git_tag, etl_runs.git_tag),
                    git_is_dirty = COALESCE(EXCLUDED.git_is_dirty, etl_runs.git_is_dirty),
                    cli_command = COALESCE(EXCLUDED.cli_command, etl_runs.cli_command),
                    pipeline_checksum = COALESCE(EXCLUDED.pipeline_checksum, etl_runs.pipeline_checksum),
                    global_config_checksum = COALESCE(EXCLUDED.global_config_checksum, etl_runs.global_config_checksum),
                    execution_config_checksum = COALESCE(EXCLUDED.execution_config_checksum, etl_runs.execution_config_checksum),
                    plugin_checksums_json = COALESCE(EXCLUDED.plugin_checksums_json, etl_runs.plugin_checksums_json)
                """,
                (
                    rec.run_id,
                    rec.pipeline,
                    rec.project_id,
                    rec.success,
                    rec.status,
                    rec.started_at,
                    rec.ended_at,
                    rec.message,
                    executor,
                    artifact_dir,
                    git_commit_sha,
                    git_branch,
                    git_tag,
                    git_is_dirty,
                    cli_command,
                    pipeline_checksum,
                    global_config_checksum,
                    execution_config_checksum,
                    plugin_checksums_json,
                ),
            )

            for step in rec.steps:
                outputs_json = json.dumps(step.get("outputs"), default=str)
                step_name = step.get("name")
                step_script = step.get("script")
                step_success = step.get("success", False)
                step_skipped = step.get("skipped", False)
                step_error = step.get("error")
                cur.execute(
                    """
                    INSERT INTO etl_run_steps (
                        run_id, step_name, script, success, skipped, error, outputs_json
                    )
                    VALUES (%s, %s, %s, %s, %s, %s, %s::jsonb)
                    ON CONFLICT (run_id, step_name)
                    DO UPDATE SET
                        script = EXCLUDED.script,
                        success = EXCLUDED.success,
                        skipped = EXCLUDED.skipped,
                        error = EXCLUDED.error,
                        outputs_json = EXCLUDED.outputs_json
                    """,
                    (
                        rec.run_id,
                        step_name,
                        step_script,
                        step_success,
                        step_skipped,
                        step_error,
                        outputs_json,
                    ),
                )
                attempts = step.get("attempts") or []
                if attempts:
                    for att in attempts:
                        att_outputs_json = json.dumps(att.get("outputs") or {}, default=str)
                        cur.execute(
                            """
                            INSERT INTO etl_run_step_attempts (
                                run_id, step_name, attempt_no, script, success, skipped, error, outputs_json, started_at, ended_at,
                                plugin_name, plugin_version, failure_category, runtime_seconds, memory_gb, cpu_cores, updated_at
                            )
                            VALUES (%s, %s, %s, %s, %s, %s, %s, %s::jsonb, %s, %s, %s, %s, %s, %s, %s, %s, NOW())
                            ON CONFLICT (run_id, step_name, attempt_no)
                            DO UPDATE SET
                                script = EXCLUDED.script,
                                success = EXCLUDED.success,
                                skipped = EXCLUDED.skipped,
                                error = EXCLUDED.error,
                                outputs_json = EXCLUDED.outputs_json,
                                started_at = EXCLUDED.started_at,
                                ended_at = EXCLUDED.ended_at,
                                plugin_name = COALESCE(EXCLUDED.plugin_name, etl_run_step_attempts.plugin_name),
                                plugin_version = COALESCE(EXCLUDED.plugin_version, etl_run_step_attempts.plugin_version),
                                failure_category = COALESCE(EXCLUDED.failure_category, etl_run_step_attempts.failure_category),
                                runtime_seconds = COALESCE(EXCLUDED.runtime_seconds, etl_run_step_attempts.runtime_seconds),
                                memory_gb = COALESCE(EXCLUDED.memory_gb, etl_run_step_attempts.memory_gb),
                                cpu_cores = COALESCE(EXCLUDED.cpu_cores, etl_run_step_attempts.cpu_cores),
                                updated_at = NOW()
                            """,
                            (
                                rec.run_id,
                                step_name,
                                int(att.get("attempt_no", step.get("attempt_no", 1))),
                                step_script,
                                bool(att.get("success", step_success)),
                                bool(att.get("skipped", step_skipped)),
                                att.get("error", step_error),
                                att_outputs_json,
                                att.get("started_at", rec.started_at),
                                att.get("ended_at", rec.ended_at),
                                att.get("plugin_name"),
                                att.get("plugin_version"),
                                att.get("failure_category"),
                                att.get("runtime_seconds"),
                                att.get("memory_gb"),
                                att.get("cpu_cores"),
                            ),
                        )
                elif int(step.get("attempt_no", 0) or 0) > 0:
                    # Backward-compatible fallback when attempt history is not present.
                    cur.execute(
                        """
                        INSERT INTO etl_run_step_attempts (
                            run_id, step_name, attempt_no, script, success, skipped, error, outputs_json, started_at, ended_at,
                            plugin_name, plugin_version, failure_category, runtime_seconds, memory_gb, cpu_cores, updated_at
                        )
                        VALUES (%s, %s, %s, %s, %s, %s, %s, %s::jsonb, %s, %s, %s, %s, %s, %s, %s, %s, NOW())
                        ON CONFLICT (run_id, step_name, attempt_no)
                        DO UPDATE SET
                            script = EXCLUDED.script,
                            success = EXCLUDED.success,
                            skipped = EXCLUDED.skipped,
                            error = EXCLUDED.error,
                            outputs_json = EXCLUDED.outputs_json,
                            started_at = EXCLUDED.started_at,
                            ended_at = EXCLUDED.ended_at,
                            plugin_name = COALESCE(EXCLUDED.plugin_name, etl_run_step_attempts.plugin_name),
                            plugin_version = COALESCE(EXCLUDED.plugin_version, etl_run_step_attempts.plugin_version),
                            failure_category = COALESCE(EXCLUDED.failure_category, etl_run_step_attempts.failure_category),
                            runtime_seconds = COALESCE(EXCLUDED.runtime_seconds, etl_run_step_attempts.runtime_seconds),
                            memory_gb = COALESCE(EXCLUDED.memory_gb, etl_run_step_attempts.memory_gb),
                            cpu_cores = COALESCE(EXCLUDED.cpu_cores, etl_run_step_attempts.cpu_cores),
                            updated_at = NOW()
                        """,
                        (
                            rec.run_id,
                            step_name,
                            int(step.get("attempt_no", 1)),
                            step_script,
                            step_success,
                            step_skipped,
                            step_error,
                            outputs_json,
                            rec.started_at,
                            rec.ended_at,
                            step.get("plugin_name"),
                            step.get("plugin_version"),
                            step.get("failure_category"),
                            step.get("runtime_seconds"),
                            step.get("memory_gb"),
                            step.get("cpu_cores"),
                        ),
                    )

            event_details = json.dumps(
                {
                    "status": rec.status,
                    "success": rec.success,
                    "step_count": len(rec.steps),
                }
            )
            cur.execute(
                """
                INSERT INTO etl_run_events (run_id, event_type, details_json)
                VALUES (%s, %s, %s::jsonb)
                """,
                (rec.run_id, "run_completed", event_details),
            )
        conn.commit()


def upsert_run_status(
    *,
    run_id: str,
    pipeline: str,
    status: str,
    success: bool,
    started_at: Optional[str] = None,
    ended_at: Optional[str] = None,
    message: str = "",
    executor: Optional[str] = None,
    artifact_dir: Optional[str] = None,
    provenance: Optional[Dict[str, Any]] = None,
    event_type: Optional[str] = None,
    event_details: Optional[Dict[str, Any]] = None,
    project_id: Optional[str] = None,
) -> None:
    db_url = get_database_url()
    if not db_url:
        return

    provenance = provenance or {}
    git_commit_sha = provenance.get("git_commit_sha")
    git_branch = provenance.get("git_branch")
    git_tag = provenance.get("git_tag")
    git_is_dirty = provenance.get("git_is_dirty")
    cli_command = provenance.get("cli_command")
    pipeline_checksum = provenance.get("pipeline_checksum")
    global_config_checksum = provenance.get("global_config_checksum")
    execution_config_checksum = provenance.get("execution_config_checksum")
    plugin_checksums_json = json.dumps(provenance.get("plugin_checksums_json")) if provenance.get("plugin_checksums_json") is not None else None

    started = started_at or _now_iso()
    ended = ended_at or _now_iso()
    with psycopg.connect(db_url) as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO etl_runs (
                    run_id, pipeline, project_id, success, status, started_at, ended_at, message, executor, artifact_dir,
                    git_commit_sha, git_branch, git_tag, git_is_dirty, cli_command, pipeline_checksum,
                    global_config_checksum, execution_config_checksum, plugin_checksums_json
                )
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s::jsonb)
                ON CONFLICT (run_id)
                DO UPDATE SET
                    pipeline = EXCLUDED.pipeline,
                    project_id = COALESCE(EXCLUDED.project_id, etl_runs.project_id),
                    success = EXCLUDED.success,
                    status = EXCLUDED.status,
                    started_at = LEAST(etl_runs.started_at, EXCLUDED.started_at),
                    ended_at = GREATEST(etl_runs.ended_at, EXCLUDED.ended_at),
                    message = EXCLUDED.message,
                    executor = EXCLUDED.executor,
                    artifact_dir = EXCLUDED.artifact_dir,
                    git_commit_sha = COALESCE(EXCLUDED.git_commit_sha, etl_runs.git_commit_sha),
                    git_branch = COALESCE(EXCLUDED.git_branch, etl_runs.git_branch),
                    git_tag = COALESCE(EXCLUDED.git_tag, etl_runs.git_tag),
                    git_is_dirty = COALESCE(EXCLUDED.git_is_dirty, etl_runs.git_is_dirty),
                    cli_command = COALESCE(EXCLUDED.cli_command, etl_runs.cli_command),
                    pipeline_checksum = COALESCE(EXCLUDED.pipeline_checksum, etl_runs.pipeline_checksum),
                    global_config_checksum = COALESCE(EXCLUDED.global_config_checksum, etl_runs.global_config_checksum),
                    execution_config_checksum = COALESCE(EXCLUDED.execution_config_checksum, etl_runs.execution_config_checksum),
                    plugin_checksums_json = COALESCE(EXCLUDED.plugin_checksums_json, etl_runs.plugin_checksums_json)
                """,
                (
                    run_id,
                    pipeline,
                    project_id,
                    success,
                    status,
                    started,
                    ended,
                    message,
                    executor,
                    artifact_dir,
                    git_commit_sha,
                    git_branch,
                    git_tag,
                    git_is_dirty,
                    cli_command,
                    pipeline_checksum,
                    global_config_checksum,
                    execution_config_checksum,
                    plugin_checksums_json,
                ),
            )
            if event_type:
                details_json = json.dumps(event_details or {})
                cur.execute(
                    """
                    INSERT INTO etl_run_events (run_id, event_type, details_json)
                    VALUES (%s, %s, %s::jsonb)
                    """,
                    (run_id, event_type, details_json),
                )
        conn.commit()


def upsert_step_attempt(
    *,
    run_id: str,
    step_name: str,
    attempt_no: int,
    script: str,
    success: bool,
    skipped: bool = False,
    error: Optional[str] = None,
    outputs: Optional[Dict[str, Any]] = None,
    plugin_name: Optional[str] = None,
    plugin_version: Optional[str] = None,
    failure_category: Optional[str] = None,
    runtime_seconds: Optional[float] = None,
    memory_gb: Optional[float] = None,
    cpu_cores: Optional[float] = None,
    started_at: Optional[str] = None,
    ended_at: Optional[str] = None,
    pipeline: Optional[str] = None,
    project_id: Optional[str] = None,
    artifact_dir: Optional[str] = None,
    executor: Optional[str] = None,
) -> None:
    """
    Upsert a single step attempt row for retry-aware tracking.
    """
    db_url = get_database_url()
    if not db_url:
        return
    started = started_at or _now_iso()
    ended = ended_at or _now_iso()
    outputs_json = json.dumps(outputs or {}, default=str)
    with psycopg.connect(db_url) as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO etl_run_steps (
                    run_id, step_name, script, success, skipped, error, outputs_json
                )
                VALUES (%s, %s, %s, %s, %s, %s, %s::jsonb)
                ON CONFLICT (run_id, step_name)
                DO UPDATE SET
                    script = EXCLUDED.script,
                    success = EXCLUDED.success,
                    skipped = EXCLUDED.skipped,
                    error = EXCLUDED.error,
                    outputs_json = EXCLUDED.outputs_json
                """,
                (
                    run_id,
                    step_name,
                    script,
                    success,
                    skipped,
                    error,
                    outputs_json,
                ),
            )
            cur.execute(
                """
                INSERT INTO etl_run_step_attempts (
                    run_id, step_name, attempt_no, script, success, skipped, error, outputs_json, started_at, ended_at,
                    plugin_name, plugin_version, failure_category, runtime_seconds, memory_gb, cpu_cores, updated_at
                )
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s::jsonb, %s, %s, %s, %s, %s, %s, %s, %s, NOW())
                ON CONFLICT (run_id, step_name, attempt_no)
                DO UPDATE SET
                    script = EXCLUDED.script,
                    success = EXCLUDED.success,
                    skipped = EXCLUDED.skipped,
                    error = EXCLUDED.error,
                    outputs_json = EXCLUDED.outputs_json,
                    started_at = EXCLUDED.started_at,
                    ended_at = EXCLUDED.ended_at,
                    plugin_name = COALESCE(EXCLUDED.plugin_name, etl_run_step_attempts.plugin_name),
                    plugin_version = COALESCE(EXCLUDED.plugin_version, etl_run_step_attempts.plugin_version),
                    failure_category = COALESCE(EXCLUDED.failure_category, etl_run_step_attempts.failure_category),
                    runtime_seconds = COALESCE(EXCLUDED.runtime_seconds, etl_run_step_attempts.runtime_seconds),
                    memory_gb = COALESCE(EXCLUDED.memory_gb, etl_run_step_attempts.memory_gb),
                    cpu_cores = COALESCE(EXCLUDED.cpu_cores, etl_run_step_attempts.cpu_cores),
                    updated_at = NOW()
                """,
                (
                    run_id,
                    step_name,
                    attempt_no,
                    script,
                    success,
                    skipped,
                    error,
                    outputs_json,
                    started,
                    ended,
                    plugin_name,
                    plugin_version,
                    failure_category,
                    runtime_seconds,
                    memory_gb,
                    cpu_cores,
                ),
            )
        conn.commit()
    if success and outputs and pipeline:
        try:
            register_step_artifacts(
                run_id=run_id,
                pipeline=pipeline,
                project_id=project_id,
                step_name=step_name,
                outputs=outputs,
                executor=executor,
            )
        except Exception:
            # Artifact registration is best-effort and should not fail execution tracking.
            pass


def record_run(
    run_result: RunResult,
    pipeline_path: str,
    store: Path,
    *,
    executor: Optional[str] = None,
    artifact_dir: Optional[str] = None,
    provenance: Optional[Dict[str, Any]] = None,
    project_id: Optional[str] = None,
) -> RunRecord:
    store.parent.mkdir(parents=True, exist_ok=True)
    started = getattr(run_result, "started_at", None) or _now_iso()
    ended = getattr(run_result, "ended_at", None) or _now_iso()
    rec = RunRecord(
        run_id=run_result.run_id,
        pipeline=str(pipeline_path),
        project_id=project_id,
        success=run_result.success,
        status="succeeded" if run_result.success else "failed",
        started_at=started,
        ended_at=ended,
        steps=[_step_to_dict(s) for s in run_result.steps],
    )
    with store.open("a", encoding="utf-8") as f:
        f.write(json.dumps(asdict(rec)) + "\n")

    _upsert_run_db(rec, executor=executor, artifact_dir=artifact_dir, provenance=provenance)
    try:
        register_run_artifacts(
            run_id=rec.run_id,
            pipeline=rec.pipeline,
            project_id=rec.project_id,
            artifact_dir=artifact_dir or getattr(run_result, "artifact_dir", None),
            steps=rec.steps,
            executor=executor,
        )
    except Exception:
        # Artifact registration is best-effort and should not fail run persistence.
        pass
    return rec


def load_runs(store: Path) -> List[RunRecord]:
    if not store.exists():
        return []
    records: List[RunRecord] = []
    with store.open("r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            data = json.loads(line)
            records.append(RunRecord(**data))
    return records


def find_run(store: Path, run_id: str) -> Optional[RunRecord]:
    for rec in load_runs(store):
        if rec.run_id == run_id:
            return rec
    return None


def load_run_step_states(run_id: str) -> Dict[str, RunStepState]:
    """
    Load latest step states for a run from DB tracking tables.
    """
    db_url = get_database_url()
    if not db_url:
        raise RuntimeError(
            "ETL_DATABASE_URL is required for --resume-run-id because step state is loaded from DB."
        )

    states: Dict[str, RunStepState] = {}
    with psycopg.connect(db_url) as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT step_name, success, skipped, COALESCE(outputs_json, '{}'::jsonb)
                FROM etl_run_steps
                WHERE run_id = %s
                """,
                (run_id,),
            )
            for step_name, success, skipped, outputs in cur.fetchall():
                if isinstance(outputs, str):
                    outputs = json.loads(outputs or "{}")
                states[step_name] = RunStepState(
                    step_name=step_name,
                    success=bool(success),
                    skipped=bool(skipped),
                    outputs=outputs or {},
                )
    return states


def _script_head(script: str) -> str:
    text = str(script or "").strip()
    if not text:
        return ""
    try:
        tokens = shlex.split(text)
    except Exception:
        tokens = text.split()
    return str(tokens[0]).strip() if tokens else ""


def _stats(values: List[float]) -> tuple[Optional[float], Optional[float], int]:
    clean = [float(v) for v in values if v is not None]
    if not clean:
        return None, None, 0
    mean = float(statistics.fmean(clean))
    std = float(statistics.pstdev(clean)) if len(clean) > 1 else 0.0
    return mean, std, len(clean)


def fetch_plugin_resource_stats(
    *,
    plugin_name: str,
    plugin_version: str,
    plugin_refs: Optional[List[str]] = None,
    executor: str = "slurm",
    limit: int = 200,
) -> Dict[str, Any]:
    """
    Fetch resource usage statistics for a plugin/version from successful attempts.
    Returns {} when DB is unavailable or no eligible samples are found.
    """
    db_url = get_database_url()
    if not db_url:
        return {}

    p_name = str(plugin_name or "").strip()
    p_ver = str(plugin_version or "").strip()
    refs = [str(r or "").strip() for r in (plugin_refs or []) if str(r or "").strip()]
    max_rows = max(1, int(limit))

    rows: List[Any] = []
    try:
        with psycopg.connect(db_url) as conn:
            with conn.cursor() as cur:
                if p_name and p_ver:
                    cur.execute(
                        """
                        SELECT
                            a.script, a.success, a.skipped, a.error,
                            a.outputs_json, a.started_at, a.ended_at,
                            a.failure_category, a.runtime_seconds, a.memory_gb, a.cpu_cores
                        FROM etl_run_step_attempts a
                        JOIN etl_runs r ON r.run_id = a.run_id
                        WHERE r.executor = %s
                          AND a.plugin_name = %s
                          AND a.plugin_version = %s
                        ORDER BY a.started_at DESC
                        LIMIT %s
                        """,
                        (str(executor or "").strip(), p_name, p_ver, max_rows),
                    )
                    rows = cur.fetchall()
                if not rows and refs:
                    cur.execute(
                        """
                        SELECT
                            a.script, a.success, a.skipped, a.error,
                            a.outputs_json, a.started_at, a.ended_at,
                            a.failure_category, a.runtime_seconds, a.memory_gb, a.cpu_cores
                        FROM etl_run_step_attempts a
                        JOIN etl_runs r ON r.run_id = a.run_id
                        WHERE r.executor = %s
                        ORDER BY a.started_at DESC
                        LIMIT %s
                        """,
                        (str(executor or "").strip(), max_rows * 5),
                    )
                    all_rows = cur.fetchall()
                    rows = [r for r in all_rows if _script_head(str(r[0] or "")) in set(refs)]
    except Exception:
        return {}

    wall_minutes_values: List[float] = []
    memory_gb_values: List[float] = []
    cpu_cores_values: List[float] = []

    for row in rows:
        success = bool(row[1])
        skipped = bool(row[2])
        if skipped or not success:
            continue
        outputs = row[4]
        if isinstance(outputs, str):
            try:
                outputs = json.loads(outputs or "{}")
            except Exception:
                outputs = {}
        runtime_seconds = row[8]
        if runtime_seconds is None and row[5] is not None and row[6] is not None:
            try:
                runtime_seconds = float((row[6] - row[5]).total_seconds())
            except Exception:
                runtime_seconds = None
        if runtime_seconds is not None:
            try:
                wall_minutes_values.append(max(0.0, float(runtime_seconds) / 60.0))
            except (TypeError, ValueError):
                pass

        mem = row[9]
        if mem is None and isinstance(outputs, dict):
            for key in ("peak_memory_gb", "memory_gb", "max_rss_gb", "rss_gb"):
                raw = outputs.get(key)
                if raw in (None, ""):
                    continue
                try:
                    mem = float(raw)
                except (TypeError, ValueError):
                    mem = None
                if mem is not None:
                    break
        if mem is not None:
            try:
                memory_gb_values.append(float(mem))
            except (TypeError, ValueError):
                pass

        cpu = row[10]
        if cpu is None and isinstance(outputs, dict):
            for key in ("cpu_cores", "peak_cpu_cores", "used_cpu_cores", "cpu_count"):
                raw = outputs.get(key)
                if raw in (None, ""):
                    continue
                try:
                    cpu = float(raw)
                except (TypeError, ValueError):
                    cpu = None
                if cpu is not None:
                    break
        if cpu is not None:
            try:
                cpu_cores_values.append(float(cpu))
            except (TypeError, ValueError):
                pass

    wall_mean, wall_std, wall_n = _stats(wall_minutes_values)
    mem_mean, mem_std, mem_n = _stats(memory_gb_values)
    cpu_mean, cpu_std, cpu_n = _stats(cpu_cores_values)
    if wall_n == 0 and mem_n == 0 and cpu_n == 0:
        return {}
    return {
        "samples": max(wall_n, mem_n, cpu_n),
        "wall_minutes_mean": wall_mean,
        "wall_minutes_std": wall_std,
        "wall_minutes_samples": wall_n,
        "memory_gb_mean": mem_mean,
        "memory_gb_std": mem_std,
        "memory_gb_samples": mem_n,
        "cpu_cores_mean": cpu_mean,
        "cpu_cores_std": cpu_std,
        "cpu_cores_samples": cpu_n,
    }


__all__ = [
    "RunRecord",
    "RunStepState",
    "record_run",
    "load_runs",
    "find_run",
    "load_run_step_states",
    "fetch_plugin_resource_stats",
    "upsert_run_status",
    "upsert_step_attempt",
]

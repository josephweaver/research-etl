"""
Local executor: runs pipelines synchronously in-process.
"""

from __future__ import annotations

from datetime import datetime
from pathlib import Path
from typing import Any, Dict, Optional

from .base import Executor, RunState, RunStatus, SubmissionResult
from ..pipeline import Pipeline, parse_pipeline, PipelineError
from ..runner import run_pipeline, RunResult
from ..tracking import record_run, load_run_step_states


class LocalExecutor(Executor):
    name = "local"

    def __init__(
        self,
        plugin_dir: Path = Path("plugins"),
        workdir: Path = Path(".runs"),
        dry_run: bool = False,
        max_retries: int = 0,
        retry_delay_seconds: float = 0.0,
    ):
        self.plugin_dir = plugin_dir
        self.workdir = workdir
        self.dry_run = dry_run
        self.max_retries = max_retries
        self.retry_delay_seconds = retry_delay_seconds
        self._statuses: Dict[str, RunStatus] = {}

    def _record_status(self, run_result: RunResult, message: str = "") -> RunStatus:
        state = RunState.SUCCEEDED if run_result.success else RunState.FAILED
        status = RunStatus(run_id=run_result.run_id, state=state, message=message)
        self._statuses[run_result.run_id] = status
        return status

    def submit(self, pipeline_path: str, context: Dict[str, Any] | None = None) -> SubmissionResult:
        context = context or {}
        try:
            pipeline: Pipeline = context.get("pipeline") or parse_pipeline(Path(pipeline_path))
        except PipelineError as exc:
            raise RuntimeError(f"Pipeline parse failed: {exc}") from exc
        resume_run_id = context.get("resume_run_id")
        resume_succeeded_steps = None
        prior_step_outputs = None
        if resume_run_id:
            states = load_run_step_states(str(resume_run_id))
            resume_succeeded_steps = {name for name, st in states.items() if st.success}
            prior_step_outputs = {name: st.outputs for name, st in states.items()}

        run_result = run_pipeline(
            pipeline,
            plugin_dir=self.plugin_dir,
            workdir=self.workdir,
            dry_run=self.dry_run,
            max_retries=self.max_retries,
            retry_delay_seconds=self.retry_delay_seconds,
            resume_succeeded_steps=resume_succeeded_steps,
            prior_step_outputs=prior_step_outputs,
            log_func=context.get("log"),
        )
        # attach timestamps
        run_result.started_at = context.get("started_at") or datetime.utcnow().isoformat() + "Z"  # type: ignore[attr-defined]
        run_result.ended_at = datetime.utcnow().isoformat() + "Z"  # type: ignore[attr-defined]

        status = self._record_status(run_result)
        record_run(
            run_result,
            pipeline_path,
            self.workdir / "runs.jsonl",
            executor=self.name,
            artifact_dir=getattr(run_result, "artifact_dir", None),
        )
        return SubmissionResult(run_id=run_result.run_id, message=status.message)

    def status(self, run_id: str) -> RunStatus:
        if run_id not in self._statuses:
            return RunStatus(run_id=run_id, state=RunState.FAILED, message="Unknown run_id")
        return self._statuses[run_id]

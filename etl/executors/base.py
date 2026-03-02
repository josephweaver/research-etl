# research-etl
# Copyright (c) 2026 Joseph Weaver
# This file is part of the research-etl project and is licensed under the MIT License.
# You may not use this file except in compliance with the License.
# See https://github.com/josephweaver/research-etl for details.

"""
Executor interface and minimal data structures.

Executors submit pipeline runs to a chosen backend (local process,
SLURM, Kubernetes, etc.) and expose status and cancellation hooks.
"""

from __future__ import annotations

from abc import ABC, abstractmethod
from dataclasses import dataclass
from datetime import datetime
from enum import Enum
from typing import Any, Dict, Optional


class RunState(str, Enum):
    QUEUED = "queued"
    RUNNING = "running"
    SUCCEEDED = "succeeded"
    FAILED = "failed"
    CANCELLED = "cancelled"


@dataclass
class SubmissionResult:
    run_id: str
    backend_run_id: Optional[str] = None
    job_ids: Optional[list] = None
    message: str = ""


@dataclass
class RunStatus:
    run_id: str
    state: RunState
    message: str = ""
    started_at: Optional[datetime] = None
    ended_at: Optional[datetime] = None
    logs_uri: Optional[str] = None

    def is_terminal(self) -> bool:
        return self.state in {RunState.SUCCEEDED, RunState.FAILED, RunState.CANCELLED}


class Executor(ABC):
    """Base class for executors."""

    name: str = "base"

    @abstractmethod
    def submit(self, pipeline_path: str, context: Dict[str, Any]) -> SubmissionResult:
        """Submit a pipeline run and return a submission result."""

    @abstractmethod
    def status(self, run_id: str) -> RunStatus:
        """Return current status for a run."""

    def capabilities(self) -> Dict[str, bool]:
        """Return supported optional executor capabilities."""
        return {
            "cancel": False,
            "artifact_tree": False,
            "artifact_file": False,
            "query_data": False,
        }

    def cancel(self, run_id: str) -> RunStatus:
        """Attempt to cancel a run if supported."""
        raise NotImplementedError("cancel not implemented for this executor")

    def artifact_tree(self, artifact_dir: str) -> Dict[str, Any]:
        """Return a JSON-like artifact tree rooted at artifact_dir."""
        raise NotImplementedError("artifact tree retrieval not implemented for this executor")

    def artifact_file(self, artifact_dir: str, relative_path: str, max_bytes: int = 256 * 1024) -> Dict[str, Any]:
        """Return file content payload for a relative path under artifact_dir."""
        raise NotImplementedError("artifact file retrieval not implemented for this executor")

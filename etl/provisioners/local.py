from __future__ import annotations

import os
import shlex
import subprocess
import uuid
from dataclasses import dataclass
from datetime import datetime
from typing import Dict, Optional

from ..transports import BackgroundSessionHandle, BackgroundSessionSpec, CommandTransport, ExecutionOptions, LocalProcessTransport
from .base import ProvisionHandle, ProvisionState, ProvisionStatus, Provisioner, ProvisionerError, WorkloadSpec


@dataclass(frozen=True)
class LocalSubmitOptions:
    detach: bool
    shell: Optional[str]
    total_timeout_seconds: Optional[float]
    stream_output: bool


class LocalProvisioner(Provisioner):
    """Provisioner that places workloads onto the current machine via a command transport."""

    name = "local"

    def __init__(
        self,
        transport: Optional[CommandTransport] = None,
        *,
        dry_run: bool = False,
    ) -> None:
        self.transport = transport or LocalProcessTransport()
        self.dry_run = bool(dry_run)
        self._statuses: Dict[str, ProvisionStatus] = {}
        self._sessions: Dict[str, BackgroundSessionHandle] = {}

    def capabilities(self) -> dict[str, bool]:
        caps = dict(self.transport.capabilities())
        return {
            "submit": True,
            "status": True,
            "cancel": bool(caps.get("background_sessions")),
            "mounts": False,
            "ports": False,
            "images": False,
            "detach": bool(caps.get("background_sessions")),
        }

    def submit(self, spec: WorkloadSpec) -> ProvisionHandle:
        opts = self._submit_options(spec)
        backend_run_id = f"local-{uuid.uuid4().hex[:12]}"
        if self.dry_run:
            status = ProvisionStatus(
                backend_run_id=backend_run_id,
                state=ProvisionState.QUEUED,
                message="dry-run",
                started_at=datetime.utcnow(),
            )
            self._statuses[backend_run_id] = status
            return ProvisionHandle(
                provisioner=self.name,
                backend_run_id=backend_run_id,
                job_ids=[backend_run_id],
                message="dry-run",
            )

        if opts.detach:
            session_name = str(spec.backend_options.get("session_name") or backend_run_id).strip() or backend_run_id
            command_text = self._command_text(spec, opts)
            handle = self.transport.start_background_session(
                BackgroundSessionSpec(
                    command=command_text,
                    session_name=session_name,
                    cwd=spec.cwd,
                    env=(dict(spec.env) if spec.env else None),
                ),
                options=ExecutionOptions(
                    total_timeout_seconds=opts.total_timeout_seconds,
                    stream_output=opts.stream_output,
                ),
            )
            self._sessions[backend_run_id] = handle
            self._statuses[backend_run_id] = ProvisionStatus(
                backend_run_id=backend_run_id,
                state=ProvisionState.RUNNING,
                message=f"background session started: {handle.session_name}",
                started_at=datetime.utcnow(),
                metadata={"session_name": handle.session_name},
            )
            return ProvisionHandle(
                provisioner=self.name,
                backend_run_id=backend_run_id,
                job_ids=[backend_run_id],
                message=f"background session started: {handle.session_name}",
                metadata={"session_name": handle.session_name, "detached": True},
            )

        if spec.command:
            result = self.transport.run(
                list(spec.command),
                cwd=spec.cwd,
                env=(dict(spec.env) if spec.env else None),
                check=False,
                options=ExecutionOptions(
                    total_timeout_seconds=opts.total_timeout_seconds,
                    stream_output=opts.stream_output,
                ),
            )
        elif spec.script_text:
            result = self.transport.run_text(
                spec.script_text,
                cwd=spec.cwd,
                env=(dict(spec.env) if spec.env else None),
                shell=opts.shell,
                check=False,
                options=ExecutionOptions(
                    total_timeout_seconds=opts.total_timeout_seconds,
                    stream_output=opts.stream_output,
                ),
            )
        else:
            raise ProvisionerError("LocalProvisioner requires WorkloadSpec.command or WorkloadSpec.script_text.")

        state = ProvisionState.SUCCEEDED if int(result.returncode) == 0 else ProvisionState.FAILED
        status = ProvisionStatus(
            backend_run_id=backend_run_id,
            state=state,
            message=(result.stderr or result.stdout or "").strip(),
            started_at=datetime.fromisoformat(result.started_at) if result.started_at else None,
            ended_at=datetime.fromisoformat(result.ended_at) if result.ended_at else datetime.utcnow(),
            metadata={
                "returncode": int(result.returncode),
                "stdout": result.stdout,
                "stderr": result.stderr,
                "timed_out": bool(result.timed_out),
                "cancelled": bool(result.cancelled),
            },
        )
        self._statuses[backend_run_id] = status
        return ProvisionHandle(
            provisioner=self.name,
            backend_run_id=backend_run_id,
            job_ids=[backend_run_id],
            message=status.message,
            metadata=dict(status.metadata),
        )

    def status(self, handle: ProvisionHandle) -> ProvisionStatus:
        run_id = str(handle.backend_run_id or "").strip()
        if not run_id:
            return ProvisionStatus(backend_run_id=None, state=ProvisionState.UNKNOWN, message="missing backend_run_id")
        session = self._sessions.get(run_id)
        if session is not None:
            bg = self.transport.background_session_status(session)
            if bg.running:
                return ProvisionStatus(
                    backend_run_id=run_id,
                    state=ProvisionState.RUNNING,
                    message=bg.details or f"background session running: {bg.session_name}",
                    started_at=self._statuses.get(run_id, ProvisionStatus(run_id, ProvisionState.RUNNING)).started_at,
                    metadata={"session_name": bg.session_name},
                )
            prior = self._statuses.get(run_id)
            terminal = ProvisionStatus(
                backend_run_id=run_id,
                state=(prior.state if prior and prior.state == ProvisionState.CANCELLED else ProvisionState.UNKNOWN),
                message=(prior.message if prior else "background session is no longer running"),
                started_at=(prior.started_at if prior else None),
                ended_at=(prior.ended_at if prior and prior.ended_at else datetime.utcnow()),
                metadata={"session_name": session.session_name},
            )
            self._statuses[run_id] = terminal
            return terminal
        return self._statuses.get(
            run_id,
            ProvisionStatus(backend_run_id=run_id, state=ProvisionState.UNKNOWN, message="unknown backend_run_id"),
        )

    def cancel(self, handle: ProvisionHandle) -> ProvisionStatus:
        run_id = str(handle.backend_run_id or "").strip()
        if not run_id:
            raise ProvisionerError("Cannot cancel without backend_run_id.")
        session = self._sessions.pop(run_id, None)
        if session is None:
            prior = self._statuses.get(run_id)
            if prior is not None:
                return prior
            raise ProvisionerError(f"Unknown local workload: {run_id}")
        self.transport.dismiss_background_session(
            session,
            options=ExecutionOptions(stream_output=False),
        )
        status = ProvisionStatus(
            backend_run_id=run_id,
            state=ProvisionState.CANCELLED,
            message=f"background session dismissed: {session.session_name}",
            started_at=self._statuses.get(run_id, ProvisionStatus(run_id, ProvisionState.CANCELLED)).started_at,
            ended_at=datetime.utcnow(),
            metadata={"session_name": session.session_name},
        )
        self._statuses[run_id] = status
        return status

    def _submit_options(self, spec: WorkloadSpec) -> LocalSubmitOptions:
        backend = dict(spec.backend_options or {})
        return LocalSubmitOptions(
            detach=bool(backend.get("detach", False)),
            shell=(str(backend.get("shell") or "").strip() or None),
            total_timeout_seconds=(
                float(backend.get("total_timeout_seconds"))
                if backend.get("total_timeout_seconds") not in (None, "")
                else None
            ),
            stream_output=bool(backend.get("stream_output", True)),
        )

    @staticmethod
    def _command_text(spec: WorkloadSpec, opts: LocalSubmitOptions) -> str:
        if spec.command:
            parts = [str(x) for x in spec.command]
            return subprocess.list2cmdline(parts) if os.name == "nt" else shlex.join(parts)
        if spec.script_text:
            shell = str(opts.shell or "bash").strip() or "bash"
            if shell.lower() in {"powershell", "pwsh"}:
                quoted = subprocess.list2cmdline([str(spec.script_text)]) if os.name == "nt" else shlex.quote(spec.script_text)
                return f"{shell} -Command {quoted}"
            return f"{shell} -lc {shlex.quote(spec.script_text)}"
        raise ProvisionerError("Detached local workload requires command or script_text.")

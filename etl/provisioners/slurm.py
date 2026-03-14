from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Optional

from ..transports import CommandTransport, ExecutionOptions
from .base import ProvisionHandle, ProvisionState, ProvisionStatus, Provisioner, ProvisionerError, WorkloadSpec


def _parse_sbatch_job_id(text: str) -> Optional[str]:
    parts = str(text or "").strip().split()
    return parts[-1] if parts else None


def _map_slurm_state(raw: str) -> ProvisionState:
    text = str(raw or "").strip().upper()
    if not text:
        return ProvisionState.UNKNOWN
    if text in {"PENDING", "CONFIGURING", "RESIZING", "SUSPENDED"}:
        return ProvisionState.QUEUED
    if text in {"RUNNING", "COMPLETING", "STAGE_OUT"}:
        return ProvisionState.RUNNING
    if text.startswith("COMPLETED"):
        return ProvisionState.SUCCEEDED
    if text.startswith("CANCELLED"):
        return ProvisionState.CANCELLED
    if text in {"FAILED", "BOOT_FAIL", "DEADLINE", "NODE_FAIL", "OUT_OF_MEMORY", "PREEMPTED", "REVOKED", "TIMEOUT"}:
        return ProvisionState.FAILED
    return ProvisionState.UNKNOWN


@dataclass(frozen=True)
class SlurmSubmitOptions:
    destination_dir: str
    file_name: str
    dependencies: list[str]
    submit_timeout_seconds: float
    transfer_timeout_seconds: float


class SlurmProvisioner(Provisioner):
    """Provisioner that places workloads onto a SLURM scheduler via a command transport."""

    name = "slurm"

    def __init__(
        self,
        transport: CommandTransport,
        *,
        dry_run: bool = False,
        default_submit_timeout_seconds: float = 120.0,
        default_transfer_timeout_seconds: float = 300.0,
    ) -> None:
        self.transport = transport
        self.dry_run = bool(dry_run)
        self.default_submit_timeout_seconds = float(default_submit_timeout_seconds)
        self.default_transfer_timeout_seconds = float(default_transfer_timeout_seconds)

    def capabilities(self) -> dict[str, bool]:
        return {
            "submit": True,
            "status": True,
            "cancel": True,
            "mounts": False,
            "ports": False,
            "images": False,
        }

    def submit(self, spec: WorkloadSpec) -> ProvisionHandle:
        if not spec.script_text:
            raise ProvisionerError("SlurmProvisioner requires WorkloadSpec.script_text.")
        opts = self._submit_options(spec)
        script_path = self._destination_path(opts.destination_dir, opts.file_name)
        if self.dry_run:
            return ProvisionHandle(
                provisioner=self.name,
                backend_run_id="dry-run",
                job_ids=["dry-run"],
                message="dry-run",
                metadata={"script_path": script_path},
            )
        put = self.transport.put_text(
            spec.script_text,
            script_path,
            newline="\n",
            options=ExecutionOptions(
                total_timeout_seconds=opts.transfer_timeout_seconds,
                stream_output=False,
            ),
        )
        if not put.ok:
            raise ProvisionerError(put.details or f"Could not stage sbatch script at {script_path}")
        submit_cmd = ["sbatch", *[f"--dependency=afterok:{jid}" for jid in opts.dependencies], script_path]
        result = self.transport.run(
            submit_cmd,
            check=False,
            options=ExecutionOptions(
                total_timeout_seconds=opts.submit_timeout_seconds,
                stream_output=False,
            ),
        )
        if result.returncode != 0:
            raise ProvisionerError(result.stderr or result.stdout or "sbatch submission failed")
        job_id = _parse_sbatch_job_id(result.stdout)
        return ProvisionHandle(
            provisioner=self.name,
            backend_run_id=job_id,
            job_ids=[job_id] if job_id else [],
            message=(result.stdout or "").strip(),
            metadata={"script_path": script_path},
        )

    def status(self, handle: ProvisionHandle) -> ProvisionStatus:
        job_id = str(handle.backend_run_id or "").strip()
        if not job_id:
            return ProvisionStatus(backend_run_id=None, state=ProvisionState.UNKNOWN, message="missing backend_run_id")
        result = self.transport.run(
            ["squeue", "-h", "-j", job_id, "-o", "%T"],
            check=False,
            options=ExecutionOptions(total_timeout_seconds=self.default_submit_timeout_seconds, stream_output=False),
        )
        if result.returncode != 0:
            return ProvisionStatus(
                backend_run_id=job_id,
                state=ProvisionState.UNKNOWN,
                message=(result.stderr or result.stdout or "squeue status failed").strip(),
            )
        raw_state = str(result.stdout or "").strip().splitlines()
        slurm_state = raw_state[0].strip() if raw_state else ""
        return ProvisionStatus(
            backend_run_id=job_id,
            state=_map_slurm_state(slurm_state),
            message=slurm_state or "no scheduler state returned",
            started_at=datetime.utcnow(),
        )

    def cancel(self, handle: ProvisionHandle) -> ProvisionStatus:
        job_id = str(handle.backend_run_id or "").strip()
        if not job_id:
            raise ProvisionerError("Cannot cancel without backend_run_id.")
        result = self.transport.run(
            ["scancel", job_id],
            check=False,
            options=ExecutionOptions(total_timeout_seconds=self.default_submit_timeout_seconds, stream_output=False),
        )
        if result.returncode != 0:
            raise ProvisionerError(result.stderr or result.stdout or "scancel failed")
        return ProvisionStatus(
            backend_run_id=job_id,
            state=ProvisionState.CANCELLED,
            message=f"cancel requested for {job_id}",
            ended_at=datetime.utcnow(),
        )

    def _submit_options(self, spec: WorkloadSpec) -> SlurmSubmitOptions:
        backend = dict(spec.backend_options or {})
        destination_dir = str(backend.get("destination_dir") or "").strip() or "/tmp"
        file_name = str(backend.get("file_name") or "").strip() or f"{spec.name}.sbatch"
        dependencies = [str(x).strip() for x in list(backend.get("dependencies") or []) if str(x).strip()]
        submit_timeout = float(backend.get("submit_timeout_seconds") or self.default_submit_timeout_seconds)
        transfer_timeout = float(backend.get("transfer_timeout_seconds") or self.default_transfer_timeout_seconds)
        return SlurmSubmitOptions(
            destination_dir=destination_dir,
            file_name=file_name,
            dependencies=dependencies,
            submit_timeout_seconds=submit_timeout,
            transfer_timeout_seconds=transfer_timeout,
        )

    @staticmethod
    def _destination_path(destination_dir: str, file_name: str) -> str:
        return (Path(destination_dir) / file_name).as_posix()

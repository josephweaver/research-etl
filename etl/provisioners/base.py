from __future__ import annotations

from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum
from typing import Any, Dict, Mapping, Optional


class ProvisionerError(RuntimeError):
    """Raised when a provisioner cannot prepare, submit, or manage workload placement."""


class ProvisionState(str, Enum):
    PENDING = "pending"
    PROVISIONING = "provisioning"
    QUEUED = "queued"
    RUNNING = "running"
    SUCCEEDED = "succeeded"
    FAILED = "failed"
    CANCELLED = "cancelled"
    UNKNOWN = "unknown"


@dataclass(frozen=True)
class ResourceRequest:
    cpu_cores: Optional[int] = None
    memory_gb: Optional[float] = None
    wall_minutes: Optional[int] = None
    gpu_count: Optional[int] = None
    accelerator: Optional[str] = None
    metadata: Dict[str, Any] = field(default_factory=dict)


@dataclass(frozen=True)
class MountSpec:
    source: str
    target: str
    read_only: bool = False


@dataclass(frozen=True)
class PortSpec:
    container_port: int
    host_port: Optional[int] = None
    protocol: str = "tcp"


@dataclass(frozen=True)
class WorkloadSpec:
    name: str
    command: Optional[list[str]] = None
    script_text: Optional[str] = None
    image: Optional[str] = None
    cwd: Optional[str] = None
    env: Mapping[str, str] = field(default_factory=dict)
    resources: Optional[ResourceRequest] = None
    mounts: list[MountSpec] = field(default_factory=list)
    ports: list[PortSpec] = field(default_factory=list)
    metadata: Dict[str, Any] = field(default_factory=dict)
    backend_options: Dict[str, Any] = field(default_factory=dict)


@dataclass(frozen=True)
class ProvisionHandle:
    provisioner: str
    backend_run_id: Optional[str] = None
    job_ids: list[str] = field(default_factory=list)
    message: str = ""
    endpoint: Optional[str] = None
    metadata: Dict[str, Any] = field(default_factory=dict)


@dataclass(frozen=True)
class ProvisionStatus:
    backend_run_id: Optional[str]
    state: ProvisionState
    message: str = ""
    started_at: Optional[datetime] = None
    ended_at: Optional[datetime] = None
    metadata: Dict[str, Any] = field(default_factory=dict)

    def is_terminal(self) -> bool:
        return self.state in {ProvisionState.SUCCEEDED, ProvisionState.FAILED, ProvisionState.CANCELLED}


class Provisioner(ABC):
    """Compute-substrate adapter for placing and managing workloads."""

    name: str = "base"

    def capabilities(self) -> Dict[str, bool]:
        return {
            "submit": True,
            "status": False,
            "cancel": False,
            "mounts": False,
            "ports": False,
            "images": False,
        }

    @abstractmethod
    def submit(self, spec: WorkloadSpec) -> ProvisionHandle:
        """Submit or start a workload on the target substrate."""

    @abstractmethod
    def status(self, handle: ProvisionHandle) -> ProvisionStatus:
        """Return current substrate status for a previously submitted workload."""

    def cancel(self, handle: ProvisionHandle) -> ProvisionStatus:
        """Attempt to cancel a previously submitted workload."""
        raise NotImplementedError("cancel not implemented for this provisioner")

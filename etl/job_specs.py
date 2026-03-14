from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from typing import Any, Optional


@dataclass(frozen=True)
class SourceRef:
    provider: str
    repo_url: Optional[str] = None
    revision: Optional[str] = None
    repo_name: Optional[str] = None
    mode: Optional[str] = None
    checkout_root: Optional[str] = None
    bundle_path: Optional[str] = None
    snapshot_path: Optional[str] = None
    local_repo_path: Optional[str] = None
    metadata: dict[str, Any] = field(default_factory=dict)


@dataclass(frozen=True)
class PipelineAssetRef:
    repo_url: str
    revision: str
    pipelines_dir: str = "pipelines"
    scripts_dir: str = "scripts"
    repo_name: Optional[str] = None
    local_repo_path: Optional[str] = None
    checkout_root: Optional[str] = None
    metadata: dict[str, Any] = field(default_factory=dict)


@dataclass(frozen=True)
class ResolvedPaths:
    workdir: str
    base_logdir: str
    setup_logdir: str
    context_file: str
    child_jobs_file: str
    checkout_root: str
    pipeline_path: str
    plugins_dir: str
    venv_path: str
    requirements_path: str
    python_bin: str
    workdirs_to_create: list[str] = field(default_factory=list)
    logdirs_to_create: list[str] = field(default_factory=list)
    global_config_path: Optional[str] = None
    projects_config_path: Optional[str] = None
    environments_config_path: Optional[str] = None


@dataclass(frozen=True)
class RunSelection:
    selected_step_indices: list[int] = field(default_factory=list)
    resume_run_id: Optional[str] = None
    run_started_at: Optional[str] = None
    max_retries: int = 0
    retry_delay_seconds: float = 0.0
    verbose: bool = False
    dry_run: bool = False


@dataclass(frozen=True)
class RunSpec:
    run_id: str
    project_id: Optional[str]
    pipeline_path_input: str
    pipeline_name: Optional[str]
    job_name: Optional[str]
    source_repo_root: str
    created_at: datetime
    run_date: str
    run_stamp: str
    run_fs_id: str
    pipeline: Any
    batches: list[list[tuple[int, Any]]] = field(default_factory=list)
    etl_source: SourceRef = field(default_factory=lambda: SourceRef(provider="git"))
    pipeline_assets: list[PipelineAssetRef] = field(default_factory=list)
    paths: Optional[ResolvedPaths] = None
    selection: RunSelection = field(default_factory=RunSelection)
    execution_env_name: Optional[str] = None
    execution_env: dict[str, Any] = field(default_factory=dict)
    global_vars: dict[str, Any] = field(default_factory=dict)
    project_vars: dict[str, Any] = field(default_factory=dict)
    commandline_vars: dict[str, Any] = field(default_factory=dict)
    provenance: dict[str, Any] = field(default_factory=dict)
    metadata: dict[str, Any] = field(default_factory=dict)


@dataclass(frozen=True)
class JobResources:
    cpu_cores: Optional[int] = None
    memory_gb: Optional[float] = None
    wall_minutes: Optional[int] = None
    metadata: dict[str, Any] = field(default_factory=dict)


@dataclass(frozen=True)
class JobSpec:
    job_id: str
    run_id: str
    name: str
    kind: str
    command: Optional[list[str]] = None
    script_text: Optional[str] = None
    cwd: Optional[str] = None
    env: dict[str, str] = field(default_factory=dict)
    workdir: Optional[str] = None
    logdir: Optional[str] = None
    output_file: Optional[str] = None
    error_file: Optional[str] = None
    dependencies: list[str] = field(default_factory=list)
    resources: Optional[JobResources] = None
    step_indices: list[int] = field(default_factory=list)
    metadata: dict[str, Any] = field(default_factory=dict)
    backend_options: dict[str, Any] = field(default_factory=dict)


@dataclass(frozen=True)
class PlannedJob:
    job_id: str
    run_id: str
    name: str
    kind: str
    label: str
    workdir: Optional[str] = None
    logdir: Optional[str] = None
    dependencies: list[str] = field(default_factory=list)
    resources: Optional[JobResources] = None
    step_indices: list[int] = field(default_factory=list)
    steps: list[Any] = field(default_factory=list)
    metadata: dict[str, Any] = field(default_factory=dict)

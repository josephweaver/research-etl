# Run And Job Spec Design

## Purpose

This document formalizes the implicit execution contract that currently exists across:

- `pipeline.yml`
- global config
- environment config
- project config
- commandline vars
- source-control resolution
- pipeline asset resolution
- executor-specific path logic

The goal is to separate:

- ETL run planning
- substrate-ready job execution

That split is represented by two different objects:

- `RunSpec`
- `JobSpec`

## Why Two Specs

The system currently mixes two levels of concern:

1. ETL/domain-level run setup
   - what run is being executed
   - what sources/config/paths apply
   - what batches/steps are selected

2. execution-level workload placement
   - what concrete job/script/command gets submitted
   - what resources it needs
   - what it depends on

Those should not be the same object.

## Layering

Intended flow:

1. parse pipeline + configs + source info
2. build `RunSpec`
3. derive one or more `JobSpec` objects from the `RunSpec`
4. translate `JobSpec` into `WorkloadSpec` for a provisioner
5. execute through provisioner/transport/runner

In short:

`Pipeline + Context -> RunSpec -> JobSpec -> WorkloadSpec -> Provisioner`

## RunSpec

`RunSpec` is the fully resolved ETL run contract.

It should be executor-independent as much as possible.

Suggested shape:

```python
@dataclass(frozen=True)
class SourceRef:
    provider: str
    repo_url: str | None = None
    revision: str | None = None
    repo_name: str | None = None
    mode: str | None = None  # workspace | git_remote | git_bundle | snapshot | auto
    checkout_root: str | None = None
    bundle_path: str | None = None
    snapshot_path: str | None = None
    local_repo_path: str | None = None
    metadata: dict[str, Any] = field(default_factory=dict)


@dataclass(frozen=True)
class PipelineAssetRef:
    repo_url: str
    revision: str
    repo_name: str | None = None
    pipelines_dir: str = "pipelines"
    scripts_dir: str = "scripts"
    local_repo_path: str | None = None
    checkout_root: str | None = None
    metadata: dict[str, Any] = field(default_factory=dict)


@dataclass(frozen=True)
class ResolvedPaths:
    workdir: str
    logdir: str
    context_file: str
    child_jobs_file: str | None = None
    checkout_root: str | None = None
    plugins_dir: str | None = None
    pipeline_path: str | None = None
    global_config_path: str | None = None
    projects_config_path: str | None = None
    environments_config_path: str | None = None
    venv_path: str | None = None
    requirements_path: str | None = None
    python_bin: str | None = None


@dataclass(frozen=True)
class RunSelection:
    selected_step_indices: list[int] = field(default_factory=list)
    resume_run_id: str | None = None
    run_started_at: str | None = None
    max_retries: int = 0
    retry_delay_seconds: float = 0.0
    verbose: bool = False
    dry_run: bool = False


@dataclass(frozen=True)
class RunSpec:
    run_id: str
    project_id: str | None
    pipeline_path_input: str
    pipeline_name: str | None
    job_name: str | None
    source_repo_root: str
    etl_source: SourceRef
    pipeline_assets: list[PipelineAssetRef]
    paths: ResolvedPaths
    selection: RunSelection
    execution_env_name: str | None
    execution_env: dict[str, Any]
    global_vars: dict[str, Any]
    project_vars: dict[str, Any]
    commandline_vars: dict[str, Any]
    provenance: dict[str, Any]
    metadata: dict[str, Any] = field(default_factory=dict)
```

### RunSpec Responsibilities

`RunSpec` should capture:

- run identity
- fully resolved ETL source selection
- pipeline asset source selection
- resolved paths
- resolved config inputs
- retry/resume/verbosity flags
- project/environment/provenance context

### RunSpec Should Not Include

`RunSpec` should not directly include:

- SLURM-specific `#SBATCH` lines
- Kubernetes manifests
- SSH command strings
- concrete provisioner handles

## JobSpec

`JobSpec` is the execution-oriented unit derived from a `RunSpec`.

It should represent one concrete batchable job.

Suggested shape:

```python
@dataclass(frozen=True)
class JobResources:
    cpu_cores: int | None = None
    memory_gb: float | None = None
    wall_minutes: int | None = None
    metadata: dict[str, Any] = field(default_factory=dict)


@dataclass(frozen=True)
class JobSpec:
    job_id: str
    run_id: str
    name: str
    kind: str  # setup | batch | controller | ad_hoc
    command: list[str] | None = None
    script_text: str | None = None
    cwd: str | None = None
    env: dict[str, str] = field(default_factory=dict)
    workdir: str | None = None
    logdir: str | None = None
    output_file: str | None = None
    error_file: str | None = None
    dependencies: list[str] = field(default_factory=list)
    resources: JobResources | None = None
    step_indices: list[int] = field(default_factory=list)
    metadata: dict[str, Any] = field(default_factory=dict)
    backend_options: dict[str, Any] = field(default_factory=dict)
```

### JobSpec Responsibilities

`JobSpec` should capture:

- one unit of executable work
- dependency edges
- command or script payload
- cwd/env/runtime paths
- resource request
- log/output conventions

### JobSpec Should Not Include

`JobSpec` should not own:

- source-resolution policy
- variable-solver policy
- project/global/env merge logic

Those belong in `RunSpec` creation.

## WorkloadSpec Relationship

`JobSpec` is not meant to replace `WorkloadSpec`.

Instead:

- `JobSpec` is ETL execution planning output
- `WorkloadSpec` is provisioner input

In many cases, translation will be nearly one-to-one:

```python
def job_to_workload(job: JobSpec) -> WorkloadSpec:
    return WorkloadSpec(
        name=job.name,
        command=job.command,
        script_text=job.script_text,
        cwd=job.cwd,
        env=job.env,
        backend_options={
            **job.backend_options,
            "dependencies": job.dependencies,
        },
    )
```

That lets:

- local batch execution run the same planned job payload
- SLURM submit the same planned job payload
- future backends adapt the same planning output

## Current Implicit Fields Already Present

The current code already computes most of this.

Examples:

- source selection and overrides:
  - `etl/source_control/runtime.py`
- pipeline asset refs:
  - `etl/pipeline_assets.py`
- resolved remote/local paths and batch grouping:
  - `etl/executors/slurm/executor.py`

So the next implementation step is not inventing new semantics.
It is moving existing resolved state into first-class types.

## Recommended Implementation Order

1. add `RunSpec` dataclasses in a neutral module
2. add `JobSpec` dataclasses in the same module
3. extract a builder that creates `RunSpec` from pipeline + context
4. extract a builder that creates `JobSpec` list from `RunSpec`
5. adapt local batch and SLURM to consume `JobSpec`
6. keep in-process local execution as a convenience mode until parity is proven

## Near-Term Goal

The near-term goal is:

- one shared job creation path
- multiple execution/provisioner paths

That is the main mechanism for enforcing:

- same behavior locally and remotely
- same path/source/runtime assumptions
- different performance, not different semantics

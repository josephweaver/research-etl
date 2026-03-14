# Provisioner Design

## Purpose

The provisioner layer is the compute-substrate boundary between ETL orchestration and where work actually gets placed.

It exists to answer these questions:

- How do we submit work to this substrate?
- How do we query workload status?
- How do we cancel work?
- What substrate features are available?

Examples of provisioners:

- SLURM
- local process placement
- VM-backed execution
- Kubernetes

## Layering

The intended split is:

- `etl.executors`
  - ETL orchestration, source selection, run metadata, pipeline planning

- `etl.provisioners`
  - substrate-specific workload placement and lifecycle

- `etl.transports`
  - command/file/session delivery

- `etl.runners`
  - shell/OS rendering

Executors may compose provisioners, and provisioners may use transports and runners internally.

## Current Direction

The initial interface lives in:

- `etl/provisioners/base.py`
- `etl/provisioners/local.py`
- `etl/provisioners/slurm.py`

Core types:

- `WorkloadSpec`
- `ProvisionHandle`
- `ProvisionStatus`
- `ProvisionState`
- `Provisioner`

Current concrete implementations:

- `LocalProvisioner`
  - runs local command/script workloads through a local transport
  - supports synchronous execution plus detached background sessions
- `SlurmProvisioner`
  - submits rendered sbatch scripts through a transport
  - runs `sbatch`, `squeue`, and `scancel`

## Boundary

Provisioners should own:

- workload submission
- substrate status lookup
- substrate cancellation
- substrate capability exposure

Provisioners should not own:

- ETL pipeline planning
- variable solving
- git/source-control logic
- low-level SSH/SCP mechanics
- shell syntax rules

## Migration Direction

Near term:

1. Keep executors focused on ETL orchestration.
2. Move substrate-specific submission/status/cancel logic into provisioners.
3. Let provisioners depend on transports/runners or direct APIs as needed.
4. Keep SLURM as the first validating implementation before adding VM and Kubernetes provisioners.

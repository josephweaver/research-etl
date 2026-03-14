# Provisioners

## Purpose

Provisioners are compute-substrate adapters.

They answer questions like:

- how do we submit work to this substrate?
- how do we check status?
- how do we cancel work?
- what substrate features are available?

Examples include:

- SLURM
- local process placement
- VM-backed execution
- Kubernetes

## Current Files

- `base.py`
  - provisioner protocol and workload/status/handle/resource types
- `local.py`
  - `LocalProvisioner`, for synchronous or detached local workload placement
- `slurm.py`
  - `SlurmProvisioner`, which submits rendered sbatch scripts and manages basic SLURM lifecycle commands

## Layering

Provisioners sit between executors and transports.

- executors own ETL orchestration and run metadata
- provisioners own workload placement on a specific substrate
- provisioners may use transports and runners internally

For example:

- `SlurmProvisioner` uses a transport to stage scripts and invoke `sbatch`, `squeue`, and `scancel`

## Non-Goals

Provisioners should not own:

- ETL pipeline planning
- variable solving
- git/source checkout logic
- low-level shell syntax rules

## Direction

The initial implementation is intentionally narrow.

Right now `SlurmProvisioner` owns:

- sbatch script placement
- submission
- status lookup
- cancel

`LocalProvisioner` gives a second concrete implementation so workload expectations can be tested locally before being pushed into remote schedulers.

Resource calculation and script rendering still largely live in the SLURM executor and can be extracted further if this boundary proves useful.

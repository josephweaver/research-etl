# Source Control

## Purpose

The source-control layer owns shared checkout and check-in intent.

It is responsible for operations like:

- resolve execution source metadata
- prepare checkout commands for ETL and pipeline asset repos
- check in a single generated file back to a repo
- translate caller-provided source overrides into normalized git behavior

## Current Files

- `base.py`
  - source-control data structures and abstractions
- `git_provider.py`
  - git-backed source provider implementation
- `runtime.py`
  - transport-aware checkout and single-file check-in helpers used by executors and plugins

## Layering

Source control sits above transports and runners.

- transports handle command delivery and file movement
- runners handle shell syntax
- source control decides what git actions should happen

This keeps checkout/check-in logic shared across:

- SLURM
- direct SSH execution
- local workflows
- plugins that need repo-backed updates

## Non-Goals

Source control should not own:

- SSH/SCP/tmux mechanics
- scheduler submission
- ETL pipeline planning
- executor run metadata

## Direction

The goal is to keep git behavior centralized so executors and plugins do not duplicate:

- repo/ref resolution
- checkout command assembly
- check-in/push behavior
- commandline source override handling

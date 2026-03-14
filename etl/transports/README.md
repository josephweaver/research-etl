# Transports

## Purpose

Transports are the execution and file-movement boundary for local or remote targets.

They answer questions like:

- how do we run a command on this target?
- how do we run a script body on this target?
- how do we upload or download files?
- how do we manage long-lived sessions like tunnels?
- how do we handle timeouts, cancellation, and streaming logs?

## Current Files

- `base.py`
  - transport protocol and common result/option types
- `local.py`
  - local process transport
- `ssh.py`
  - SSH transport with command execution, file transfer, and tmux-backed background sessions

## Layering

Transports sit below source control and provisioners.

- source control uses transports to perform git operations
- provisioners may use transports to reach a scheduler or remote machine
- executors should not own low-level SSH/SCP/session mechanics long term

## Capabilities

The transport interface is intentionally broader than command execution.

Current design includes:

- `run(...)`
- `run_text(...)`
- `put_file(...)`
- `put_text(...)`
- `fetch_file(...)`
- background session lifecycle
- timeout, cancel, and log streaming support

## Non-Goals

Transports should not decide:

- ETL resource planning
- SLURM/Kubernetes/VM scheduling policy
- ETL variable/path planning
- pipeline orchestration

## Related Docs

- repo root: `TRANSPORT.md`

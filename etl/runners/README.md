# Runners

## Purpose

Runners are shell and OS adapters.

They are responsible for rendering commands into shell-safe text for a specific environment, including:

- command quoting
- environment export syntax
- cwd changes
- shell-local error handling
- shell-specific path rendering when needed

Runners do not execute commands themselves.

## Current Files

- `base.py`
  - shared runner protocol and callback/token types
- `posix_shell.py`
  - POSIX shell renderer for Bash-like environments

## Layering

Runners sit below transports.

- transports decide how commands get delivered or executed
- runners decide what the command/script syntax should look like for a target shell

Example:

- `SshTransport` may use `PosixShellRunner` when it needs shell text for a Linux target

## Non-Goals

Runners should not own:

- process execution
- file transfer
- scheduler APIs
- ETL pipeline orchestration
- git/source-control intent

## Direction

Keep runners small and syntax-focused.

If a behavior is about:

- delivery, retries, sessions, or copying files: it belongs in transports
- workload placement: it belongs in provisioners
- git checkout/check-in intent: it belongs in source control

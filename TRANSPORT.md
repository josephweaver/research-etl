# Transport Design

## Purpose

The transport layer is the execution and file-movement boundary between Research ETL and a target environment.

It exists to answer these questions:

- How do we run a command on this target?
- How do we run a script body on this target?
- How do we move a file or text payload to or from this target?
- How do we start and manage long-lived background sessions such as SSH tunnels?
- How do we stream logs, apply timeouts, and handle cancellation consistently?

The transport layer should be reusable by executors and plugins. It should not be tied to SLURM-only behavior.

## Layering

The current intended split is:

- `etl.runners`
  - Shell/OS adapters.
  - Responsible for rendering shell syntax, exports, quoting, and shell-specific behavior.
  - Example: `PosixShellRunner`.

- `etl.transports`
  - Delivery and execution channels.
  - Responsible for local process execution, SSH execution, file movement, and background session lifecycle.
  - Examples: local process transport, SSH transport.

- `etl.source_control`
  - Shared git/source-control operations built on top of transports and runners.
  - Responsible for checkout/check-in intent, not SSH/SCP/tmux mechanics.

- Executors
  - Responsible for planning, orchestration, resource decisions, and run metadata.
  - They should not own low-level SSH/SCP/tunnel/git mechanics long term.

## Non-Goals

The transport layer is not intended to:

- plan dependency graphs
- decide SLURM resource settings
- own ETL variable/path planning
- replace the variable solver
- become a generic template engine

## Design Principles

1. Keep lifecycle simple by default.
   Start with one-shot transports. Add persistent connections or sessions only when performance justifies it.

2. Separate process invocation from script execution.
   `run(...)` means argv/process execution.
   `run_text(...)` means shell/interpreter script execution in one session.

3. Make file movement first-class.
   Transports must support file and text upload/download, not just process execution.

4. Make background sessions first-class.
   Tunnels and server-like sessions are transport concerns and should be represented explicitly.

5. Make timeouts, cancellation, and streaming logs part of the contract.

6. Preserve transport-specific freedom.
   The transport decides how `run_text(...)` is implemented.
   For example, SSH may stream via stdin, use `bash -lc`, or persist a temp file depending on context.

## Current Decisions

### Transport lifecycle

- Default to simple one-shot behavior.
- If performance requires it later, transports may add connection/session reuse modes.
- If that happens, it should be a transport concern, not exposed as executor-specific one-off behavior.

### `run(...)` vs `run_text(...)`

- `run(...)`
  - process/argv execution
  - no shell semantics required
  - preferred for direct command invocation like `git`, `scp`, `sbatch`, `python`

- `run_text(...)`
  - executes script text in one shell/interpreter session
  - required when shell state matters:
    - `cd`
    - `export`
    - `trap`
    - loops/conditionals/heredocs

`run_text(...)` must not be implemented as “split into lines and call `run(...)` repeatedly”.

### SSH script execution

Initial recommendation:

- default to non-persistent script execution via stdin or `bash -lc`
- use persisted temp files only when needed:
  - scripts must remain on disk
  - scripts are submitted to another tool such as `sbatch`
  - debugging/postmortem requires the file to remain

### `put_text(...)`

`put_text(...)` should be atomic by contract:

- create parent directories
- write/upload to a temp file first
- move into final destination
- normalize newline style
- use explicit encoding, default UTF-8 unless overridden

### Background sessions

Background sessions are first-class. Typical examples:

- SSH DB tunnel
- long-lived helper process
- server-like process that remains active until dismissed

Minimum intended operations:

- `start_background_session(...)`
- `dismiss_background_session(...)`
- `background_session_status(...)`

### Timeouts

Timeout support should include, when relevant:

- connect timeout
- idle timeout
- total timeout

Not every transport must implement every timeout mode the same way, but the transport owns the behavior.

### Cancellation

- Cancellation is transport-owned.
- Shell-specific cancellation/trap behavior may delegate to the runner.
- Transport may terminate/kill underlying local or remote process/session depending on implementation.

### Logging

Streaming logs are part of the transport contract.

The transport layer should support:

- real-time stdout/stderr line streaming
- lifecycle logging for:
  - command start/finish
  - transfer start/finish
  - session start/dismiss/status

The current callback shape can remain lightweight, but the design should allow structured events later.

### File transfer scope

Single-file support is sufficient for the first pass.

Desired metadata support:

- chmod/mode
- chown/owner
- chgrp/group

Plugins may eventually consume this layer directly, so file transfer should not stay executor-only.

### Error model

- Nonzero process exit may return a result object.
- Transport-layer failures should raise `TransportError`.
- Runner-level failures should be wrapped and raised as transport/source-control errors at the appropriate boundary.

### Capabilities

Transports should expose capabilities rather than assuming every transport supports every feature.

Example capabilities:

- `run`
- `run_text`
- `put_file`
- `put_text`
- `fetch_file`
- `background_sessions`
- `stream_logs`
- `chmod`
- `chown`
- `persistent_connections`

### Configuration ownership

Target configuration belongs in `config/environments.yml` and related execution environment config.

Examples:

- host/user/port/jump
- timeout values
- connection mode
- remote repo roots
- transport-specific extra options

## Proposed Interface Direction

This is a design target, not yet a finalized implementation.

### Runner

```python
class Runner(Protocol):
    def render(
        self,
        command: str,
        *,
        cwd: str | None = None,
        env: Mapping[str, str] | None = None,
        on_error: str | None = None,
        timeout_seconds: float | None = None,
        cancel_token: CancelToken | None = None,
        log_callback: LogCallback | None = None,
    ) -> list[str]:
        ...
```

Runner scope:

- quoting
- shell syntax
- env export syntax
- shell-local helpers
- path localization helpers when needed

### Execution options

```python
@dataclass(frozen=True)
class ExecutionOptions:
    connect_timeout_seconds: float | None = None
    idle_timeout_seconds: float | None = None
    total_timeout_seconds: float | None = None
    cancel_token: CancelToken | None = None
    log_callback: LogCallback | None = None
    stream_output: bool = True
```

### Results

```python
@dataclass(frozen=True)
class CommandResult:
    argv: tuple[str, ...]
    returncode: int
    stdout: str = ""
    stderr: str = ""
    timed_out: bool = False
    cancelled: bool = False
    started_at: str | None = None
    ended_at: str | None = None
    duration_seconds: float | None = None
```

```python
@dataclass(frozen=True)
class FileTransferResult:
    source: str
    destination: str
    ok: bool
    details: str = ""
```

```python
@dataclass(frozen=True)
class BackgroundSessionHandle:
    session_name: str
    transport: str
    target: str | None = None
    dismiss_hint: str | None = None
```

### Transport

```python
class CommandTransport(Protocol):
    def capabilities(self) -> dict[str, bool]:
        ...

    def run(
        self,
        argv: list[str],
        *,
        cwd: str | Path | None = None,
        env: Mapping[str, str] | None = None,
        check: bool = False,
        options: ExecutionOptions | None = None,
    ) -> CommandResult:
        ...

    def run_text(
        self,
        text: str,
        *,
        cwd: str | None = None,
        env: Mapping[str, str] | None = None,
        shell: str | None = None,
        check: bool = False,
        options: ExecutionOptions | None = None,
    ) -> CommandResult:
        ...

    def put_file(
        self,
        source: str | Path,
        destination: str | Path,
        *,
        mode: str | None = None,
        owner: str | None = None,
        group: str | None = None,
        options: ExecutionOptions | None = None,
    ) -> FileTransferResult:
        ...

    def put_text(
        self,
        text: str,
        destination: str | Path,
        *,
        atomic: bool = True,
        create_parents: bool = True,
        newline: str = "\\n",
        encoding: str = "utf-8",
        mode: str | None = None,
        owner: str | None = None,
        group: str | None = None,
        options: ExecutionOptions | None = None,
    ) -> FileTransferResult:
        ...

    def fetch_file(
        self,
        source: str | Path,
        destination: str | Path,
        *,
        options: ExecutionOptions | None = None,
    ) -> FileTransferResult:
        ...

    def start_background_session(
        self,
        spec: BackgroundSessionSpec,
        *,
        options: ExecutionOptions | None = None,
    ) -> BackgroundSessionHandle:
        ...

    def dismiss_background_session(
        self,
        handle: BackgroundSessionHandle,
        *,
        options: ExecutionOptions | None = None,
    ) -> CommandResult:
        ...

    def background_session_status(
        self,
        handle: BackgroundSessionHandle,
        *,
        options: ExecutionOptions | None = None,
    ) -> dict[str, Any]:
        ...
```

## Fast Followers

These are intentionally deferred until the transport interface stabilizes:

- first-class `ScriptBuilder`
- persistent SSH connection/session modes
- shared SSH transport replacing duplicated executor SSH/SCP/tmux code
- richer structured transport event model
- directory sync/mirror operations

## Migration Direction

Near-term migration target:

1. Stabilize transport interfaces.
2. Move shared SSH/SCP/tmux behavior from executors into `etl.transports`.
3. Keep executor code focused on orchestration and planning.
4. Add `ScriptBuilder` as a follow-up once transport behavior is stable.


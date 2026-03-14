from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Mapping, Optional, Protocol

from ..runners import CancelToken, LogCallback, Runner


class TransportError(RuntimeError):
    """Raised when a command transport cannot execute or render commands."""


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


@dataclass(frozen=True)
class ExecutionOptions:
    connect_timeout_seconds: Optional[float] = None
    idle_timeout_seconds: Optional[float] = None
    total_timeout_seconds: Optional[float] = None
    cancel_token: Optional[CancelToken] = None
    log_callback: Optional[LogCallback] = None
    stream_output: bool = True


@dataclass(frozen=True)
class FileTransferResult:
    source: str
    destination: str
    ok: bool
    details: str = ""
    started_at: str | None = None
    ended_at: str | None = None
    duration_seconds: float | None = None


@dataclass(frozen=True)
class BackgroundSessionSpec:
    command: str
    session_name: str
    cwd: Optional[str] = None
    env: Optional[Mapping[str, str]] = None
    keep_alive: bool = True


@dataclass(frozen=True)
class BackgroundSessionHandle:
    session_name: str
    transport: str
    target: Optional[str] = None
    dismiss_hint: Optional[str] = None


@dataclass(frozen=True)
class BackgroundSessionStatus:
    session_name: str
    running: bool
    transport: str
    target: Optional[str] = None
    details: str = ""


class CommandTransport(Protocol):
    """Execution-channel interface for local/remote command delivery."""

    def capabilities(self) -> dict[str, bool]: ...

    def run(
        self,
        argv: list[str],
        *,
        cwd: Optional[str | Path] = None,
        env: Optional[Mapping[str, str]] = None,
        check: bool = False,
        options: Optional[ExecutionOptions] = None,
    ) -> CommandResult: ...

    def run_text(
        self,
        text: str,
        *,
        cwd: Optional[str] = None,
        env: Optional[Mapping[str, str]] = None,
        shell: Optional[str] = None,
        check: bool = False,
        options: Optional[ExecutionOptions] = None,
    ) -> CommandResult: ...

    def render(
        self,
        runner: Runner,
        command: str,
        *,
        cwd: Optional[str] = None,
        env: Optional[Mapping[str, str]] = None,
        on_error: Optional[str] = None,
        options: Optional[ExecutionOptions] = None,
    ) -> list[str]: ...

    def put_file(
        self,
        source: str | Path,
        destination: str | Path,
        *,
        mode: str | None = None,
        owner: str | None = None,
        group: str | None = None,
        options: Optional[ExecutionOptions] = None,
    ) -> FileTransferResult: ...

    def put_text(
        self,
        text: str,
        destination: str | Path,
        *,
        atomic: bool = True,
        create_parents: bool = True,
        newline: str = "\n",
        encoding: str = "utf-8",
        mode: str | None = None,
        owner: str | None = None,
        group: str | None = None,
        options: Optional[ExecutionOptions] = None,
    ) -> FileTransferResult: ...

    def fetch_file(
        self,
        source: str | Path,
        destination: str | Path,
        *,
        options: Optional[ExecutionOptions] = None,
    ) -> FileTransferResult: ...

    def start_background_session(
        self,
        spec: BackgroundSessionSpec,
        *,
        options: Optional[ExecutionOptions] = None,
    ) -> BackgroundSessionHandle: ...

    def dismiss_background_session(
        self,
        handle: BackgroundSessionHandle,
        *,
        options: Optional[ExecutionOptions] = None,
    ) -> CommandResult: ...

    def background_session_status(
        self,
        handle: BackgroundSessionHandle,
        *,
        options: Optional[ExecutionOptions] = None,
    ) -> BackgroundSessionStatus: ...

    def render_background_session(
        self,
        runner: Runner,
        spec: BackgroundSessionSpec,
        *,
        options: Optional[ExecutionOptions] = None,
    ) -> list[str]: ...

    def render_dismiss_background_session(
        self,
        runner: Runner,
        handle: BackgroundSessionHandle,
        *,
        options: Optional[ExecutionOptions] = None,
    ) -> list[str]: ...

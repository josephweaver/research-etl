from __future__ import annotations

import subprocess
import threading
import time
import shutil
from datetime import datetime, timezone
from pathlib import Path
from typing import Mapping, Optional

from ..runners import Runner
from .base import (
    BackgroundSessionHandle,
    BackgroundSessionSpec,
    BackgroundSessionStatus,
    CommandResult,
    CommandTransport,
    ExecutionOptions,
    FileTransferResult,
    TransportError,
)


class LocalProcessTransport(CommandTransport):
    """Run commands on the current machine via subprocess."""

    def __init__(self) -> None:
        self._sessions: dict[str, subprocess.Popen] = {}

    def capabilities(self) -> dict[str, bool]:
        return {
            "run": True,
            "run_text": True,
            "put_file": True,
            "put_text": True,
            "fetch_file": True,
            "background_sessions": True,
            "stream_logs": True,
            "chmod": False,
            "chown": False,
            "persistent_connections": False,
        }

    def run(
        self,
        argv: list[str],
        *,
        cwd: Optional[str | Path] = None,
        env: Optional[Mapping[str, str]] = None,
        check: bool = False,
        options: Optional[ExecutionOptions] = None,
    ) -> CommandResult:
        opts = options or ExecutionOptions()
        started_ts = datetime.now(timezone.utc)
        started = time.monotonic()
        try:
            proc = subprocess.Popen(
                argv,
                cwd=str(cwd) if cwd is not None else None,
                env=dict(env) if env is not None else None,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                text=True,
            )
        except FileNotFoundError as exc:
            raise TransportError(f"Executable not found: {argv[0] if argv else '<empty>'}") from exc
        stdout_buf: list[str] = []
        stderr_buf: list[str] = []

        def _reader(stream, bucket: list[str], stream_name: str) -> None:
            if stream is None:
                return
            try:
                for line in iter(stream.readline, ""):
                    if line == "":
                        break
                    bucket.append(line)
                    if opts.log_callback and opts.stream_output:
                        level = "ERROR" if stream_name == "stderr" else "INFO"
                        opts.log_callback(level, line.rstrip("\r\n"))
            finally:
                try:
                    stream.close()
                except Exception:
                    pass

        t_out = threading.Thread(target=_reader, args=(proc.stdout, stdout_buf, "stdout"), daemon=True)
        t_err = threading.Thread(target=_reader, args=(proc.stderr, stderr_buf, "stderr"), daemon=True)
        t_out.start()
        t_err.start()

        started = time.monotonic()
        timed_out = False
        cancelled = False
        while True:
            rc = proc.poll()
            if rc is not None:
                break
            if opts.cancel_token is not None and opts.cancel_token.is_cancelled():
                cancelled = True
                proc.terminate()
                break
            if opts.total_timeout_seconds not in (None, 0) and (time.monotonic() - started) > float(opts.total_timeout_seconds):
                timed_out = True
                proc.kill()
                break
            time.sleep(0.05)

        try:
            rc = proc.wait(timeout=2)
        except subprocess.TimeoutExpired:
            proc.kill()
            rc = proc.wait(timeout=2)
        t_out.join(timeout=2)
        t_err.join(timeout=2)
        ended_ts = datetime.now(timezone.utc)
        result = CommandResult(
            argv=tuple(str(x) for x in argv),
            returncode=int(rc),
            stdout="".join(stdout_buf),
            stderr="".join(stderr_buf),
            timed_out=timed_out,
            cancelled=cancelled,
            started_at=started_ts.isoformat(),
            ended_at=ended_ts.isoformat(),
            duration_seconds=max(0.0, time.monotonic() - started),
        )
        if check and result.returncode != 0:
            if result.cancelled:
                raise TransportError("command cancelled")
            if result.timed_out:
                raise TransportError(f"command timed out after {opts.total_timeout_seconds}s")
            detail = result.stderr.strip() or result.stdout.strip() or "command failed"
            raise TransportError(detail)
        return result

    def run_text(
        self,
        text: str,
        *,
        cwd: Optional[str] = None,
        env: Optional[Mapping[str, str]] = None,
        shell: Optional[str] = None,
        check: bool = False,
        options: Optional[ExecutionOptions] = None,
    ) -> CommandResult:
        shell_cmd = str(shell or "bash")
        if shell_cmd.lower() in {"powershell", "pwsh"}:
            argv = [shell_cmd, "-Command", str(text)]
        else:
            argv = [shell_cmd, "-lc", str(text)]
        return self.run(argv, cwd=cwd, env=env, check=check, options=options)

    def render(
        self,
        runner: Runner,
        command: str,
        *,
        cwd: Optional[str] = None,
        env: Optional[Mapping[str, str]] = None,
        on_error: Optional[str] = None,
        options: Optional[ExecutionOptions] = None,
    ) -> list[str]:
        opts = options or ExecutionOptions()
        return runner.render(
            command,
            cwd=cwd,
            env=env,
            on_error=on_error,
            timeout_seconds=opts.total_timeout_seconds,
            cancel_token=opts.cancel_token,
            log_callback=opts.log_callback,
        )

    def put_file(
        self,
        source: str | Path,
        destination: str | Path,
        *,
        mode: str | None = None,
        owner: str | None = None,
        group: str | None = None,
        options: Optional[ExecutionOptions] = None,
    ) -> FileTransferResult:
        _ = (options, mode, owner, group)
        started = time.monotonic()
        started_ts = datetime.now(timezone.utc)
        src = Path(source).expanduser().resolve()
        dst = Path(destination).expanduser()
        dst.parent.mkdir(parents=True, exist_ok=True)
        shutil.copy2(src, dst)
        ended_ts = datetime.now(timezone.utc)
        return FileTransferResult(
            source=str(src),
            destination=str(dst),
            ok=True,
            started_at=started_ts.isoformat(),
            ended_at=ended_ts.isoformat(),
            duration_seconds=max(0.0, time.monotonic() - started),
        )

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
    ) -> FileTransferResult:
        _ = (options, mode, owner, group)
        started = time.monotonic()
        started_ts = datetime.now(timezone.utc)
        dst = Path(destination).expanduser()
        if create_parents:
            dst.parent.mkdir(parents=True, exist_ok=True)
        normalized = str(text).replace("\r\n", "\n").replace("\r", "\n")
        if newline != "\n":
            normalized = normalized.replace("\n", newline)
        target = dst
        tmp = dst.with_name(f"{dst.name}.tmp")
        if atomic:
            target = tmp
        target.write_text(normalized, encoding=encoding, newline="")
        if atomic:
            target.replace(dst)
        ended_ts = datetime.now(timezone.utc)
        return FileTransferResult(
            source="<text>",
            destination=str(dst),
            ok=True,
            started_at=started_ts.isoformat(),
            ended_at=ended_ts.isoformat(),
            duration_seconds=max(0.0, time.monotonic() - started),
        )

    def fetch_file(
        self,
        source: str | Path,
        destination: str | Path,
        *,
        options: Optional[ExecutionOptions] = None,
    ) -> FileTransferResult:
        return self.put_file(source, destination, options=options)

    def start_background_session(
        self,
        spec: BackgroundSessionSpec,
        *,
        options: Optional[ExecutionOptions] = None,
    ) -> BackgroundSessionHandle:
        opts = options or ExecutionOptions()
        proc = subprocess.Popen(
            str(spec.command),
            cwd=str(spec.cwd) if spec.cwd else None,
            env=dict(spec.env) if spec.env is not None else None,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
            shell=True,
        )
        self._sessions[spec.session_name] = proc
        if opts.log_callback:
            opts.log_callback("INFO", f"background session started: {spec.session_name}")
        return BackgroundSessionHandle(
            session_name=spec.session_name,
            transport="local",
            dismiss_hint=f"terminate pid={proc.pid}",
        )

    def dismiss_background_session(
        self,
        handle: BackgroundSessionHandle,
        *,
        options: Optional[ExecutionOptions] = None,
    ) -> CommandResult:
        opts = options or ExecutionOptions()
        proc = self._sessions.pop(handle.session_name, None)
        if proc is None:
            return CommandResult(argv=("dismiss", handle.session_name), returncode=0, stdout="", stderr="")
        proc.terminate()
        try:
            _ = proc.wait(timeout=2)
        except subprocess.TimeoutExpired:
            proc.kill()
            _ = proc.wait(timeout=2)
        if opts.log_callback:
            opts.log_callback("INFO", f"background session dismissed: {handle.session_name}")
        return CommandResult(argv=("dismiss", handle.session_name), returncode=0)

    def background_session_status(
        self,
        handle: BackgroundSessionHandle,
        *,
        options: Optional[ExecutionOptions] = None,
    ) -> BackgroundSessionStatus:
        _ = options
        proc = self._sessions.get(handle.session_name)
        running = bool(proc is not None and proc.poll() is None)
        return BackgroundSessionStatus(
            session_name=handle.session_name,
            running=running,
            transport="local",
            target=handle.target,
            details="" if running else "not running",
        )

    def render_background_session(
        self,
        runner: Runner,
        spec: BackgroundSessionSpec,
        *,
        options: Optional[ExecutionOptions] = None,
    ) -> list[str]:
        opts = options or ExecutionOptions()
        cmd = f"{spec.command} &"
        if spec.keep_alive:
            cmd = f"nohup {spec.command} >/dev/null 2>&1 &"
        return runner.render(
            cmd,
            cwd=spec.cwd,
            env=spec.env,
            timeout_seconds=opts.total_timeout_seconds,
            cancel_token=opts.cancel_token,
            log_callback=opts.log_callback,
        )

    def render_dismiss_background_session(
        self,
        runner: Runner,
        handle: BackgroundSessionHandle,
        *,
        options: Optional[ExecutionOptions] = None,
    ) -> list[str]:
        opts = options or ExecutionOptions()
        return runner.render(
            f"# dismiss background session: {handle.session_name}",
            timeout_seconds=opts.total_timeout_seconds,
            cancel_token=opts.cancel_token,
            log_callback=opts.log_callback,
        )

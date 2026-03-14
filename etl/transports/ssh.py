from __future__ import annotations

import logging
import shlex
import subprocess
import tempfile
import threading
import time
import os
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable, Mapping, Optional

from ..runners import Runner
from ..subprocess_logging import run_logged_subprocess
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

_LOG = logging.getLogger("etl.transports.ssh")


def build_target_host(ssh_user: Optional[str], ssh_host: Optional[str]) -> str:
    host = str(ssh_host or "").strip()
    user = str(ssh_user or "").strip()
    return f"{user + '@' if user else ''}{host}"


def build_ssh_common_args(
    *,
    ssh_jump: Optional[str],
    ssh_connect_timeout: int,
    ssh_strict_host_key_checking: Optional[str],
) -> list[str]:
    args: list[str] = []
    if ssh_jump:
        args += ["-J", str(ssh_jump)]
    args += [
        "-o",
        "BatchMode=yes",
        "-o",
        f"ConnectTimeout={int(ssh_connect_timeout)}",
        "-o",
        "ConnectionAttempts=1",
        "-o",
        "ServerAliveInterval=15",
        "-o",
        "ServerAliveCountMax=2",
    ]
    if ssh_strict_host_key_checking:
        args += ["-o", f"StrictHostKeyChecking={ssh_strict_host_key_checking}"]
    return args


def build_ssh_cmd(target: str, *remote_parts: str, common_args: Optional[list[str]] = None) -> list[str]:
    return ["ssh"] + list(common_args or []) + [target, *remote_parts]


def build_scp_cmd(*parts: str, common_args: Optional[list[str]] = None) -> list[str]:
    return ["scp"] + list(common_args or []) + list(parts)


def run_cmd_with_retries(
    cmd: list[str],
    *,
    timeout: int,
    retries: int,
    op_name: str,
    retry_delay_seconds: float,
    verbose: bool,
    error_factory: Callable[[str], Exception],
    logger: Optional[logging.Logger] = None,
) -> subprocess.CompletedProcess:
    log = logger or _LOG
    attempts = max(1, int(retries) + 1)
    last_timeout: Optional[subprocess.TimeoutExpired] = None
    for attempt in range(1, attempts + 1):
        try:
            proc = run_logged_subprocess(
                cmd,
                logger=log,
                action=op_name,
                timeout=timeout,
                check=False,
            )
        except subprocess.TimeoutExpired as exc:
            last_timeout = exc
            if attempt < attempts:
                if verbose:
                    print(f"{op_name} timeout (attempt {attempt}/{attempts}); retrying")
                time.sleep(float(retry_delay_seconds) * attempt)
                continue
            raise error_factory(f"{op_name} timed out after {timeout}s (attempt {attempt}/{attempts})") from exc

        if proc.returncode == 0:
            return proc

        if attempt < attempts:
            if verbose:
                err = (proc.stderr or proc.stdout or "").strip()
                print(f"{op_name} failed (attempt {attempt}/{attempts}): {err} | retrying")
            time.sleep(float(retry_delay_seconds) * attempt)
            continue
        return proc

    if last_timeout is not None:
        raise error_factory(f"{op_name} timed out after {attempts} attempts")
    raise error_factory(f"{op_name} failed after {attempts} attempts")


def _iso_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _total_timeout_seconds(options: Optional[ExecutionOptions], default: Optional[float]) -> Optional[float]:
    if options and options.total_timeout_seconds not in (None, 0):
        return float(options.total_timeout_seconds)
    if default not in (None, 0):
        return float(default)
    return None


def _command_result_to_completed_process(result: CommandResult) -> subprocess.CompletedProcess[str]:
    return subprocess.CompletedProcess(list(result.argv), int(result.returncode), result.stdout, result.stderr)


class SshTransport(CommandTransport):
    """Remote command transport abstraction for SSH-delivered execution."""

    def __init__(
        self,
        *,
        target: Optional[str] = None,
        ssh_host: Optional[str] = None,
        ssh_user: Optional[str] = None,
        ssh_jump: Optional[str] = None,
        ssh_connect_timeout: int = 30,
        ssh_strict_host_key_checking: Optional[str] = "accept-new",
        timeout_seconds: int = 120,
        retries: int = 0,
        retry_delay_seconds: float = 0.0,
        verbose: bool = False,
        logger: Optional[logging.Logger] = None,
    ) -> None:
        self.target = str(target or "").strip() or build_target_host(ssh_user, ssh_host)
        self.ssh_host = str(ssh_host or "").strip() or None
        self.ssh_user = str(ssh_user or "").strip() or None
        self.ssh_jump = str(ssh_jump or "").strip() or None
        self.ssh_connect_timeout = max(1, int(ssh_connect_timeout or 30))
        self.ssh_strict_host_key_checking = str(ssh_strict_host_key_checking or "").strip() or None
        self.timeout_seconds = max(1, int(timeout_seconds or 120))
        self.retries = max(0, int(retries or 0))
        self.retry_delay_seconds = max(0.0, float(retry_delay_seconds or 0.0))
        self.verbose = bool(verbose)
        self.logger = logger or _LOG

    def _require_target(self) -> str:
        if not self.target:
            raise TransportError("SSH transport requires target or ssh_host.")
        return self.target

    def capabilities(self) -> dict[str, bool]:
        return {
            "run": True,
            "run_text": True,
            "put_file": True,
            "put_text": True,
            "fetch_file": True,
            "background_sessions": True,
            "stream_logs": True,
            "chmod": True,
            "chown": True,
            "persistent_connections": False,
        }

    def _common_args(self) -> list[str]:
        return build_ssh_common_args(
            ssh_jump=self.ssh_jump,
            ssh_connect_timeout=self.ssh_connect_timeout,
            ssh_strict_host_key_checking=self.ssh_strict_host_key_checking,
        )

    def _ssh_cmd(self, *remote_parts: str) -> list[str]:
        return build_ssh_cmd(self._require_target(), *remote_parts, common_args=self._common_args())

    def _scp_cmd(self, *parts: str) -> list[str]:
        return build_scp_cmd(*parts, common_args=self._common_args())

    def _build_remote_script(self, text: str, *, cwd: Optional[str], env: Optional[Mapping[str, str]]) -> str:
        lines: list[str] = [
            "set -eo pipefail",
            "set +u; [ -f /etc/profile ] && source /etc/profile || true; set -u",
            "if ! command -v module >/dev/null 2>&1; then [ -f /usr/lmod/lmod/init/bash ] && source /usr/lmod/lmod/init/bash || true; fi",
        ]
        if cwd:
            lines.append(f"cd {shlex.quote(str(cwd))}")
        for key, value in dict(env or {}).items():
            lines.append(f"export {key}={shlex.quote(str(value))}")
        lines.append(str(text or ""))
        return "\n".join(lines)

    def _stream_process(
        self,
        cmd: list[str],
        *,
        options: ExecutionOptions,
        action: str,
    ) -> CommandResult:
        started_at = _iso_now()
        started = time.monotonic()
        proc = subprocess.Popen(
            cmd,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
            encoding="utf-8",
            errors="replace",
        )
        stdout_parts: list[str] = []
        stderr_parts: list[str] = []

        def _reader(stream, bucket: list[str], stream_name: str) -> None:
            if stream is None:
                return
            try:
                for line in iter(stream.readline, ""):
                    if line == "":
                        break
                    bucket.append(line)
                    text = line.rstrip("\r\n")
                    if text:
                        self.logger.info("[ssh][%s] %s", stream_name, text)
                        if options.log_callback and options.stream_output:
                            level = "ERROR" if stream_name == "stderr" else "INFO"
                            options.log_callback(level, text)
            finally:
                try:
                    stream.close()
                except Exception:
                    pass

        t_out = threading.Thread(target=_reader, args=(proc.stdout, stdout_parts, "stdout"), daemon=True)
        t_err = threading.Thread(target=_reader, args=(proc.stderr, stderr_parts, "stderr"), daemon=True)
        t_out.start()
        t_err.start()

        timed_out = False
        cancelled = False
        timeout = _total_timeout_seconds(options, self.timeout_seconds)
        while True:
            rc = proc.poll()
            if rc is not None:
                break
            if options.cancel_token is not None and options.cancel_token.is_cancelled():
                cancelled = True
                proc.terminate()
                break
            if timeout not in (None, 0) and (time.monotonic() - started) > float(timeout):
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
        return CommandResult(
            argv=tuple(str(x) for x in cmd),
            returncode=int(rc),
            stdout="".join(stdout_parts),
            stderr="".join(stderr_parts),
            timed_out=timed_out,
            cancelled=cancelled,
            started_at=started_at,
            ended_at=_iso_now(),
            duration_seconds=max(0.0, time.monotonic() - started),
        )

    def _invoke_with_retries(
        self,
        cmd: list[str],
        *,
        action: str,
        options: ExecutionOptions,
    ) -> CommandResult:
        attempts = max(1, int(self.retries) + 1)
        timeout = _total_timeout_seconds(options, self.timeout_seconds)
        last_error: Optional[Exception] = None
        for attempt in range(1, attempts + 1):
            try:
                if options.stream_output:
                    result = self._stream_process(cmd, options=options, action=action)
                else:
                    started_at = _iso_now()
                    started = time.monotonic()
                    proc = run_logged_subprocess(
                        cmd,
                        logger=self.logger,
                        action=action,
                        timeout=timeout,
                        check=False,
                    )
                    result = CommandResult(
                        argv=tuple(str(x) for x in cmd),
                        returncode=int(proc.returncode),
                        stdout=str(proc.stdout or ""),
                        stderr=str(proc.stderr or ""),
                        started_at=started_at,
                        ended_at=_iso_now(),
                        duration_seconds=max(0.0, time.monotonic() - started),
                    )
            except subprocess.TimeoutExpired as exc:
                last_error = exc
                if attempt >= attempts:
                    raise
                if self.verbose:
                    print(f"{action} timeout (attempt {attempt}/{attempts}); retrying")
                time.sleep(float(self.retry_delay_seconds) * attempt)
                continue

            if result.returncode == 0 or attempt >= attempts:
                return result
            if self.verbose:
                detail = result.stderr.strip() or result.stdout.strip()
                print(f"{action} failed (attempt {attempt}/{attempts}): {detail} | retrying")
            time.sleep(float(self.retry_delay_seconds) * attempt)

        if last_error is not None:
            raise last_error
        raise TransportError(f"{action} failed after {attempts} attempts")

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
        remote_cmd = shlex.join([str(x) for x in argv])
        remote_script = self._build_remote_script(remote_cmd, cwd=str(cwd) if cwd is not None else None, env=env)
        ssh_cmd = self._ssh_cmd(f"bash --login -lc {shlex.quote(remote_script)}")
        result = self._invoke_with_retries(ssh_cmd, action="ssh.run", options=opts)
        if check and result.returncode != 0:
            if result.cancelled:
                raise TransportError("ssh command cancelled")
            if result.timed_out:
                raise TransportError(f"ssh command timed out after {opts.total_timeout_seconds or self.timeout_seconds}s")
            detail = result.stderr.strip() or result.stdout.strip() or "ssh command failed"
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
        opts = options or ExecutionOptions()
        interpreter = str(shell or "bash --login").strip() or "bash --login"
        remote_script = self._build_remote_script(str(text), cwd=cwd, env=env)
        ssh_cmd = self._ssh_cmd(f"{interpreter} -lc {shlex.quote(remote_script)}")
        result = self._invoke_with_retries(ssh_cmd, action="ssh.run_text", options=opts)
        if check and result.returncode != 0:
            detail = result.stderr.strip() or result.stdout.strip() or "ssh script failed"
            raise TransportError(detail)
        return result

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
        opts = options or ExecutionOptions(stream_output=False)
        started_at = _iso_now()
        started = time.monotonic()
        src = Path(source).expanduser().resolve()
        dest_text = str(destination)
        dest_dir = Path(dest_text).parent.as_posix()
        target = self._require_target()
        self.run(
            ["mkdir", "-p", dest_dir],
            check=True,
            options=ExecutionOptions(
                total_timeout_seconds=opts.total_timeout_seconds,
                log_callback=opts.log_callback,
                stream_output=False,
            ),
        )
        scp_cmd = self._scp_cmd(str(src), f"{target}:{dest_text}")
        proc = _command_result_to_completed_process(
            self._invoke_scp_with_retries(scp_cmd, action="ssh.put_file", options=opts)
        )
        if proc.returncode != 0:
            raise TransportError((proc.stderr or proc.stdout or "").strip() or "scp upload failed")
        post_lines: list[str] = []
        if mode:
            post_lines.append(f"chmod {shlex.quote(str(mode))} {shlex.quote(dest_text)}")
        if owner or group:
            owner_group = f"{str(owner or '')}:{str(group or '')}".rstrip(":")
            post_lines.append(f"chown {shlex.quote(owner_group)} {shlex.quote(dest_text)}")
        if post_lines:
            self.run_text("\n".join(post_lines), check=True, options=ExecutionOptions(
                total_timeout_seconds=opts.total_timeout_seconds,
                log_callback=opts.log_callback,
                stream_output=False,
            ))
        return FileTransferResult(
            source=str(src),
            destination=dest_text,
            ok=True,
            started_at=started_at,
            ended_at=_iso_now(),
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
        normalized = str(text).replace("\r\n", "\n").replace("\r", "\n")
        if newline != "\n":
            normalized = normalized.replace("\n", newline)
        with tempfile.NamedTemporaryFile("w", encoding=encoding, newline="", delete=False) as tmp:
            tmp.write(normalized)
            tmp_path = Path(tmp.name)
        dest_text = str(destination)
        try:
            parent_dir = Path(dest_text).parent.as_posix()
            if create_parents and parent_dir not in ("", "."):
                self.run(
                    ["mkdir", "-p", parent_dir],
                    check=True,
                    options=ExecutionOptions(
                        total_timeout_seconds=(options.total_timeout_seconds if options else None),
                        log_callback=(options.log_callback if options else None),
                        stream_output=False,
                    ),
                )
            if not atomic:
                return self.put_file(tmp_path, dest_text, mode=mode, owner=owner, group=group, options=options)
            remote_tmp = f"{dest_text}.tmp"
            result = self.put_file(tmp_path, remote_tmp, options=options)
            post_lines = []
            post_lines.append(f"mv {shlex.quote(remote_tmp)} {shlex.quote(dest_text)}")
            if mode:
                post_lines.append(f"chmod {shlex.quote(str(mode))} {shlex.quote(dest_text)}")
            if owner or group:
                owner_group = f"{str(owner or '')}:{str(group or '')}".rstrip(":")
                post_lines.append(f"chown {shlex.quote(owner_group)} {shlex.quote(dest_text)}")
            self.run_text("\n".join(post_lines), check=True, options=ExecutionOptions(
                total_timeout_seconds=(options.total_timeout_seconds if options else None),
                log_callback=(options.log_callback if options else None),
                stream_output=False,
            ))
            return FileTransferResult(
                source=result.source,
                destination=dest_text,
                ok=True,
                started_at=result.started_at,
                ended_at=_iso_now(),
                duration_seconds=result.duration_seconds,
            )
        finally:
            try:
                tmp_path.unlink(missing_ok=True)
            except Exception:
                pass

    def fetch_file(
        self,
        source: str | Path,
        destination: str | Path,
        *,
        options: Optional[ExecutionOptions] = None,
    ) -> FileTransferResult:
        opts = options or ExecutionOptions(stream_output=False)
        started_at = _iso_now()
        started = time.monotonic()
        dst = Path(destination).expanduser()
        dst.parent.mkdir(parents=True, exist_ok=True)
        scp_cmd = self._scp_cmd(f"{self._require_target()}:{str(source)}", str(dst))
        proc = _command_result_to_completed_process(
            self._invoke_scp_with_retries(scp_cmd, action="ssh.fetch_file", options=opts)
        )
        if proc.returncode != 0:
            raise TransportError((proc.stderr or proc.stdout or "").strip() or "scp fetch failed")
        return FileTransferResult(
            source=str(source),
            destination=str(dst),
            ok=True,
            started_at=started_at,
            ended_at=_iso_now(),
            duration_seconds=max(0.0, time.monotonic() - started),
        )

    def start_background_session(
        self,
        spec: BackgroundSessionSpec,
        *,
        options: Optional[ExecutionOptions] = None,
    ) -> BackgroundSessionHandle:
        self.run_text(
            "\n".join(self.render_background_session(_BarePosixRunner(), spec, options=options)),
            check=True,
            options=ExecutionOptions(
                total_timeout_seconds=(options.total_timeout_seconds if options else None),
                log_callback=(options.log_callback if options else None),
                stream_output=False,
            ),
        )
        return BackgroundSessionHandle(
            session_name=spec.session_name,
            transport="ssh",
            target=self._require_target(),
            dismiss_hint=f"tmux kill-session -t {spec.session_name}",
        )

    def dismiss_background_session(
        self,
        handle: BackgroundSessionHandle,
        *,
        options: Optional[ExecutionOptions] = None,
    ) -> CommandResult:
        return self.run_text(
            "\n".join(self.render_dismiss_background_session(_BarePosixRunner(), handle, options=options)),
            check=False,
            options=ExecutionOptions(
                total_timeout_seconds=(options.total_timeout_seconds if options else None),
                log_callback=(options.log_callback if options else None),
                stream_output=False,
            ),
        )

    def background_session_status(
        self,
        handle: BackgroundSessionHandle,
        *,
        options: Optional[ExecutionOptions] = None,
    ) -> BackgroundSessionStatus:
        result = self.run_text(
            f"tmux has-session -t {shlex.quote(handle.session_name)} >/dev/null 2>&1",
            check=False,
            options=ExecutionOptions(
                total_timeout_seconds=(options.total_timeout_seconds if options else None),
                log_callback=(options.log_callback if options else None),
                stream_output=False,
            ),
        )
        return BackgroundSessionStatus(
            session_name=handle.session_name,
            running=result.returncode == 0,
            transport="ssh",
            target=handle.target or self.target,
            details="" if result.returncode == 0 else "not running",
        )

    def run_completed(
        self,
        argv: list[str],
        *,
        cwd: Optional[str | Path] = None,
        env: Optional[Mapping[str, str]] = None,
        check: bool = False,
        options: Optional[ExecutionOptions] = None,
    ) -> subprocess.CompletedProcess[str]:
        return _command_result_to_completed_process(
            self.run(argv, cwd=cwd, env=env, check=check, options=options)
        )

    def run_text_completed(
        self,
        text: str,
        *,
        cwd: Optional[str] = None,
        env: Optional[Mapping[str, str]] = None,
        shell: Optional[str] = None,
        check: bool = False,
        options: Optional[ExecutionOptions] = None,
    ) -> subprocess.CompletedProcess[str]:
        return _command_result_to_completed_process(
            self.run_text(text, cwd=cwd, env=env, shell=shell, check=check, options=options)
        )

    def put_file_completed(
        self,
        source: str | Path,
        destination: str | Path,
        *,
        mode: str | None = None,
        owner: str | None = None,
        group: str | None = None,
        options: Optional[ExecutionOptions] = None,
    ) -> subprocess.CompletedProcess[str]:
        result = self.put_file(source, destination, mode=mode, owner=owner, group=group, options=options)
        return subprocess.CompletedProcess(
            ["scp", os.fspath(source), f"{self._require_target()}:{os.fspath(destination)}"],
            0 if result.ok else 1,
            "",
            result.details,
        )

    def _invoke_scp_with_retries(
        self,
        cmd: list[str],
        *,
        action: str,
        options: ExecutionOptions,
    ) -> CommandResult:
        attempts = max(1, int(self.retries) + 1)
        timeout = _total_timeout_seconds(options, self.timeout_seconds)
        last_timeout: Optional[subprocess.TimeoutExpired] = None
        for attempt in range(1, attempts + 1):
            started_at = _iso_now()
            started = time.monotonic()
            try:
                proc = run_logged_subprocess(
                    cmd,
                    logger=self.logger,
                    action=action,
                    timeout=timeout,
                    check=False,
                )
            except subprocess.TimeoutExpired as exc:
                last_timeout = exc
                if attempt >= attempts:
                    raise
                if self.verbose:
                    print(f"{action} timeout (attempt {attempt}/{attempts}); retrying")
                time.sleep(float(self.retry_delay_seconds) * attempt)
                continue
            result = CommandResult(
                argv=tuple(str(x) for x in cmd),
                returncode=int(proc.returncode),
                stdout=str(proc.stdout or ""),
                stderr=str(proc.stderr or ""),
                started_at=started_at,
                ended_at=_iso_now(),
                duration_seconds=max(0.0, time.monotonic() - started),
            )
            if result.returncode == 0 or attempt >= attempts:
                return result
            if self.verbose:
                detail = result.stderr.strip() or result.stdout.strip()
                print(f"{action} failed (attempt {attempt}/{attempts}): {detail} | retrying")
            time.sleep(float(self.retry_delay_seconds) * attempt)
        if last_timeout is not None:
            raise last_timeout
        raise TransportError(f"{action} failed after {attempts} attempts")

    def render_background_session(
        self,
        runner: Runner,
        spec: BackgroundSessionSpec,
        *,
        options: Optional[ExecutionOptions] = None,
    ) -> list[str]:
        opts = options or ExecutionOptions()
        lines = []
        if spec.cwd:
            lines.extend(runner.render(f"cd {shlex.quote(spec.cwd)}", on_error="\"[etl][transport][ssh] cannot cd to session cwd\""))
        if spec.env:
            for key, value in spec.env.items():
                lines.extend(runner.render(f"export {key}={shlex.quote(str(value))}"))
        lines.extend(runner.render(f"tmux has-session -t {shlex.quote(spec.session_name)} >/dev/null 2>&1 && tmux kill-session -t {shlex.quote(spec.session_name)} >/dev/null 2>&1 || true"))
        lines.extend(
            runner.render(
                f"tmux new-session -d -s {shlex.quote(spec.session_name)} \"bash -lc {shlex.quote(spec.command)}\"",
                on_error="\"[etl][transport][ssh] failed to start background session\"",
                timeout_seconds=opts.total_timeout_seconds,
                cancel_token=opts.cancel_token,
                log_callback=opts.log_callback,
            )
        )
        return lines

    def render_dismiss_background_session(
        self,
        runner: Runner,
        handle: BackgroundSessionHandle,
        *,
        options: Optional[ExecutionOptions] = None,
    ) -> list[str]:
        opts = options or ExecutionOptions()
        return runner.render(
            f"tmux has-session -t {shlex.quote(handle.session_name)} >/dev/null 2>&1 && tmux kill-session -t {shlex.quote(handle.session_name)} >/dev/null 2>&1 || true",
            timeout_seconds=opts.total_timeout_seconds,
            cancel_token=opts.cancel_token,
            log_callback=opts.log_callback,
        )


class _BarePosixRunner:
    def render(
        self,
        command: str,
        *,
        cwd: Optional[str] = None,
        env: Optional[Mapping[str, str]] = None,
        on_error: Optional[str] = None,
        timeout_seconds: Optional[float] = None,
        cancel_token: Any = None,
        log_callback: Any = None,
    ) -> list[str]:
        _ = (timeout_seconds, cancel_token, log_callback)
        lines: list[str] = []
        if cwd:
            lines.append(f"cd {cwd} || {{ echo \"[etl][runner] cannot cd: {cwd}\" >&2; exit 1; }}")
        if env:
            for key, value in env.items():
                lines.append(f"export {key}={value}")
        cmd = str(command)
        if on_error:
            cmd = f"{cmd} || {{ echo {on_error} >&2; exit 1; }}"
        lines.append(cmd)
        return lines

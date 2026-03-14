from __future__ import annotations

from typing import Mapping, Optional

from .base import CancelToken, LogCallback, Runner


class PosixShellRunner(Runner):
    """Render commands as POSIX shell lines for batch/remote scripts."""

    def render(
        self,
        command: str,
        *,
        cwd: Optional[str] = None,
        env: Optional[Mapping[str, str]] = None,
        on_error: Optional[str] = None,
        timeout_seconds: Optional[float] = None,
        cancel_token: Optional[CancelToken] = None,
        log_callback: Optional[LogCallback] = None,
    ) -> list[str]:
        lines: list[str] = []
        if cwd:
            lines.append(f"cd {cwd} || {{ echo \"[etl][runner] cannot cd: {cwd}\" >&2; exit 1; }}")
        if env:
            for key, value in env.items():
                lines.append(f"export {key}={value}")
        if timeout_seconds not in (None, 0):
            lines.append(f"# timeout_seconds={float(timeout_seconds)}")
        if cancel_token is not None:
            lines.append("# cancel_token=provided")
        if log_callback is not None:
            lines.append("# log_callback=provided")
        cmd = str(command)
        if on_error:
            cmd = f"{cmd} || {{ echo {on_error} >&2; exit 1; }}"
        lines.append(cmd)
        return lines

from __future__ import annotations

import logging
import subprocess
import time
from typing import Callable, List, Optional

from ...subprocess_logging import run_logged_subprocess

_LOG = logging.getLogger("etl.executors.slurm.ssh")


def build_ssh_common_args(
    *,
    ssh_jump: Optional[str],
    ssh_connect_timeout: int,
    ssh_strict_host_key_checking: Optional[str],
) -> List[str]:
    args: List[str] = []
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


def build_ssh_cmd(target: str, *remote_parts: str, common_args: Optional[List[str]] = None) -> List[str]:
    return ["ssh"] + list(common_args or []) + [target, *remote_parts]


def build_scp_cmd(*parts: str, common_args: Optional[List[str]] = None) -> List[str]:
    return ["scp"] + list(common_args or []) + list(parts)


def run_cmd_with_retries(
    cmd: List[str],
    *,
    timeout: int,
    retries: int,
    op_name: str,
    retry_delay_seconds: float,
    verbose: bool,
    error_factory: Callable[[str], Exception],
) -> subprocess.CompletedProcess:
    attempts = max(1, int(retries) + 1)
    last_timeout: Optional[subprocess.TimeoutExpired] = None
    for attempt in range(1, attempts + 1):
        try:
            proc = run_logged_subprocess(
                cmd,
                logger=_LOG,
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


def build_target_host(ssh_user: Optional[str], ssh_host: Optional[str]) -> str:
    host = str(ssh_host or "").strip()
    user = str(ssh_user or "").strip()
    return f"{user + '@' if user else ''}{host}"

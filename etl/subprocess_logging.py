# research-etl
# Copyright (c) 2026 Joseph Weaver
# This file is part of the research-etl project and is licensed under the MIT License.
# You may not use this file except in compliance with the License.
# See https://github.com/josephweaver/research-etl for details.

from __future__ import annotations

import logging
import shlex
import subprocess
from pathlib import Path
from typing import Mapping, Sequence

from .app_logging import get_app_logger


def _cmd_text(cmd: Sequence[str]) -> str:
    return shlex.join([str(x) for x in cmd])


def run_logged_subprocess(
    cmd: Sequence[str],
    *,
    logger: logging.Logger | None = None,
    action: str = "subprocess",
    cwd: Path | None = None,
    env: Mapping[str, str] | None = None,
    timeout: int | None = None,
    text: bool = True,
    check: bool = False,
) -> subprocess.CompletedProcess:
    log = logger or get_app_logger("process")
    cmd_list = [str(x) for x in cmd]
    log.info("[%s] starting command: %s", action, _cmd_text(cmd_list))
    if cwd is not None:
        log.info("[%s] cwd=%s", action, str(cwd))

    proc = subprocess.run(
        cmd_list,
        cwd=str(cwd) if cwd else None,
        env=dict(env) if env is not None else None,
        capture_output=True,
        text=text,
        timeout=timeout,
        check=False,
    )

    if text:
        stdout_text = str(proc.stdout or "").strip()
        stderr_text = str(proc.stderr or "").strip()
        if stdout_text:
            for line in stdout_text.splitlines():
                log.info("[%s][stdout] %s", action, line)
        if stderr_text:
            for line in stderr_text.splitlines():
                log.info("[%s][stderr] %s", action, line)
    log.info("[%s] command finished rc=%s", action, proc.returncode)

    if check and proc.returncode != 0:
        raise subprocess.CalledProcessError(proc.returncode, cmd_list, output=proc.stdout, stderr=proc.stderr)
    return proc

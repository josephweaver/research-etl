# research-etl
# Copyright (c) 2026 Joseph Weaver
# This file is part of the research-etl project and is licensed under the MIT License.
# You may not use this file except in compliance with the License.
# See https://github.com/josephweaver/research-etl for details.

from __future__ import annotations

import logging
import shlex
import subprocess
import threading
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
    stream_output: bool = False,
) -> subprocess.CompletedProcess:
    log = logger or get_app_logger("process")
    cmd_list = [str(x) for x in cmd]
    log.info("[%s] starting command: %s", action, _cmd_text(cmd_list))
    if cwd is not None:
        log.info("[%s] cwd=%s", action, str(cwd))

    if stream_output and text:
        popen_kwargs = {
            "stdout": subprocess.PIPE,
            "stderr": subprocess.PIPE,
            "text": True,
            "bufsize": 1,
        }
        if cwd is not None:
            popen_kwargs["cwd"] = str(cwd)
        if env is not None:
            popen_kwargs["env"] = dict(env)

        proc = subprocess.Popen(cmd_list, **popen_kwargs)  # noqa: S603
        stdout_lines: list[str] = []
        stderr_lines: list[str] = []

        def _reader(pipe, bucket: list[str], stream_name: str) -> None:
            try:
                if pipe is None:
                    return
                for line in iter(pipe.readline, ""):
                    bucket.append(line)
                    log.info("[%s][%s] %s", action, stream_name, line.rstrip("\n"))
            finally:
                if pipe is not None:
                    pipe.close()

        stdout_thread = threading.Thread(
            target=_reader,
            args=(proc.stdout, stdout_lines, "stdout"),
            daemon=True,
        )
        stderr_thread = threading.Thread(
            target=_reader,
            args=(proc.stderr, stderr_lines, "stderr"),
            daemon=True,
        )
        stdout_thread.start()
        stderr_thread.start()

        try:
            proc.wait(timeout=timeout)
        except subprocess.TimeoutExpired:
            proc.kill()
            proc.wait()
            stdout_thread.join()
            stderr_thread.join()
            raise

        stdout_thread.join()
        stderr_thread.join()
        completed = subprocess.CompletedProcess(
            cmd_list,
            proc.returncode,
            "".join(stdout_lines),
            "".join(stderr_lines),
        )
        log.info("[%s] command finished rc=%s", action, completed.returncode)
        if check and completed.returncode != 0:
            raise subprocess.CalledProcessError(
                completed.returncode,
                cmd_list,
                output=completed.stdout,
                stderr=completed.stderr,
            )
        return completed

    run_kwargs = {
        "capture_output": True,
        "text": text,
        "check": False,
    }
    if cwd is not None:
        run_kwargs["cwd"] = str(cwd)
    if env is not None:
        run_kwargs["env"] = dict(env)
    if timeout is not None:
        run_kwargs["timeout"] = timeout

    proc = subprocess.run(cmd_list, **run_kwargs)

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


def spawn_logged_subprocess(
    cmd: Sequence[str],
    *,
    logger: logging.Logger | None = None,
    action: str = "subprocess",
    cwd: Path | None = None,
    env: Mapping[str, str] | None = None,
    stdout=None,
    stderr=None,
    text: bool = True,
) -> subprocess.Popen:
    log = logger or get_app_logger("process")
    cmd_list = [str(x) for x in cmd]
    log.info("[%s] spawning command: %s", action, _cmd_text(cmd_list))
    if cwd is not None:
        log.info("[%s] cwd=%s", action, str(cwd))

    popen_kwargs = {
        "stdout": stdout,
        "stderr": stderr,
        "text": text,
    }
    if cwd is not None:
        popen_kwargs["cwd"] = str(cwd)
    if env is not None:
        popen_kwargs["env"] = dict(env)
    return subprocess.Popen(cmd_list, **popen_kwargs)  # noqa: S603

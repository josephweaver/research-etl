from __future__ import annotations

import atexit
import os
import re
import socket
import subprocess
import threading
import time
from dataclasses import dataclass
from typing import Optional

from .db_urls import rewrite_tunneled_database_url


_FORWARD_RE = re.compile(r"(?P<prefix>(?:^|\s)-L\s+)(?P<port>\d+)(?=:)")


@dataclass
class _TunnelState:
    proc: subprocess.Popen
    host: str
    port: int
    command: str


_LOCK = threading.Lock()
_STATE: Optional[_TunnelState] = None
_ATEXIT_REGISTERED = False


def _flag_enabled(raw: Optional[str]) -> bool:
    text = str(raw or "").strip().lower()
    return text in {"1", "true", "yes", "on", "y"}


def db_tunnel_verbose_enabled() -> bool:
    return _flag_enabled(os.environ.get("ETL_DB_TUNNEL_VERBOSE")) or _flag_enabled(os.environ.get("ETL_DB_VERBOSE"))


def _log(message: str) -> None:
    if db_tunnel_verbose_enabled():
        print(f"[db_tunnel] {message}")


def _register_atexit() -> None:
    global _ATEXIT_REGISTERED
    if _ATEXIT_REGISTERED:
        return
    atexit.register(close_process_tunnel)
    _ATEXIT_REGISTERED = True


def _choose_free_port(host: str) -> int:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.bind((host, 0))
        return int(s.getsockname()[1])


def _rewrite_local_forward_command(command: str, *, local_port: int) -> str:
    text = str(command or "").strip()
    if not text:
        raise RuntimeError("db tunnel command is empty")
    if _FORWARD_RE.search(text):
        return _FORWARD_RE.sub(lambda m: f"{m.group('prefix')}{int(local_port)}", text, count=1)
    raise RuntimeError("db tunnel command must include an SSH local forward like '-L 6543:HOST:PORT'")


def _port_ready(host: str, port: int) -> bool:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.settimeout(0.5)
        return s.connect_ex((host, int(port))) == 0


def _wait_for_port(host: str, port: int, *, proc: subprocess.Popen, timeout_seconds: float = 20.0) -> None:
    deadline = time.time() + float(timeout_seconds)
    while time.time() < deadline:
        if proc.poll() is not None:
            raise RuntimeError(f"db tunnel process exited early rc={proc.returncode}")
        if _port_ready(host, port):
            return
        time.sleep(0.25)
    raise RuntimeError(f"db tunnel not ready at {host}:{port}")


def _build_launch_kwargs(command: str) -> tuple[list[str], dict[str, object]]:
    text = str(command or "").strip()
    if not text:
        raise RuntimeError("db tunnel command is empty")
    kwargs: dict[str, object] = {
        "stdout": subprocess.DEVNULL,
        "stderr": subprocess.DEVNULL,
        "stdin": subprocess.DEVNULL,
    }
    if os.name == "nt":
        kwargs["creationflags"] = getattr(subprocess, "CREATE_NEW_PROCESS_GROUP", 0)
        return ["cmd", "/c", text], kwargs
    kwargs["start_new_session"] = True
    return ["bash", "-lc", text], kwargs


def _start_tunnel(*, host: str, base_command: str) -> _TunnelState:
    local_port = _choose_free_port(host)
    command = _rewrite_local_forward_command(base_command, local_port=local_port)
    _log(f"starting process-local tunnel host={host} port={local_port}")
    argv, kwargs = _build_launch_kwargs(command)
    proc = subprocess.Popen(argv, **kwargs)
    _wait_for_port(host, local_port, proc=proc)
    _log(f"process-local tunnel ready pid={proc.pid} host={host} port={local_port}")
    return _TunnelState(proc=proc, host=host, port=local_port, command=command)


def _is_live(state: _TunnelState) -> bool:
    if state.proc.poll() is not None:
        return False
    return _port_ready(state.host, state.port)


def close_process_tunnel() -> None:
    global _STATE
    with _LOCK:
        state = _STATE
        _STATE = None
    if state is None:
        return
    try:
        if state.proc.poll() is None:
            _log(f"stopping process-local tunnel port={state.port}")
            state.proc.terminate()
            try:
                state.proc.wait(timeout=3)
            except subprocess.TimeoutExpired:
                state.proc.kill()
    except Exception:
        pass


def ensure_process_tunneled_database_url(raw_database_url: str) -> str:
    global _STATE
    mode = str(os.environ.get("ETL_DB_TUNNEL_MODE") or "").strip().lower()
    if mode != "process":
        return str(raw_database_url or "").strip()

    host = str(os.environ.get("ETL_DB_TUNNEL_HOST") or "127.0.0.1").strip() or "127.0.0.1"
    command = str(
        os.environ.get("ETL_DB_TUNNEL_COMMAND_RAW")
        or os.environ.get("ETL_DB_TUNNEL_COMMAND")
        or ""
    ).strip()
    if not command:
        raise RuntimeError("ETL_DB_TUNNEL_MODE=process requires ETL_DB_TUNNEL_COMMAND_RAW or ETL_DB_TUNNEL_COMMAND")

    _register_atexit()
    with _LOCK:
        if _STATE is None or not _is_live(_STATE):
            if _STATE is None:
                _log("no live process-local tunnel cached; creating one")
            else:
                _log(
                    f"cached process-local tunnel unhealthy pid={_STATE.proc.pid} host={_STATE.host} port={_STATE.port}; restarting"
                )
            if _STATE is not None:
                try:
                    if _STATE.proc.poll() is None:
                        _STATE.proc.terminate()
                except Exception:
                    pass
            _STATE = _start_tunnel(host=host, base_command=command)
        state = _STATE
        _log(f"using process-local tunnel pid={state.proc.pid} host={state.host} port={state.port}")
    return rewrite_tunneled_database_url(str(raw_database_url or "").strip(), host=state.host, port=state.port)


__all__ = [
    "ensure_process_tunneled_database_url",
    "close_process_tunnel",
    "_rewrite_local_forward_command",
    "_build_launch_kwargs",
]

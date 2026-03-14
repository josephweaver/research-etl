from __future__ import annotations

import threading
import time
from pathlib import Path

from etl.transports import (
    BackgroundSessionHandle,
    BackgroundSessionSpec,
    ExecutionOptions,
    LocalProcessTransport,
    SshTransport,
)
from etl.runners import PosixShellRunner


class _CancelToken:
    def __init__(self) -> None:
        self._flag = False

    def cancel(self) -> None:
        self._flag = True

    def is_cancelled(self) -> bool:
        return self._flag


def test_local_process_transport_streams_logs() -> None:
    transport = LocalProcessTransport()
    seen: list[tuple[str, str]] = []

    result = transport.run(
        ["python", "-c", "print('hello'); import sys; sys.stderr.write('oops\\n')"],
        options=ExecutionOptions(log_callback=lambda level, message: seen.append((level, message))),
    )

    assert result.returncode == 0
    assert ("INFO", "hello") in seen
    assert ("ERROR", "oops") in seen


def test_local_process_transport_times_out() -> None:
    transport = LocalProcessTransport()

    result = transport.run(
        ["python", "-c", "import time; time.sleep(2)"],
        options=ExecutionOptions(total_timeout_seconds=0.2),
    )

    assert result.timed_out is True
    assert result.returncode != 0


def test_local_process_transport_cancels() -> None:
    transport = LocalProcessTransport()
    token = _CancelToken()

    def _cancel_soon() -> None:
        time.sleep(0.2)
        token.cancel()

    thread = threading.Thread(target=_cancel_soon, daemon=True)
    thread.start()
    result = transport.run(
        ["python", "-c", "import time; time.sleep(2)"],
        options=ExecutionOptions(cancel_token=token),
    )
    thread.join(timeout=1)

    assert result.cancelled is True
    assert result.returncode != 0


def test_local_process_transport_put_and_fetch_file(tmp_path: Path) -> None:
    transport = LocalProcessTransport()
    src = tmp_path / "src.txt"
    mid = tmp_path / "mid" / "copy.txt"
    dst = tmp_path / "dst.txt"
    src.write_text("payload", encoding="utf-8")

    put = transport.put_file(src, mid)
    fetch = transport.fetch_file(mid, dst)

    assert put.ok is True
    assert fetch.ok is True
    assert dst.read_text(encoding="utf-8") == "payload"


def test_local_process_transport_run_text_and_put_text(tmp_path: Path) -> None:
    transport = LocalProcessTransport()
    result = transport.run_text("Write-Output 'ok'", shell="powershell")
    out_file = tmp_path / "nested" / "script.sh"
    write = transport.put_text("line1\nline2\n", out_file, newline="\n")

    assert result.returncode == 0
    assert result.stdout == "ok\n"
    assert write.ok is True
    assert out_file.read_text(encoding="utf-8") == "line1\nline2\n"


def test_local_process_transport_background_session_status() -> None:
    transport = LocalProcessTransport()
    handle = transport.start_background_session(
        BackgroundSessionSpec(command="python -c \"import time; time.sleep(1)\"", session_name="bg1")
    )
    status = transport.background_session_status(handle)
    dismiss = transport.dismiss_background_session(handle)

    assert status.running is True
    assert dismiss.returncode == 0


def test_ssh_transport_renders_background_session_and_dismiss() -> None:
    transport = SshTransport(target="alice@example.org")
    runner = PosixShellRunner()
    spec = BackgroundSessionSpec(
        command="ssh -N -L 6543:db.host:5432 alice@jump",
        session_name="etl-db-tunnel",
        cwd="/tmp/work",
        env={"A": "1"},
    )
    start_lines = transport.render_background_session(runner, spec)
    stop_lines = transport.render_dismiss_background_session(
        runner,
        BackgroundSessionHandle(session_name="etl-db-tunnel", transport="ssh", target="alice@example.org"),
    )

    assert any("tmux new-session -d -s etl-db-tunnel" in line for line in start_lines)
    assert any("export A=1" in line for line in start_lines)
    assert any("kill-session -t etl-db-tunnel" in line for line in stop_lines)


def test_transport_capabilities_are_exposed() -> None:
    local_caps = LocalProcessTransport().capabilities()
    ssh_caps = SshTransport(target="alice@example.org").capabilities()

    assert local_caps["run"] is True
    assert local_caps["put_text"] is True
    assert ssh_caps["background_sessions"] is True
    assert ssh_caps["run"] is True
    assert ssh_caps["put_text"] is True

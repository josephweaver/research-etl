from __future__ import annotations

from etl.common.db_tunnel import _build_launch_kwargs, _rewrite_local_forward_command


def test_rewrite_local_forward_command_replaces_local_port() -> None:
    raw = "ssh -N -L 6543:example.host:5432 user@login"
    out = _rewrite_local_forward_command(raw, local_port=7123)
    assert "-L 7123:example.host:5432" in out


def test_rewrite_local_forward_command_requires_forward() -> None:
    raw = "ssh -N user@login"
    try:
        _rewrite_local_forward_command(raw, local_port=7123)
    except RuntimeError as exc:
        assert "must include an SSH local forward" in str(exc)
    else:
        raise AssertionError("expected RuntimeError")


def test_build_launch_kwargs_windows(monkeypatch) -> None:
    monkeypatch.setattr("etl.common.db_tunnel.os.name", "nt")
    argv, kwargs = _build_launch_kwargs("ssh -N user@login")
    assert argv == ["cmd", "/c", "ssh -N user@login"]
    assert "creationflags" in kwargs
    assert "start_new_session" not in kwargs


def test_build_launch_kwargs_unix(monkeypatch) -> None:
    monkeypatch.setattr("etl.common.db_tunnel.os.name", "posix")
    argv, kwargs = _build_launch_kwargs("ssh -N user@login")
    assert argv == ["bash", "-lc", "ssh -N user@login"]
    assert kwargs.get("start_new_session") is True

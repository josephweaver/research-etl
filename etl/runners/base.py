from __future__ import annotations

from typing import Callable, Mapping, Optional, Protocol


class CancelToken(Protocol):
    def is_cancelled(self) -> bool: ...


LogCallback = Callable[[str, str], None]


class Runner(Protocol):
    """OS/shell adapter used to render command execution snippets."""

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
    ) -> list[str]: ...

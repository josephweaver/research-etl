from __future__ import annotations

from pathlib import Path
from typing import List, Protocol


class LogSink(Protocol):
    def emit(self, level: str, message: str) -> None:
        ...


class ConsoleLogSink:
    def __init__(self, run_id: str, step_name: str):
        self.run_id = run_id
        self.step_name = step_name

    def emit(self, level: str, message: str) -> None:
        print(f"[{self.run_id}] [{self.step_name}] [{level}] {message}")


class FileLogSink:
    def __init__(self, path: Path):
        self.path = path

    def emit(self, level: str, message: str) -> None:
        self.path.parent.mkdir(parents=True, exist_ok=True)
        with self.path.open("a", encoding="utf-8") as f:
            f.write(f"[{level}] {message}\n")


class CompositeLogSink:
    def __init__(self, sinks: List[LogSink]):
        self.sinks = sinks

    def emit(self, level: str, message: str) -> None:
        for sink in self.sinks:
            sink.emit(level, message)


class CallbackLogSink:
    """
    Adapter sink that forwards log records to a callback.
    """

    def __init__(self, callback):
        self.callback = callback

    def emit(self, level: str, message: str) -> None:
        self.callback(str(level), str(message))


class RedactingLogSink:
    """
    Sink wrapper that redacts configured sensitive values from log messages.
    """

    def __init__(self, sink: LogSink, redactions: List[str] | None = None):
        self.sink = sink
        values = [str(x) for x in list(redactions or []) if str(x)]
        # Replace longer tokens first to avoid partial leakage.
        self.redactions = sorted(values, key=len, reverse=True)

    def emit(self, level: str, message: str) -> None:
        text = str(message)
        for value in self.redactions:
            if value and value in text:
                text = text.replace(value, "[REDACTED]")
        self.sink.emit(level, text)


class StepLogger:
    """
    Standard plugin-facing logger.
    Plugins can call:
      ctx.log("message")
      ctx.log("message", "WARN")
      ctx.info(...), ctx.warn(...), ctx.error(...)
    """

    def __init__(self, sink: LogSink):
        self.sink = sink

    def __call__(self, message: str, level: str = "INFO") -> None:
        self.sink.emit(str(level).upper(), str(message))

    def info(self, message: str) -> None:
        self.sink.emit("INFO", str(message))

    def warn(self, message: str) -> None:
        self.sink.emit("WARN", str(message))

    def error(self, message: str) -> None:
        self.sink.emit("ERROR", str(message))

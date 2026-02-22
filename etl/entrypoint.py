# research-etl
# Copyright (c) 2026 Joseph Weaver
# This file is part of the research-etl project and is licensed under the MIT License.
# You may not use this file except in compliance with the License.
# See https://github.com/josephweaver/research-etl for details.

from __future__ import annotations

from typing import Callable

from .app_logging import get_app_logger


def guarded_entrypoint(
    fn: Callable[[], int],
    *,
    logger_name: str,
    label: str,
    on_error: Callable[[Exception], None] | None = None,
) -> int:
    try:
        return int(fn())
    except Exception as exc:  # noqa: BLE001
        logger = get_app_logger(logger_name)
        logger.exception("Unhandled %s error: %s", label, exc)
        if on_error is not None:
            try:
                on_error(exc)
            except Exception:
                pass
        return 1

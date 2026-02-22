# research-etl
# Copyright (c) 2026 Joseph Weaver
# This file is part of the research-etl project and is licensed under the MIT License.
# You may not use this file except in compliance with the License.
# See https://github.com/josephweaver/research-etl for details.

from __future__ import annotations

import logging
import os
import sys
import time
from pathlib import Path
from typing import Optional

ETL_LOGGER_NAME = "etl"
LOG_FORMAT = "[%(asctime)s] [%(levelname)s] [%(name)s] %(message)s"
LOG_DATEFMT = "%Y-%m-%dT%H:%M:%SZ"


def _parse_level(value: str | None, *, default: int = logging.INFO) -> int:
    text = str(value or "").strip().upper()
    if not text:
        return default
    return getattr(logging, text, default)


def configure_app_logger(
    *,
    logger_name: str = ETL_LOGGER_NAME,
    level: str | None = None,
    log_file: str | Path | None = None,
    force: bool = True,
) -> logging.Logger:
    logger = logging.getLogger(logger_name)
    logger.setLevel(_parse_level(level or os.environ.get("ETL_LOG_LEVEL")))
    logger.propagate = False

    if force:
        for handler in list(logger.handlers):
            logger.removeHandler(handler)

    formatter = logging.Formatter(LOG_FORMAT, datefmt=LOG_DATEFMT)
    formatter.converter = time.gmtime

    stream_handler = logging.StreamHandler(sys.stdout)
    stream_handler.setFormatter(formatter)
    logger.addHandler(stream_handler)

    target_file = str(log_file or os.environ.get("ETL_LOG_FILE") or "").strip()
    if target_file:
        path = Path(target_file).expanduser().resolve()
        path.parent.mkdir(parents=True, exist_ok=True)
        file_handler = logging.FileHandler(path, encoding="utf-8")
        file_handler.setFormatter(formatter)
        logger.addHandler(file_handler)

    return logger


def get_app_logger(name: Optional[str] = None) -> logging.Logger:
    if not name:
        return logging.getLogger(ETL_LOGGER_NAME)
    return logging.getLogger(f"{ETL_LOGGER_NAME}.{name}")

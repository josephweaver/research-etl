# research-etl
# Copyright (c) 2026 Joseph Weaver
# This file is part of the research-etl project and is licensed under the MIT License.
# You may not use this file except in compliance with the License.
# See https://github.com/josephweaver/research-etl for details.

from __future__ import annotations

from typing import Any, Dict

from .local_fs import fetch_local_fs, transfer_local_fs
from .rclone import fetch_rclone, transfer_rclone


class DatasetTransportError(RuntimeError):
    """Raised when a dataset transport operation fails."""


def transfer_via_transport(
    *,
    transport: str,
    source_path: str,
    target_uri: str,
    dry_run: bool = False,
    options: Dict[str, Any] | None = None,
) -> Dict[str, Any]:
    name = str(transport or "").strip().lower()
    opts = dict(options or {})
    if name == "local_fs":
        return transfer_local_fs(source_path=source_path, target_uri=target_uri, dry_run=dry_run)
    if name == "rclone":
        return transfer_rclone(source_path=source_path, target_uri=target_uri, dry_run=dry_run, options=opts)
    if name == "rsync":
        raise DatasetTransportError(
            "Transport 'rsync' is not implemented yet; provide --transport local_fs/rclone or add rsync adapter."
        )
    raise DatasetTransportError(f"Unsupported transport: {transport}")


def fetch_via_transport(
    *,
    transport: str,
    source_uri: str,
    target_path: str,
    dry_run: bool = False,
    options: Dict[str, Any] | None = None,
) -> Dict[str, Any]:
    name = str(transport or "").strip().lower()
    opts = dict(options or {})
    if name == "local_fs":
        return fetch_local_fs(source_uri=source_uri, target_path=target_path, dry_run=dry_run)
    if name == "rclone":
        return fetch_rclone(source_uri=source_uri, target_path=target_path, dry_run=dry_run, options=opts)
    if name == "rsync":
        raise DatasetTransportError(
            "Transport 'rsync' is not implemented yet for fetch; provide --transport local_fs/rclone or add rsync adapter."
        )
    raise DatasetTransportError(f"Unsupported transport: {transport}")


__all__ = ["DatasetTransportError", "transfer_via_transport", "fetch_via_transport"]

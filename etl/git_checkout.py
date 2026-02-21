# research-etl
# Copyright (c) 2026 Joseph Weaver
# This file is part of the research-etl project and is licensed under the MIT License.
# You may not use this file except in compliance with the License.
# See https://github.com/josephweaver/research-etl for details.

"""
Helpers for strict git-versioned execution checkouts.
"""

from __future__ import annotations

import re
import shutil
import subprocess
import tarfile
import zipfile
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, Optional
from urllib.parse import urlparse


class GitCheckoutError(RuntimeError):
    """Raised when a strict git checkout cannot be prepared."""


@dataclass(frozen=True)
class GitExecutionSpec:
    commit_sha: str
    origin_url: Optional[str]
    repo_name: str
    git_is_dirty: Optional[bool] = None


def _git_out(args: list[str], repo_root: Path) -> Optional[str]:
    try:
        out = subprocess.check_output(
            ["git", "-C", str(repo_root), *args],
            stderr=subprocess.DEVNULL,
            text=True,
        ).strip()
        return out or None
    except Exception:
        return None


def _git_is_dirty(repo_root: Path) -> Optional[bool]:
    try:
        proc = subprocess.run(
            ["git", "-C", str(repo_root), "status", "--porcelain", "--untracked-files=no"],
            stdout=subprocess.PIPE,
            stderr=subprocess.DEVNULL,
            text=True,
            check=False,
        )
        if proc.returncode != 0:
            return None
        return bool((proc.stdout or "").strip())
    except Exception:
        return None


def infer_repo_name(origin_url: str) -> str:
    raw = (origin_url or "").strip()
    if not raw:
        return "repo"
    parsed = urlparse(raw)
    path = parsed.path or ""
    if not path and "@" in raw and ":" in raw:
        # scp-like URL, e.g. git@github.com:org/repo.git
        path = raw.split(":", 1)[1]
    name = Path(path).name or "repo"
    if name.endswith(".git"):
        name = name[: -len(".git")]
    safe = re.sub(r"[^A-Za-z0-9._-]+", "-", name).strip("-")
    return safe or "repo"


def repo_relative_path(path: Path, repo_root: Path, label: str) -> Path:
    repo_abs = repo_root.resolve()
    if path.is_absolute():
        path_abs = path.resolve()
    else:
        path_abs = (repo_abs / path).resolve()
    try:
        rel = path_abs.relative_to(repo_abs)
    except ValueError as exc:
        raise GitCheckoutError(f"{label} must be inside repository root: {path}") from exc
    if any(part == ".." for part in rel.parts):
        raise GitCheckoutError(f"{label} path is invalid: {path}")
    return rel


def map_to_checkout(path: Path, repo_root: Path, checkout_root: Path, label: str) -> Path:
    rel = repo_relative_path(path, repo_root, label)
    return checkout_root / rel


def resolve_execution_spec(
    *,
    repo_root: Path,
    provenance: Optional[Dict[str, Any]] = None,
    require_clean: bool = True,
    require_origin: bool = True,
) -> GitExecutionSpec:
    provenance = provenance or {}
    commit_sha = str(provenance.get("git_commit_sha") or "").strip() or _git_out(["rev-parse", "HEAD"], repo_root)
    origin_url = str(provenance.get("git_origin_url") or "").strip() or _git_out(
        ["config", "--get", "remote.origin.url"], repo_root
    )
    dirty = provenance.get("git_is_dirty")
    if dirty is None:
        dirty = _git_is_dirty(repo_root)
    if not commit_sha:
        raise GitCheckoutError("Missing git commit SHA. Commit your changes before running.")
    if require_origin and not origin_url:
        raise GitCheckoutError("Missing git origin URL. Configure remote 'origin' before running.")
    if require_clean and dirty:
        raise GitCheckoutError(
            "Working tree is dirty. Commit/push changes (or enable allow-dirty mode) before running strict git execution."
        )
    return GitExecutionSpec(
        commit_sha=commit_sha,
        origin_url=origin_url,
        repo_name=infer_repo_name(origin_url or repo_root.name),
        git_is_dirty=dirty,
    )


def _run_git(cmd: list[str], cwd: Optional[Path] = None) -> None:
    try:
        proc = subprocess.run(
            cmd,
            cwd=str(cwd) if cwd else None,
            capture_output=True,
            text=True,
            check=False,
        )
    except FileNotFoundError as exc:
        raise GitCheckoutError("Git executable not found on PATH.") from exc
    if proc.returncode != 0:
        detail = (proc.stderr or proc.stdout or "").strip()
        raise GitCheckoutError(detail or f"Command failed: {' '.join(cmd)}")


def ensure_repo_checkout(base_dir: Path, spec: GitExecutionSpec) -> Path:
    if not spec.origin_url:
        raise GitCheckoutError("Git remote checkout requires origin URL.")
    checkout_root = base_dir / f"{spec.repo_name}-{spec.commit_sha[:12]}"
    checkout_root.parent.mkdir(parents=True, exist_ok=True)
    if not (checkout_root / ".git").exists():
        _run_git(["git", "clone", "--no-checkout", str(spec.origin_url), str(checkout_root)])
    _run_git(["git", "-C", str(checkout_root), "fetch", "--tags", "--prune", "origin"])
    _run_git(["git", "-C", str(checkout_root), "checkout", "--detach", spec.commit_sha])
    _run_git(["git", "-C", str(checkout_root), "reset", "--hard", spec.commit_sha])
    return checkout_root


def ensure_bundle_checkout(base_dir: Path, spec: GitExecutionSpec, bundle_path: Path) -> Path:
    if not bundle_path.exists() or not bundle_path.is_file():
        raise GitCheckoutError(f"Git bundle not found: {bundle_path}")
    checkout_root = base_dir / f"{spec.repo_name}-{spec.commit_sha[:12]}"
    checkout_root.parent.mkdir(parents=True, exist_ok=True)
    if not (checkout_root / ".git").exists():
        _run_git(["git", "clone", "--no-checkout", str(bundle_path), str(checkout_root)])
    _run_git(["git", "-C", str(checkout_root), "fetch", str(bundle_path), "--tags"])
    _run_git(["git", "-C", str(checkout_root), "checkout", "--detach", spec.commit_sha])
    _run_git(["git", "-C", str(checkout_root), "reset", "--hard", spec.commit_sha])
    return checkout_root


def ensure_snapshot_checkout(base_dir: Path, spec: GitExecutionSpec, snapshot_path: Path) -> Path:
    if not snapshot_path.exists() or not snapshot_path.is_file():
        raise GitCheckoutError(f"Snapshot not found: {snapshot_path}")
    checkout_root = base_dir / f"{spec.repo_name}-{spec.commit_sha[:12]}"
    if checkout_root.exists():
        shutil.rmtree(checkout_root)
    checkout_root.mkdir(parents=True, exist_ok=True)
    if tarfile.is_tarfile(snapshot_path):
        with tarfile.open(snapshot_path, "r:*") as tf:
            tf.extractall(checkout_root)
    elif snapshot_path.suffix.lower() == ".zip":
        with zipfile.ZipFile(snapshot_path) as zf:
            zf.extractall(checkout_root)
    else:
        raise GitCheckoutError(f"Unsupported snapshot format: {snapshot_path}")
    return _normalize_extracted_root(checkout_root)


def _normalize_extracted_root(root: Path) -> Path:
    children = [p for p in root.iterdir() if p.name != "__MACOSX"]
    if len(children) == 1 and children[0].is_dir():
        return children[0]
    return root


__all__ = [
    "GitCheckoutError",
    "GitExecutionSpec",
    "ensure_bundle_checkout",
    "ensure_repo_checkout",
    "ensure_snapshot_checkout",
    "infer_repo_name",
    "map_to_checkout",
    "repo_relative_path",
    "resolve_execution_spec",
]

"""
Run provenance capture helpers (Git + file checksums).
"""

from __future__ import annotations

import hashlib
import shlex
import subprocess
from pathlib import Path
from typing import Any, Dict, Optional

from .pipeline import Pipeline
from .git_checkout import infer_repo_name
from .source_control import SourceControlError, make_git_source_provider


def _sha256_file(path: Optional[Path]) -> Optional[str]:
    if not path or not path.exists() or not path.is_file():
        return None
    h = hashlib.sha256()
    with path.open("rb") as f:
        for chunk in iter(lambda: f.read(1024 * 1024), b""):
            h.update(chunk)
    return h.hexdigest()


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
            ["git", "-C", str(repo_root), "diff-index", "--quiet", "HEAD", "--"],
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
            check=False,
        )
        return proc.returncode == 1
    except Exception:
        return None


def _git_origin_url(repo_root: Path) -> Optional[str]:
    return _git_out(["config", "--get", "remote.origin.url"], repo_root)


def _collect_plugin_checksums(plugin_dir: Path, pipeline: Pipeline) -> Dict[str, str]:
    checksums: Dict[str, str] = {}
    for step in pipeline.steps:
        try:
            tokens = shlex.split(step.script)
        except Exception:
            continue
        if not tokens:
            continue
        plugin_ref = tokens[0]
        candidate = Path(plugin_ref)
        if not candidate.suffix:
            candidate = candidate.with_suffix(".py")
        if not candidate.is_absolute():
            candidate = plugin_dir / candidate
        digest = _sha256_file(candidate)
        if digest:
            checksums[candidate.as_posix()] = digest
    return checksums


def collect_run_provenance(
    *,
    repo_root: Path,
    pipeline_path: Path,
    global_config_path: Optional[Path],
    environments_config_path: Optional[Path],
    plugin_dir: Path,
    pipeline: Pipeline,
    cli_command: Optional[str],
) -> Dict[str, Any]:
    plugin_checksums = _collect_plugin_checksums(plugin_dir, pipeline)
    provider = make_git_source_provider()
    source_provider = provider.name
    source_revision: Optional[str] = None
    source_origin_url: Optional[str] = None
    source_repo_name: Optional[str] = None
    source_is_dirty: Optional[bool] = None

    try:
        spec = provider.resolve_execution_spec(
            repo_root=repo_root,
            provenance={},
            require_clean=False,
            require_origin=False,
        )
        source_revision = spec.revision or None
        source_origin_url = spec.origin_url
        source_repo_name = spec.repo_name or None
        source_is_dirty = spec.is_dirty
    except SourceControlError:
        # Preserve historical behavior: provenance should still return with best-effort fields.
        source_revision = _git_out(["rev-parse", "HEAD"], repo_root)
        source_origin_url = _git_origin_url(repo_root)
        source_repo_name = infer_repo_name(source_origin_url or "")
        source_is_dirty = _git_is_dirty(repo_root)

    origin_url = source_origin_url
    return {
        "source_provider": source_provider,
        "source_revision": source_revision,
        "source_origin_url": source_origin_url,
        "source_repo_name": source_repo_name,
        "source_is_dirty": source_is_dirty,
        "git_commit_sha": source_revision,
        "git_branch": _git_out(["rev-parse", "--abbrev-ref", "HEAD"], repo_root),
        "git_tag": _git_out(["describe", "--tags", "--exact-match"], repo_root),
        "git_origin_url": origin_url,
        "git_repo_name": source_repo_name or infer_repo_name(origin_url or ""),
        "git_is_dirty": source_is_dirty,
        "cli_command": cli_command,
        "pipeline_checksum": _sha256_file(pipeline_path),
        "global_config_checksum": _sha256_file(global_config_path),
        "execution_config_checksum": _sha256_file(environments_config_path),
        "plugin_checksums_json": plugin_checksums if plugin_checksums else None,
    }


__all__ = ["collect_run_provenance"]

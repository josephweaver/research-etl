# research-etl
# Copyright (c) 2026 Joseph Weaver
# This file is part of the research-etl project and is licensed under the MIT License.
# You may not use this file except in compliance with the License.
# See https://github.com/josephweaver/research-etl for details.

from __future__ import annotations

import hashlib
import logging
import re
import subprocess
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

from .git_checkout import infer_repo_name
from .subprocess_logging import run_logged_subprocess


_LOG = logging.getLogger("etl.pipeline_assets")


class PipelineAssetError(RuntimeError):
    """Raised when pipeline asset source resolution fails."""


@dataclass(frozen=True)
class PipelineAssetSource:
    repo_url: str
    pipelines_dir: str = "pipelines"
    scripts_dir: str = "scripts"
    ref: str = "main"
    priority: int = 100
    local_repo_path: Optional[str] = None


_SYNCED_REPOS: set[Tuple[str, str]] = set()


def _slug(text: str) -> str:
    cleaned = re.sub(r"[^A-Za-z0-9._-]+", "-", str(text or "").strip())
    return cleaned.strip("-") or "repo"


def _repo_cache_dir(cache_root: Path, repo_url: str) -> Path:
    repo_name = _slug(infer_repo_name(repo_url))
    digest = hashlib.sha1(str(repo_url).encode("utf-8")).hexdigest()[:10]
    return cache_root / f"{repo_name}-{digest}"


def _run_git(args: List[str], *, cwd: Optional[Path] = None) -> str:
    proc = run_logged_subprocess(
        ["git", *args],
        logger=_LOG,
        action="git",
        cwd=cwd,
        check=False,
    )
    if proc.returncode != 0:
        detail = (proc.stderr or proc.stdout or "").strip()
        raise PipelineAssetError(detail or f"git {' '.join(args)} failed")
    return str(proc.stdout or "").strip()


def _normalize_pipeline_input(raw: Path) -> List[Path]:
    text = raw.as_posix()
    if text.lower().startswith("pipelines/"):
        text = text[len("pipelines/") :]
    base = Path(text)
    candidates = [base]
    if base.suffix.lower() not in {".yml", ".yaml"}:
        candidates.append(base.with_suffix(".yml"))
        candidates.append(base.with_suffix(".yaml"))
    return candidates


def pipeline_asset_sources_from_project_vars(project_vars: Dict[str, Any]) -> List[PipelineAssetSource]:
    vars_map = dict(project_vars or {})
    out: List[PipelineAssetSource] = []

    raw_sources = vars_map.get("pipeline_asset_sources")
    if isinstance(raw_sources, list):
        for idx, raw in enumerate(raw_sources):
            if not isinstance(raw, dict):
                raise PipelineAssetError(f"pipeline_asset_sources[{idx}] must be a mapping")
            repo_url = str(raw.get("repo_url") or raw.get("url") or "").strip()
            if not repo_url:
                raise PipelineAssetError(f"pipeline_asset_sources[{idx}] missing repo_url")
            out.append(
                PipelineAssetSource(
                    repo_url=repo_url,
                    pipelines_dir=str(raw.get("pipelines_dir") or "pipelines").strip() or "pipelines",
                    scripts_dir=str(raw.get("scripts_dir") or "scripts").strip() or "scripts",
                    ref=str(raw.get("ref") or "main").strip() or "main",
                    priority=int(raw.get("priority", 100) or 100),
                    local_repo_path=str(raw.get("local_repo_path") or raw.get("local_path") or "").strip() or None,
                )
            )

    # Backward-compatible single-source keys.
    legacy_repo = str(vars_map.get("pipeline_assets_repo_url") or "").strip()
    if legacy_repo:
        out.append(
            PipelineAssetSource(
                repo_url=legacy_repo,
                pipelines_dir=str(vars_map.get("pipeline_assets_pipelines_dir") or "pipelines").strip() or "pipelines",
                scripts_dir=str(vars_map.get("pipeline_assets_scripts_dir") or "scripts").strip() or "scripts",
                ref=str(vars_map.get("pipeline_assets_ref") or "main").strip() or "main",
                priority=int(vars_map.get("pipeline_assets_priority", 1000) or 1000),
                local_repo_path=str(vars_map.get("pipeline_assets_local_repo_path") or "").strip() or None,
            )
        )

    # De-duplicate by (repo, ref, pipelines_dir, scripts_dir, local_repo_path); preserve priority.
    uniq: Dict[Tuple[str, str, str, str, str], PipelineAssetSource] = {}
    for src in out:
        key = (src.repo_url, src.ref, src.pipelines_dir, src.scripts_dir, str(src.local_repo_path or ""))
        if key in uniq:
            continue
        uniq[key] = src
    return sorted(uniq.values(), key=lambda s: int(s.priority))


def sync_pipeline_asset_source(source: PipelineAssetSource, *, cache_root: Path, repo_root: Optional[Path] = None) -> Path:
    if str(source.local_repo_path or "").strip():
        root = Path(repo_root or Path(".").resolve()).resolve()
        local = Path(str(source.local_repo_path)).expanduser()
        if not local.is_absolute():
            local = (root / local).resolve()
        if local.exists() and local.is_dir():
            is_repo = run_logged_subprocess(
                ["git", "-C", str(local), "rev-parse", "--is-inside-work-tree"],
                logger=_LOG,
                action="git",
                check=False,
            )
            if is_repo.returncode == 0:
                return local
            _LOG.warning(
                "pipeline asset local_repo_path is not a git repo; falling back to repo_url sync: %s",
                local,
            )
        else:
            _LOG.warning(
                "pipeline asset local_repo_path not found; falling back to repo_url sync: %s",
                local,
            )

    cache_root = Path(cache_root).resolve()
    cache_root.mkdir(parents=True, exist_ok=True)
    repo_dir = _repo_cache_dir(cache_root, source.repo_url)
    sync_key = (str(repo_dir), str(source.ref))
    if sync_key in _SYNCED_REPOS and repo_dir.exists():
        return repo_dir
    if not repo_dir.exists():
        _run_git(["clone", source.repo_url, str(repo_dir)])
    else:
        _run_git(["fetch", "--all", "--tags", "--prune"], cwd=repo_dir)
    _run_git(["checkout", source.ref], cwd=repo_dir)
    _run_git(["pull", "--ff-only", "origin", source.ref], cwd=repo_dir)
    _SYNCED_REPOS.add(sync_key)
    return repo_dir


def resolve_pipeline_path_from_project_sources(
    pipeline_path: Path,
    *,
    project_vars: Dict[str, Any],
    repo_root: Path,
    cache_root: Optional[Path] = None,
) -> Path:
    original = Path(pipeline_path)
    if original.exists():
        return original.resolve()
    # Resolve relative to repo root before trying external sources.
    if not original.is_absolute():
        local = (Path(repo_root).resolve() / original).resolve()
        if local.exists():
            return local

    sources = pipeline_asset_sources_from_project_vars(project_vars)
    if not sources:
        return original

    repo_root_resolved = Path(repo_root).resolve()
    candidates: List[Path] = []
    # External sources are rooted at each source's pipelines_dir, so absolute
    # local paths should be converted to repo-relative candidates when possible.
    if original.is_absolute():
        try:
            rel_from_pipelines = original.resolve().relative_to((repo_root_resolved / "pipelines").resolve())
            candidates.extend(_normalize_pipeline_input(rel_from_pipelines))
        except Exception:
            pass
        try:
            rel_from_repo = original.resolve().relative_to(repo_root_resolved)
            candidates.extend(_normalize_pipeline_input(rel_from_repo))
        except Exception:
            pass
    candidates.extend(_normalize_pipeline_input(original))
    # De-duplicate while preserving order.
    seen: set[str] = set()
    uniq_candidates: List[Path] = []
    for c in candidates:
        key = c.as_posix()
        if key in seen:
            continue
        seen.add(key)
        uniq_candidates.append(c)
    candidates = uniq_candidates
    root = Path(cache_root or (Path(repo_root).resolve() / ".pipeline_assets_cache"))
    for src in sources:
        repo_dir = sync_pipeline_asset_source(src, cache_root=root)
        pipelines_root = (repo_dir / src.pipelines_dir).resolve()
        for rel in candidates:
            candidate = (pipelines_root / rel).resolve()
            if candidate.exists():
                return candidate
    return original


__all__ = [
    "PipelineAssetError",
    "PipelineAssetSource",
    "pipeline_asset_sources_from_project_vars",
    "resolve_pipeline_path_from_project_sources",
    "sync_pipeline_asset_source",
]

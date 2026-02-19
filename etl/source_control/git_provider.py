from __future__ import annotations

from pathlib import Path
from typing import Any, Dict, Optional

from etl.git_checkout import (
    GitCheckoutError,
    GitExecutionSpec,
    ensure_bundle_checkout,
    ensure_repo_checkout,
    ensure_snapshot_checkout,
    repo_relative_path,
    resolve_execution_spec,
)

from .base import SourceControlError, SourceExecutionSpec


class GitSourceProvider:
    """Git-backed source provider adapter around existing git_checkout helpers."""

    @property
    def name(self) -> str:
        return "git"

    def resolve_execution_spec(
        self,
        *,
        repo_root: Path,
        provenance: Optional[Dict[str, Any]] = None,
        require_clean: bool = True,
        require_origin: bool = True,
    ) -> SourceExecutionSpec:
        try:
            spec = resolve_execution_spec(
                repo_root=repo_root,
                provenance=provenance or {},
                require_clean=require_clean,
                require_origin=require_origin,
            )
        except GitCheckoutError as exc:
            raise SourceControlError(str(exc)) from exc
        return SourceExecutionSpec(
            provider=self.name,
            revision=spec.commit_sha,
            origin_url=spec.origin_url,
            repo_name=spec.repo_name,
            is_dirty=spec.git_is_dirty,
            extra={"commit_sha": spec.commit_sha},
        )

    def repo_relative_path(self, path: Path, repo_root: Path, label: str) -> Path:
        try:
            return repo_relative_path(path, repo_root, label)
        except GitCheckoutError as exc:
            raise SourceControlError(str(exc)) from exc

    def ensure_repo_checkout(self, base_dir: Path, spec: SourceExecutionSpec) -> Path:
        try:
            return ensure_repo_checkout(base_dir, self._to_git_spec(spec))
        except GitCheckoutError as exc:
            raise SourceControlError(str(exc)) from exc

    def ensure_bundle_checkout(self, base_dir: Path, spec: SourceExecutionSpec, bundle_path: Path) -> Path:
        try:
            return ensure_bundle_checkout(base_dir, self._to_git_spec(spec), bundle_path)
        except GitCheckoutError as exc:
            raise SourceControlError(str(exc)) from exc

    def ensure_snapshot_checkout(self, base_dir: Path, spec: SourceExecutionSpec, snapshot_path: Path) -> Path:
        try:
            return ensure_snapshot_checkout(base_dir, self._to_git_spec(spec), snapshot_path)
        except GitCheckoutError as exc:
            raise SourceControlError(str(exc)) from exc

    def _to_git_spec(self, spec: SourceExecutionSpec) -> GitExecutionSpec:
        if str(spec.provider or "").strip().lower() not in {"", "git"}:
            raise SourceControlError(f"GitSourceProvider cannot use provider='{spec.provider}'")
        commit = str(spec.extra.get("commit_sha") or spec.revision or "").strip()
        if not commit:
            raise SourceControlError("Missing revision/commit_sha for git checkout")
        return GitExecutionSpec(
            commit_sha=commit,
            origin_url=spec.origin_url,
            repo_name=spec.repo_name,
            git_is_dirty=spec.is_dirty,
        )


def make_git_source_provider() -> GitSourceProvider:
    return GitSourceProvider()

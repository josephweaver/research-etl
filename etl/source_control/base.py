from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Dict, Optional, Protocol


class SourceControlError(RuntimeError):
    """Raised when a source-control provider cannot resolve or prepare execution source."""


@dataclass(frozen=True)
class SourceExecutionSpec:
    """Provider-agnostic source execution identity."""

    provider: str
    revision: str
    origin_url: Optional[str]
    repo_name: str
    is_dirty: Optional[bool] = None
    extra: Dict[str, Any] = field(default_factory=dict)

    @property
    def commit_sha(self) -> str:
        """Compatibility alias for Git-centric call sites."""
        return str(self.revision or "")

    @property
    def git_is_dirty(self) -> Optional[bool]:
        """Compatibility alias for Git-centric call sites."""
        return self.is_dirty


class SourceProvider(Protocol):
    """Contract for source-control providers used by execution/provenance layers."""

    @property
    def name(self) -> str: ...

    def resolve_execution_spec(
        self,
        *,
        repo_root: Path,
        provenance: Optional[Dict[str, Any]] = None,
        require_clean: bool = True,
        require_origin: bool = True,
    ) -> SourceExecutionSpec: ...

    def repo_relative_path(self, path: Path, repo_root: Path, label: str) -> Path: ...

    def ensure_repo_checkout(self, base_dir: Path, spec: SourceExecutionSpec) -> Path: ...

    def ensure_bundle_checkout(self, base_dir: Path, spec: SourceExecutionSpec, bundle_path: Path) -> Path: ...

    def ensure_snapshot_checkout(self, base_dir: Path, spec: SourceExecutionSpec, snapshot_path: Path) -> Path: ...

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, Optional

from ..transports import CommandTransport, ExecutionOptions, LocalProcessTransport, SshTransport
from ..git_checkout import infer_repo_name
from ..pipeline_assets import pipeline_asset_sources_from_project_vars
from ..runners import PosixShellRunner, Runner
from .base import SourceControlError, SourceExecutionSpec
from .git_provider import make_git_source_provider

_SOURCE_PROVIDER = make_git_source_provider()


@dataclass(frozen=True)
class CheckoutSpec:
    repo_url: str
    revision: str
    checkout_root: str
    mode: str = "detached"
    remote_name: str = "origin"
    clean: bool = False
    fetch_tags: bool = True
    prune: bool = True
    set_remote_url: bool = False


def _lookup_dotted(values: Dict[str, Any], dotted: str) -> tuple[Any, bool]:
    cur: Any = values
    for part in str(dotted or "").split("."):
        if isinstance(cur, dict) and part in cur:
            cur = cur[part]
            continue
        return None, False
    return cur, True


def _pick_source_var(commandline_vars: Dict[str, Any], kind: str, key: str) -> Optional[str]:
    value, ok = _lookup_dotted(commandline_vars, f"source.{kind}.{key}")
    if not ok:
        return None
    text = str(value or "").strip()
    return text or None


def resolve_source_override(
    *,
    commandline_vars: Dict[str, Any],
    kind: str,
    fallback: Optional[SourceExecutionSpec] = None,
) -> Optional[SourceExecutionSpec]:
    repo_url = _pick_source_var(commandline_vars, kind, "repo_url") or str(getattr(fallback, "origin_url", "") or "").strip() or None
    revision = _pick_source_var(commandline_vars, kind, "revision") or str(getattr(fallback, "revision", "") or "").strip()
    repo_name = _pick_source_var(commandline_vars, kind, "repo_name") or str(getattr(fallback, "repo_name", "") or "").strip()
    provider = _pick_source_var(commandline_vars, kind, "provider") or str(getattr(fallback, "provider", "") or "").strip() or "git"
    if not revision:
        return fallback
    if not repo_name:
        repo_name = infer_repo_name(repo_url or kind)
    return SourceExecutionSpec(
        provider=provider,
        revision=revision,
        origin_url=repo_url,
        repo_name=repo_name,
        is_dirty=getattr(fallback, "is_dirty", None),
        extra=dict(getattr(fallback, "extra", {}) or {}),
    )


def build_source_commandline_vars(
    *,
    repo_root: Path,
    project_vars: Dict[str, Any],
    provenance: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    out: Dict[str, Any] = {"source": {}}
    try:
        etl_spec = _SOURCE_PROVIDER.resolve_execution_spec(
            repo_root=repo_root,
            provenance=provenance or {},
            require_clean=False,
            require_origin=False,
        )
        out["source"]["etl"] = {
            "provider": etl_spec.provider,
            "repo_url": etl_spec.origin_url or "",
            "revision": etl_spec.revision,
            "repo_name": etl_spec.repo_name,
        }
    except SourceControlError:
        pass

    pipeline_section: Dict[str, Any] = {}
    try:
        sources = pipeline_asset_sources_from_project_vars(project_vars)
    except Exception:
        sources = []
    preferred = sources[0] if sources else None
    if preferred is not None:
        repo_url = str(getattr(preferred, "repo_url", "") or "").strip()
        repo_name = infer_repo_name(repo_url or "pipeline")
        revision = str(getattr(preferred, "ref", "") or "").strip() or "main"
        local_repo_path = str(getattr(preferred, "local_repo_path", "") or "").strip()
        if local_repo_path:
            local = Path(local_repo_path).expanduser()
            if not local.is_absolute():
                local = (repo_root / local).resolve()
            if local.exists() and local.is_dir():
                try:
                    pipeline_spec = _SOURCE_PROVIDER.resolve_execution_spec(
                        repo_root=local,
                        provenance={},
                        require_clean=False,
                        require_origin=False,
                    )
                    repo_url = pipeline_spec.origin_url or repo_url
                    repo_name = pipeline_spec.repo_name or repo_name
                    revision = pipeline_spec.revision or revision
                except SourceControlError:
                    pass
        pipeline_section = {
            "provider": "git",
            "repo_url": repo_url,
            "revision": revision,
            "repo_name": repo_name,
        }
    else:
        legacy_repo_url = str(project_vars.get("pipeline_assets_repo_url") or "").strip()
        legacy_ref = str(project_vars.get("pipeline_assets_ref") or "main").strip() or "main"
        if legacy_repo_url:
            pipeline_section = {
                "provider": "git",
                "repo_url": legacy_repo_url,
                "revision": legacy_ref,
                "repo_name": infer_repo_name(legacy_repo_url),
            }
    if pipeline_section:
        out["source"]["pipeline"] = pipeline_section
    return out


def merge_source_commandline_vars(
    commandline_vars: Optional[Dict[str, Any]],
    *,
    repo_root: Path,
    project_vars: Dict[str, Any],
    provenance: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    merged = dict(commandline_vars or {})
    source_defaults = build_source_commandline_vars(repo_root=repo_root, project_vars=project_vars, provenance=provenance)
    if not source_defaults.get("source"):
        return merged
    source_ns = dict(merged.get("source") or {})
    for kind, payload in dict(source_defaults.get("source") or {}).items():
        current = dict(source_ns.get(kind) or {})
        for key, value in dict(payload or {}).items():
            if key not in current or current.get(key) in (None, ""):
                current[key] = value
        source_ns[kind] = current
    merged["source"] = source_ns
    return merged


def checkout(
    spec: CheckoutSpec,
    *,
    runner: Optional[Runner] = None,
    transport: Optional[CommandTransport] = None,
) -> list[str]:
    shell = runner or PosixShellRunner()
    channel = transport or SshTransport()
    repo_url = str(spec.repo_url or "").strip()
    revision = str(spec.revision or "").strip()
    checkout_root = str(spec.checkout_root or "").strip()
    remote_name = str(spec.remote_name or "origin").strip() or "origin"
    mode = str(spec.mode or "detached").strip().lower() or "detached"
    lines = [
        f"CHECKOUT_ROOT={checkout_root}",
        f"REPO_URL={repo_url}",
        f"REPO_REVISION={revision}",
    ]
    lines.extend(channel.render(shell, "mkdir -p \"$(dirname \\\"$CHECKOUT_ROOT\\\")\""))
    lines.extend(channel.render(shell, "if [ ! -d \"$CHECKOUT_ROOT\" ]; then mkdir -p \"$CHECKOUT_ROOT\"; fi"))
    lines.extend(
        channel.render(
            shell,
            "if [ -d \"$CHECKOUT_ROOT\" ] && [ ! -d \"$CHECKOUT_ROOT/.git\" ]; then rm -rf \"$CHECKOUT_ROOT\"; fi"
            ,
            options=ExecutionOptions(log_callback=None),
        )
    )
    lines.extend(
        channel.render(
            shell,
            "if [ ! -d \"$CHECKOUT_ROOT/.git\" ]; then git clone --no-checkout \"$REPO_URL\" \"$CHECKOUT_ROOT\"; fi",
            on_error="\"[etl][source_control] git clone failed\"",
            options=ExecutionOptions(log_callback=None),
        )
    )
    lines.extend(
        channel.render(
            shell,
            "cd \"$CHECKOUT_ROOT\"",
            on_error="\"[etl][source_control] cannot cd checkout root: $CHECKOUT_ROOT\"",
            options=ExecutionOptions(log_callback=None),
        )
    )
    fetch_cmd = "git fetch"
    if spec.fetch_tags:
        fetch_cmd += " --tags"
    if spec.prune:
        fetch_cmd += " --prune"
    fetch_cmd += f" {remote_name}"
    lines.extend(channel.render(shell, fetch_cmd, on_error="\"[etl][source_control] git fetch failed\"", options=ExecutionOptions(log_callback=None)))
    if spec.set_remote_url:
        lines.extend(channel.render(shell, f"git remote set-url {remote_name} \"$REPO_URL\" || true", options=ExecutionOptions(log_callback=None)))
    if mode == "branch":
        lines.extend(
            channel.render(
                shell,
                "git checkout \"$REPO_REVISION\"",
                on_error="\"[etl][source_control] git checkout failed for requested ref\"",
                options=ExecutionOptions(log_callback=None),
            )
        )
        lines.extend(channel.render(shell, f"git pull --ff-only {remote_name} \"$REPO_REVISION\" >/dev/null 2>&1 || true", options=ExecutionOptions(log_callback=None)))
    else:
        lines.extend(
            channel.render(
                shell,
                "git checkout --detach \"$REPO_REVISION\"",
                on_error="\"[etl][source_control] git checkout failed for requested SHA\"",
                options=ExecutionOptions(log_callback=None),
            )
        )
        lines.extend(
            channel.render(
                shell,
                "git reset --hard \"$REPO_REVISION\"",
                on_error="\"[etl][source_control] git reset failed for requested SHA\"",
                options=ExecutionOptions(log_callback=None),
            )
        )
    if spec.clean:
        lines.extend(channel.render(shell, "git clean -fd || true", options=ExecutionOptions(log_callback=None)))
    return lines


def checkin_single_file(
    *,
    file_path: str | Path,
    message: str,
    push: bool = False,
    remote: str = "origin",
    branch: str = "",
    env: Optional[Dict[str, str]] = None,
    transport: Optional[CommandTransport] = None,
) -> Dict[str, Any]:
    local_transport = transport or LocalProcessTransport()
    path = Path(file_path).expanduser()
    if not path.exists():
        return {"committed": False, "reason": "workspace_path_missing"}
    target = path.resolve()
    cur = target.parent if target.is_file() else target
    repo_root: Optional[Path] = None
    while True:
        if (cur / ".git").exists():
            repo_root = cur
            break
        if cur.parent == cur:
            break
        cur = cur.parent
    if repo_root is None:
        return {"committed": False, "reason": "git_repo_not_found"}
    rel = target.relative_to(repo_root).as_posix()
    proc_add = local_transport.run(
        ["git", "add", "--", rel],
        cwd=repo_root,
        env=env,
        check=False,
        options=ExecutionOptions(),
    )
    if proc_add.returncode != 0:
        return {"committed": False, "reason": "git_add_failed", "stderr": (proc_add.stderr or "").strip()}
    proc_diff = local_transport.run(
        ["git", "diff", "--cached", "--quiet", "--", rel],
        cwd=repo_root,
        env=env,
        check=False,
        options=ExecutionOptions(),
    )
    if proc_diff.returncode == 0:
        return {"committed": False, "reason": "no_staged_changes", "workspace_path": target.as_posix()}
    proc_commit = local_transport.run(
        ["git", "commit", "--only", "-m", str(message), "--", rel],
        cwd=repo_root,
        env=env,
        check=False,
        options=ExecutionOptions(),
    )
    if proc_commit.returncode != 0:
        return {
            "committed": False,
            "reason": "git_commit_failed",
            "stdout": (proc_commit.stdout or "").strip(),
            "stderr": (proc_commit.stderr or "").strip(),
        }
    out = {
        "committed": True,
        "workspace_path": target.as_posix(),
        "repo_root": repo_root.as_posix(),
        "message": str(message),
    }
    if push:
        if str(branch or "").strip():
            proc_push = local_transport.run(
                ["git", "push", str(remote or "origin"), f"HEAD:{str(branch).strip()}"],
                cwd=repo_root,
                env=env,
                check=False,
                options=ExecutionOptions(),
            )
        else:
            proc_push = local_transport.run(
                ["git", "push", str(remote or "origin"), "HEAD"],
                cwd=repo_root,
                env=env,
                check=False,
                options=ExecutionOptions(),
            )
        out["pushed"] = proc_push.returncode == 0
        out["push_stderr"] = (proc_push.stderr or "").strip()
    return out


__all__ = [
    "CheckoutSpec",
    "build_source_commandline_vars",
    "checkin_single_file",
    "checkout",
    "merge_source_commandline_vars",
    "resolve_source_override",
]

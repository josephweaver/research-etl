from __future__ import annotations

import subprocess
from dataclasses import replace
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, Optional

from ...git_checkout import infer_repo_name
from ...job_specs import PipelineAssetRef, ResolvedPaths, RunSelection, RunSpec, SourceRef
from ...pipeline import parse_pipeline
from ...pipeline_assets import pipeline_asset_sources_from_project_vars
from ...source_control import SourceControlError, make_git_source_provider, resolve_source_override
from ...variable_solver import VariableSolver

_SOURCE_PROVIDER = make_git_source_provider()


def _parse_bool(value: Any, default: bool = False) -> bool:
    if value is None:
        return bool(default)
    if isinstance(value, bool):
        return value
    text = str(value).strip().lower()
    if not text:
        return bool(default)
    if text in {"1", "true", "yes", "on", "y"}:
        return True
    if text in {"0", "false", "no", "off", "n"}:
        return False
    return bool(default)


def _parse_step_indices(value: Any, step_count: int) -> list[int]:
    if value is None or value == "":
        return []
    raw_items: list[Any]
    if isinstance(value, (list, tuple, set)):
        raw_items = list(value)
    else:
        raw_items = [x.strip() for x in str(value).replace(";", ",").split(",")]
    out: list[int] = []
    seen: set[int] = set()
    for raw in raw_items:
        text = str(raw).strip()
        if not text:
            continue
        idx = int(text)
        if idx < 0 or idx >= int(step_count):
            raise RuntimeError(f"Step index out of range in context.step_indices: {idx} (step_count={step_count})")
        if idx in seen:
            continue
        seen.add(idx)
        out.append(idx)
    out.sort()
    return out


def _rewrite_asset_cache_pipeline_rel(pipeline_rel: Path) -> Optional[Path]:
    parts = list(pipeline_rel.parts)
    for idx, part in enumerate(parts):
        if part in {".pipeline_assets_cache", ".pipeline_assets"}:
            try:
                pipe_idx = parts.index("pipelines", idx + 1)
            except ValueError:
                return None
            tail = parts[pipe_idx + 1 :]
            return Path("pipelines", *tail) if tail else Path("pipelines")
    return None


def _git_current_branch(repo_path: Path) -> Optional[str]:
    try:
        proc = subprocess.run(
            ["git", "-C", str(repo_path), "rev-parse", "--abbrev-ref", "HEAD"],
            capture_output=True,
            text=True,
            check=False,
        )
    except Exception:
        return None
    if proc.returncode != 0:
        return None
    branch = str(proc.stdout or "").strip()
    if not branch or branch == "HEAD":
        return None
    return branch


def _resolve_pipeline_asset_refs(
    project_vars: Dict[str, Any],
    repo_root: Path,
    commandline_vars: Optional[Dict[str, Any]] = None,
) -> list[PipelineAssetRef]:
    try:
        sources = pipeline_asset_sources_from_project_vars(project_vars)
    except Exception:
        return []

    overlays: list[PipelineAssetRef] = []
    seen: set[tuple[str, str, str, str]] = set()
    base_root = Path(repo_root).resolve()
    for src in sources:
        repo_url = str(getattr(src, "repo_url", "") or "").strip()
        if not repo_url:
            continue
        ref = str(getattr(src, "ref", "") or "main").strip() or "main"
        local_repo_path = str(getattr(src, "local_repo_path", "") or "").strip() or None
        if local_repo_path:
            local = Path(local_repo_path).expanduser()
            if not local.is_absolute():
                local = (base_root / local).resolve()
            if local.exists() and local.is_dir():
                local_branch = _git_current_branch(local)
                if local_branch:
                    ref = local_branch
        pipelines_dir = str(getattr(src, "pipelines_dir", "") or "pipelines").strip() or "pipelines"
        scripts_dir = str(getattr(src, "scripts_dir", "") or "scripts").strip() or "scripts"
        key = (repo_url, ref, pipelines_dir, scripts_dir)
        if key in seen:
            continue
        seen.add(key)
        overlays.append(
            PipelineAssetRef(
                repo_url=repo_url,
                revision=ref,
                repo_name=infer_repo_name(repo_url),
                pipelines_dir=pipelines_dir,
                scripts_dir=scripts_dir,
                local_repo_path=local_repo_path,
            )
        )
    override = resolve_source_override(
        commandline_vars=dict(commandline_vars or {}),
        kind="pipeline",
        fallback=None,
    )
    if override and overlays:
        first = overlays[0]
        overlays[0] = replace(
            first,
            repo_url=str(override.origin_url or first.repo_url or "").strip(),
            revision=str(override.revision or first.revision or "").strip() or "main",
        )
    return overlays


class SlurmRunSpecBuilder:
    def __init__(
        self,
        executor: Any,
        *,
        parse_pipeline_fn=None,
        resolve_execution_spec_fn=None,
        repo_relative_path_fn=None,
    ) -> None:
        self.executor = executor
        self.parse_pipeline_fn = parse_pipeline_fn or parse_pipeline
        self.resolve_execution_spec_fn = resolve_execution_spec_fn or _SOURCE_PROVIDER.resolve_execution_spec
        self.repo_relative_path_fn = repo_relative_path_fn or _SOURCE_PROVIDER.repo_relative_path

    def build(self, pipeline_path: str, context: Dict[str, Any]) -> RunSpec:
        executor = self.executor
        pipeline_input = Path(pipeline_path)
        pipeline_path_text = pipeline_input.as_posix()
        provenance = dict(context.get("provenance") or {})
        source_repo_root = Path(context.get("repo_root") or Path(".").resolve()).resolve()
        run_id = str(context.get("run_id") or "").strip()
        if not run_id:
            import uuid

            run_id = uuid.uuid4().hex
        ts = datetime.utcnow()
        run_date = ts.strftime("%y%m%d")
        run_stamp = ts.strftime("%H%M%S")
        run_started_at = ts.isoformat() + "Z"
        run_fs_id = f"{run_stamp}-{run_id[:8]}"
        global_ns = dict(context.get("global_vars") or {})
        env_ns = dict(context.get("execution_env") or {})
        project_ns = dict(context.get("project_vars") or {})
        commandline_ns = dict(context.get("commandline_vars") or {})
        pipeline = self.parse_pipeline_fn(
            Path(pipeline_path_text),
            global_vars=global_ns,
            env_vars=env_ns,
            project_vars=project_ns,
            context_vars=commandline_ns,
        )
        batches = executor._group_steps_with_indices(pipeline.steps)
        requested_step_indices = _parse_step_indices(context.get("step_indices"), len(pipeline.steps))
        if requested_step_indices:
            wanted = set(requested_step_indices)
            batches = [[pair for pair in batch if int(pair[0]) in wanted] for batch in batches]
            batches = [batch for batch in batches if batch]

        jobname = str(pipeline.vars.get("jobname") or pipeline.vars.get("name") or "run")
        pipeline_workdir_template = str((getattr(pipeline, "dirs", {}) or {}).get("workdir") or "").strip()
        resolve_max_passes = max(1, int(getattr(pipeline, "resolve_max_passes", 20) or 20))
        path_style = str(env_ns.get("path_style") or "").strip()
        style_norm = VariableSolver._normalize_path_style(path_style) or "unix"

        solver = VariableSolver(max_passes=resolve_max_passes)
        solver.overlay("global", global_ns, add_namespace=True, add_flat=True)
        solver.overlay("globals", global_ns, add_namespace=True, add_flat=False)
        solver.overlay("env", env_ns, add_namespace=True, add_flat=True)
        solver.overlay("project", project_ns, add_namespace=True, add_flat=True)
        solver.overlay("pipe", dict(getattr(pipeline, "vars", {}) or {}), add_namespace=True, add_flat=True)
        solver.overlay("vars", dict(getattr(pipeline, "vars", {}) or {}), add_namespace=True, add_flat=False)
        solver.overlay("dirs", dict(getattr(pipeline, "dirs", {}) or {}), add_namespace=True, add_flat=True)
        solver.overlay("commandline", commandline_ns, add_namespace=True, add_flat=True)
        solver.with_sys(
            {
                "run": {"id": run_id, "short_id": run_id[:8]},
                "job": {"id": run_id, "name": jobname},
                "step": {"id": "", "name": "", "index": ""},
                "now": {"yymmdd": run_date, "hhmmss": run_stamp},
            }
        )
        solver.update({"run_id": run_id, "jobname": jobname})
        env_workdir_default = solver.get_path(
            "env.workdir",
            str(executor.env.workdir or executor.workdir),
            path_style=style_norm,
        )
        if "{" in env_workdir_default or "}" in env_workdir_default:
            env_workdir_default = Path(executor.env.workdir or executor.workdir).as_posix()
        default_remote_workdir = (Path(env_workdir_default) / jobname / run_date / run_fs_id).as_posix()
        remote_workdir = default_remote_workdir
        if pipeline_workdir_template:
            solver.update({"_candidate_workdir": pipeline_workdir_template})
            candidate = str(solver.get("_candidate_workdir", "", resolve=True) or "").strip()
            if "{" in candidate or "}" in candidate:
                raise RuntimeError(
                    "Could not fully resolve pipeline dirs.workdir for SLURM submit: "
                    f"{pipeline_workdir_template}"
                )
            if candidate:
                resolved_candidate = solver.get_path(
                    "_candidate_workdir",
                    candidate,
                    resolve=True,
                    path_style=style_norm,
                )
                candidate_path = Path(resolved_candidate)
                remote_workdir = (
                    candidate_path.as_posix()
                    if candidate_path.is_absolute()
                    else (Path(env_workdir_default) / candidate_path).as_posix()
                )
        solver.update({"workdir": remote_workdir})
        remote_workdir_root = Path(remote_workdir)
        child_jobs_file = f"{remote_workdir}/child_jobs.txt"
        context_file = f"{remote_workdir}/context.json"
        checkout_root = (executor.remote_base / executor.local_repo_name).as_posix()
        source_mode = str(context.get("execution_source") or executor.execution_source or "auto").strip().lower()
        source_bundle = context.get("source_bundle") or executor.source_bundle
        source_snapshot = context.get("source_snapshot") or executor.source_snapshot
        allow_workspace_source = _parse_bool(
            context.get("allow_workspace_source", executor.allow_workspace_source),
            default=executor.allow_workspace_source,
        )
        selected_source_mode = "workspace"
        git_remote_override = (
            str(env_ns.get("git_remote_url") or "").strip()
            or str(global_ns.get("etl_git_remote_url") or "").strip()
            or str(executor.env.git_remote_url or "").strip()
            or None
        )
        git_origin_url = None
        git_commit_sha = None
        git_repo_name = None

        if executor.enforce_git_checkout:
            if source_mode == "workspace" and not allow_workspace_source:
                raise RuntimeError("execution_source=workspace requires allow_workspace_source=true")
            spec = None
            spec_error: Optional[Exception] = None
            if source_mode in {"git_remote", "auto"}:
                try:
                    spec = self.resolve_execution_spec_fn(
                        repo_root=source_repo_root,
                        provenance=provenance,
                        require_clean=executor.require_clean_git,
                        require_origin=not bool(git_remote_override),
                    )
                except Exception as exc:
                    spec_error = exc
            elif source_mode == "git_bundle":
                try:
                    spec = self.resolve_execution_spec_fn(
                        repo_root=source_repo_root,
                        provenance=provenance,
                        require_clean=executor.require_clean_git,
                        require_origin=False,
                    )
                except Exception as exc:
                    spec_error = exc
            elif source_mode == "snapshot":
                try:
                    spec = self.resolve_execution_spec_fn(
                        repo_root=source_repo_root,
                        provenance=provenance,
                        require_clean=False,
                        require_origin=False,
                    )
                except Exception as exc:
                    spec_error = exc
            if source_mode in {"git_remote", "git_bundle", "snapshot"} and spec is None:
                raise RuntimeError(str(spec_error or "Could not resolve execution source metadata"))

            if spec is not None:
                if git_remote_override:
                    spec = replace(spec, origin_url=git_remote_override)
                spec = resolve_source_override(
                    commandline_vars=commandline_ns,
                    kind="etl",
                    fallback=spec,
                ) or spec
                git_origin_url = spec.origin_url
                git_commit_sha = spec.commit_sha
                git_repo_name = spec.repo_name
                provenance["git_repo_name"] = spec.repo_name
                provenance["git_commit_sha"] = spec.commit_sha
                if spec.origin_url:
                    provenance["git_origin_url"] = spec.origin_url
                if source_mode == "git_remote":
                    selected_source_mode = "git_remote"
                elif source_mode == "git_bundle":
                    selected_source_mode = "git_bundle"
                elif source_mode == "snapshot":
                    selected_source_mode = "snapshot"
                else:
                    selected_source_mode = "auto"
                if selected_source_mode in {"git_remote", "auto", "git_bundle", "snapshot"}:
                    if executor.env.ssh_host:
                        remote_base = (executor.env.remote_repo or "$HOME/.etl/checkouts").rstrip("/")
                        checkout_root = f"{remote_base}/{spec.repo_name}-{spec.commit_sha[:12]}"
                    else:
                        checkout_root = (Path(executor.workdir) / "_code" / f"{spec.repo_name}-{spec.commit_sha[:12]}").as_posix()
            elif source_mode == "workspace":
                selected_source_mode = "workspace"
                checkout_root = (
                    (
                        Path(executor.env.remote_repo).as_posix()
                        if (executor.env.ssh_host and executor.env.remote_repo)
                        else (executor.remote_base / executor.local_repo_name).as_posix()
                    )
                    if executor.env.ssh_host
                    else source_repo_root.as_posix()
                )
            elif source_mode == "auto":
                selected_source_mode = "auto"
                checkout_root = (
                    (
                        Path(executor.env.remote_repo).as_posix()
                        if (executor.env.ssh_host and executor.env.remote_repo)
                        else (executor.remote_base / executor.local_repo_name).as_posix()
                    )
                    if executor.env.ssh_host
                    else source_repo_root.as_posix()
                )
            provenance["source_mode"] = selected_source_mode

        venv_path = (Path(executor.env.venv) if executor.env.venv else Path(checkout_root) / ".venv").as_posix()
        req_path = (
            Path(executor.env.requirements).as_posix()
            if executor.env.requirements
            else (Path(checkout_root) / "requirements.txt").as_posix()
        )
        python_bin = executor.env.python or "python3"
        use_repo_relative_paths = executor.enforce_git_checkout and selected_source_mode != "workspace"
        workspace_repo_root = Path(checkout_root)
        pipeline_remote_hint = str(context.get("pipeline_remote_hint") or "").strip()

        if use_repo_relative_paths:
            try:
                pipeline_rel = self.repo_relative_path_fn(pipeline_input, source_repo_root, "pipeline")
                rewritten_rel = _rewrite_asset_cache_pipeline_rel(pipeline_rel)
                if rewritten_rel is not None:
                    pipeline_rel = rewritten_rel
                pipeline_remote = (Path(checkout_root) / pipeline_rel).as_posix()
            except SourceControlError as exc:
                if pipeline_remote_hint:
                    pipeline_remote = (Path(checkout_root) / Path(pipeline_remote_hint)).as_posix()
                else:
                    raise RuntimeError(
                        "Pipeline path is outside the execution source repository and no pipeline_remote_hint was "
                        f"provided: {pipeline_input}"
                    ) from exc
        else:
            pipeline_remote = (
                Path(pipeline_path_text).as_posix()
                if Path(pipeline_path_text).is_absolute()
                else (workspace_repo_root / Path(pipeline_path_text)).as_posix()
            )

        if use_repo_relative_paths:
            plugins_rel = self.repo_relative_path_fn(executor.plugins_dir, source_repo_root, "plugins_dir")
            plugins_remote = (Path(checkout_root) / plugins_rel).as_posix()
        else:
            plugins_remote = (
                (workspace_repo_root / executor.plugins_dir).as_posix()
                if not executor.plugins_dir.is_absolute()
                else executor.plugins_dir.as_posix()
            )

        global_config_remote = None
        if executor.global_config:
            gc_path = Path(executor.global_config)
            if use_repo_relative_paths:
                gc_rel = self.repo_relative_path_fn(gc_path, source_repo_root, "global_config")
                global_config_remote = (Path(checkout_root) / gc_rel).as_posix()
            else:
                global_config_remote = (
                    (Path(checkout_root) / gc_path).as_posix() if not gc_path.is_absolute() else gc_path.as_posix()
                )
        environments_config_remote = None
        if executor.environments_config and executor.env_name:
            ec_path = Path(executor.environments_config)
            if use_repo_relative_paths:
                ec_rel = self.repo_relative_path_fn(ec_path, source_repo_root, "environments_config")
                environments_config_remote = (Path(checkout_root) / ec_rel).as_posix()
            else:
                environments_config_remote = (
                    (Path(checkout_root) / ec_path).as_posix() if not ec_path.is_absolute() else ec_path.as_posix()
                )
        projects_config_remote = None
        if executor.projects_config:
            pc_path = Path(executor.projects_config)
            if use_repo_relative_paths:
                pc_rel = self.repo_relative_path_fn(pc_path, source_repo_root, "projects_config")
                projects_config_remote = (Path(checkout_root) / pc_rel).as_posix()
            else:
                projects_config_remote = (
                    (Path(checkout_root) / pc_path).as_posix() if not pc_path.is_absolute() else pc_path.as_posix()
                )

        pipeline_logdir_resolved = str(solver.get_path("dirs.logdir", "", path_style=style_norm) or "").strip()
        if "{" in pipeline_logdir_resolved or "}" in pipeline_logdir_resolved:
            pipeline_logdir_resolved = ""
        base_logdir = (
            Path(pipeline_logdir_resolved)
            if pipeline_logdir_resolved
            else (Path(remote_workdir) / "logs")
        )
        setup_logdir = (base_logdir / "setup").as_posix()
        workdirs_to_create: list[str] = []
        logdirs_to_create: list[str] = [setup_logdir]
        for batch_idx, batch in enumerate(batches):
            if len(batch) == 1:
                step_idx, step = batch[0]
                step_name = getattr(step, "name", f"step{step_idx}")
                label = step_name
                workdirs_to_create.append((remote_workdir_root / label).as_posix())
                logdirs_to_create.append((base_logdir / label).as_posix())
            else:
                chunk_size = min(executor.array_task_limit, len(batch))
                start = 0
                while start < len(batch):
                    chunk = batch[start : start + chunk_size]
                    first_name = getattr(chunk[0][1], "name", f"step{chunk[0][0]}")
                    label = f"{first_name}_array{batch_idx}_chunk{start}"
                    workdirs_to_create.append((remote_workdir_root / label).as_posix())
                    logdirs_to_create.append((base_logdir / label).as_posix())
                    start += chunk_size

        pipeline_asset_refs = _resolve_pipeline_asset_refs(project_ns, source_repo_root, commandline_ns)
        etl_source = SourceRef(
            provider="git",
            repo_url=git_origin_url,
            revision=git_commit_sha,
            repo_name=git_repo_name or infer_repo_name(git_origin_url or executor.local_repo_name),
            mode=selected_source_mode,
            checkout_root=checkout_root,
            bundle_path=str(source_bundle or "").strip() or None,
            snapshot_path=str(source_snapshot or "").strip() or None,
            local_repo_path=(source_repo_root.as_posix() if selected_source_mode == "workspace" else None),
            metadata={
                "use_repo_relative_paths": bool(use_repo_relative_paths),
                "allow_workspace_source": bool(allow_workspace_source),
                "pipeline_remote_hint": pipeline_remote_hint or None,
                "path_style": style_norm,
            },
        )
        paths = ResolvedPaths(
            workdir=remote_workdir,
            base_logdir=base_logdir.as_posix(),
            setup_logdir=setup_logdir,
            context_file=context_file,
            child_jobs_file=child_jobs_file,
            checkout_root=checkout_root,
            pipeline_path=pipeline_remote,
            plugins_dir=plugins_remote,
            venv_path=venv_path,
            requirements_path=req_path,
            python_bin=python_bin,
            workdirs_to_create=workdirs_to_create,
            logdirs_to_create=logdirs_to_create,
            global_config_path=global_config_remote,
            projects_config_path=projects_config_remote,
            environments_config_path=environments_config_remote,
        )
        selection = RunSelection(
            selected_step_indices=requested_step_indices,
            resume_run_id=(str(context.get("resume_run_id") or "").strip() or None),
            run_started_at=run_started_at,
            max_retries=int(executor.step_max_retries or 0),
            retry_delay_seconds=float(executor.step_retry_delay_seconds or 0.0),
            verbose=bool(executor.verbose),
            dry_run=bool(executor.dry_run),
        )
        return RunSpec(
            run_id=run_id,
            project_id=(str(context.get("project_id") or "").strip() or None),
            pipeline_path_input=pipeline_path_text,
            pipeline_name=(str(getattr(pipeline, "name", "") or "").strip() or None),
            job_name=jobname,
            source_repo_root=source_repo_root.as_posix(),
            created_at=ts,
            run_date=run_date,
            run_stamp=run_stamp,
            run_fs_id=run_fs_id,
            pipeline=pipeline,
            batches=batches,
            etl_source=etl_source,
            pipeline_assets=pipeline_asset_refs,
            paths=paths,
            selection=selection,
            execution_env_name=executor.env_name,
            execution_env=env_ns,
            global_vars=global_ns,
            project_vars=project_ns,
            commandline_vars=commandline_ns,
            provenance=provenance,
            metadata={
                "selected_source_mode": selected_source_mode,
                "allow_workspace_source": bool(allow_workspace_source),
                "source_bundle_path": str(source_bundle or "").strip() or None,
                "source_snapshot_path": str(source_snapshot or "").strip() or None,
                "pipeline_logdir_resolved": pipeline_logdir_resolved or None,
                "use_repo_relative_paths": bool(use_repo_relative_paths),
            },
        )

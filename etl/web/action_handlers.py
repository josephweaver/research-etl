# research-etl
# Copyright (c) 2026 Joseph Weaver
# This file is part of the research-etl project and is licensed under the MIT License.
# You may not use this file except in compliance with the License.
# See https://github.com/josephweaver/research-etl for details.

from __future__ import annotations

from datetime import datetime
from pathlib import Path
from typing import Any, Optional
import uuid

from fastapi import HTTPException, Request
from etl.source_control import merge_source_commandline_vars


def parse_action_payload(payload: Optional[dict[str, Any]], deps: dict[str, Any]) -> dict[str, Any]:
    payload = payload or {}
    pipeline_raw = str(payload.get("pipeline") or "").strip()
    if not pipeline_raw:
        raise HTTPException(status_code=400, detail="`pipeline` is required.")
    pipeline_path = Path(pipeline_raw).expanduser()

    global_config_raw = str(payload.get("global_config") or "").strip()
    environments_config_raw = str(payload.get("environments_config") or "").strip()
    projects_config_raw = str(payload.get("projects_config") or "").strip()
    env_name_raw = str(payload.get("env") or "").strip()
    global_config_path = Path(global_config_raw).expanduser() if global_config_raw else None
    environments_config_path = Path(environments_config_raw).expanduser() if environments_config_raw else None
    projects_config_path = Path(projects_config_raw).expanduser() if projects_config_raw else None
    env_name = env_name_raw or None

    executor = str(payload.get("executor") or "").strip().lower()
    if env_name:
        try:
            resolved_env_cfg = deps["resolve_execution_config_path"](environments_config_path)
        except deps["ExecutionConfigError"] as exc:
            raise HTTPException(status_code=400, detail=f"Environments config error: {exc}") from exc
        if resolved_env_cfg:
            try:
                envs = deps["load_execution_config"](resolved_env_cfg)
            except deps["ExecutionConfigError"] as exc:
                raise HTTPException(status_code=400, detail=f"Environments config error: {exc}") from exc
            env_spec = envs.get(env_name)
            if isinstance(env_spec, dict):
                env_exec = str(env_spec.get("executor") or "").strip().lower()
                if env_exec:
                    executor = env_exec

    if not executor:
        executor = "local"
    if executor not in {"local", "slurm", "hpcc_direct"}:
        raise HTTPException(status_code=400, detail="`executor` must be one of: local, slurm, hpcc_direct.")

    max_retries = deps["parse_optional_int"](payload.get("max_retries"), field_name="max_retries")
    retry_delay_seconds = deps["parse_optional_float"](payload.get("retry_delay_seconds"), field_name="retry_delay_seconds")
    if max_retries is not None and max_retries < 0:
        raise HTTPException(status_code=400, detail="Invalid max_retries: must be >= 0.")
    if retry_delay_seconds is not None and retry_delay_seconds < 0:
        raise HTTPException(status_code=400, detail="Invalid retry_delay_seconds: must be >= 0.")

    execution_source = str(payload.get("execution_source") or "").strip().lower() or None
    if execution_source and execution_source not in {"auto", "git_remote", "git_bundle", "snapshot", "workspace"}:
        raise HTTPException(
            status_code=400,
            detail="`execution_source` must be one of: auto, git_remote, git_bundle, snapshot, workspace.",
        )

    workdir_raw = str(payload.get("workdir") or "").strip()
    run_id = str(payload.get("run_id") or "").strip() or None
    run_started_at = str(payload.get("run_started_at") or "").strip() or None

    return {
        "payload": payload,
        "pipeline_path": pipeline_path,
        "global_config_path": global_config_path,
        "environments_config_path": environments_config_path,
        "projects_config_path": projects_config_path,
        "env_name": env_name,
        "executor": executor,
        "plugins_dir": Path(payload.get("plugins_dir") or "plugins"),
        "workdir_raw": workdir_raw,
        "workdir": Path(workdir_raw or ".runs"),
        "dry_run": deps["parse_bool"](payload.get("dry_run"), default=False),
        "verbose": deps["parse_bool"](payload.get("verbose"), default=False),
        "max_retries": max_retries,
        "retry_delay_seconds": retry_delay_seconds,
        "execution_source": execution_source,
        "source_bundle": str(payload.get("source_bundle") or "").strip() or None,
        "source_snapshot": str(payload.get("source_snapshot") or "").strip() or None,
        "allow_workspace_source": deps["parse_bool"](payload.get("allow_workspace_source"), default=False),
        "project_id": deps["normalize_project_id"](str(payload.get("project_id") or "").strip() or None),
        "pipeline_source": str(payload.get("pipeline_source") or "").strip() or None,
        "run_id": run_id,
        "run_started_at": run_started_at,
    }


def resolve_execution_env(
    environments_config_path: Optional[Path],
    env_name: Optional[str],
    *,
    executor: str,
    global_vars: Optional[dict[str, Any]] = None,
    deps: dict[str, Any],
) -> tuple[dict[str, Any], Optional[Path], Optional[str]]:
    try:
        resolved = deps["resolve_execution_config_path"](environments_config_path)
    except deps["ExecutionConfigError"] as exc:
        raise HTTPException(status_code=400, detail=f"Environments config error: {exc}") from exc
    selected_env_name = str(env_name or "").strip() or ("local" if executor == "local" else None)
    if not resolved and not selected_env_name:
        return {}, None, None
    if not resolved and selected_env_name:
        if selected_env_name == "local" and not env_name:
            return {}, None, selected_env_name
        raise HTTPException(status_code=400, detail="`env` was provided but no environments config was found.")
    if not resolved:
        return {}, None, selected_env_name
    try:
        if not selected_env_name:
            return {}, resolved, None
        envs = deps["load_execution_config"](resolved)
        env = envs.get(str(selected_env_name), {})
        if not env:
            raise HTTPException(status_code=400, detail=f"Execution env '{selected_env_name}' not found in config.")
        deps["validate_environment_executor"](str(selected_env_name), env, executor=executor)
        resolved_env = deps["apply_execution_env_overrides"](env)
        resolved_env = deps["resolve_execution_env_templates"](resolved_env, global_vars=global_vars or {})
        return resolved_env, resolved, selected_env_name
    except HTTPException:
        raise
    except deps["ExecutionConfigError"] as exc:
        raise HTTPException(status_code=400, detail=f"Environments config error: {exc}") from exc


def resolve_action_pipeline_context(
    *,
    pipeline_path: Path,
    requested_project_id: Optional[str],
    pipeline_source: Optional[str],
    projects_config_path: Optional[Path],
    global_vars: dict[str, Any],
    execution_env: dict[str, Any],
    deps: dict[str, Any],
) -> tuple[Path, str, dict[str, Any], Optional[Path]]:
    try:
        selected_projects_config = deps["resolve_projects_config_path"](projects_config_path)
    except deps["ProjectConfigError"] as exc:
        raise HTTPException(status_code=400, detail=f"Projects config error: {exc}") from exc

    tentative_project_id = deps["resolve_project_id"](
        explicit_project_id=requested_project_id,
        pipeline_project_id=None,
        pipeline_path=pipeline_path,
    )
    try:
        tentative_project_vars = deps["load_project_vars"](
            project_id=tentative_project_id,
            projects_config_path=selected_projects_config,
        )
    except deps["ProjectConfigError"] as exc:
        raise HTTPException(status_code=400, detail=f"Projects config error: {exc}") from exc

    if tentative_project_id and pipeline_source:
        resolved_pipeline_path = deps["resolve_project_writable_pipeline_path"](
            pipeline=str(pipeline_path.as_posix()),
            project_id=tentative_project_id,
            projects_config=str(selected_projects_config) if selected_projects_config else None,
            pipeline_source=pipeline_source,
        )
    else:
        try:
            resolved_pipeline_path = deps["resolve_pipeline_path_from_project_sources"](
                pipeline_path,
                project_vars=tentative_project_vars,
                repo_root=Path(".").resolve(),
            )
        except deps["PipelineAssetError"] as exc:
            raise HTTPException(status_code=400, detail=f"Pipeline asset resolution error: {exc}") from exc
    if not resolved_pipeline_path.exists():
        raise HTTPException(status_code=400, detail=f"Pipeline path not found: {pipeline_path}")

    try:
        pre_pipeline = deps["parse_pipeline"](
            resolved_pipeline_path,
            global_vars=global_vars,
            env_vars=execution_env,
        )
    except (deps["PipelineError"], FileNotFoundError) as exc:
        raise HTTPException(status_code=400, detail=f"Invalid pipeline: {exc}") from exc

    project_id = deps["resolve_project_id"](
        explicit_project_id=requested_project_id,
        pipeline_project_id=getattr(pre_pipeline, "project_id", None),
        pipeline_path=resolved_pipeline_path,
    )
    try:
        project_vars = deps["load_project_vars"](project_id=project_id, projects_config_path=selected_projects_config)
    except deps["ProjectConfigError"] as exc:
        raise HTTPException(status_code=400, detail=f"Projects config error: {exc}") from exc
    return resolved_pipeline_path, project_id, project_vars, selected_projects_config


def api_action_validate(request: Request, payload: Optional[dict[str, Any]], deps: dict[str, Any]) -> dict:
    scope = deps["resolve_user_scope"](request)
    args = parse_action_payload(payload, deps)
    requested_project_id = deps["normalize_project_id"](args.get("project_id"))
    deps["require_project_access"](scope, requested_project_id)
    global_vars = deps["resolve_global_vars"](args["global_config_path"])
    execution_env, _environments_config_path, _env_name = resolve_execution_env(
        args["environments_config_path"],
        args["env_name"],
        executor=args["executor"],
        global_vars=global_vars,
        deps=deps,
    )
    resolved_pipeline_path, project_id, project_vars, _selected_projects_config = resolve_action_pipeline_context(
        pipeline_path=args["pipeline_path"],
        requested_project_id=requested_project_id,
        pipeline_source=args.get("pipeline_source"),
        projects_config_path=args["projects_config_path"],
        global_vars=global_vars,
        execution_env=execution_env,
        deps=deps,
    )
    deps["require_project_access"](scope, project_id)
    try:
        solver = deps["build_web_request_solver"](
            global_vars=global_vars,
            env_vars=execution_env,
            project_vars=project_vars,
        )
        pipeline = deps["parse_pipeline"](
            resolved_pipeline_path,
            global_vars=global_vars,
            env_vars=execution_env,
            project_vars=project_vars,
            context_vars=solver.get("context", {}, resolve=False) or {},
        )
    except (deps["PipelineError"], FileNotFoundError) as exc:
        deps["record_pipeline_validation"](
            pipeline=str(resolved_pipeline_path),
            project_id=project_id,
            valid=False,
            step_count=0,
            step_names=[],
            error=str(exc),
            source="api_validate",
        )
        raise HTTPException(status_code=400, detail=f"Invalid pipeline: {exc}") from exc
    deps["record_pipeline_validation"](
        pipeline=str(resolved_pipeline_path),
        project_id=project_id,
        valid=True,
        step_count=len(pipeline.steps),
        step_names=[s.name for s in pipeline.steps],
        error=None,
        source="api_validate",
    )
    return {
        "valid": True,
        "pipeline": str(resolved_pipeline_path),
        "steps": [s.name for s in pipeline.steps],
        "step_count": len(pipeline.steps),
    }


def api_action_run(request: Request, payload: Optional[dict[str, Any]], deps: dict[str, Any]) -> dict:
    scope = deps["resolve_user_scope"](request)
    args = parse_action_payload(payload, deps)
    requested_project_id = deps["normalize_project_id"](args.get("project_id"))
    deps["require_project_access"](scope, requested_project_id)
    global_vars = deps["resolve_global_vars"](args["global_config_path"])
    execution_env, environments_config_path, env_name = resolve_execution_env(
        args["environments_config_path"],
        args["env_name"],
        executor=args["executor"],
        global_vars=global_vars,
        deps=deps,
    )
    max_retries = args["max_retries"] if args["max_retries"] is not None else int(execution_env.get("step_max_retries", 0) or 0)
    retry_delay_seconds = (
        args["retry_delay_seconds"] if args["retry_delay_seconds"] is not None else float(execution_env.get("step_retry_delay_seconds", 0.0) or 0.0)
    )
    execution_env["step_max_retries"] = max_retries
    execution_env["step_retry_delay_seconds"] = retry_delay_seconds
    execution_source = str(args["execution_source"] or execution_env.get("execution_source") or "auto").strip().lower()
    source_bundle = args["source_bundle"] or execution_env.get("source_bundle")
    source_snapshot = args["source_snapshot"] or execution_env.get("source_snapshot")
    allow_workspace_source = deps["parse_bool"](
        args["allow_workspace_source"],
        default=deps["parse_bool"](execution_env.get("allow_workspace_source"), default=False),
    )
    resolved_pipeline_path, project_id, project_vars, selected_projects_config = resolve_action_pipeline_context(
        pipeline_path=args["pipeline_path"],
        requested_project_id=requested_project_id,
        pipeline_source=args.get("pipeline_source"),
        projects_config_path=args["projects_config_path"],
        global_vars=global_vars,
        execution_env=execution_env,
        deps=deps,
    )
    deps["require_project_access"](scope, project_id)
    repo_root = Path(".").resolve()
    try:
        resolved_pipeline_path.resolve().relative_to(repo_root)
        pipeline_inside_repo = True
    except ValueError:
        pipeline_inside_repo = False
    pipeline_remote_hint: Optional[str] = None
    if args["executor"] in {"slurm", "hpcc_direct"} and not pipeline_inside_repo:
        if execution_source == "workspace":
            execution_source = str(execution_env.get("execution_source") or "auto").strip().lower() or "auto"
            allow_workspace_source = False
        pipeline_remote_hint = deps["infer_external_pipeline_remote_hint"](
            pipeline_path=resolved_pipeline_path,
            project_vars=project_vars,
            repo_root=repo_root,
        )
        if not pipeline_remote_hint:
            raise HTTPException(
                status_code=400,
                detail=(
                    "External pipeline path could not be mapped to any configured project pipeline asset source. "
                    "Check project pipeline_asset_sources/pipelines_dir config or run locally."
                ),
            )
    try:
        pipeline = deps["parse_pipeline"](
            resolved_pipeline_path,
            global_vars=global_vars,
            env_vars=execution_env,
            project_vars=project_vars,
        )
    except (deps["PipelineError"], FileNotFoundError) as exc:
        raise HTTPException(status_code=400, detail=f"Invalid pipeline: {exc}") from exc
    commandline_vars = {}
    workdir_raw = str(args.get("workdir_raw") or "").strip()
    if workdir_raw:
        commandline_vars["workdir"] = workdir_raw
    solver = deps["build_web_request_solver"](
        global_vars=global_vars,
        env_vars=execution_env,
        project_vars=project_vars,
        commandline_vars=commandline_vars,
        pipeline=pipeline,
    )
    max_passes = int(getattr(solver, "max_passes", deps["DEFAULT_RESOLVE_MAX_PASSES"]) or deps["DEFAULT_RESOLVE_MAX_PASSES"])
    resolved_workdir_text = deps["resolve_workdir_from_solver"](
        solver=solver,
        pipeline_workdir=str(getattr(pipeline, "workdir", "") or ""),
        resolve_text_iterative=lambda value, ctx: deps["resolve_text_with_ctx_iterative"](value, ctx, max_passes=max_passes),
        fallback=".runs",
    )

    provenance = deps["collect_run_provenance"](
        repo_root=repo_root,
        pipeline_path=resolved_pipeline_path,
        global_config_path=args["global_config_path"],
        environments_config_path=environments_config_path,
        plugin_dir=args["plugins_dir"],
        pipeline=pipeline,
        cli_command=f"etl web run {resolved_pipeline_path}",
    )
    commandline_vars = merge_source_commandline_vars(
        commandline_vars,
        repo_root=repo_root,
        project_vars=project_vars,
        provenance=provenance,
    )
    if args["executor"] == "slurm":
        ex = deps["SlurmExecutor"](
            env_config=execution_env,
            repo_root=repo_root,
            plugins_dir=args["plugins_dir"],
            workdir=Path(resolved_workdir_text),
            global_config=args["global_config_path"],
            projects_config=selected_projects_config,
            environments_config=environments_config_path,
            env_name=env_name,
            dry_run=args["dry_run"],
            verbose=args["verbose"],
            enforce_git_checkout=True,
            require_clean_git=True,
            execution_source=execution_source,
            source_bundle=source_bundle,
            source_snapshot=source_snapshot,
            allow_workspace_source=allow_workspace_source,
        )
    elif args["executor"] == "hpcc_direct":
        ex = deps["HpccDirectExecutor"](
            env_config=execution_env,
            repo_root=repo_root,
            plugins_dir=args["plugins_dir"],
            workdir=Path(resolved_workdir_text),
            global_config=args["global_config_path"],
            projects_config=selected_projects_config,
            environments_config=environments_config_path,
            env_name=env_name,
            dry_run=args["dry_run"],
            verbose=args["verbose"],
        )
    else:
        run_id = str(args.get("run_id") or "").strip() or uuid.uuid4().hex
        run_started_at = str(args.get("run_started_at") or "").strip() or (datetime.utcnow().isoformat() + "Z")
        pipeline_path_resolved = resolved_pipeline_path.resolve()
        dedupe_key = deps["local_submission_key"](
            pipeline_path=pipeline_path_resolved,
            project_id=project_id,
            env_name=env_name,
            execution_source=execution_source,
        )
        with deps["LOCAL_RUN_LOCK"]:
            existing = deps["ACTIVE_LOCAL_RUN_KEYS"].get(dedupe_key)
            if existing:
                snap = dict(deps["LOCAL_RUN_SNAPSHOT"].get(existing) or {})
                raise HTTPException(
                    status_code=409,
                    detail={
                        "message": "A local run for this pipeline/environment is already active.",
                        "run_id": existing,
                        "state": str(snap.get("state") or "running"),
                    },
                )
            deps["ACTIVE_LOCAL_RUN_KEYS"][dedupe_key] = run_id
            deps["LOCAL_RUN_SNAPSHOT"][run_id] = {
                "state": "queued",
                "pipeline": str(resolved_pipeline_path),
                "executor": "local",
                "project_id": project_id,
                "env_name": env_name,
            }

        ex = deps["LocalExecutor"](
            plugin_dir=args["plugins_dir"],
            workdir=Path(resolved_workdir_text),
            dry_run=args["dry_run"],
            max_retries=max_retries,
            retry_delay_seconds=retry_delay_seconds,
            enforce_git_checkout=True,
            require_clean_git=True,
            execution_source=execution_source,
            source_bundle=source_bundle,
            source_snapshot=source_snapshot,
            allow_workspace_source=allow_workspace_source,
        )
        context = {
            "run_id": run_id,
            "run_started_at": run_started_at,
            "started_at": run_started_at,
            "pipeline": pipeline,
            "execution_env": execution_env,
            "provenance": provenance,
            "repo_root": repo_root,
            "global_vars": global_vars,
            "project_vars": project_vars,
            "commandline_vars": commandline_vars,
            "execution_source": execution_source,
            "source_bundle": source_bundle,
            "source_snapshot": source_snapshot,
            "allow_workspace_source": allow_workspace_source,
            "project_id": project_id,
        }
        deps["submit_local_run_async"](
            run_id=run_id,
            dedupe_key=dedupe_key,
            executor=ex,
            pipeline_path=resolved_pipeline_path,
            context=context,
            project_id=project_id,
        )
        return {
            "run_id": run_id,
            "state": "queued",
            "pipeline": str(resolved_pipeline_path),
            "executor": args["executor"],
            "project_id": project_id,
            "job_ids": [],
            "message": "Run accepted and queued for local execution.",
        }

    try:
        submit = ex.submit(
            str(resolved_pipeline_path),
            context={
                "pipeline": pipeline,
                "execution_env": execution_env,
                "provenance": provenance,
                "repo_root": repo_root,
                "global_vars": global_vars,
                "project_vars": project_vars,
                "commandline_vars": commandline_vars,
                "execution_source": execution_source,
                "source_bundle": source_bundle,
                "source_snapshot": source_snapshot,
                "allow_workspace_source": allow_workspace_source,
                "project_id": project_id,
                "pipeline_remote_hint": pipeline_remote_hint,
            },
        )
        st = ex.status(submit.run_id)
    except Exception as exc:  # noqa: BLE001
        raise HTTPException(status_code=500, detail=f"Run failed: {exc}") from exc
    state = st.state.value if hasattr(st.state, "value") else str(st.state)
    return {
        "run_id": submit.run_id,
        "state": state,
        "pipeline": str(resolved_pipeline_path),
        "executor": args["executor"],
        "project_id": project_id,
        "job_ids": submit.job_ids or [],
        "message": st.message or submit.message or "",
    }


def api_stop_run(run_id: str, request: Request, payload: Optional[dict[str, Any]], deps: dict[str, Any]) -> dict:
    scope = deps["resolve_user_scope"](request)
    payload = payload or {}
    try:
        hdr = deps["fetch_run_header"](run_id)
    except deps["WebQueryError"] as exc:
        raise HTTPException(status_code=503, detail=str(exc)) from exc
    if hdr is None:
        raise HTTPException(status_code=404, detail=f"Run not found: {run_id}")
    deps["require_project_access"](scope, hdr.get("project_id"))

    executor_name = str(payload.get("executor") or hdr.get("executor") or "local").strip().lower()
    if executor_name != "local":
        raise HTTPException(status_code=400, detail="UI stop currently supports executor=local only.")

    pipeline = str(hdr.get("pipeline") or "")
    project_id = hdr.get("project_id")
    with deps["LOCAL_RUN_LOCK"]:
        snap = dict(deps["LOCAL_RUN_SNAPSHOT"].get(run_id) or {})
        future = deps["LOCAL_RUN_FUTURES"].get(run_id)
        state = str(snap.get("state") or "").strip().lower()

    if not state:
        raise HTTPException(status_code=409, detail="Run is not managed by this web process (or already finished); stop unavailable.")
    if state in {"succeeded", "failed", "cancelled"}:
        return {"run_id": run_id, "state": state, "message": f"Run already terminal: {state}."}

    if state == "queued":
        with deps["LOCAL_RUN_LOCK"]:
            deps["LOCAL_RUN_CANCEL_REQUESTED"].add(run_id)
        cancelled_now = bool(future.cancel()) if future is not None else False
        if cancelled_now:
            deps["set_local_run_snapshot"](run_id, state="cancelled", message="Cancelled before execution started.")
            deps["append_local_run_log"](run_id, "Run cancelled before execution started.", "WARN")
            deps["release_local_run"](run_id)
            try:
                deps["upsert_run_status"](
                    run_id=run_id,
                    pipeline=pipeline,
                    project_id=project_id,
                    status="cancelled",
                    success=False,
                    message="cancelled before execution started",
                    executor="local",
                    event_type="run_cancelled",
                    event_details={"source": "web"},
                )
            except Exception:
                pass
            return {"run_id": run_id, "state": "cancelled", "message": "Run cancelled before execution started."}

        deps["set_local_run_snapshot"](run_id, state="cancel_requested", message="Cancel requested; waiting for worker handoff.")
        deps["append_local_run_log"](run_id, "Cancel requested while queued.", "WARN")
        return {"run_id": run_id, "state": "cancel_requested", "message": "Cancel requested."}

    with deps["LOCAL_RUN_LOCK"]:
        deps["LOCAL_RUN_CANCEL_REQUESTED"].add(run_id)
    deps["set_local_run_snapshot"](run_id, state="cancel_requested", message="Stop requested; local in-process run cannot be force-killed yet.")
    deps["append_local_run_log"](run_id, "Stop requested while running. Waiting for cooperative stop support.", "WARN")
    try:
        deps["upsert_run_status"](
            run_id=run_id,
            pipeline=pipeline,
            project_id=project_id,
            status="running",
            success=False,
            message="cancel requested from web UI",
            executor="local",
            event_type="run_cancel_requested",
            event_details={"source": "web"},
        )
    except Exception:
        pass
    return {
        "run_id": run_id,
        "state": "cancel_requested",
        "message": "Stop requested. Running local step cannot be force-stopped yet.",
    }


def api_resume_run(run_id: str, request: Request, payload: Optional[dict[str, Any]], deps: dict[str, Any]) -> dict:
    scope = deps["resolve_user_scope"](request)
    payload = payload or {}
    try:
        hdr = deps["fetch_run_header"](run_id)
    except deps["WebQueryError"] as exc:
        raise HTTPException(status_code=503, detail=str(exc)) from exc
    if hdr is None:
        raise HTTPException(status_code=404, detail=f"Run not found: {run_id}")
    deps["require_project_access"](scope, hdr.get("project_id"))

    executor_name = str(payload.get("executor") or hdr.get("executor") or "local").strip().lower()
    if executor_name != "local":
        raise HTTPException(status_code=400, detail="UI resume currently supports executor=local only. Use CLI for SLURM resume.")

    pipeline_path = Path(hdr["pipeline"])

    plugins_dir = Path(payload.get("plugins_dir") or "plugins")
    workdir = Path(payload.get("workdir") or ".runs")
    max_retries = int(payload.get("max_retries", 0) or 0)
    retry_delay_seconds = float(payload.get("retry_delay_seconds", 0.0) or 0.0)
    execution_source = str(payload.get("execution_source") or "auto").strip().lower()
    source_bundle = str(payload.get("source_bundle") or "").strip() or None
    source_snapshot = str(payload.get("source_snapshot") or "").strip() or None
    allow_workspace_source = deps["parse_bool"](payload.get("allow_workspace_source"), default=False)

    try:
        pipeline = deps["parse_pipeline"](pipeline_path, global_vars={}, env_vars={})
    except (deps["PipelineError"], FileNotFoundError) as exc:
        raise HTTPException(status_code=400, detail=f"Invalid pipeline for resume: {exc}") from exc

    provenance = deps["collect_run_provenance"](
        repo_root=Path(".").resolve(),
        pipeline_path=pipeline_path,
        global_config_path=None,
        environments_config_path=None,
        plugin_dir=plugins_dir,
        pipeline=pipeline,
        cli_command=f"etl web resume {run_id}",
    )
    ex = deps["LocalExecutor"](
        plugin_dir=plugins_dir,
        workdir=workdir,
        dry_run=False,
        max_retries=max_retries,
        retry_delay_seconds=retry_delay_seconds,
        enforce_git_checkout=True,
        require_clean_git=True,
        execution_source=execution_source,
        source_bundle=source_bundle,
        source_snapshot=source_snapshot,
        allow_workspace_source=allow_workspace_source,
    )
    try:
        submit = ex.submit(
            str(pipeline_path),
            context={
                "pipeline": pipeline,
                "resume_run_id": run_id,
                "project_id": hdr.get("project_id"),
                "provenance": provenance,
                "repo_root": Path(".").resolve(),
                "execution_source": execution_source,
                "source_bundle": source_bundle,
                "source_snapshot": source_snapshot,
                "allow_workspace_source": allow_workspace_source,
            },
        )
        st = ex.status(submit.run_id)
    except Exception as exc:  # noqa: BLE001
        raise HTTPException(status_code=500, detail=f"Resume failed: {exc}") from exc
    state = st.state.value if hasattr(st.state, "value") else str(st.state)
    return {"run_id": submit.run_id, "state": state, "pipeline": str(pipeline_path)}

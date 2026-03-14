# research-etl
# Copyright (c) 2026 Joseph Weaver
# This file is part of the research-etl project and is licensed under the MIT License.
# You may not use this file except in compliance with the License.
# See https://github.com/josephweaver/research-etl for details.

from __future__ import annotations

import inspect
import logging
import threading
import uuid
from datetime import datetime
from pathlib import Path
from queue import Empty, Queue
from typing import Any, Optional

from fastapi import HTTPException

from ..common.parsing import last_non_empty_text
from ..pipeline import Pipeline, Step
from ..variable_solver import VariableSolver

_BUILDER_STEP_TEST_LOCK = threading.Lock()
_BUILDER_STEP_TESTS: dict[str, dict[str, Any]] = {}
_LOG = logging.getLogger("etl.web.builder_handlers")

def api_builder_source(
    pipeline: str,
    project_id: Optional[str],
    projects_config: Optional[str],
    pipeline_source: Optional[str],
    deps: dict[str, Any],
) -> dict:
    raw = (pipeline or "").strip()
    if not raw:
        raise HTTPException(status_code=400, detail="`pipeline` query param is required.")
    repo_root = Path(".").resolve()
    pipelines_root = (repo_root / "pipelines").resolve()
    path = Path(raw).expanduser()
    if not path.is_absolute():
        parts = list(path.parts)
        if parts and parts[0].lower() == "pipelines":
            path = Path(*parts[1:]) if len(parts) > 1 else Path("")
        if path.suffix.lower() not in {".yml", ".yaml"}:
            path = path.with_suffix(".yml")
        path = (pipelines_root / path).resolve()
        if not path.exists():
            raw_name = Path(raw).name
            candidate_names = [raw_name]
            if Path(raw_name).suffix.lower() not in {".yml", ".yaml"}:
                candidate_names.extend([f"{raw_name}.yml", f"{raw_name}.yaml"])
            has_path_hint = ("/" in raw) or ("\\" in raw)
            if not has_path_hint:
                matches: list[Path] = []
                for name in candidate_names:
                    matches.extend(sorted(pipelines_root.rglob(name)))
                uniq: list[Path] = []
                seen: set[str] = set()
                for m in matches:
                    key = m.resolve().as_posix().lower()
                    if key in seen:
                        continue
                    seen.add(key)
                    uniq.append(m.resolve())
                if len(uniq) == 1:
                    path = uniq[0]
                elif len(uniq) > 1:
                    rels = [p.relative_to(pipelines_root).as_posix() for p in uniq[:10]]
                    raise HTTPException(
                        status_code=409,
                        detail=(
                            f"Ambiguous pipeline filename '{raw}'. "
                            f"Matches: {rels}. Pass a relative path like 'yanroy/{raw_name}'."
                        ),
                    )
    selected_source_label = str(pipeline_source or "").strip()
    pid = deps["normalize_project_id"](project_id)
    if pid:
        _pid, project_vars, _cfg = deps["builder_project_context"](project_id=project_id, projects_config=projects_config)
        source_views, _warnings = deps["builder_pipeline_source_views"](project_vars=project_vars, repo_root=repo_root)
        view_by_label = {str(v["label"]): v for v in source_views}
        if selected_source_label and selected_source_label not in view_by_label:
            raise HTTPException(status_code=400, detail=f"Unknown pipeline_source '{selected_source_label}'.")
        source_label = selected_source_label
        source_rel = deps["normalize_pipeline_relpath"](raw).as_posix()
        source_parts = [p for p in source_rel.split("/") if p]
        if source_parts and source_parts[0] in view_by_label and len(source_parts) > 1:
            source_label = source_parts[0]
            source_rel = "/".join(source_parts[1:])

        should_resolve_from_source = bool(source_views) or bool(source_label) or (not path.exists() or not path.is_file())
        if not should_resolve_from_source:
            source_label = ""

        def _candidates(rel_text: str) -> list[Path]:
            rel = Path(rel_text)
            out = [rel]
            if rel.suffix.lower() not in {".yml", ".yaml"}:
                out.append(rel.with_suffix(".yml"))
                out.append(rel.with_suffix(".yaml"))
            return out

        if should_resolve_from_source:
            selected_views = [view_by_label[source_label]] if source_label and source_label in view_by_label else list(source_views)
            matches: list[tuple[str, Path]] = []
            has_path_hint = ("/" in source_rel) or ("\\" in source_rel)
            if has_path_hint:
                for view in selected_views:
                    root = Path(view["pipelines_root"]).resolve()
                    for rel_candidate in _candidates(source_rel):
                        cand = (root / rel_candidate).resolve()
                        if cand.exists() and cand.is_file():
                            matches.append((str(view["label"]), cand))
            else:
                raw_name = Path(source_rel).name
                candidate_names = [raw_name]
                if Path(raw_name).suffix.lower() not in {".yml", ".yaml"}:
                    candidate_names.extend([f"{raw_name}.yml", f"{raw_name}.yaml"])
                for view in selected_views:
                    root = Path(view["pipelines_root"]).resolve()
                    for name in candidate_names:
                        for cand in sorted(root.rglob(name)):
                            if cand.is_file():
                                matches.append((str(view["label"]), cand.resolve()))
            uniq: list[tuple[str, Path]] = []
            seen: set[str] = set()
            for label, m in matches:
                key = f"{label}|{m.as_posix().lower()}"
                if key in seen:
                    continue
                seen.add(key)
                uniq.append((label, m))
            if len(uniq) == 1:
                selected_source_label = str(uniq[0][0])
                path = uniq[0][1]
            elif len(uniq) > 1:
                rels: list[str] = []
                for label, m in uniq[:10]:
                    try:
                        root = Path(view_by_label[label]["pipelines_root"]).resolve()
                        rels.append(f"{label}/{m.relative_to(root).as_posix()}")
                    except Exception:
                        rels.append(f"{label}/{m.name}")
                raise HTTPException(
                    status_code=409,
                    detail=(
                        f"Ambiguous pipeline filename '{raw}'. "
                        f"Matches: {rels}. Pass a relative path like '{rels[0]}' if desired."
                    ),
                )
    if not path.exists() or not path.is_file():
        raise HTTPException(status_code=404, detail=f"Pipeline file not found: {path}")
    try:
        text = path.read_text(encoding="utf-8")
    except Exception as exc:  # noqa: BLE001
        raise HTTPException(status_code=500, detail=f"Failed to read pipeline file: {exc}") from exc
    return {
        "pipeline": str(path),
        "pipeline_source": selected_source_label or None,
        "yaml_text": text,
        "model": deps["pipeline_to_builder_model_from_yaml"](text),
    }


def api_builder_files(project_id: Optional[str], projects_config: Optional[str], deps: dict[str, Any]) -> dict:
    repo_root = Path(".").resolve()
    pid, project_vars, resolved_projects_cfg = deps["builder_project_context"](
        project_id=project_id,
        projects_config=projects_config,
    )
    if pid:
        views, warnings = deps["builder_pipeline_source_views"](project_vars=project_vars, repo_root=repo_root)
        if views:
            files_obj: list[dict[str, str]] = []
            dirs_set: set[str] = set()
            for view in views:
                label = str(view["label"])
                root = Path(view["pipelines_root"]).resolve()
                dirs_set.add(label)
                for pat in ("*.yml", "*.yaml"):
                    for p in root.rglob(pat):
                        if not p.is_file():
                            continue
                        rel = p.relative_to(root).as_posix()
                        tree_path = f"{label}/{rel}" if rel else label
                        files_obj.append({"tree_path": tree_path, "pipeline": rel, "source": label})
                for d in root.rglob("*"):
                    if not d.is_dir():
                        continue
                    rel_d = d.relative_to(root).as_posix()
                    if rel_d and rel_d != ".":
                        dirs_set.add(f"{label}/{rel_d}")
            files_obj.sort(key=lambda x: str(x.get("tree_path") or "").lower())
            return {
                "project_id": pid,
                "projects_config": str(resolved_projects_cfg) if resolved_projects_cfg else None,
                "pipelines_root": None,
                "files": files_obj,
                "dirs": sorted(dirs_set),
                "sources": [str(v["label"]) for v in views],
                "warnings": warnings,
            }

    pipelines_root = (repo_root / "pipelines").resolve()
    pipelines_root.mkdir(parents=True, exist_ok=True)
    files_set: set[str] = set()
    for pat in ("*.yml", "*.yaml"):
        for p in pipelines_root.rglob(pat):
            if not p.is_file():
                continue
            files_set.add(p.relative_to(pipelines_root).as_posix())
    dirs_set: set[str] = set()
    for d in pipelines_root.rglob("*"):
        if not d.is_dir():
            continue
        rel = d.relative_to(pipelines_root).as_posix()
        if rel and rel != ".":
            dirs_set.add(rel)
    return {
        "project_id": pid,
        "projects_config": str(resolved_projects_cfg) if resolved_projects_cfg else None,
        "pipelines_root": str(pipelines_root),
        "files": sorted(files_set),
        "dirs": sorted(dirs_set),
        "sources": ["local"],
        "warnings": [],
    }


def api_builder_plugins(global_config: Optional[str], plugins_dir: Optional[str], deps: dict[str, Any]) -> dict:
    global_config_path = Path(global_config).expanduser() if (global_config or "").strip() else None
    deps["resolve_global_vars"](global_config_path)
    root = deps["resolve_builder_plugins_dir"](global_config_path=global_config_path, plugins_dir=plugins_dir)
    if not root.exists() or not root.is_dir():
        raise HTTPException(status_code=400, detail=f"Plugins directory not found: {root}")
    entries: list[dict[str, Any]] = []
    for f in sorted(root.rglob("*.py")):
        if f.name.startswith("_"):
            continue
        rel = f.relative_to(root).as_posix()
        try:
            pd = deps["load_plugin"](f)
            entries.append(
                {
                    "path": rel,
                    "name": pd.meta.name,
                    "version": pd.meta.version,
                    "description": pd.meta.description,
                    "params": pd.meta.params or {},
                    "resources": pd.meta.resources or {},
                }
            )
        except deps["PluginLoadError"]:
            entries.append({"path": rel, "name": rel, "version": "", "description": "unloadable plugin", "params": {}, "resources": {}})
    return {"plugins_dir": str(root), "plugins": entries}


def api_builder_environments(environments_config: Optional[str], deps: dict[str, Any]) -> dict:
    raw = str(environments_config or "").strip()
    cfg_path = Path(raw).expanduser() if raw else None
    try:
        resolved = deps["resolve_execution_config_path"](cfg_path)
    except deps["ExecutionConfigError"] as exc:
        raise HTTPException(status_code=400, detail=f"Environments config error: {exc}") from exc
    if not resolved:
        return {"environments_config": None, "environments": []}
    try:
        envs = deps["load_execution_config"](resolved)
    except deps["ExecutionConfigError"] as exc:
        raise HTTPException(status_code=400, detail=f"Environments config error: {exc}") from exc
    env_specs: list[dict[str, str]] = []
    for name, spec in sorted(envs.items(), key=lambda kv: str(kv[0])):
        executor = ""
        if isinstance(spec, dict):
            executor = str(spec.get("executor") or "").strip().lower()
        env_specs.append({"name": str(name), "executor": executor})
    return {
        "environments_config": str(resolved),
        "environments": sorted(str(k) for k in envs.keys()),
        "environment_specs": env_specs,
    }


def api_builder_projects(projects_config: Optional[str], deps: dict[str, Any]) -> dict:
    raw = str(projects_config or "").strip()
    cfg_path = Path(raw).expanduser() if raw else None
    try:
        resolved = deps["resolve_projects_config_path"](cfg_path)
    except deps["ProjectConfigError"] as exc:
        raise HTTPException(status_code=400, detail=f"Projects config error: {exc}") from exc
    if not resolved:
        return {"projects_config": None, "projects": []}
    try:
        import yaml

        data = yaml.safe_load(resolved.read_text(encoding="utf-8")) or {}
    except Exception as exc:  # noqa: BLE001
        raise HTTPException(status_code=400, detail=f"Projects config error: {exc}") from exc
    if not isinstance(data, dict):
        return {"projects_config": str(resolved), "projects": []}
    projects_section = data.get("projects")
    projects_map = projects_section if isinstance(projects_section, dict) else data
    project_ids: list[str] = []
    for k in projects_map.keys():
        pid = deps["normalize_project_id"](str(k))
        if not pid or pid == "default":
            continue
        project_ids.append(pid)
    return {"projects_config": str(resolved), "projects": sorted(set(project_ids))}


def api_builder_project_vars(project_id: Optional[str], projects_config: Optional[str], deps: dict[str, Any]) -> dict:
    raw = str(projects_config or "").strip()
    cfg_path = Path(raw).expanduser() if raw else None
    try:
        resolved = deps["resolve_projects_config_path"](cfg_path)
    except deps["ProjectConfigError"] as exc:
        raise HTTPException(status_code=400, detail=f"Projects config error: {exc}") from exc
    pid = deps["normalize_project_id"](project_id)
    if not pid:
        return {"project_id": None, "projects_config": str(resolved) if resolved else None, "project_vars": {}}
    try:
        project_vars = deps["load_project_vars"](project_id=pid, projects_config_path=resolved)
    except deps["ProjectConfigError"] as exc:
        raise HTTPException(status_code=400, detail=f"Projects config error: {exc}") from exc
    return {
        "project_id": pid,
        "projects_config": str(resolved) if resolved else None,
        "project_vars": project_vars,
    }


def api_builder_git_status(project_id: Optional[str], projects_config: Optional[str], pipeline_source: Optional[str], deps: dict[str, Any]) -> dict:
    pid = deps["normalize_project_id"](project_id)
    psource = str(pipeline_source or "").strip() or None
    target, from_project_source = deps["builder_git_target_repo_root"](
        project_id=pid,
        projects_config=str(projects_config or "").strip() or None,
        pipeline_source=psource,
    )
    payload = deps["git_repo_status"](target)
    payload["sync_repo_configured"] = bool(from_project_source or deps["builder_git_sync_repo_root_from_env"]() is not None)
    payload["repo_from_project_source"] = bool(from_project_source)
    if not payload["sync_repo_configured"]:
        payload["sync_repo_hint"] = "Set ETL_BUILDER_GIT_SYNC_REPO to enable builder git_sync."
    return payload


def api_builder_git_main_check(project_id: Optional[str], projects_config: Optional[str], pipeline_source: Optional[str], deps: dict[str, Any]) -> dict:
    pid = deps["normalize_project_id"](project_id)
    psource = str(pipeline_source or "").strip() or None
    target, from_project_source = deps["builder_git_target_repo_root"](
        project_id=pid,
        projects_config=str(projects_config or "").strip() or None,
        pipeline_source=psource,
    )
    health = deps["git_main_health"](target)
    health["repo_root"] = str(target)
    health["repo_from_project_source"] = bool(from_project_source)
    return health


def api_builder_git_sync(payload: Optional[dict[str, Any]], deps: dict[str, Any]) -> dict:
    payload = payload or {}
    pipeline = str(payload.get("pipeline") or "").strip()
    branch = str(payload.get("branch") or "").strip() or None
    push = deps["parse_bool"](payload.get("push"), default=True)
    create_branch = deps["parse_bool"](payload.get("create_branch"), default=True)
    publish_to_main = deps["parse_bool"](payload.get("publish_to_main"), default=False)
    checkout_main_after_publish = deps["parse_bool"](payload.get("checkout_main_after_publish"), default=True)
    project_id = deps["normalize_project_id"](str(payload.get("project_id") or "").strip() or None)
    projects_config = str(payload.get("projects_config") or "").strip() or None
    pipeline_source = str(payload.get("pipeline_source") or "").strip() or None
    return deps["builder_git_sync"](
        pipeline=pipeline,
        branch=branch,
        push=push,
        create_branch=create_branch,
        publish_to_main=publish_to_main,
        checkout_main_after_publish=checkout_main_after_publish,
        project_id=project_id,
        projects_config=projects_config,
        pipeline_source=pipeline_source,
    )


def resolve_builder_runtime_from_payload(payload: dict[str, Any], deps: dict[str, Any]) -> dict[str, Any]:
    yaml_text = str(payload.get("yaml_text") or "")
    global_config_raw = str(payload.get("global_config") or "").strip()
    global_config_path = Path(global_config_raw).expanduser() if global_config_raw else None
    environments_config_raw = str(payload.get("environments_config") or "").strip()
    environments_config_path = Path(environments_config_raw).expanduser() if environments_config_raw else None
    default_env_name_raw = str(payload.get("_default_env_name") or "").strip()
    env_name = str(payload.get("env") or "").strip() or (default_env_name_raw or None)
    commandline_vars = dict(payload.get("_commandline_vars") or {})
    session_id = str(payload.get("session_id") or "").strip() or None
    context_file = str(payload.get("context_file") or "").strip() or ""
    if session_id and "load_builder_session" in deps:
        try:
            sess = dict(deps["load_builder_session"](session_id) or {})
            if not context_file:
                context_file = str(sess.get("context_file") or "").strip()
        except Exception:
            pass
    session_context: dict[str, Any] = {}
    if context_file and "builder_step_session_load_context" in deps:
        try:
            loaded = deps["builder_step_session_load_context"](context_file=context_file) or {}
            if isinstance(loaded, dict):
                session_context = dict(loaded)
        except Exception:
            session_context = {}
    preview_run_id = str(payload.get("_preview_run_id") or "").strip() or None
    preview_run_started = payload.get("_preview_run_started")
    global_vars = deps["resolve_global_vars"](global_config_path)
    env_vars = deps["resolve_builder_env_vars"](
        environments_config_path=environments_config_path,
        env_name=env_name,
        global_vars=global_vars,
    )
    project_id = deps["normalize_project_id"](str(payload.get("project_id") or "").strip() or None)
    projects_config_raw = str(payload.get("projects_config") or "").strip()
    projects_config_path = Path(projects_config_raw).expanduser() if projects_config_raw else None
    pipeline_hint = str(payload.get("pipeline") or "").strip() or None
    _, project_vars = deps["resolve_builder_project_vars"](
        yaml_text,
        explicit_project_id=project_id,
        projects_config_path=projects_config_path,
        pipeline_hint=pipeline_hint,
    )
    raw_vars, raw_dirs = deps["raw_vars_dirs_from_yaml_text"](yaml_text)
    parse_kwargs = {
        "yaml_text": yaml_text,
        "global_config_path": global_config_path,
        "environments_config_path": environments_config_path,
        "env_name": env_name,
        "project_id": project_id,
        "projects_config_path": projects_config_path,
        "pipeline_hint": pipeline_hint,
    }
    try:
        supported_params = set(inspect.signature(deps["parse_pipeline_from_yaml_text"]).parameters.keys())
        parse_kwargs = {k: v for k, v in parse_kwargs.items() if k in supported_params}
    except Exception:
        pass
    pipeline = deps["parse_pipeline_from_yaml_text"](**parse_kwargs)
    namespace = deps["build_builder_namespace"](
        pipeline=pipeline,
        global_vars=global_vars,
        env_vars=env_vars,
        project_vars=project_vars,
        raw_vars=raw_vars,
        raw_dirs=raw_dirs,
        preview_run_id=preview_run_id,
        preview_run_started=preview_run_started,
    )
    if session_context:
        output_ns = dict(namespace.get("outputs") or {})
        for key, value in session_context.items():
            name = str(key)
            namespace[name] = value
            output_ns[name] = value
        namespace["outputs"] = output_ns
        namespace["state"] = dict(session_context)
    solver = deps["build_web_request_solver"](
        global_vars=global_vars,
        env_vars=env_vars,
        project_vars=project_vars,
        commandline_vars=commandline_vars,
        context_vars=namespace,
        pipeline=pipeline,
    )
    return {
        "yaml_text": yaml_text,
        "pipeline": pipeline,
        "namespace": namespace,
        "solver": solver,
        "global_vars": global_vars,
        "env_vars": env_vars,
        "project_vars": project_vars,
        "project_id": project_id,
        "pipeline_hint": pipeline_hint,
        "global_config_path": global_config_path,
        "environments_config_path": environments_config_path,
        "projects_config_path": projects_config_path,
        "env_name": env_name,
        "session_id": session_id,
        "context_file": context_file or None,
        "session_context": session_context,
    }


def api_builder_resolve_text(payload: Optional[dict[str, Any]], deps: dict[str, Any]) -> dict:
    payload = payload or {}
    value = str(payload.get("value") or "")
    runtime = resolve_builder_runtime_from_payload(payload, deps)
    solver: VariableSolver = runtime["solver"]
    namespace = dict(runtime.get("namespace") or {})
    resolved = deps["resolve_text_with_ctx_iterative"](value, namespace, max_passes=int(getattr(solver, "max_passes", 10) or 10))
    if deps["extract_unresolved_tokens"](resolved):
        resolved_raw = solver.resolve(value, context=solver.resolved_context())
        if not isinstance(resolved_raw, (dict, list)):
            resolved = str(resolved_raw or "")
    return {"value": value, "resolved": resolved, "changed": resolved != value}


def api_builder_namespace(payload: Optional[dict[str, Any]], deps: dict[str, Any]) -> dict:
    payload = payload or {}
    runtime = resolve_builder_runtime_from_payload(payload, deps)
    namespace = dict(runtime["namespace"] or {})
    global_vars = dict(runtime["global_vars"] or {})
    env_vars = dict(runtime["env_vars"] or {})
    project_vars = dict(runtime["project_vars"] or {})
    pipeline: Pipeline = runtime["pipeline"]
    return {
        "namespace": namespace,
        "counts": {
            "sys": len(dict(namespace.get("sys") or {})),
            "global": len(global_vars),
            "env": len(env_vars),
            "project": len(project_vars),
            "vars": len(dict(pipeline.vars or {})),
            "dirs": len(dict(pipeline.dirs or {})),
            "flat": len(namespace),
        },
    }


def api_builder_validate(payload: Optional[dict[str, Any]], deps: dict[str, Any]) -> dict:
    payload = payload or {}
    pipeline_label = str(payload.get("pipeline") or "<builder:draft>")
    require_dir_contract = deps["parse_bool"](payload.get("require_dir_contract"), default=True)
    require_resolved_inputs = deps["parse_bool"](payload.get("require_resolved_inputs"), default=True)
    unresolved_inputs: list[dict[str, Any]] = []
    try:
        runtime = resolve_builder_runtime_from_payload(payload, deps)
        pipeline: Pipeline = runtime["pipeline"]
        namespace = dict(runtime["namespace"] or {})
        if require_dir_contract:
            deps["validate_pipeline_dir_contract"](pipeline)
        unresolved_inputs = deps["filter_builder_unresolved_issues"](
            deps["collect_unresolved_step_inputs"](pipeline),
            pipeline,
            namespace,
        )
        if require_resolved_inputs and unresolved_inputs:
            preview = []
            for issue in unresolved_inputs[:8]:
                tokens = ", ".join(issue.get("tokens") or [])
                preview.append(
                    f"step[{issue.get('step_index')}] {issue.get('step_name')} -> {issue.get('field')} unresolved {{{tokens}}}"
                )
            if len(unresolved_inputs) > 8:
                preview.append(f"... and {len(unresolved_inputs) - 8} more unresolved input(s)")
            message = "Unresolved step inputs detected. " + "; ".join(preview)
            raise HTTPException(
                status_code=400,
                detail={
                    "message": message,
                    "unresolved_inputs": unresolved_inputs,
                },
            )
    except HTTPException as exc:
        deps["record_pipeline_validation"](
            pipeline=pipeline_label,
            valid=False,
            step_count=0,
            step_names=[],
            error=str(exc.detail),
            source="builder_validate",
        )
        raise
    deps["record_pipeline_validation"](
        pipeline=pipeline_label,
        valid=True,
        step_count=len(pipeline.steps),
        step_names=[s.name for s in pipeline.steps],
        error=None,
        source="builder_validate",
    )
    return {
        "valid": True,
        "step_count": len(pipeline.steps),
        "step_names": [s.name for s in pipeline.steps],
        "vars": pipeline.vars,
        "dirs": pipeline.dirs,
        "unresolved_inputs": unresolved_inputs,
    }


def api_builder_generate(payload: Optional[dict[str, Any]], deps: dict[str, Any]) -> dict:
    payload = payload or {}
    intent = str(payload.get("intent") or "").strip()
    constraints = str(payload.get("constraints") or "").strip() or None
    existing_yaml = str(payload.get("yaml_text") or "").strip() or None
    model = str(payload.get("model") or "").strip() or None
    project_id = deps["normalize_project_id"](str(payload.get("project_id") or "").strip() or None)
    projects_config_raw = str(payload.get("projects_config") or "").strip()
    projects_config_path = Path(projects_config_raw).expanduser() if projects_config_raw else None
    pipeline_hint = str(payload.get("pipeline") or "").strip() or None
    auto_repair = deps["parse_bool"](payload.get("auto_repair"), default=True)
    if not intent:
        raise HTTPException(status_code=400, detail="`intent` is required.")
    try:
        yaml_text = deps["generate_pipeline_draft"](
            intent=intent,
            constraints=constraints,
            existing_yaml=existing_yaml,
            model=model,
        )
    except deps["AIPipelineError"] as exc:
        raise HTTPException(status_code=500, detail=str(exc)) from exc

    attempts = 1
    repaired = False
    repair_error = None
    pipeline, validation_error = deps["validate_draft_yaml"](
        yaml_text,
        project_id=project_id,
        projects_config_path=projects_config_path,
        pipeline_hint=pipeline_hint,
    )
    if validation_error and auto_repair:
        repaired = True
        attempts = 2
        repair_constraints = ((constraints + "\n\n") if constraints else "") + f"Fix these validation errors exactly:\n{validation_error}"
        try:
            yaml_text = deps["generate_pipeline_draft"](
                intent=intent,
                constraints=repair_constraints,
                existing_yaml=yaml_text,
                model=model,
            )
            pipeline, validation_error = deps["validate_draft_yaml"](
                yaml_text,
                project_id=project_id,
                projects_config_path=projects_config_path,
                pipeline_hint=pipeline_hint,
            )
        except deps["AIPipelineError"] as exc:
            repair_error = str(exc)

    if pipeline is not None:
        model_payload = deps["pipeline_to_builder_model_from_yaml"](yaml_text)
        return {
            "yaml_text": yaml_text,
            "model": model_payload,
            "valid": True,
            "repaired": repaired,
            "attempts": attempts,
            "step_count": len(pipeline.steps),
            "step_names": [s.name for s in pipeline.steps],
        }
    return {
        "yaml_text": yaml_text,
        "model": deps["pipeline_to_builder_model_from_yaml"](yaml_text),
        "valid": False,
        "repaired": repaired,
        "attempts": attempts,
        "validation_error": validation_error or "Unknown validation error.",
        "repair_error": repair_error,
        "step_count": 0,
        "step_names": [],
    }


def execute_builder_step_test(payload: dict[str, Any], deps: dict[str, Any]) -> dict[str, Any]:
    payload = payload or {}
    session_id = str(payload.get("session_id") or "").strip() or None
    if "builder_step_session_prepare" in deps:
        prepared = deps["builder_step_session_prepare"](payload)
        payload = dict(prepared.get("payload") or payload)
        session = dict(prepared.get("session") or {})
        session_id = str(session.get("session_id") or payload.get("session_id") or "").strip() or session_id
    run_id_seed = str(payload.get("run_id") or "").strip() or None
    run_started_seed = str(payload.get("run_started_at") or "").strip() or None
    context_file = str(payload.get("context_file") or "").strip() or ""
    session_context = {}
    if context_file and "builder_step_session_load_context" in deps:
        try:
            session_context = dict(deps["builder_step_session_load_context"](context_file=context_file) or {})
        except Exception:
            session_context = {}
    run_started_dt = None
    if run_started_seed:
        try:
            run_started_dt = datetime.fromisoformat(run_started_seed.replace("Z", "+00:00")).replace(tzinfo=None)
        except Exception:
            run_started_dt = None

    runtime_payload = dict(payload)
    runtime_payload["_default_env_name"] = "local"
    runtime_payload["_preview_run_id"] = run_id_seed
    runtime_payload["_preview_run_started"] = run_started_dt
    runtime_payload["_commandline_vars"] = {
        "workdir": str(payload.get("workdir") or "").strip(),
        "executor": str(payload.get("executor") or "").strip(),
    }
    runtime = resolve_builder_runtime_from_payload(runtime_payload, deps)
    pipeline: Pipeline = runtime["pipeline"]
    solver: VariableSolver = runtime["solver"]
    global_vars = dict(runtime["global_vars"] or {})
    env_vars = dict(runtime["env_vars"] or {})
    project_vars = dict(runtime["project_vars"] or {})
    global_config_path = runtime.get("global_config_path")
    environments_config_path = runtime.get("environments_config_path")
    projects_config_path = runtime.get("projects_config_path")
    env_name = runtime.get("env_name")
    pipeline_hint = runtime.get("pipeline_hint")
    step_name = str(payload.get("step_name") or "").strip()
    step_index_raw = payload.get("step_index")
    step_index: Optional[int] = None
    if step_index_raw not in (None, ""):
        try:
            step_index = int(step_index_raw)
        except (TypeError, ValueError) as exc:
            raise HTTPException(status_code=400, detail="`step_index` must be an integer.") from exc
        if step_index < 0:
            raise HTTPException(status_code=400, detail="`step_index` must be >= 0.")
    target_step: Optional[Step] = None
    target_step_index: Optional[int] = None
    if step_index is not None:
        if step_index >= len(pipeline.steps):
            raise HTTPException(status_code=400, detail=f"Step index out of range: {step_index} (step_count={len(pipeline.steps)}).")
        target_step = pipeline.steps[step_index]
        target_step_index = step_index
    elif step_name:
        for s in pipeline.steps:
            if s.name == step_name:
                target_step = s
                break
        if target_step is None:
            raise HTTPException(status_code=400, detail=f"Step not found in draft: {step_name}")
        target_step_index = next((i for i, s in enumerate(pipeline.steps) if s.name == target_step.name), None)
    else:
        target_step = pipeline.steps[0] if pipeline.steps else None
        target_step_index = 0 if target_step is not None else None
    if target_step is None:
        raise HTTPException(status_code=400, detail="Draft has no steps to test.")

    plugins_dir = Path(payload.get("plugins_dir") or "plugins")
    pipeline_workdir = ""
    for key in ("work", "workdir", "work_dir"):
        candidate = str((pipeline.vars or {}).get(key) or "").strip()
        if candidate:
            pipeline_workdir = candidate
            break
        candidate = str((pipeline.dirs or {}).get(key) or "").strip()
        if candidate:
            pipeline_workdir = candidate
            break
    max_passes = int(getattr(solver, "max_passes", deps["DEFAULT_RESOLVE_MAX_PASSES"]) or deps["DEFAULT_RESOLVE_MAX_PASSES"])
    workdir_resolved = deps["resolve_workdir_from_solver"](
        solver=solver,
        pipeline_workdir=pipeline_workdir,
        context_vars=dict(runtime.get("namespace") or {}),
        resolve_text_iterative=lambda value, ctx: deps["resolve_text_with_ctx_iterative"](value, ctx, max_passes=max_passes),
        fallback=".runs/builder",
    )
    workdir = Path(workdir_resolved or ".runs/builder")
    logdir_raw = ""
    for key in ("log", "logdir", "log_dir"):
        candidate = str(pipeline.dirs.get(key) or "").strip()
        if candidate:
            logdir_raw = candidate
            break
    logdir_resolved = ""
    if logdir_raw:
        resolved_logdir = solver.resolve(logdir_raw, context=solver.resolved_context())
        logdir_resolved = str(resolved_logdir or "").strip()
        if deps["extract_unresolved_tokens"](logdir_resolved):
            logdir_resolved = ""
    logdir = Path(logdir_resolved) if logdir_resolved else None
    dry_run = deps["parse_bool"](payload.get("dry_run"), default=False)
    max_retries = int(payload.get("max_retries", 0) or 0)
    retry_delay_seconds = float(payload.get("retry_delay_seconds", 0.0) or 0.0)
    if max_retries < 0 or retry_delay_seconds < 0:
        raise HTTPException(status_code=400, detail="Retry settings must be >= 0.")

    mini = Pipeline(
        vars=dict(pipeline.vars),
        dirs=dict(pipeline.dirs),
        resolve_max_passes=int(getattr(pipeline, "resolve_max_passes", solver.max_passes) or solver.max_passes),
        steps=[target_step],
    )
    if session_context:
        mini.vars.update(dict(session_context))
    executor_name = str(payload.get("executor") or env_vars.get("executor") or "local").strip().lower()
    local_run_result = None
    local_step_result = None
    remote_submit = None
    remote_status = None
    _LOG.info(
        "builder step test start step=%s index=%s executor=%s env=%s project_id=%s pipeline_hint=%s",
        str(target_step.name),
        target_step_index,
        executor_name,
        str(env_name or ""),
        str(payload.get("project_id") or getattr(pipeline, "project_id", "") or ""),
        str(pipeline_hint or ""),
    )

    if executor_name == "local":
        try:
            result = deps["run_pipeline"](
                mini,
                plugin_dir=plugins_dir,
                workdir=workdir,
                logdir=logdir,
                run_id=run_id_seed,
                run_started=run_started_dt,
                dry_run=dry_run,
                max_retries=max_retries,
                retry_delay_seconds=retry_delay_seconds,
                context_snapshot_func=(
                    (lambda **snap: deps["upsert_run_context_snapshot"](
                        run_id=str((snap.get("context") or {}).get("sys", {}).get("run", {}).get("id") or run_id_seed or ""),
                        pipeline=str(payload.get("pipeline") or pipeline_hint or ""),
                        project_id=deps["normalize_project_id"](str(payload.get("project_id") or getattr(pipeline, "project_id", "") or "").strip() or None),
                        executor="builder_step",
                        event_type=str(snap.get("event_type") or "snapshot"),
                        context=dict(snap.get("context") or {}),
                        step_name=str(snap.get("step_name") or "").strip() or None,
                        step_index=snap.get("step_index"),
                        snapshot_file=(Path(workdir) / "_context_snapshots" / f"{str((snap.get('context') or {}).get('sys', {}).get('run', {}).get('id') or run_id_seed or 'run')}.jsonl"),
                    ))
                    if "upsert_run_context_snapshot" in deps
                    else None
                ),
            )
        except Exception as exc:  # noqa: BLE001
            raise HTTPException(status_code=500, detail=f"Step test failed: {exc}") from exc

        step_result = result.steps[0] if result.steps else None
        local_run_result = result
        local_step_result = step_result
        last_log_line = ""
        if step_result is not None:
            step_name_safe = str(getattr(step_result.step, "name", target_step.name) or target_step.name)
            step_id = str(getattr(step_result, "step_id", "") or "").strip()
            candidates: list[Path] = []
            if step_id and result.artifact_dir:
                candidates.append(Path(result.artifact_dir) / step_name_safe / step_id / "logs" / "step.log")
            if step_id and logdir is not None:
                candidates.append(Path(logdir) / step_name_safe / step_id / "logs" / "step.log")
            for path in candidates:
                tail = deps["tail_text_lines"](path, limit=30)
                last_log_line = deps["last_non_empty_line"](tail)
                if last_log_line:
                    break
        return {
            "session_id": session_id,
            "context_file": context_file or None,
            "run_id": result.run_id,
            "artifact_dir": result.artifact_dir,
            "step_name": target_step.name,
            "step_index": target_step_index,
            "executor": executor_name,
            "success": bool(step_result.success if step_result else False),
            "skipped": bool(step_result.skipped if step_result else False),
            "error": step_result.error if step_result else "No step result produced.",
            "outputs": step_result.outputs if step_result else {},
            "attempts": step_result.attempts if step_result else [],
            "last_log_line": last_non_empty_text(last_log_line),
        }

    import yaml

    step_payload = {
        "name": str(target_step.name),
        "script": str(target_step.script),
        "output_var": target_step.output_var,
        "env": dict(target_step.env or {}),
        "resources": dict(target_step.resources or {}),
        "when": target_step.when,
        "parallel_with": target_step.parallel_with,
        "foreach": getattr(target_step, "foreach", None),
        "sequential_foreach": getattr(target_step, "sequential_foreach", None),
        "foreach_glob": getattr(target_step, "foreach_glob", None),
        "foreach_kind": getattr(target_step, "foreach_kind", None),
    }
    step_payload = {k: v for k, v in step_payload.items() if v not in (None, "", {}, [])}
    temp_pipeline_doc = {
        "project_id": str(getattr(pipeline, "project_id", "") or ""),
        "vars": dict(mini.vars or {}),
        "dirs": dict(mini.dirs or {}),
        "steps": [step_payload],
    }
    if not temp_pipeline_doc["project_id"]:
        temp_pipeline_doc.pop("project_id", None)
    allow_dirty_git = deps["parse_bool"](payload.get("allow_dirty_git"), default=True)
    temp_pipeline_path = Path("")
    try:
        repo_root = Path(".").resolve()
        run_id = run_id_seed or uuid.uuid4().hex
        run_started_at = run_started_seed or (datetime.utcnow().isoformat() + "Z")
        pipeline_ref = str(payload.get("pipeline") or pipeline_hint or "").strip()
        resolved_pipeline_path: Optional[Path] = None
        pipeline_remote_hint = ""
        if pipeline_ref:
            resolved_pipeline_path = deps["resolve_project_writable_pipeline_path"](
                pipeline=pipeline_ref,
                project_id=deps["normalize_project_id"](str(payload.get("project_id") or getattr(pipeline, "project_id", "") or "").strip() or None),
                projects_config=str(payload.get("projects_config") or "").strip() or None,
                pipeline_source=str(payload.get("pipeline_source") or "").strip() or None,
            )
            if not resolved_pipeline_path.exists():
                raise HTTPException(status_code=404, detail=f"Pipeline file not found for remote step test: {resolved_pipeline_path}")
            pipeline_remote_hint = (
                deps["infer_external_pipeline_remote_hint"](
                    pipeline_path=resolved_pipeline_path,
                    project_vars=project_vars,
                    repo_root=repo_root,
                )
                or ""
            )

        if resolved_pipeline_path is not None:
            if executor_name == "hpcc_direct":
                ex = deps["HpccDirectExecutor"](
                    env_config=env_vars,
                    repo_root=repo_root,
                    plugins_dir=plugins_dir,
                    workdir=workdir,
                    global_config=global_config_path,
                    projects_config=projects_config_path,
                    environments_config=environments_config_path,
                    env_name=env_name,
                    dry_run=dry_run,
                    verbose=deps["parse_bool"](payload.get("verbose"), default=False),
                )
            elif executor_name == "slurm":
                ex = deps["SlurmExecutor"](
                    env_config=env_vars,
                    repo_root=repo_root,
                    plugins_dir=plugins_dir,
                    workdir=workdir,
                    global_config=global_config_path,
                    projects_config=projects_config_path,
                    environments_config=environments_config_path,
                    env_name=env_name,
                    dry_run=dry_run,
                    verbose=deps["parse_bool"](payload.get("verbose"), default=False),
                    require_clean_git=not allow_dirty_git,
                )
            else:
                raise HTTPException(status_code=400, detail=f"Unsupported executor for step test: {executor_name}")

            submit = ex.submit(
                str(resolved_pipeline_path),
                context={
                    "run_id": run_id,
                    "run_started_at": run_started_at,
                    "context_file": context_file,
                    "execution_env": env_vars,
                    "global_vars": global_vars,
                    "project_vars": project_vars,
                    "allow_dirty_git": allow_dirty_git,
                    "project_id": deps["normalize_project_id"](str(payload.get("project_id") or getattr(pipeline, "project_id", "") or "").strip() or None),
                    "pipeline_remote_hint": pipeline_remote_hint,
                    "step_indices": [int(target_step_index if target_step_index is not None else 0)],
                },
            )
            _LOG.info(
                "builder step test submitted remote run_id=%s executor=%s pipeline=%s",
                str(submit.run_id),
                executor_name,
                str(resolved_pipeline_path),
            )
            st = ex.status(submit.run_id)
            remote_submit = submit
            remote_status = st
            state = st.state.value if hasattr(st.state, "value") else str(st.state)
            success = str(state).lower() in {"succeeded", "success"}
            return {
                "session_id": session_id,
                "context_file": context_file or None,
                "run_id": submit.run_id,
                "artifact_dir": None,
                "step_name": target_step.name,
                "step_index": target_step_index,
                "executor": executor_name,
                "state": state,
                "success": success,
                "skipped": False,
                "error": None if success else (st.message or submit.message or f"executor state={state}"),
                "outputs": {},
                "attempts": [],
                "last_log_line": last_non_empty_text(st.message or submit.message or ""),
            }

        temp_pipeline_dir = (repo_root / "pipelines" / ".builder_step_tests").resolve()
        temp_pipeline_dir.mkdir(parents=True, exist_ok=True)
        temp_pipeline_path = temp_pipeline_dir / f"step_test_{uuid.uuid4().hex}.yml"
        temp_pipeline_path.write_text(yaml.safe_dump(temp_pipeline_doc, sort_keys=False), encoding="utf-8")
        if executor_name == "hpcc_direct":
            ex = deps["HpccDirectExecutor"](
                env_config=env_vars,
                repo_root=repo_root,
                plugins_dir=plugins_dir,
                workdir=workdir,
                global_config=global_config_path,
                projects_config=None,
                environments_config=environments_config_path,
                env_name=env_name,
                dry_run=dry_run,
                verbose=deps["parse_bool"](payload.get("verbose"), default=False),
            )
        elif executor_name == "slurm":
            ex = deps["SlurmExecutor"](
                env_config=env_vars,
                repo_root=repo_root,
                plugins_dir=plugins_dir,
                workdir=workdir,
                global_config=global_config_path,
                projects_config=None,
                environments_config=environments_config_path,
                env_name=env_name,
                dry_run=dry_run,
                verbose=deps["parse_bool"](payload.get("verbose"), default=False),
                require_clean_git=not allow_dirty_git,
            )
        else:
            raise HTTPException(status_code=400, detail=f"Unsupported executor for step test: {executor_name}")

        submit = ex.submit(
            str(temp_pipeline_path),
            context={
                "run_id": run_id,
                "run_started_at": run_started_at,
                "context_file": context_file,
                "execution_env": env_vars,
                "global_vars": global_vars,
                "project_vars": project_vars,
                "allow_dirty_git": allow_dirty_git,
                "project_id": deps["normalize_project_id"](str(payload.get("project_id") or getattr(pipeline, "project_id", "") or "").strip() or None),
            },
        )
        _LOG.info(
            "builder step test submitted remote run_id=%s executor=%s pipeline=%s",
            str(submit.run_id),
            executor_name,
            str(temp_pipeline_path),
        )
        st = ex.status(submit.run_id)
        remote_submit = submit
        remote_status = st
        state = st.state.value if hasattr(st.state, "value") else str(st.state)
        success = str(state).lower() in {"succeeded", "success"}
        return {
            "session_id": session_id,
            "context_file": context_file or None,
            "run_id": submit.run_id,
            "artifact_dir": None,
            "step_name": target_step.name,
            "step_index": target_step_index,
            "executor": executor_name,
            "state": state,
            "success": success,
            "skipped": False,
            "error": None if success else (st.message or submit.message or f"executor state={state}"),
            "outputs": {},
            "attempts": [],
            "last_log_line": last_non_empty_text(st.message or submit.message or ""),
        }
    except HTTPException:
        raise
    except Exception as exc:  # noqa: BLE001
        raise HTTPException(status_code=500, detail=f"Step test failed: {exc}") from exc
    finally:
        try:
            # Persist successful step outputs for session replay across step tests.
            if context_file and "builder_step_session_save_context" in deps:
                step_result = local_step_result
                if (
                    step_result is not None
                    and bool(getattr(step_result, "success", False))
                    and not bool(getattr(step_result, "skipped", False))
                    and bool(getattr(step_result, "step", None))
                    and bool(getattr(step_result.step, "output_var", None))
                ):
                    merged_ctx = dict(session_context or {})
                    merged_ctx[str(step_result.step.output_var)] = dict(getattr(step_result, "outputs", {}) or {})
                    deps["builder_step_session_save_context"](context_file=context_file, context=merged_ctx)
        except Exception:
            pass
        try:
            if "builder_step_session_record_result" in deps:
                final_result: dict[str, Any] = {}
                if local_run_result is not None and local_step_result is not None:
                    step_result = local_step_result
                    final_result = {
                        "run_id": str(getattr(local_run_result, "run_id", "") or ""),
                        "step_name": str(getattr(getattr(step_result, "step", None), "name", target_step.name) or target_step.name),
                        "step_index": target_step_index,
                        "success": bool(getattr(step_result, "success", False)),
                        "error": str(getattr(step_result, "error", "") or ""),
                    }
                elif remote_submit is not None and remote_status is not None:
                    state = remote_status.state.value if hasattr(remote_status.state, "value") else str(remote_status.state)
                    success = str(state).lower() in {"succeeded", "success"}
                    final_result = {
                        "run_id": str(getattr(remote_submit, "run_id", "") or ""),
                        "step_name": str(target_step.name),
                        "step_index": target_step_index,
                        "success": bool(success),
                        "error": "" if success else str(getattr(remote_status, "message", "") or getattr(remote_submit, "message", "") or ""),
                    }
                if final_result:
                    deps["builder_step_session_record_result"](session_id=session_id, result=final_result)
        except Exception:
            pass
        try:
            if temp_pipeline_path:
                temp_pipeline_path.unlink(missing_ok=True)
        except Exception:
            pass


def _builder_step_tests_compact() -> None:
    now = datetime.utcnow().timestamp()
    with _BUILDER_STEP_TEST_LOCK:
        stale: list[str] = []
        for test_id, rec in list(_BUILDER_STEP_TESTS.items()):
            state = str(rec.get("state") or "")
            done_ts = float(rec.get("done_ts") or 0.0)
            if state in {"completed", "failed", "cancelled"} and done_ts > 0 and (now - done_ts) > 1800:
                stale.append(test_id)
        for test_id in stale:
            _BUILDER_STEP_TESTS.pop(test_id, None)


def _builder_step_test_worker(payload: dict[str, Any], result_queue, deps: dict[str, Any]) -> None:
    try:
        result = execute_builder_step_test(dict(payload or {}), deps)
        result_queue.put({"ok": True, "result": result})
    except HTTPException as exc:
        result_queue.put(
            {
                "ok": False,
                "status_code": int(getattr(exc, "status_code", 500) or 500),
                "detail": exc.detail,
            }
        )
    except Exception as exc:  # noqa: BLE001
        result_queue.put({"ok": False, "status_code": 500, "detail": str(exc)})


def _builder_step_test_snapshot(test_id: str) -> Optional[dict[str, Any]]:
    _builder_step_tests_compact()
    with _BUILDER_STEP_TEST_LOCK:
        rec = _BUILDER_STEP_TESTS.get(test_id)
        if not rec:
            return None
        proc = rec.get("proc")
        result_queue = rec.get("queue")
    msg = None
    if result_queue is not None:
        while True:
            try:
                msg = result_queue.get_nowait()
            except Empty:
                break
    now_ts = datetime.utcnow().timestamp()
    with _BUILDER_STEP_TEST_LOCK:
        rec2 = _BUILDER_STEP_TESTS.get(test_id)
        if not rec2:
            return None
        if msg is not None:
            if rec2.get("state") == "cancelled":
                rec2["done_ts"] = rec2.get("done_ts") or now_ts
                return dict(rec2)
            if bool(msg.get("ok")):
                rec2["state"] = "completed"
                rec2["result"] = dict(msg.get("result") or {})
                rec2["error"] = ""
                rec2["done_ts"] = now_ts
            else:
                rec2["state"] = "failed"
                rec2["error"] = str(msg.get("detail") or "Step test failed.")
                rec2["status_code"] = int(msg.get("status_code") or 500)
                rec2["done_ts"] = now_ts
        elif rec2.get("state") == "running" and proc is not None and not proc.is_alive():
            rec2["state"] = "failed"
            rec2["error"] = str(rec2.get("error") or "Step test process exited unexpectedly.")
            rec2["status_code"] = int(rec2.get("status_code") or 500)
            rec2["done_ts"] = now_ts
        return dict(rec2)


def api_builder_test_step_start(payload: Optional[dict[str, Any]], deps: dict[str, Any]) -> dict:
    payload = dict(payload or {})
    _builder_step_tests_compact()
    prepared = deps["builder_step_session_prepare"](payload) if "builder_step_session_prepare" in deps else {"payload": payload}
    payload = dict(prepared.get("payload") or payload)
    test_id = uuid.uuid4().hex
    payload["session_id"] = str(payload.get("session_id") or "").strip() or str(payload.get("run_id") or "").strip() or uuid.uuid4().hex
    payload["run_id"] = str(payload.get("run_id") or "").strip() or uuid.uuid4().hex
    payload["run_started_at"] = str(payload.get("run_started_at") or "").strip() or (datetime.utcnow().isoformat() + "Z")
    if "builder_step_session_prepare" in deps:
        payload = dict(deps["builder_step_session_prepare"](payload).get("payload") or payload)

    result_queue: Queue = Queue()
    proc = threading.Thread(
        target=_builder_step_test_worker,
        args=(payload, result_queue, deps),
        daemon=True,
        name=f"etl-step-test-{test_id[:8]}",
    )
    proc.start()
    with _BUILDER_STEP_TEST_LOCK:
        _BUILDER_STEP_TESTS[test_id] = {
            "test_id": test_id,
            "state": "running",
            "created_ts": datetime.utcnow().timestamp(),
            "done_ts": 0.0,
            "proc": proc,
            "queue": result_queue,
            "result": None,
            "error": "",
            "status_code": 500,
            "run_id": payload.get("run_id"),
            "session_id": payload.get("session_id"),
            "context_file": payload.get("context_file"),
        }
    return {
        "test_id": test_id,
        "state": "running",
        "run_id": payload.get("run_id"),
        "session_id": payload.get("session_id"),
        "context_file": payload.get("context_file"),
    }


def api_builder_test_step_status(test_id: str) -> dict:
    key = str(test_id or "").strip()
    if not key:
        raise HTTPException(status_code=400, detail="`test_id` is required.")
    snap = _builder_step_test_snapshot(key)
    if not snap:
        raise HTTPException(status_code=404, detail=f"Step test not found: {key}")
    state = str(snap.get("state") or "running")
    if state == "completed":
        return {"test_id": key, "state": state, "result": dict(snap.get("result") or {})}
    if state == "failed":
        return {
            "test_id": key,
            "state": state,
            "error": str(snap.get("error") or "Step test failed."),
            "status_code": int(snap.get("status_code") or 500),
        }
    if state == "cancelled":
        return {"test_id": key, "state": state, "error": str(snap.get("error") or "Step test cancelled.")}
    return {
        "test_id": key,
        "state": "running",
        "run_id": snap.get("run_id"),
        "session_id": snap.get("session_id"),
        "context_file": snap.get("context_file"),
    }


def api_builder_test_step_stop(payload: Optional[dict[str, Any]]) -> dict:
    payload = payload or {}
    key = str(payload.get("test_id") or "").strip()
    if not key:
        raise HTTPException(status_code=400, detail="`test_id` is required.")
    with _BUILDER_STEP_TEST_LOCK:
        rec = _BUILDER_STEP_TESTS.get(key)
        if not rec:
            raise HTTPException(status_code=404, detail=f"Step test not found: {key}")
        state = str(rec.get("state") or "running")
        proc = rec.get("proc")
        if state in {"completed", "failed", "cancelled"}:
            return {"test_id": key, "state": state}
        if proc is not None and hasattr(proc, "is_alive") and proc.is_alive() and hasattr(proc, "terminate"):
            try:
                proc.terminate()
                proc.join(timeout=2.0)
            except Exception:
                pass
        rec["state"] = "cancelled"
        rec["error"] = "Step test cancelled by user."
        rec["done_ts"] = datetime.utcnow().timestamp()
    return {"test_id": key, "state": "cancelled"}

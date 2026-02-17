from __future__ import annotations

import tempfile
import json
import shlex
import re
import threading
import uuid
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timezone
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Optional

import psycopg
from fastapi import Body, FastAPI, HTTPException, Query, Request
from fastapi.responses import HTMLResponse

from .config import ConfigError, load_global_config, resolve_global_config_path
from .ai_pipeline import AIPipelineError, generate_pipeline_draft
from .db import get_database_url
from .execution_config import (
    ExecutionConfigError,
    apply_execution_env_overrides,
    load_execution_config,
    resolve_execution_env_templates,
    resolve_execution_config_path,
    validate_environment_executor,
)
from .executors.local import LocalExecutor
from .executors.slurm import SlurmExecutor
from .pipeline import (
    DEFAULT_RESOLVE_MAX_PASSES,
    Pipeline,
    Step,
    parse_pipeline,
    PipelineError,
    resolve_max_passes_setting,
)
from .provenance import collect_run_provenance
from .projects import infer_project_id_from_pipeline_path, normalize_project_id, resolve_project_id
from .plugins.base import PluginLoadError, load_plugin
from .runner import run_pipeline
from .tracking import fetch_plugin_resource_stats, upsert_run_status
from .variable_solver import VariableSolver
from .web_queries import (
    WebQueryError,
    fetch_dataset_detail,
    fetch_datasets,
    fetch_pipeline_detail,
    fetch_pipeline_runs,
    fetch_pipeline_validations,
    fetch_pipelines,
    fetch_run_detail,
    fetch_run_header,
    fetch_runs,
)


app = FastAPI(title="Research ETL UI", version="0.1.0")

MAX_FILE_VIEW_BYTES = 256 * 1024
_TPL_RE = re.compile(r"\{([^{}]+)\}")
_LOCAL_RUN_POOL = ThreadPoolExecutor(max_workers=4, thread_name_prefix="etl-web-local")
_LOCAL_RUN_LOCK = threading.Lock()
_ACTIVE_LOCAL_RUN_KEYS: dict[str, str] = {}
_LOCAL_RUN_SNAPSHOT: dict[str, dict[str, Any]] = {}
_LOCAL_RUN_KEY_BY_RUN_ID: dict[str, str] = {}
_LOCAL_RUN_FUTURES: dict[str, Any] = {}
_LOCAL_RUN_CANCEL_REQUESTED: set[str] = set()
_LOCAL_RUN_LOG_RING: dict[str, list[str]] = {}
_LOCAL_RUN_LOG_RING_MAX = 2000


@dataclass(frozen=True)
class UserScope:
    user_id: str
    allowed_projects: set[str]


def _local_submission_key(
    *,
    pipeline_path: Path,
    project_id: Optional[str],
    env_name: Optional[str],
    execution_source: Optional[str],
) -> str:
    return "||".join(
        [
            str(pipeline_path.resolve()).lower(),
            str(project_id or "").lower(),
            str(env_name or "").lower(),
            str(execution_source or "").lower(),
            "local",
        ]
    )


def _set_local_run_snapshot(run_id: str, **updates: Any) -> None:
    with _LOCAL_RUN_LOCK:
        base = dict(_LOCAL_RUN_SNAPSHOT.get(run_id) or {})
        base.update(updates)
        _LOCAL_RUN_SNAPSHOT[run_id] = base


def _append_local_run_log(run_id: str, message: str, level: str = "INFO") -> None:
    ts = datetime.utcnow().isoformat() + "Z"
    line = f"[{ts}] [{str(level or 'INFO').upper()}] {str(message or '').rstrip()}"
    with _LOCAL_RUN_LOCK:
        ring = _LOCAL_RUN_LOG_RING.setdefault(run_id, [])
        ring.append(line)
        if len(ring) > _LOCAL_RUN_LOG_RING_MAX:
            del ring[: len(ring) - _LOCAL_RUN_LOG_RING_MAX]
        snap = dict(_LOCAL_RUN_SNAPSHOT.get(run_id) or {})
        log_file = str(snap.get("log_file") or "").strip()
    if log_file:
        try:
            p = Path(log_file)
            p.parent.mkdir(parents=True, exist_ok=True)
            with p.open("a", encoding="utf-8", errors="replace") as f:
                f.write(line + "\n")
        except Exception:
            pass


def _tail_text_lines(path: Path, limit: int = 200) -> list[str]:
    if not path.exists() or not path.is_file():
        return []
    try:
        text = path.read_text(encoding="utf-8", errors="replace")
    except Exception:
        return []
    lines = text.splitlines()
    if limit <= 0:
        return lines
    return lines[-limit:]


def _release_local_run(run_id: str) -> None:
    with _LOCAL_RUN_LOCK:
        key = _LOCAL_RUN_KEY_BY_RUN_ID.pop(run_id, None)
        if key:
            _ACTIVE_LOCAL_RUN_KEYS.pop(key, None)
        _LOCAL_RUN_FUTURES.pop(run_id, None)
        _LOCAL_RUN_CANCEL_REQUESTED.discard(run_id)


def _submit_local_run_async(
    *,
    run_id: str,
    dedupe_key: str,
    executor: LocalExecutor,
    pipeline_path: Path,
    context: dict[str, Any],
    project_id: Optional[str],
) -> None:
    live_log_file = (executor.workdir / "_live" / f"{run_id}.log").resolve().as_posix()
    _set_local_run_snapshot(
        run_id,
        state="queued",
        pipeline=str(pipeline_path),
        executor="local",
        project_id=project_id,
        log_file=live_log_file,
    )
    _append_local_run_log(run_id, "Run queued from web UI.", "INFO")
    try:
        upsert_run_status(
            run_id=run_id,
            pipeline=str(pipeline_path),
            status="queued",
            success=False,
            message="queued from web UI",
            executor="local",
            project_id=project_id,
            provenance=context.get("provenance"),
            event_type="run_queued",
            event_details={"source": "web"},
        )
    except Exception:
        pass

    def _worker() -> None:
        with _LOCAL_RUN_LOCK:
            if run_id in _LOCAL_RUN_CANCEL_REQUESTED:
                _set_local_run_snapshot(run_id, state="cancelled", message="Cancelled before execution started.")
                _append_local_run_log(run_id, "Run cancelled before execution started.", "WARN")
                try:
                    upsert_run_status(
                        run_id=run_id,
                        pipeline=str(pipeline_path),
                        status="cancelled",
                        success=False,
                        message="cancelled before execution started",
                        executor="local",
                        project_id=project_id,
                        provenance=context.get("provenance"),
                        event_type="run_cancelled",
                        event_details={"source": "web"},
                    )
                except Exception:
                    pass
                _release_local_run(run_id)
                return
        _set_local_run_snapshot(run_id, state="running")
        _append_local_run_log(run_id, "Run started.", "INFO")
        run_context = dict(context or {})
        run_context["log"] = lambda msg, level="INFO": _append_local_run_log(run_id, str(msg), str(level or "INFO"))
        run_context["step_log"] = (
            lambda step_name, msg, level="INFO": _append_local_run_log(
                run_id,
                f"[{step_name}] {str(msg)}",
                str(level or "INFO"),
            )
        )
        try:
            upsert_run_status(
                run_id=run_id,
                pipeline=str(pipeline_path),
                status="running",
                success=False,
                message="running from web UI",
                executor="local",
                project_id=project_id,
                provenance=context.get("provenance"),
                event_type="run_started",
                event_details={"source": "web"},
            )
        except Exception:
            pass

        try:
            submit = executor.submit(str(pipeline_path), context=run_context)
            status = executor.status(submit.run_id)
            state = status.state.value if hasattr(status.state, "value") else str(status.state)
            _set_local_run_snapshot(run_id, state=state, message=status.message or submit.message or "")
            _append_local_run_log(run_id, f"Run finished with state={state}.", "INFO")
        except Exception as exc:  # noqa: BLE001
            _set_local_run_snapshot(run_id, state="failed", message=str(exc))
            _append_local_run_log(run_id, f"Run failed: {exc}", "ERROR")
            try:
                upsert_run_status(
                    run_id=run_id,
                    pipeline=str(pipeline_path),
                    status="failed",
                    success=False,
                    message=str(exc),
                    executor="local",
                    project_id=project_id,
                    provenance=context.get("provenance"),
                    event_type="run_failed",
                    event_details={"source": "web", "error": str(exc)},
                )
            except Exception:
                pass
        finally:
            _release_local_run(run_id)

    future = _LOCAL_RUN_POOL.submit(_worker)
    with _LOCAL_RUN_LOCK:
        _LOCAL_RUN_KEY_BY_RUN_ID[run_id] = dedupe_key
        _LOCAL_RUN_FUTURES[run_id] = future


INDEX_HTML = """<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8" />
  <meta name="viewport" content="width=device-width, initial-scale=1" />
  <title>Research ETL UI</title>
  <link rel="stylesheet" href="https://cdn.jsdelivr.net/npm/jstree@3.3.16/dist/themes/default/style.min.css" />
  <style>
    :root { --bg:#f5f7fb; --panel:#ffffff; --ink:#13223a; --muted:#5f6e86; --ok:#0a8f57; --bad:#b42318; --line:#dbe2ef; }
    body { margin:0; font-family:"Segoe UI",Tahoma,sans-serif; color:var(--ink); background:linear-gradient(160deg,#eef3ff,#f9fbff); }
    .wrap { max-width:min(1840px, calc(100vw - 24px)); margin:16px auto; padding:0 10px; }
    .topnav { display:flex; align-items:center; justify-content:space-between; gap:8px; margin-bottom:8px; padding:6px 8px; background:#f7f9ff; border:1px solid var(--line); border-radius:8px; }
    .topnav .links { display:flex; gap:6px; flex-wrap:wrap; }
    .topnav a { text-decoration:none; color:#274066; border:1px solid var(--line); border-radius:999px; padding:3px 8px; font-size:12px; line-height:1.2; background:#fff; }
    .topnav a.active { background:#0d3b8e; color:#fff; border-color:#0d3b8e; }
    .topnav a.context { background:#eef3ff; border-style:dashed; }
    .topnav .jump { display:flex; gap:6px; align-items:center; }
    .topnav .jump input { width:180px; padding:4px 6px; font-size:12px; }
    .topnav .jump button { padding:4px 8px; font-size:12px; }
    .topnav .who { display:flex; gap:6px; align-items:center; }
    .topnav .who label { font-size:12px; color:#496184; }
    .topnav .who select { padding:4px 6px; font-size:12px; border-radius:6px; }
    .head { display:flex; justify-content:space-between; align-items:center; margin-bottom:14px; gap:10px; flex-wrap:wrap; }
    h1 { margin:0; font-size:24px; }
    .muted { color:var(--muted); font-size:13px; }
    .grid { display:grid; grid-template-columns: 1fr 1fr; gap:14px; }
    body.builder-mode .grid { grid-template-columns: 1fr; }
    body.builder-mode .grid > section:first-child { display:none; }
    body.builder-mode .grid > section:last-child { max-width:none; width:100%; margin:0; }
    body.plugins-mode .grid { grid-template-columns: 1fr; }
    body.plugins-mode .grid > section:first-child { display:none; }
    body.plugins-mode .grid > section:last-child { max-width:none; width:100%; margin:0; }
    .panel { background:var(--panel); border:1px solid var(--line); border-radius:10px; padding:12px; box-shadow:0 2px 10px rgba(10,25,60,.06); }
    .controls { display:flex; gap:8px; flex-wrap:wrap; margin-bottom:8px; }
    input, select, button { border:1px solid var(--line); border-radius:8px; padding:6px 8px; font-size:13px; }
    button { background:#0d3b8e; color:white; border-color:#0d3b8e; cursor:pointer; }
    table { width:100%; border-collapse:collapse; font-size:13px; }
    th, td { border-bottom:1px solid var(--line); padding:8px 6px; text-align:left; vertical-align:top; }
    th { color:var(--muted); font-weight:600; }
    tr:hover { background:#f7f9ff; cursor:pointer; }
    .ok { color:var(--ok); font-weight:600; }
    .bad { color:var(--bad); font-weight:600; }
    pre { white-space:pre-wrap; word-break:break-word; font-size:12px; background:#f8f9fc; border:1px solid var(--line); border-radius:8px; padding:10px; }
    .filesplit { display:grid; grid-template-columns: 42% 58%; gap:8px; margin-top:10px; }
    .filetree { border:1px solid var(--line); border-radius:8px; padding:8px; max-height:340px; overflow:auto; background:#fafcff; }
    .viewer { border:1px solid var(--line); border-radius:8px; padding:8px; max-height:340px; overflow:auto; background:#fafcff; }
    .node { padding:3px 4px; border-radius:6px; font-size:12px; }
    .node.file { cursor:pointer; }
    .node.file:hover { background:#edf3ff; }
    .node.dir { font-weight:600; color:#334e73; }
    .builder-surface { display:grid; grid-template-columns: minmax(0, 2.3fr) minmax(460px, 1fr); gap:12px; }
    .builder-surface.builder-right-collapsed { grid-template-columns: minmax(0, 1fr) 56px; }
    .builder-surface.builder-right-collapsed #builder_preview_card { padding:8px 6px; }
    .builder-surface.builder-right-collapsed #builder_preview_card h4 { display:none; }
    .builder-surface.builder-right-collapsed #builder_preview_card .builder-head { justify-content:center; margin:0; }
    .builder-surface.builder-right-collapsed #btn_builder_toggle_preview { writing-mode:vertical-rl; transform:rotate(180deg); min-height:120px; padding:8px 4px; }
    .builder-card { border:1px solid var(--line); border-radius:8px; background:#fafcff; padding:10px; }
    .builder-card h4 { margin:0 0 8px 0; font-size:14px; }
    .builder-head { display:flex; align-items:center; justify-content:space-between; gap:8px; margin:0 0 8px 0; }
    .builder-head h4 { margin:0; }
    .builder-list { display:grid; gap:8px; margin-bottom:10px; }
    .builder-item { border:1px dashed var(--line); border-radius:8px; padding:8px; background:#fff; }
    .builder-item .controls { margin-bottom:6px; }
    .builder-item h5 { margin:0 0 6px 0; font-size:13px; color:#334e73; }
    .builder-insert-row { display:flex; justify-content:center; margin:4px 0; }
    .builder-insert-row button { background:#eef3ff; color:#274066; border-color:#b9c8e6; font-size:12px; padding:4px 10px; }
    .step-head { display:flex; align-items:center; justify-content:space-between; gap:8px; margin-bottom:6px; }
    .status-pill { display:inline-block; border-radius:999px; padding:2px 8px; font-size:11px; border:1px solid var(--line); background:#f4f7ff; color:#3b4f70; text-transform:lowercase; }
    .status-pill.not-run { background:#f3f4f6; color:#4b5563; border-color:#d1d5db; }
    .status-pill.valid { background:#ecfdf3; color:#0a8f57; border-color:#b7ebcf; }
    .status-pill.failed { background:#fff1f1; color:#b42318; border-color:#f3c6c6; }
    .status-pill.successful { background:#e7f6ff; color:#0b6fb3; border-color:#bfe3fa; }
    .param-panel { border:1px solid var(--line); border-radius:8px; padding:8px; background:#f7faff; margin-top:6px; }
    .param-panel-title { font-size:12px; color:#4a648a; font-weight:600; margin-bottom:6px; text-transform:uppercase; letter-spacing:.03em; }
    .param-grid { display:grid; grid-template-columns:1fr; gap:6px; }
    .param-row { display:grid; grid-template-columns: 180px minmax(0,1fr); gap:8px; align-items:center; }
    .param-label { font-size:12px; color:#334e73; white-space:nowrap; overflow:hidden; text-overflow:ellipsis; display:flex; align-items:center; gap:6px; }
    .param-label.issue { color:#8b1e1e; font-weight:600; }
    .param-issue-badge { display:inline-flex; align-items:center; justify-content:center; width:16px; height:16px; border-radius:999px; border:1px solid #e2a9a9; color:#8b1e1e; background:#fff1f1; font-size:11px; font-weight:700; line-height:1; cursor:help; }
    .param-value input.issue, .param-value select.issue { border-color:#e2a9a9; background:#fff7f7; }
    .param-value { display:flex; align-items:center; gap:8px; }
    .param-value input[type="text"], .param-value input[type="number"], .param-value select { width:100%; }
    .combo-picker { position:relative; flex:1 1 460px; min-width:280px; max-width:700px; }
    .combo-picker input { width:100%; }
    .combo-dropdown { position:absolute; left:0; right:0; top:calc(100% + 4px); display:none; z-index:35; border:1px solid var(--line); border-radius:8px; background:#fff; box-shadow:0 10px 22px rgba(10,25,60,.15); padding:6px; }
    .combo-picker.open .combo-dropdown { display:block; }
    #b_pipeline_tree { max-height:260px; overflow:auto; }
    #builder_namespace_tree { max-height:260px; overflow:auto; border:1px solid var(--line); border-radius:8px; background:#fff; padding:6px; margin-bottom:8px; }
    #builder_namespace_tree .ns-row { display:grid; grid-template-columns: minmax(140px, 38%) minmax(0, 1fr); gap:8px; width:100%; align-items:center; }
    #builder_namespace_tree .ns-key { font-weight:600; color:#304a70; white-space:nowrap; overflow:hidden; text-overflow:ellipsis; }
    #builder_namespace_tree .ns-value { color:#4f6384; white-space:nowrap; overflow:hidden; text-overflow:ellipsis; font-family:Consolas,monospace; font-size:11px; }
    #builder_namespace_tree .jstree-ocl { display:none !important; width:0 !important; }
    #builder_namespace_tree .jstree-anchor { width:calc(100% - 2px); display:flex; align-items:center; gap:4px; padding:1px 2px; }
    #builder_namespace_tree .jstree-themeicon { flex:0 0 auto; margin-right:0; width:14px; height:14px; background:none !important; }
    #builder_namespace_tree .jstree-anchor .ns-row { flex:1 1 auto; min-width:0; }
    #builder_namespace_tree .jstree-themeicon.ns-folder-closed::before { content:"[+]"; color:#36557e; font-size:10px; }
    #builder_namespace_tree .jstree-themeicon.ns-folder-open::before { content:"[-]"; color:#36557e; font-size:10px; }
    #builder_namespace_tree .jstree-themeicon.ns-var-leaf::before { content:"*"; color:#5e6f89; font-size:10px; }
    .builder-preview-collapsed #builder_preview_body { display:none; }
    .builder-preview-collapsed #btn_builder_toggle_preview { background:#eef3ff; color:#274066; border-color:#b9c8e6; }
    .builder-subsection { border:1px solid var(--line); border-radius:8px; background:#fff; margin-bottom:8px; overflow:hidden; }
    .builder-subsection-head { display:flex; align-items:center; justify-content:space-between; gap:8px; padding:6px 8px; background:#f7faff; border-bottom:1px solid var(--line); }
    .builder-subsection-head h4 { margin:0; font-size:12px; color:#36557e; text-transform:uppercase; letter-spacing:.02em; }
    .builder-subsection-head button { padding:3px 8px; font-size:11px; line-height:1.2; background:#eef3ff; color:#274066; border-color:#b9c8e6; }
    .builder-subsection-body { padding:8px; }
    .builder-subsection.collapsed .builder-subsection-body { display:none; }
    .spin-btn { position:relative; min-width:96px; }
    .spin-btn.loading { color:transparent; }
    .spin-btn .spin { display:none; position:absolute; right:10px; top:50%; width:13px; height:13px; margin-top:-6.5px; border:2px solid rgba(255,255,255,.55); border-top-color:#fff; border-radius:50%; animation:spin .8s linear infinite; }
    .spin-btn.loading .spin { display:block; }
    .step-output { border:1px solid var(--line); border-radius:8px; background:#f9fbff; margin-top:6px; overflow:hidden; }
    .step-output-head { display:flex; align-items:center; justify-content:space-between; gap:8px; padding:4px 8px; background:#f3f7ff; border-bottom:1px solid var(--line); }
    .step-output-head .title { font-size:12px; color:#47638a; font-weight:600; }
    .step-output-head button { background:#eef3ff; color:#274066; border-color:#b9c8e6; font-size:11px; padding:2px 8px; }
    .step-output pre { margin:0; border:0; border-radius:0; background:#fff; padding:8px; max-height:220px; overflow:auto; }
    .step-output.collapsed pre { display:none; }
    @keyframes spin { to { transform:rotate(360deg); } }
    @media (max-width: 1280px) {
      .builder-surface { grid-template-columns: 1fr; }
      .builder-surface.builder-right-collapsed { grid-template-columns: 1fr; }
      .builder-surface.builder-right-collapsed #btn_builder_toggle_preview { writing-mode:horizontal-tb; transform:none; min-height:unset; padding:6px 8px; }
    }
    @media (max-width: 960px) { .grid { grid-template-columns: 1fr; } }
    @media (min-width: 1500px) {
      .param-grid { grid-template-columns: 1fr 1fr; column-gap:12px; }
    }
    @media (min-width: 1820px) {
      .param-grid { grid-template-columns: 1fr 1fr 1fr; column-gap:12px; }
    }
  </style>
</head>
<body>
  <div class="wrap">
    <nav class="topnav">
      <div class="links">
        <a id="nav_ops" href="/">Operations</a>
        <a id="nav_pipelines" href="/pipelines">Pipelines</a>
        <a id="nav_datasets" href="/datasets">Datasets</a>
        <a id="nav_plugins" href="/plugins">Plugins</a>
        <a id="nav_new_pipeline" href="/pipelines/new">New Pipeline</a>
        <a id="nav_context_back" class="context" href="#" style="display:none;">Back</a>
      </div>
      <div class="jump">
        <div class="who">
          <label for="nav_user">User</label>
          <select id="nav_user">
            <option value="admin">admin</option>
            <option value="land-core">land-core</option>
            <option value="gee-lee">gee-lee</option>
          </select>
        </div>
        <input id="nav_live_id" placeholder="run id for live view" />
        <button id="btn_nav_live">Live</button>
      </div>
    </nav>
    <input id="b_file_picker" type="file" accept=".yml,.yaml" style="display:none;" />
    <div class="head">
      <h1 id="page_title">Research ETL Runs</h1>
      <div class="muted">Auto-refresh every 12s</div>
    </div>
    <div class="grid">
      <section class="panel">
        <h3 id="left_title">Recent Runs</h3>
        <div id="ops_panel">
          <div class="controls">
            <button id="btn_ops_refresh">Refresh Ops</button>
            <span class="muted">Failed/running triage inbox</span>
          </div>
          <div class="filesplit">
            <div class="filetree">
              <div class="muted"><b>Failed Runs</b></div>
              <div id="ops_failed" class="muted">Loading...</div>
            </div>
            <div class="viewer">
              <div class="muted"><b>Running Runs</b></div>
              <div id="ops_running" class="muted">Loading...</div>
            </div>
          </div>
        </div>
        <div id="pipelines_panel" style="display:none;">
          <div class="controls">
            <input id="p_q" placeholder="Search pipeline path" />
            <button id="btn_pipelines">Refresh Pipelines</button>
          </div>
          <table>
            <thead><tr><th>Pipeline</th><th>Last</th><th>Started</th><th>Runs</th><th>Failure</th></tr></thead>
            <tbody id="pipelines"></tbody>
          </table>
        </div>
        <div class="controls">
          <select id="f_status">
            <option value="">All status</option>
            <option value="queued">queued</option>
            <option value="running">running</option>
            <option value="succeeded">succeeded</option>
            <option value="failed">failed</option>
          </select>
          <select id="f_executor">
            <option value="">All executors</option>
            <option value="local">local</option>
            <option value="slurm">slurm</option>
          </select>
          <input id="f_q" placeholder="Search run_id/pipeline" />
          <button id="btn_apply">Apply</button>
        </div>
        <table>
          <thead><tr><th>Run ID</th><th>Status</th><th>Started</th><th>Pipeline</th></tr></thead>
          <tbody id="runs"></tbody>
        </table>
      </section>
      <section class="panel">
        <h3 id="right_title">Run Detail</h3>
        <div id="pipeline_summary" class="muted" style="display:none;"></div>
        <div id="pipeline_validations" class="muted" style="display:none;"></div>
        <div id="plugins_controls" class="controls" style="display:none;">
          <select id="plugins_env">
            <option value="">env (optional)</option>
          </select>
          <button id="btn_plugins_refresh">Refresh Plugins</button>
          <span id="plugins_msg" class="muted"></span>
        </div>
        <div id="builder_panel" style="display:none;">
          <div class="builder-surface" id="builder_surface">
            <div class="builder-card" id="builder_preview_card">
              <div class="builder-head">
                <h4>Pipeline Config</h4>
                <span id="builder_pipeline_status" class="status-pill not-run">not run</span>
              </div>
              <div class="controls">
                <div id="b_pipeline_combo" class="combo-picker">
                  <input id="b_pipeline_path" placeholder="pipeline name (stored under pipelines/)" />
                  <div class="combo-dropdown">
                    <div id="b_pipeline_tree"></div>
                  </div>
                </div>
                <button id="btn_builder_import_local">Import Local</button>
                <button id="btn_builder_save">Save Draft</button>
                <button id="btn_builder_generate">Generate</button>
                <button id="btn_builder_validate">Validate Draft</button>
                <button id="btn_builder_run" class="spin-btn"><span>Run Pipeline</span><span class="spin"></span></button>
                <span id="builder_msg" class="muted"></span>
              </div>
              <div class="controls">
                <input id="b_intent" placeholder="intent for AI draft generation" />
                <input id="b_constraints" placeholder="constraints (optional)" />
              </div>
              <div class="controls">
                <select id="b_env_name">
                  <option value="">env (optional)</option>
                </select>
                <select id="b_run_mode">
                  <option value="draft">Draft Mode (workspace)</option>
                  <option value="repro">Repro Mode (auto/git-pinned)</option>
                </select>
                <label class="muted"><input type="checkbox" id="b_dry_run" /> dry_run</label>
              </div>
              <div class="controls">
                <input id="b_max_retries" placeholder="max_retries (default: 0)" />
                <input id="b_retry_delay" placeholder="retry_delay_seconds (default: 0.0)" />
              </div>
              <div class="builder-head">
                <h4>Requires Pipelines</h4>
                <button id="btn_builder_add_req">Add Require</button>
              </div>
              <div id="b_requires" class="builder-list"></div>
              <div class="builder-head">
                <h4>Pipeline Vars</h4>
                <button id="btn_builder_add_var">Add Var</button>
              </div>
              <div id="b_vars" class="builder-list"></div>
              <div class="builder-head">
                <h4>Directories</h4>
                <select id="b_dir_type">
                  <option value="workdir">workdir</option>
                  <option value="logdir">logdir</option>
                  <option value="artifact">artifact</option>
                  <option value="tmp">tmp</option>
                  <option value="stage">stage</option>
                  <option value="custom">&lt;enter custom name&gt;</option>
                </select>
                <input id="b_dir_custom" placeholder="custom dir key" />
                <button id="btn_builder_add_dir">Add Dir</button>
              </div>
              <div id="b_dirs" class="builder-list"></div>
              <div class="builder-head">
                <h4>Steps</h4>
                <button id="btn_builder_add_step">Add Step</button>
              </div>
              <div id="b_steps" class="builder-list"></div>
            </div>
            <div class="builder-card">
              <div class="builder-head">
                <h4>YAML Preview / Builder Output</h4>
                <button id="btn_builder_toggle_preview" type="button">Expand</button>
              </div>
              <div id="builder_preview_body">
                <section class="builder-subsection" id="builder_section_yaml">
                  <div class="builder-subsection-head">
                    <h4>YAML Preview (read-only)</h4>
                    <button id="btn_builder_toggle_yaml" type="button">Collapse</button>
                  </div>
                  <div class="builder-subsection-body">
                    <textarea id="b_yaml" readonly style="width:100%; min-height:420px; font-family:Consolas,monospace; font-size:12px;"></textarea>
                  </div>
                </section>
                <section class="builder-subsection" id="builder_section_output">
                  <div class="builder-subsection-head">
                    <h4>Builder Output</h4>
                    <button id="btn_builder_toggle_output" type="button">Collapse</button>
                  </div>
                  <div class="builder-subsection-body">
                    <pre id="builder_output">No draft action yet.</pre>
                  </div>
                </section>
                <section class="builder-subsection" id="builder_section_vars">
                  <div class="builder-subsection-head">
                    <h4>Variable Tracker</h4>
                    <button id="btn_builder_toggle_vars" type="button">Collapse</button>
                  </div>
                  <div class="builder-subsection-body">
                    <div id="builder_namespace_tree"></div>
                    <pre id="builder_namespace">Loading...</pre>
                  </div>
                </section>
                <section class="builder-subsection" id="builder_section_plugins">
                  <div class="builder-subsection-head">
                    <h4>Plugin Stats</h4>
                    <button id="btn_builder_toggle_plugins" type="button">Collapse</button>
                  </div>
                  <div class="builder-subsection-body">
                    <div id="builder_plugin_stats">Loading plugin stats...</div>
                  </div>
                </section>
              </div>
            </div>
          </div>
        </div>
        <div class="controls">
          <input id="a_pipeline" placeholder="pipeline path (e.g. pipelines/sample.yml)" />
          <select id="a_executor">
            <option value="local">local</option>
            <option value="slurm">slurm</option>
          </select>
          <button id="btn_validate">Validate</button>
          <button id="btn_run">Run</button>
          <span id="action_msg" class="muted"></span>
        </div>
        <div class="controls">
          <input id="a_global_config" placeholder="global_config (optional)" />
          <input id="a_environments_config" placeholder="environments_config (optional)" />
          <input id="a_env" placeholder="env name (when environments_config set)" />
        </div>
        <div class="controls">
          <input id="a_plugins_dir" placeholder="plugins_dir (default: plugins)" />
          <input id="a_workdir" placeholder="workdir (default: .runs)" />
          <input id="a_max_retries" placeholder="max_retries (optional)" />
          <input id="a_retry_delay" placeholder="retry_delay_seconds (optional)" />
          <label class="muted"><input type="checkbox" id="a_dry_run" /> dry_run</label>
          <label class="muted"><input type="checkbox" id="a_verbose" /> verbose (slurm)</label>
        </div>
        <div class="controls">
          <button id="btn_resume">Resume Selected</button>
          <button id="btn_stop">Stop Selected</button>
          <span id="resume_msg" class="muted"></span>
        </div>
        <div class="controls">
          <input id="r_plugins_dir" placeholder="plugins_dir (default: plugins)" />
          <input id="r_workdir" placeholder="workdir (default: .runs)" />
        </div>
        <div class="controls">
          <input id="r_max_retries" placeholder="max_retries (default: 0)" />
          <input id="r_retry_delay" placeholder="retry_delay_seconds (default: 0.0)" />
          <select id="r_executor">
            <option value="">executor override (default: original)</option>
            <option value="local">local</option>
            <option value="slurm">slurm</option>
          </select>
        </div>
        <div id="detail" class="muted">Select a run to view details.</div>
      </section>
    </div>
  </div>
  <script src="https://code.jquery.com/jquery-3.7.1.min.js"></script>
  <script src="https://cdn.jsdelivr.net/npm/jstree@3.3.16/dist/jstree.min.js"></script>
  <script>
    let selected = null;
    let selectedPipeline = null;
    let selectedDataset = null;
    const isPipelinesView = window.location.pathname.startsWith("/pipelines");
    const isDatasetsView = window.location.pathname.startsWith("/datasets");
    const isPluginsView = window.location.pathname === "/plugins";
    const isOperationsView = window.location.pathname === "/";
    const liveMatch = window.location.pathname.match(/^\/runs\/(.+)\/live$/);
    const isLiveRunView = !!liveMatch;
    const liveRunIdFromPath = isLiveRunView ? decodeURIComponent(liveMatch[1]) : null;
    const isBuilderNewView = window.location.pathname === "/pipelines/new";
    const builderEditMatch = window.location.pathname.match(/^\/pipelines\/(.+)\/edit$/);
    const isBuilderEditView = !!builderEditMatch;
    const isBuilderView = isBuilderNewView || isBuilderEditView;
    const builderPipelineFromPath = isBuilderEditView ? decodeURIComponent(builderEditMatch[1]) : "";
    const isPipelineDetailView = isPipelinesView && window.location.pathname.length > "/pipelines/".length;
    const pipelineFromPath = isPipelineDetailView
      ? decodeURIComponent(window.location.pathname.slice("/pipelines/".length))
      : null;
    const isDatasetDetailView = isDatasetsView && window.location.pathname.length > "/datasets/".length;
    const datasetFromPath = isDatasetDetailView
      ? decodeURIComponent(window.location.pathname.slice("/datasets/".length))
      : null;
    let builderLoaded = false;
    let builderModel = { vars: {}, dirs: {}, requires_pipelines: [], steps: [] };
    let builderPlugins = [];
    let builderPluginMeta = {};
    let builderPluginStats = {};
    let builderValidationState = "unknown";
    let builderStepStatus = {};
    let builderStepTesting = {};
    let builderStepOutput = {};
    let builderStepOutputCollapsed = {};
    let builderParamIssues = {};
    let builderPipelineRunState = "not-run";
    let builderPipelineRunning = false;
    let builderPreviewCollapsed = false;
    let builderPreviewSectionCollapsed = { yaml: false, output: false, vars: false, plugins: false };
    let builderTreeFiles = [];
    let builderTreeFileSelection = "";
    let builderNamespaceTimer = null;
    let builderLastTextTarget = null;
    let builderAutoValidateTimer = null;
    let builderValidateInFlight = false;
    let builderNamespaceDigest = "";
    let builderRunSeed = null;
    const USER_STORAGE_KEY = "etl_ui_user";
    const VALID_UI_USERS = new Set(["admin", "land-core", "gee-lee"]);
    const _nativeFetch = window.fetch.bind(window);
    function currentAsUser(){
      const el = document.getElementById("nav_user");
      const raw = el ? String(el.value || "").trim() : "";
      const val = raw || "admin";
      return VALID_UI_USERS.has(val) ? val : "admin";
    }
    function withAsUserUrl(inputUrl){
      const txt = String(inputUrl || "");
      if(!txt.startsWith("/api/")){
        return txt;
      }
      const u = new URL(txt, window.location.origin);
      if(!u.searchParams.get("as_user")){
        u.searchParams.set("as_user", currentAsUser());
      }
      return u.pathname + (u.search || "") + (u.hash || "");
    }
    window.fetch = function(input, init){
      if(typeof input === "string"){
        return _nativeFetch(withAsUserUrl(input), init);
      }
      return _nativeFetch(input, init);
    };
    function initUserScope(){
      const sel = document.getElementById("nav_user");
      if(!sel) return;
      let fromQuery = "";
      try {
        const qp = new URLSearchParams(window.location.search);
        fromQuery = String(qp.get("as_user") || "").trim();
      } catch {}
      const stored = String(localStorage.getItem(USER_STORAGE_KEY) || "").trim();
      const chosen = fromQuery || stored || "admin";
      sel.value = VALID_UI_USERS.has(chosen) ? chosen : "admin";
      localStorage.setItem(USER_STORAGE_KEY, sel.value);
      sel.onchange = async () => {
        const next = String(sel.value || "admin").trim();
        localStorage.setItem(USER_STORAGE_KEY, VALID_UI_USERS.has(next) ? next : "admin");
        await tick();
      };
    }
    function defaultBuilderDirs(){
      return {
        workdir: "{env.workdir}/{sys.now.yymmdd}/{sys.now.hhmmss}-{sys.run.short_id}",
        logdir: "{workdir}/logs",
      };
    }
    function ensureBuilderDefaultDirs(model){
      const out = model || {};
      out.dirs = out.dirs || {};
      if(Object.keys(out.dirs).length){
        return out;
      }
      out.dirs = { ...defaultBuilderDirs() };
      return out;
    }
    function deriveBuilderWorkdir(){
      const dirs = (builderModel && builderModel.dirs) || {};
      const candidates = ["workdir", "work", "work_dir"];
      for(const k of candidates){
        const v = String(dirs[k] || "").trim();
        if(v) return v;
      }
      return "";
    }
    function qp(){
      const p = new URLSearchParams();
      const s = document.getElementById("f_status").value;
      const e = document.getElementById("f_executor").value;
      const q = document.getElementById("f_q").value.trim();
      if(s) p.set("status", s);
      if(e) p.set("executor", e);
      if(q && !isPipelineDetailView) p.set("q", q);
      p.set("limit", "100");
      return p.toString();
    }
    function pipelineQp(){
      const p = new URLSearchParams();
      const q = document.getElementById("p_q").value.trim();
      if(q) p.set("q", q);
      p.set("limit", "100");
      return p.toString();
    }
    function esc(v){return String(v ?? "").replaceAll("&","&amp;").replaceAll("<","&lt;").replaceAll(">","&gt;")}
    function makeClientRunId(){
      const hex = "0123456789abcdef";
      let out = "";
      for(let i=0;i<32;i++){
        out += hex[Math.floor(Math.random() * 16)];
      }
      return out;
    }
    function makeRunSeed(){
      return { run_id: makeClientRunId(), run_started_at: new Date().toISOString() };
    }
    function ensureBuilderRunSeed(){
      if(!builderRunSeed){
        builderRunSeed = makeRunSeed();
      }
      return builderRunSeed;
    }
    function normalizeBuilderPipelineName(raw){
      let s = String(raw || "").trim().replaceAll("\\\\","/");
      if (!s) return "";
      if (s.toLowerCase().startsWith("pipelines/")) s = s.slice("pipelines/".length);
      if (!s.toLowerCase().endsWith(".yml") && !s.toLowerCase().endsWith(".yaml")) s += ".yml";
      return s;
    }
    function setActiveNav(){
      const path = window.location.pathname;
      const ops = document.getElementById("nav_ops");
      const pipes = document.getElementById("nav_pipelines");
      const datasets = document.getElementById("nav_datasets");
      const plugins = document.getElementById("nav_plugins");
      const newp = document.getElementById("nav_new_pipeline");
      const back = document.getElementById("nav_context_back");
      [ops, pipes, datasets, plugins, newp].forEach(el => el.classList.remove("active"));
      back.style.display = "none";
      if (path === "/") {
        ops.classList.add("active");
      } else if (path === "/plugins") {
        plugins.classList.add("active");
      } else if (path.startsWith("/datasets")) {
        datasets.classList.add("active");
      } else if (path === "/pipelines/new") {
        newp.classList.add("active");
      } else if (path.startsWith("/pipelines")) {
        pipes.classList.add("active");
      }
      if (isBuilderEditView && builderPipelineFromPath) {
        back.style.display = "inline-block";
        back.textContent = "Back to Pipeline";
        back.href = `/pipelines/${encodeURIComponent(builderPipelineFromPath)}`;
      } else if (isLiveRunView) {
        back.style.display = "inline-block";
        back.textContent = "Back to Pipelines";
        back.href = "/pipelines";
      }
      if (isLiveRunView && liveRunIdFromPath) {
        document.getElementById("nav_live_id").value = liveRunIdFromPath;
      }
    }
    function initViewMode(){
      if(isOperationsView){
        document.getElementById("page_title").textContent = "Research ETL Operations";
        document.getElementById("left_title").textContent = "Operations Inbox";
      }
      if(isPluginsView){
        document.body.classList.add("plugins-mode");
        document.getElementById("page_title").textContent = "Research ETL Plugins";
        document.getElementById("right_title").textContent = "Plugin Catalog + Recommendations";
        document.getElementById("ops_panel").style.display = "none";
        document.getElementById("pipelines_panel").style.display = "none";
        document.getElementById("pipeline_summary").style.display = "none";
        document.getElementById("pipeline_validations").style.display = "none";
        document.getElementById("plugins_controls").style.display = "flex";
        document.getElementById("detail").textContent = "Loading plugin stats...";
      }
      if(isPipelinesView){
        document.getElementById("page_title").textContent = "Research ETL Pipelines";
        document.getElementById("ops_panel").style.display = "none";
      }
      if(isDatasetsView){
        document.getElementById("page_title").textContent = "Research ETL Datasets";
        document.getElementById("left_title").textContent = "Datasets";
        document.getElementById("right_title").textContent = "Dataset Detail";
        document.getElementById("ops_panel").style.display = "none";
        document.getElementById("pipelines_panel").style.display = "none";
        document.getElementById("pipeline_summary").style.display = "none";
        document.getElementById("pipeline_validations").style.display = "none";
        document.getElementById("f_q").placeholder = "search datasets";
        if(isDatasetDetailView){
          selectedDataset = datasetFromPath;
          document.getElementById("f_q").value = datasetFromPath || "";
        }
      }
      if(isBuilderView){
        document.body.classList.add("builder-mode");
        document.getElementById("page_title").textContent = isBuilderNewView ? "Research ETL Pipeline Builder" : "Research ETL Pipeline Editor";
        document.getElementById("right_title").textContent = "Draft Builder";
        document.getElementById("ops_panel").style.display = "none";
        document.getElementById("pipelines_panel").style.display = "none";
        document.getElementById("pipeline_summary").style.display = "none";
        document.getElementById("builder_panel").style.display = "block";
        document.getElementById("detail").style.display = "none";
        if (builderPipelineFromPath) {
          document.getElementById("b_pipeline_path").value = builderPipelineFromPath;
        }
      }
      if(isLiveRunView){
        selected = liveRunIdFromPath;
        document.getElementById("page_title").textContent = "Research ETL Live Run";
        document.getElementById("left_title").textContent = "Recent Runs";
        document.getElementById("right_title").textContent = "Live Run View";
        document.getElementById("ops_panel").style.display = "none";
        document.getElementById("detail").textContent = "Loading live run status...";
      }
      if(isPipelinesView && !isPipelineDetailView && !isBuilderView){
        document.getElementById("left_title").textContent = "Pipelines";
        document.getElementById("pipelines_panel").style.display = "block";
      }
      if(isPipelineDetailView && !isBuilderView){
        selectedPipeline = pipelineFromPath;
        document.getElementById("page_title").textContent = "Research ETL Pipeline Detail";
        document.getElementById("left_title").textContent = "Pipeline Runs";
        document.getElementById("right_title").textContent = "Pipeline + Run Detail";
        document.getElementById("pipeline_summary").style.display = "block";
        document.getElementById("pipeline_validations").style.display = "block";
        document.getElementById("a_pipeline").value = selectedPipeline;
        document.getElementById("f_q").value = selectedPipeline;
      }
    }
    function renderOpsRows(rows, mode){
      if(!rows || !rows.length){
        return `<div class="muted">None</div>`;
      }
      return rows.map(r => {
        const resumeBtn = mode === "failed" ? `<button data-op="resume" data-id="${esc(r.run_id)}">Resume</button>` : "";
        const stopBtn = mode === "running" ? `<button data-op="stop" data-id="${esc(r.run_id)}">Stop</button>` : "";
        return `
          <div class="node file" data-op="view" data-id="${esc(r.run_id)}">
            <div><b>${esc(r.run_id)}</b> <span class="${r.success ? "ok" : "bad"}">${esc(r.status)}</span></div>
            <div class="muted">${esc(r.pipeline)} | ${esc(r.executor)}</div>
            <div class="controls">
              <button data-op="view" data-id="${esc(r.run_id)}">View</button>
              ${resumeBtn}
              ${stopBtn}
            </div>
          </div>
        `;
      }).join("");
    }
    async function quickResume(runId){
      const res = await fetch(`/api/runs/${encodeURIComponent(runId)}/resume`, {
        method:"POST",
        headers: {"Content-Type":"application/json"},
        body: "{}",
      });
      if(!res.ok){
        return await readMessage(res);
      }
      const payload = await res.json();
      selected = payload.run_id;
      return `Resumed as ${payload.run_id}`;
    }
    async function loadOps(){
      if(!isOperationsView) return;
      const failedEl = document.getElementById("ops_failed");
      const runningEl = document.getElementById("ops_running");
      const [failedRes, runningRes] = await Promise.all([
        fetch(`/api/runs?status=failed&limit=20`),
        fetch(`/api/runs?status=running&limit=20`),
      ]);
      if(!failedRes.ok){
        failedEl.innerHTML = `<div>${esc(await readMessage(failedRes))}</div>`;
      } else {
        failedEl.innerHTML = renderOpsRows(await failedRes.json(), "failed");
      }
      if(!runningRes.ok){
        runningEl.innerHTML = `<div>${esc(await readMessage(runningRes))}</div>`;
      } else {
        runningEl.innerHTML = renderOpsRows(await runningRes.json(), "running");
      }
      for (const holder of [failedEl, runningEl]){
        [...holder.querySelectorAll("button[data-op='view']")].forEach(btn => {
          btn.onclick = async (ev) => {
            ev.stopPropagation();
            selected = btn.dataset.id;
            await loadDetail();
          };
        });
        [...holder.querySelectorAll("button[data-op='resume']")].forEach(btn => {
          btn.onclick = async (ev) => {
            ev.stopPropagation();
            const msg = await quickResume(btn.dataset.id);
            document.getElementById("resume_msg").textContent = msg;
            await tick();
          };
        });
        [...holder.querySelectorAll("button[data-op='stop']")].forEach(btn => {
          btn.onclick = async (ev) => {
            ev.stopPropagation();
            const msg = await quickStop(btn.dataset.id);
            document.getElementById("resume_msg").textContent = msg;
            await tick();
          };
        });
        [...holder.querySelectorAll("div[data-op='view']")].forEach(card => {
          card.onclick = async () => {
            selected = card.dataset.id;
            await loadDetail();
          };
        });
      }
    }
    async function readMessage(res){
      const txt = await res.text();
      try {
        const payload = JSON.parse(txt);
        const detail = payload.detail;
        if(detail && typeof detail === "object"){
          return detail.message || detail.error || JSON.stringify(detail);
        }
        return detail || payload.message || txt;
      } catch {
        return txt;
      }
    }
    function actionPayload(){
      const body = {};
      const pipeline = document.getElementById("a_pipeline").value.trim();
      if (pipeline) body.pipeline = pipeline;
      body.executor = document.getElementById("a_executor").value.trim() || "local";
      const globalConfig = document.getElementById("a_global_config").value.trim();
      const environmentsConfig = document.getElementById("a_environments_config").value.trim();
      const env = document.getElementById("a_env").value.trim();
      const pluginsDir = document.getElementById("a_plugins_dir").value.trim();
      const workdir = document.getElementById("a_workdir").value.trim();
      const retries = document.getElementById("a_max_retries").value.trim();
      const delay = document.getElementById("a_retry_delay").value.trim();
      if (globalConfig) body.global_config = globalConfig;
      if (environmentsConfig) body.environments_config = environmentsConfig;
      if (env) body.env = env;
      if (pluginsDir) body.plugins_dir = pluginsDir;
      if (workdir) body.workdir = workdir;
      if (retries) body.max_retries = Number(retries);
      if (delay) body.retry_delay_seconds = Number(delay);
      body.dry_run = document.getElementById("a_dry_run").checked;
      body.verbose = document.getElementById("a_verbose").checked;
      return body;
    }
    function _yamlEsc(v){
      const s = String(v ?? "");
      if (!s.length) return '""';
      if (/^[A-Za-z0-9_./:-]+$/.test(s) && s.toLowerCase() !== "true" && s.toLowerCase() !== "false") return s;
      return `"${s.replaceAll("\\\\","\\\\\\\\").replaceAll('"','\\\\\\"')}"`;
    }
    function _yamlArgVal(v){
      if (typeof v === "boolean") return v ? "true" : "false";
      if (typeof v === "number" && Number.isFinite(v)) return String(v);
      return _yamlEsc(v);
    }
    function _scriptFromStep(st){
      const base = (st.plugin || "").trim();
      const parts = [];
      for(const [k,v] of Object.entries(st.params || {})){
        const vv = String(v ?? "").trim();
        if (!vv.length) continue;
        parts.push(`${k}=${_yamlEsc(vv)}`);
      }
      return [base, ...parts].filter(Boolean).join(" ");
    }
    function buildYamlFromModel(){
      const m = builderModel || { vars:{}, dirs:{}, requires_pipelines:[], steps:[] };
      const lines = [];
      if ((m.requires_pipelines || []).length){
        lines.push("requires_pipelines:");
        for(const r of m.requires_pipelines){ lines.push(`  - ${_yamlEsc(r)}`); }
      }
      lines.push("vars:");
      const vars = m.vars || {};
      const vkeys = Object.keys(vars);
      if(!vkeys.length){ lines.push("  {}"); } else {
        for(const k of vkeys){ lines.push(`  ${k}: ${_yamlEsc(vars[k])}`); }
      }
      lines.push("dirs:");
      const dirs = m.dirs || {};
      const dkeys = Object.keys(dirs);
      if(!dkeys.length){ lines.push("  {}"); } else {
        for(const k of dkeys){ lines.push(`  ${k}: ${_yamlEsc(dirs[k])}`); }
      }
      lines.push("steps:");
      const steps = m.steps || [];
      if(!steps.length){
        lines.push("  - plugin: echo.py");
      } else {
        for(const st of steps){
          lines.push(`  - plugin: ${_yamlEsc(st.plugin || "echo.py")}`);
          const pentries = Object.entries(st.params || {}).filter(([_, v]) => {
            if (v === null || v === undefined) return false;
            if (typeof v === "string") return String(v).trim().length > 0;
            return true;
          });
          if (pentries.length){
            lines.push("    args:");
            for(const [k, v] of pentries){
              lines.push(`      ${k}: ${_yamlArgVal(v)}`);
            }
          }
          if ((st.type || "sequential") === "parallel" && (st.parallel_with || "").trim()) {
            lines.push(`    parallel_with: ${_yamlEsc(st.parallel_with)}`);
          }
          if ((st.type || "sequential") === "foreach") {
            const foreachMode = String(st.foreach_mode || (String(st.foreach_glob || "").trim() ? "glob" : "var")).trim().toLowerCase() === "glob" ? "glob" : "var";
            if (foreachMode === "glob" && (st.foreach_glob || "").trim()) {
              lines.push(`    foreach_glob: ${_yamlEsc(st.foreach_glob)}`);
              if ((st.foreach_kind || "").trim()) {
                lines.push(`    foreach_kind: ${_yamlEsc(st.foreach_kind)}`);
              }
            } else if ((st.foreach || "").trim()) {
              lines.push(`    foreach: ${_yamlEsc(st.foreach)}`);
            }
          }
          const rentries = Object.entries(st.resources || {}).filter(([_, v]) => {
            if (v === null || v === undefined) return false;
            if (typeof v === "string") return String(v).trim().length > 0;
            return true;
          });
          if (rentries.length){
            lines.push("    resources:");
            for(const [k, v] of rentries){
              lines.push(`      ${k}: ${_yamlArgVal(v)}`);
            }
          }
          if ((st.output_var || "").trim()) lines.push(`    output_var: ${_yamlEsc(st.output_var)}`);
          if ((st.when || "").trim()) lines.push(`    when: ${_yamlEsc(st.when)}`);
        }
      }
      return lines.join("\\n") + "\\n";
    }
    function syncYamlPreview(){
      const area = document.getElementById("b_yaml");
      const next = buildYamlFromModel();
      const changed = area.value !== next;
      area.value = next;
      if(changed){
        builderRunSeed = null;
        builderValidationState = "unknown";
        builderStepStatus = {};
        builderStepTesting = {};
        builderStepOutput = {};
        builderStepOutputCollapsed = {};
        builderParamIssues = {};
        builderPipelineRunState = "not-run";
      }
      renderBuilderPipelineStatus();
      if (builderNamespaceTimer) {
        clearTimeout(builderNamespaceTimer);
      }
      builderNamespaceTimer = setTimeout(() => { refreshBuilderNamespace(); }, 120);
    }
    function builderPipelineStatusMeta(){
      if(builderPipelineRunState === "failed"){
        return { klass: "failed", text: "failed" };
      }
      if(builderPipelineRunState === "run_ok"){
        if(builderValidationState === "valid"){
          return { klass: "successful", text: "successful" };
        }
        return { klass: "valid", text: "valid" };
      }
      if(builderValidationState === "valid"){
        return { klass: "valid", text: "valid" };
      }
      return { klass: "not-run", text: "not run" };
    }
    function renderBuilderPipelineStatus(){
      const pill = document.getElementById("builder_pipeline_status");
      if(!pill) return;
      const meta = builderPipelineStatusMeta();
      pill.className = `status-pill ${meta.klass}`;
      pill.textContent = meta.text;
      const runBtn = document.getElementById("btn_builder_run");
      if(runBtn){
        runBtn.classList.toggle("loading", !!builderPipelineRunning);
        runBtn.disabled = !!builderPipelineRunning;
      }
    }
    function renderBuilderPreviewPanel(){
      const card = document.getElementById("builder_preview_card");
      const surface = document.getElementById("builder_surface");
      const btn = document.getElementById("btn_builder_toggle_preview");
      if(!card || !btn || !surface) return;
      card.classList.toggle("builder-preview-collapsed", !!builderPreviewCollapsed);
      surface.classList.toggle("builder-right-collapsed", !!builderPreviewCollapsed);
      btn.textContent = builderPreviewCollapsed ? "Expand Preview" : "Collapse Preview";
    }
    function renderBuilderPreviewSections(){
      const defs = [
        { key: "yaml", sectionId: "builder_section_yaml", btnId: "btn_builder_toggle_yaml", title: "YAML" },
        { key: "output", sectionId: "builder_section_output", btnId: "btn_builder_toggle_output", title: "Output" },
        { key: "vars", sectionId: "builder_section_vars", btnId: "btn_builder_toggle_vars", title: "Variables" },
        { key: "plugins", sectionId: "builder_section_plugins", btnId: "btn_builder_toggle_plugins", title: "Plugins" },
      ];
      for(const d of defs){
        const section = document.getElementById(d.sectionId);
        const btn = document.getElementById(d.btnId);
        if(!section || !btn) continue;
        const collapsed = !!builderPreviewSectionCollapsed[d.key];
        section.classList.toggle("collapsed", collapsed);
        btn.textContent = collapsed ? `Expand ${d.title}` : `Collapse ${d.title}`;
      }
    }
    function builderPayload(){
      const body = { yaml_text: document.getElementById("b_yaml").value || "" };
      const pipeline = normalizeBuilderPipelineName(document.getElementById("b_pipeline_path").value.trim());
      const intent = document.getElementById("b_intent").value.trim();
      const constraints = document.getElementById("b_constraints").value.trim();
      const envName = document.getElementById("b_env_name").value.trim();
      const workdir = deriveBuilderWorkdir();
      const retries = document.getElementById("b_max_retries").value.trim();
      const delay = document.getElementById("b_retry_delay").value.trim();
      if (pipeline) body.pipeline = pipeline;
      if (intent) body.intent = intent;
      if (constraints) body.constraints = constraints;
      if (envName) body.env = envName;
      if (workdir) body.workdir = workdir;
      if (retries) body.max_retries = Number(retries);
      if (delay) body.retry_delay_seconds = Number(delay);
      body.dry_run = document.getElementById("b_dry_run").checked;
      return body;
    }
    async function loadBuilderEnvironments(){
      if(!isBuilderView) return;
      const sel = document.getElementById("b_env_name");
      const current = String(sel.value || "").trim();
      const qp = new URLSearchParams();
      const res = await fetch(`/api/builder/environments?${qp.toString()}`);
      if(!res.ok){
        document.getElementById("builder_msg").textContent = await readMessage(res);
        sel.innerHTML = `<option value="">env (optional)</option>`;
        return;
      }
      const payload = await res.json();
      const envs = Array.isArray(payload.environments) ? payload.environments : [];
      sel.innerHTML = `<option value="">env (optional)</option>` + envs.map(e => `<option value="${esc(e)}">${esc(e)}</option>`).join("");
      if(current && envs.includes(current)){
        sel.value = current;
      } else if(envs.includes("local")){
        sel.value = "local";
      }
    }
    async function loadBuilderPlugins(){
      if(!isBuilderView) return;
      const res = await fetch(`/api/builder/plugins`);
      if(!res.ok){
        document.getElementById("builder_msg").textContent = await readMessage(res);
        return;
      }
      const payload = await res.json();
      builderPlugins = payload.plugins || [];
      builderPluginMeta = {};
      for(const p of builderPlugins){ builderPluginMeta[p.path] = p; }
      await loadBuilderPluginStats();
      renderBuilderPluginStats();
      renderBuilderModel();
    }
    function pluginRecommendation(path){
      const st = builderPluginStats[String(path || "")] || {};
      return st.recommendation || {};
    }
    async function loadBuilderPluginStats(){
      if(!isBuilderView) return;
      const qp = new URLSearchParams();
      const envName = String((document.getElementById("b_env_name") || {}).value || "").trim();
      if(envName) qp.set("env", envName);
      const res = await fetch(`/api/plugins/stats?${qp.toString()}`);
      if(!res.ok){
        builderPluginStats = {};
        return;
      }
      const payload = await res.json();
      const statsMap = {};
      for(const p of (payload.plugins || [])){
        statsMap[String(p.path || "")] = p;
      }
      builderPluginStats = statsMap;
    }
    async function loadPluginsPage(){
      if(!isPluginsView) return;
      const msgEl = document.getElementById("plugins_msg");
      const envSel = document.getElementById("plugins_env");
      const envName = String((envSel || {}).value || "").trim();
      const qp = new URLSearchParams();
      if(envName) qp.set("env", envName);
      const res = await fetch(`/api/plugins/stats?${qp.toString()}`);
      if(!res.ok){
        document.getElementById("detail").textContent = await readMessage(res);
        if(msgEl) msgEl.textContent = "Failed to load plugin stats.";
        return;
      }
      const payload = await res.json();
      const caps = payload.caps || {};
      const rows = (payload.plugins || []).map((p) => {
        const rec = p.recommendation || {};
        const stats = p.stats || {};
        return `
          <tr>
            <td>${esc(String(p.path || ""))}</td>
            <td>${esc(String(p.name || ""))}</td>
            <td>${esc(String(p.version || ""))}</td>
            <td>${esc(String(rec.samples || 0))}</td>
            <td>${esc(rec.cpu_cores === null || rec.cpu_cores === undefined ? "" : Number(rec.cpu_cores).toFixed(2))}</td>
            <td>${esc(rec.memory_gb === null || rec.memory_gb === undefined ? "" : Number(rec.memory_gb).toFixed(2))}</td>
            <td>${esc(rec.wall_minutes === null || rec.wall_minutes === undefined ? "" : Number(rec.wall_minutes).toFixed(2))}</td>
            <td>${esc(stats.wall_minutes_mean === null || stats.wall_minutes_mean === undefined ? "" : Number(stats.wall_minutes_mean).toFixed(2))}</td>
          </tr>
        `;
      }).join("");
      document.getElementById("detail").innerHTML = `
        <div class="muted">Caps: cpu=${esc(String(caps.max_cpus_per_task ?? "-"))}, mem_gb=${esc(String(caps.max_mem_gb ?? "-"))}, wall_min=${esc(String(caps.max_wall_minutes ?? "-"))}</div>
        <table>
          <thead>
            <tr>
              <th>Path</th><th>Name</th><th>Version</th><th>Samples</th>
              <th>Rec CPU</th><th>Rec Mem GB</th><th>Rec Wall Min</th><th>Mean Wall Min</th>
            </tr>
          </thead>
          <tbody>${rows || `<tr><td colspan="8" class="muted">No plugins found.</td></tr>`}</tbody>
        </table>
      `;
      if(msgEl) msgEl.textContent = `Loaded ${Array.isArray(payload.plugins) ? payload.plugins.length : 0} plugins`;
    }
    async function loadPluginEnvOptions(){
      if(!isPluginsView) return;
      const sel = document.getElementById("plugins_env");
      if(!sel) return;
      const current = String(sel.value || "").trim();
      const res = await fetch(`/api/builder/environments`);
      if(!res.ok){
        sel.innerHTML = `<option value="">env (optional)</option>`;
        return;
      }
      const payload = await res.json();
      const envs = Array.isArray(payload.environments) ? payload.environments : [];
      sel.innerHTML = `<option value="">env (optional)</option>` + envs.map(e => `<option value="${esc(e)}">${esc(e)}</option>`).join("");
      if(current && envs.includes(current)){
        sel.value = current;
      }
    }
    function renderBuilderPluginStats(){
      const el = document.getElementById("builder_plugin_stats");
      if(!el) return;
      const rows = (builderPlugins || []).map((p) => {
        const path = String(p.path || "");
        const stat = builderPluginStats[path] || {};
        const rec = stat.recommendation || {};
        const samples = Number(rec.samples || 0);
        const cpu = rec.cpu_cores === null || rec.cpu_cores === undefined ? "" : Number(rec.cpu_cores).toFixed(2);
        const mem = rec.memory_gb === null || rec.memory_gb === undefined ? "" : Number(rec.memory_gb).toFixed(2);
        const wall = rec.wall_minutes === null || rec.wall_minutes === undefined ? "" : Number(rec.wall_minutes).toFixed(2);
        return `
          <tr>
            <td>${esc(path)}</td>
            <td>${esc(String(p.version || stat.version || ""))}</td>
            <td>${esc(String(samples || 0))}</td>
            <td>${esc(cpu)}</td>
            <td>${esc(mem)}</td>
            <td>${esc(wall)}</td>
          </tr>
        `;
      }).join("");
      if(!rows){
        el.innerHTML = `<span class="muted">No plugins loaded.</span>`;
        return;
      }
      el.innerHTML = `
        <table>
          <thead>
            <tr>
              <th>Plugin</th>
              <th>Version</th>
              <th>Samples</th>
              <th>Rec CPU</th>
              <th>Rec Mem GB</th>
              <th>Rec Wall Min</th>
            </tr>
          </thead>
          <tbody>${rows}</tbody>
        </table>
      `;
    }
    function addBuilderRequire(){
      builderModel.requires_pipelines = builderModel.requires_pipelines || [];
      builderModel.requires_pipelines.push("");
      renderBuilderModel();
      syncYamlPreview();
    }
    function addBuilderDir(){
      builderModel.dirs = builderModel.dirs || {};
      const dtype = String(document.getElementById("b_dir_type").value || "custom").trim().toLowerCase();
      const defaults = defaultBuilderDirs();
      if(dtype && dtype !== "custom"){
        if(!Object.prototype.hasOwnProperty.call(builderModel.dirs, dtype)){
          builderModel.dirs[dtype] = defaults[dtype] || "";
        } else {
          let i = 2;
          while(Object.prototype.hasOwnProperty.call(builderModel.dirs, `${dtype}_${i}`)) i++;
          builderModel.dirs[`${dtype}_${i}`] = defaults[dtype] || "";
        }
      } else {
        const customRaw = String(document.getElementById("b_dir_custom").value || "").trim();
        const custom = customRaw.replaceAll(" ", "_");
        if(!custom){
          document.getElementById("builder_msg").textContent = "Enter a custom directory name.";
          return;
        }
        if(!Object.prototype.hasOwnProperty.call(builderModel.dirs, custom)){
          builderModel.dirs[custom] = "";
        } else {
          let i = 2;
          while(Object.prototype.hasOwnProperty.call(builderModel.dirs, `${custom}_${i}`)) i++;
          builderModel.dirs[`${custom}_${i}`] = "";
        }
      }
      document.getElementById("builder_msg").textContent = "";
      renderBuilderModel();
      syncYamlPreview();
    }
    function addBuilderVar(){
      builderModel.vars = builderModel.vars || {};
      let i = 1;
      while(Object.prototype.hasOwnProperty.call(builderModel.vars, `var_${i}`)) i++;
      builderModel.vars[`var_${i}`] = "";
      renderBuilderModel();
      syncYamlPreview();
    }
    function addBuilderStep(){
      const firstPlugin = builderPlugins.length ? builderPlugins[0].path : "echo.py";
      builderModel.steps = builderModel.steps || [];
      builderModel.steps.push({
        type:"sequential",
        plugin:firstPlugin,
        params:{},
        resources:{},
        output_var:"",
        when:"",
        parallel_with:"",
        foreach:"",
        foreach_mode:"var",
        foreach_glob:"",
        foreach_kind:"dirs",
      });
      renderBuilderModel();
      syncYamlPreview();
    }
    function insertBuilderStepAt(index){
      const firstPlugin = builderPlugins.length ? builderPlugins[0].path : "echo.py";
      const steps = builderModel.steps || [];
      const idx = Math.max(0, Math.min(Number(index || 0), steps.length));
      steps.splice(idx, 0, {
        type:"sequential",
        plugin:firstPlugin,
        params:{},
        resources:{},
        output_var:"",
        when:"",
        parallel_with:"",
        foreach:"",
        foreach_mode:"var",
        foreach_glob:"",
        foreach_kind:"dirs",
      });
      builderModel.steps = steps;
      renderBuilderModel();
      syncYamlPreview();
    }
    function nextParallelGroupKey(preferred){
      const used = new Set();
      for(const st of (builderModel.steps || [])){
        const k = String(st.parallel_with || "").trim();
        if(k) used.add(k);
      }
      const pref = String(preferred || "").trim();
      if(pref && !used.has(pref)) return pref;
      let i = 1;
      while(used.has(`p${i}`)) i++;
      return `p${i}`;
    }
    function stepDisplayLabels(steps){
      const labels = [];
      let base = 1;
      let activeKey = "";
      let activeBase = 0;
      let activeCount = 0;
      for(let i=0; i<steps.length; i++){
        const st = steps[i] || {};
        const type = st.type || "sequential";
        const key = String(st.parallel_with || "").trim();
        if(type === "parallel" && key){
          if(activeKey === key){
            activeCount += 1;
          } else {
            activeKey = key;
            activeBase = base;
            activeCount = 1;
            base += 1;
          }
          labels.push(`Step ${activeBase}.${activeCount}`);
        } else {
          activeKey = "";
          activeBase = 0;
          activeCount = 0;
          labels.push(`Step ${base}`);
          base += 1;
        }
      }
      return labels;
    }
    function stepStatusMeta(idx){
      if(builderStepTesting[idx]){
        return { klass: "valid", text: "valid" };
      }
      const run = builderStepStatus[idx];
      if(run === "failed"){
        return { klass: "failed", text: "failed" };
      }
      if(run === "run_ok"){
        if(builderValidationState === "valid"){
          return { klass: "successful", text: "successful" };
        }
        return { klass: "valid", text: "valid" };
      }
      if(builderValidationState === "valid"){
        return { klass: "valid", text: "valid" };
      }
      return { klass: "not-run", text: "not run" };
    }
    function setBuilderParamIssues(issues){
      builderParamIssues = {};
      for(const issue of (Array.isArray(issues) ? issues : [])){
        const sidx = Number(issue?.step_index);
        const field = String(issue?.field || "").trim();
        if(!Number.isFinite(sidx) || !field) continue;
        builderParamIssues[`${sidx}|${field}`] = issue;
      }
    }
    function issueMetaForField(stepIndex, field){
      const issue = builderParamIssues[`${stepIndex}|${String(field || "")}`];
      if(!issue){
        return { labelClass:"", inputClass:"", badge:"" };
      }
      const tokens = Array.isArray(issue.tokens) ? issue.tokens.join(", ") : "";
      const title = `Unresolved: ${tokens || "unknown token"}`;
      return {
        labelClass: " issue",
        inputClass: "issue",
        badge: `<span class="param-issue-badge" title="${esc(title)}">!</span>`,
      };
    }
    function renderBuilderModel(){
      const reqEl = document.getElementById("b_requires");
      const varEl = document.getElementById("b_vars");
      const dirEl = document.getElementById("b_dirs");
      const stepsEl = document.getElementById("b_steps");
      reqEl.innerHTML = "";
      varEl.innerHTML = "";
      dirEl.innerHTML = "";
      stepsEl.innerHTML = "";

      (builderModel.requires_pipelines || []).forEach((val, idx) => {
        const row = document.createElement("div");
        row.className = "builder-item";
        row.innerHTML = `<div class="controls"><input data-kind="req" data-idx="${idx}" value="${esc(val)}" placeholder="pipelines/dependency.yml" /><button data-del-req="${idx}">Remove</button></div>`;
        reqEl.appendChild(row);
      });

      Object.entries(builderModel.vars || {}).forEach(([k,v]) => {
        const row = document.createElement("div");
        row.className = "builder-item";
        row.innerHTML = `<div class="controls"><input data-kind="var-key" data-key="${esc(k)}" value="${esc(k)}" placeholder="var key" /><input data-kind="var-val" data-key="${esc(k)}" value="${esc(v)}" placeholder="value" /><button data-del-var="${esc(k)}">Remove</button></div>`;
        varEl.appendChild(row);
      });

      Object.entries(builderModel.dirs || {}).forEach(([k,v]) => {
        const row = document.createElement("div");
        row.className = "builder-item";
        row.innerHTML = `<div class="controls"><input data-kind="dir-key" data-key="${esc(k)}" value="${esc(k)}" placeholder="dir key" /><input data-kind="dir-val" data-key="${esc(k)}" value="${esc(v)}" placeholder="path/value" /><button data-del-dir="${esc(k)}">Remove</button></div>`;
        dirEl.appendChild(row);
      });

      const steps = builderModel.steps || [];
      const stepLabels = stepDisplayLabels(steps);
      const renderInsertRow = (insertIdx) => {
        const row = document.createElement("div");
        row.className = "builder-insert-row";
        row.innerHTML = `<button data-insert-step="${insertIdx}">+ Insert Step</button>`;
        stepsEl.appendChild(row);
      };
      renderInsertRow(0);
      function defaultForSpec(pspec){
        if(pspec && Object.prototype.hasOwnProperty.call(pspec, "default")) return pspec.default;
        return "";
      }
      function toNumberLike(v, kind){
        if(v === null || v === undefined || String(v).trim() === "") return "";
        const n = Number(v);
        if(Number.isNaN(n)) return String(v);
        return kind === "int" ? Math.trunc(n) : n;
      }
      function isBoolLike(v){
        if(typeof v === "boolean") return v;
        const s = String(v ?? "").trim().toLowerCase();
        return s === "true" || s === "1" || s === "yes" || s === "on";
      }
      steps.forEach((st, idx) => {
        const meta = builderPluginMeta[st.plugin] || {params:{}};
        const pluginOptions = builderPlugins.map(p => `<option value="${esc(p.path)}" ${p.path===st.plugin?"selected":""}>${esc(p.path)}</option>`).join("");
        const type = st.type || "sequential";
        let paramsHtml = "";
        for(const [pk, pspec] of Object.entries(meta.params || {})){
          const ptype = String((pspec && (pspec.type || pspec["type"])) || "str").toLowerCase();
          const pchoices = Array.isArray(pspec?.choices) ? pspec.choices : (Array.isArray(pspec?.enum) ? pspec.enum : []);
          const pdefault = defaultForSpec(pspec);
          const pvalRaw = (st.params || {})[pk];
          const pval = pvalRaw !== undefined ? pvalRaw : pdefault;
          const hint = pspec?.description ? ` title="${esc(String(pspec.description))}"` : "";
          const issueMeta = issueMetaForField(idx, `args.${pk}`);
          if(pchoices.length){
            const options = [`<option value="">(empty)</option>`]
              .concat(pchoices.map(opt => {
                const oval = String(opt ?? "");
                const selected = String(pval ?? "") === oval ? "selected" : "";
                return `<option value="${esc(oval)}" ${selected}>${esc(oval)}</option>`;
              }))
              .join("");
            paramsHtml += `
              <div class="param-row">
                <div class="param-label${issueMeta.labelClass}" title="${esc(pk)}">${esc(pk)}${issueMeta.badge}</div>
                <div class="param-value"><select class="${issueMeta.inputClass}" data-kind="step-param-select" data-idx="${idx}" data-param="${esc(pk)}" data-ptype="${esc(ptype)}"${hint}>${options}</select></div>
              </div>
            `;
          } else if(ptype === "bool"){
            paramsHtml += `
              <div class="param-row">
                <div class="param-label${issueMeta.labelClass}" title="${esc(pk)}">${esc(pk)}${issueMeta.badge}</div>
                <div class="param-value"><label class="muted"><input type="checkbox" data-kind="step-param-bool" data-idx="${idx}" data-param="${esc(pk)}" ${isBoolLike(pval)?"checked":""}${hint} /> enabled</label></div>
              </div>
            `;
          } else if(ptype === "int" || ptype === "float"){
            const nval = toNumberLike(pval, ptype);
            paramsHtml += `
              <div class="param-row">
                <div class="param-label${issueMeta.labelClass}" title="${esc(pk)}">${esc(pk)}${issueMeta.badge}</div>
                <div class="param-value"><input class="${issueMeta.inputClass}" type="number" data-kind="step-param-number" data-idx="${idx}" data-param="${esc(pk)}" data-ptype="${esc(ptype)}" value="${esc(nval)}" placeholder="${esc(ptype)}"${ptype==="int" ? ' step="1"' : ' step="any"'}${hint} /></div>
              </div>
            `;
          } else {
            paramsHtml += `
              <div class="param-row">
                <div class="param-label${issueMeta.labelClass}" title="${esc(pk)}">${esc(pk)}${issueMeta.badge}</div>
                <div class="param-value"><input class="${issueMeta.inputClass}" data-kind="step-param" data-idx="${idx}" data-param="${esc(pk)}" data-ptype="${esc(ptype)}" value="${esc(pval ?? "")}" placeholder="${esc(ptype)}"${hint} /></div>
              </div>
            `;
          }
        }
        const card = document.createElement("div");
        card.className = "builder-item";
        const badge = stepStatusMeta(idx);
        const loading = !!builderStepTesting[idx];
        const rec = pluginRecommendation(st.plugin);
        const recCpu = rec.cpu_cores === null || rec.cpu_cores === undefined ? "" : Number(rec.cpu_cores).toFixed(2);
        const recMem = rec.memory_gb === null || rec.memory_gb === undefined ? "" : Number(rec.memory_gb).toFixed(2);
        const recWall = rec.wall_minutes === null || rec.wall_minutes === undefined ? "" : Number(rec.wall_minutes).toFixed(2);
        const hasRec = !!(recCpu || recMem || recWall);
        st.resources = st.resources || {};
        let typeSpecificHtml = "";
        if(type === "parallel"){
          typeSpecificHtml = `<div class="controls"><input data-kind="step-parallel" data-idx="${idx}" value="${esc(st.parallel_with || "")}" placeholder="parallel_with group key" /></div>`;
        } else if (type === "foreach"){
          const foreachMode = String(st.foreach_mode || (String(st.foreach_glob || "").trim() ? "glob" : "var")).toLowerCase() === "glob" ? "glob" : "var";
          const foreachKind = String(st.foreach_kind || "dirs").trim().toLowerCase() || "dirs";
          typeSpecificHtml = `
            <div class="controls">
              <select data-kind="step-foreach-mode" data-idx="${idx}">
                <option value="var" ${foreachMode==="var"?"selected":""}>foreach variable</option>
                <option value="glob" ${foreachMode==="glob"?"selected":""}>foreach filesystem glob</option>
              </select>
              ${foreachMode==="var"
                ? `<input data-kind="step-foreach" data-idx="${idx}" value="${esc(st.foreach || "")}" placeholder="foreach var name (e.g. datasets)" />`
                : `<input data-kind="step-foreach-glob" data-idx="${idx}" value="${esc(st.foreach_glob || "")}" placeholder="glob pattern (e.g. {fieldsdir}/**/*field_segments)" />`
              }
              ${foreachMode==="glob"
                ? `<select data-kind="step-foreach-kind" data-idx="${idx}">
                    <option value="dirs" ${foreachKind==="dirs"?"selected":""}>directories</option>
                    <option value="files" ${foreachKind==="files"?"selected":""}>files</option>
                    <option value="any" ${foreachKind==="any"?"selected":""}>any</option>
                  </select>`
                : ``
              }
            </div>
          `;
        }
        card.innerHTML = `
          <div class="step-head">
            <h5>${stepLabels[idx] || `Step ${idx+1}`}</h5>
            <span class="status-pill ${badge.klass}">${badge.text}</span>
          </div>
          <div class="controls">
            <select data-kind="step-type" data-idx="${idx}">
              <option value="sequential" ${type==="sequential"?"selected":""}>sequential</option>
              <option value="parallel" ${type==="parallel"?"selected":""}>parallel</option>
              <option value="foreach" ${type==="foreach"?"selected":""}>foreach</option>
            </select>
            <select data-kind="step-plugin" data-idx="${idx}">${pluginOptions}</select>
            <button class="spin-btn ${loading ? "loading" : ""}" data-test-step="${idx}" ${loading ? "disabled" : ""}>
              <span>Test Step</span><span class="spin"></span>
            </button>
            <button data-apply-step-rec="${idx}" ${hasRec ? "" : "disabled"}>Apply Recommended</button>
            <button data-del-step="${idx}">Remove Step</button>
          </div>
          <div class="param-panel">
            <div class="param-panel-title">Input Parameters</div>
            <div class="param-grid">${paramsHtml || '<span class="muted">No plugin params</span>'}</div>
          </div>
          <div class="param-panel">
            <div class="param-panel-title">Resources</div>
            <div class="param-grid">
              <div class="param-row"><div class="param-label">cpu_cores</div><div class="param-value"><input data-kind="step-res-cpu" data-idx="${idx}" value="${esc(st.resources.cpu_cores ?? "")}" placeholder="e.g. 4" /></div></div>
              <div class="param-row"><div class="param-label">memory_gb</div><div class="param-value"><input data-kind="step-res-mem" data-idx="${idx}" value="${esc(st.resources.memory_gb ?? "")}" placeholder="e.g. 16" /></div></div>
              <div class="param-row"><div class="param-label">wall_minutes</div><div class="param-value"><input data-kind="step-res-wall" data-idx="${idx}" value="${esc(st.resources.wall_minutes ?? "")}" placeholder="e.g. 60" /></div></div>
            </div>
            <div class="muted">Recommended: cpu=${esc(recCpu || "-")}, mem_gb=${esc(recMem || "-")}, wall_min=${esc(recWall || "-")} (samples=${esc(String(rec.samples || 0))})</div>
          </div>
          <div class="controls">
            <input data-kind="step-output" data-idx="${idx}" value="${esc(st.output_var || "")}" placeholder="output_var (optional)" />
            <input data-kind="step-when" data-idx="${idx}" value="${esc(st.when || "")}" placeholder="when (optional)" />
          </div>
          ${typeSpecificHtml}
          <div class="step-output ${builderStepOutputCollapsed[idx] ? "collapsed" : ""}">
            <div class="step-output-head">
              <span class="title">Step Output</span>
              <button type="button" data-toggle-step-output="${idx}">${builderStepOutputCollapsed[idx] ? "Expand" : "Collapse"}</button>
            </div>
            <pre>${esc(builderStepOutput[idx] || "No step output yet.")}</pre>
          </div>
        `;
        stepsEl.appendChild(card);
        renderInsertRow(idx + 1);
      });
    }
    function handleBuilderInput(ev){
      const t = ev.target;
      if (!(t instanceof HTMLElement)) return;
      const kind = t.getAttribute("data-kind");
      if (!kind) return;
      const eventType = ev.type || "input";
      let changed = false;
      if (kind === "req"){
        const idx = Number(t.getAttribute("data-idx") || "-1");
        if(idx >= 0){
          builderModel.requires_pipelines[idx] = t.value;
          changed = true;
        }
      } else if (kind === "var-key"){
        if (eventType === "input") return;
        const oldKey = t.getAttribute("data-key") || "";
        const newKey = String(t.value || "").trim();
        if(oldKey && newKey && oldKey !== newKey){
          const val = builderModel.vars[oldKey];
          delete builderModel.vars[oldKey];
          builderModel.vars[newKey] = val;
          renderBuilderModel();
          changed = true;
        }
      } else if (kind === "var-val"){
        if (eventType === "input") return;
        const key = t.getAttribute("data-key") || "";
        if(key){
          builderModel.vars[key] = t.value;
          changed = true;
        }
      } else if (kind === "dir-key"){
        if (eventType === "input") return;
        const oldKey = t.getAttribute("data-key") || "";
        const newKey = String(t.value || "").trim();
        if(oldKey && newKey && oldKey !== newKey){
          const val = builderModel.dirs[oldKey];
          delete builderModel.dirs[oldKey];
          builderModel.dirs[newKey] = val;
          renderBuilderModel();
          changed = true;
        }
      } else if (kind === "dir-val"){
        const key = t.getAttribute("data-key") || "";
        if(key){
          builderModel.dirs[key] = t.value;
          changed = true;
        }
      } else if (kind.startsWith("step-")){
        const idx = Number(t.getAttribute("data-idx") || "-1");
        if(idx < 0 || !builderModel.steps[idx]) return;
        const st = builderModel.steps[idx];
        if(kind === "step-type"){
          st.type = t.value;
          if(st.type === "parallel" && !(String(st.parallel_with || "").trim())){
            st.parallel_with = nextParallelGroupKey(`step${idx+2}`);
          }
          if(st.type === "foreach"){
            const hasGlob = String(st.foreach_glob || "").trim().length > 0;
            st.foreach_mode = hasGlob ? "glob" : (String(st.foreach_mode || "var").trim().toLowerCase() === "glob" ? "glob" : "var");
            if(!String(st.foreach_kind || "").trim()){
              st.foreach_kind = "dirs";
            }
          }
          renderBuilderModel();
          changed = true;
        }
        if(kind === "step-plugin"){ st.plugin = t.value; st.params = {}; renderBuilderModel(); changed = true; }
        if(kind === "step-output"){ st.output_var = t.value; changed = true; }
        if(kind === "step-when"){ st.when = t.value; changed = true; }
        if(kind === "step-parallel"){ st.parallel_with = t.value; changed = true; }
        if(kind === "step-foreach"){ st.foreach = t.value; changed = true; }
        if(kind === "step-foreach-mode"){
          st.foreach_mode = String(t.value || "var").trim().toLowerCase() === "glob" ? "glob" : "var";
          renderBuilderModel();
          changed = true;
        }
        if(kind === "step-foreach-glob"){ st.foreach_glob = t.value; changed = true; }
        if(kind === "step-foreach-kind"){ st.foreach_kind = t.value; changed = true; }
        if(kind === "step-res-cpu" || kind === "step-res-mem" || kind === "step-res-wall"){
          st.resources = st.resources || {};
          const key = kind === "step-res-cpu" ? "cpu_cores" : (kind === "step-res-mem" ? "memory_gb" : "wall_minutes");
          const raw = String(t.value ?? "").trim();
          if(!raw.length){
            delete st.resources[key];
          } else {
            const n = Number(raw);
            st.resources[key] = Number.isNaN(n) ? raw : n;
          }
          changed = true;
        }
        if(kind === "step-param"){
          st.params = st.params || {};
          st.params[t.getAttribute("data-param")] = t.value;
          changed = true;
        }
        if(kind === "step-param-select"){
          st.params = st.params || {};
          const key = t.getAttribute("data-param");
          const raw = String(t.value ?? "");
          if(!raw.trim().length){
            delete st.params[key];
          } else {
            st.params[key] = raw;
          }
          changed = true;
        }
        if(kind === "step-param-number"){
          st.params = st.params || {};
          const key = t.getAttribute("data-param");
          const ptype = String(t.getAttribute("data-ptype") || "float").toLowerCase();
          const raw = String(t.value ?? "").trim();
          if(!raw.length){
            delete st.params[key];
          } else {
            const n = Number(raw);
            st.params[key] = Number.isNaN(n) ? raw : (ptype === "int" ? Math.trunc(n) : n);
          }
          changed = true;
        }
        if(kind === "step-param-bool"){
          st.params = st.params || {};
          st.params[t.getAttribute("data-param")] = !!t.checked;
          changed = true;
        }
      }
      if (changed) syncYamlPreview();
    }
    async function handleBuilderClicks(ev){
      const t = ev.target;
      if (!(t instanceof HTMLElement)) return;
      const testStepBtn = t.closest("[data-test-step]");
      if(testStepBtn){
        const idx = Number(testStepBtn.getAttribute("data-test-step") || "-1");
        if(idx >= 0){
          await testBuilderStepAt(idx);
        }
        return;
      }
      const applyRec = t.getAttribute("data-apply-step-rec");
      if (applyRec !== null){
        const idx = Number(applyRec);
        if(idx >= 0 && builderModel.steps && builderModel.steps[idx]){
          const st = builderModel.steps[idx];
          const rec = pluginRecommendation(st.plugin);
          st.resources = st.resources || {};
          if(rec.cpu_cores !== null && rec.cpu_cores !== undefined){
            st.resources.cpu_cores = Math.max(1, Math.ceil(Number(rec.cpu_cores)));
          }
          if(rec.memory_gb !== null && rec.memory_gb !== undefined){
            st.resources.memory_gb = Number(Number(rec.memory_gb).toFixed(2));
          }
          if(rec.wall_minutes !== null && rec.wall_minutes !== undefined){
            st.resources.wall_minutes = Math.max(1, Math.ceil(Number(rec.wall_minutes)));
          }
          renderBuilderModel();
          syncYamlPreview();
        }
        return;
      }
      const ins = t.getAttribute("data-insert-step");
      if (ins !== null){ insertBuilderStepAt(Number(ins)); return; }
      const tog = t.getAttribute("data-toggle-step-output");
      if (tog !== null){
        const idx = Number(tog);
        builderStepOutputCollapsed[idx] = !builderStepOutputCollapsed[idx];
        renderBuilderModel();
        return;
      }
      const delReq = t.getAttribute("data-del-req");
      if (delReq !== null){ builderModel.requires_pipelines.splice(Number(delReq),1); renderBuilderModel(); syncYamlPreview(); return; }
      const delVar = t.getAttribute("data-del-var");
      if (delVar !== null){ delete builderModel.vars[delVar]; renderBuilderModel(); syncYamlPreview(); return; }
      const delDir = t.getAttribute("data-del-dir");
      if (delDir !== null){ delete builderModel.dirs[delDir]; renderBuilderModel(); syncYamlPreview(); return; }
      const delStep = t.getAttribute("data-del-step");
      if (delStep !== null){
        const didx = Number(delStep);
        builderModel.steps.splice(didx,1);
        delete builderStepOutput[didx];
        delete builderStepOutputCollapsed[didx];
        renderBuilderModel();
        syncYamlPreview();
        return;
      }
    }
    async function suggestNewPipelineName(){
      const res = await fetch(`/api/builder/files`);
      if(!res.ok) return "new_pipeline_1.yml";
      const payload = await res.json();
      const files = new Set((payload.files || []).map(x => String(x).toLowerCase()));
      let i = 1;
      while(files.has(`new_pipeline_${i}.yml`)) i++;
      return `new_pipeline_${i}.yml`;
    }
    function buildBuilderJsTreeData(files){
      const nodes = [];
      const seenDirs = new Set();
      const all = Array.isArray(files) ? files.map(x => String(x || "").replaceAll("\\\\", "/").trim()).filter(Boolean) : [];
      for(const rel of all){
        const parts = rel.split("/").filter(Boolean);
        if(!parts.length) continue;
        let parentId = "#";
        for(let i=0; i<parts.length - 1; i++){
          const seg = parts[i];
          const dpath = parts.slice(0, i + 1).join("/");
          const did = `d:${dpath}`;
          if(!seenDirs.has(did)){
            seenDirs.add(did);
            nodes.push({ id: did, parent: parentId, text: seg, type: "dir", icon: "jstree-folder" });
          }
          parentId = did;
        }
        const fname = parts[parts.length - 1];
        nodes.push({ id: `f:${rel}`, parent: parentId, text: fname, type: "file", icon: "jstree-file", relpath: rel });
      }
      return nodes;
    }
    function applyBuilderTreeSelectionFromPipeline(pipeline){
      const p = normalizeBuilderPipelineName(String(pipeline || "").trim());
      if(!p){
        builderTreeFileSelection = "";
        return;
      }
      builderTreeFileSelection = p;
    }
    function renderBuilderJsTree(files){
      const holder = document.getElementById("b_pipeline_tree");
      if(!holder) return;
      const $ = window.jQuery;
      if(!$ || !$.fn || !$.fn.jstree){
        holder.innerHTML = `<span class="muted">jsTree not available in this browser context.</span>`;
        return;
      }
      const data = buildBuilderJsTreeData(files);
      holder.innerHTML = "";
      const $holder = $(holder);
      try { $holder.jstree("destroy"); } catch {}
      $holder.off(".jstree");
      $holder.jstree({
        core: { data, multiple: false },
        plugins: ["wholerow", "sort", "search"],
        search: { show_only_matches: true, case_insensitive: true },
        sort: function(a, b){
          const na = this.get_node(a);
          const nb = this.get_node(b);
          const ta = na?.original?.type || "";
          const tb = nb?.original?.type || "";
          if(ta !== tb) return ta === "dir" ? -1 : 1;
          return String(na?.text || "").localeCompare(String(nb?.text || ""));
        },
      });
      $holder.on("select_node.jstree", async function(_ev, payload){
        const node = payload?.node;
        if(!node) return;
        const inst = $holder.jstree(true);
        const ntype = String(node.original?.type || "");
        if(ntype === "dir"){
          if(inst){
            if(inst.is_open(node)) inst.close_node(node);
            else inst.open_node(node);
          }
          return;
        }
        if(ntype !== "file") return;
        const rel = String(node.original.relpath || "").trim();
        if(!rel) return;
        builderTreeFileSelection = rel;
        document.getElementById("b_pipeline_path").value = normalizeBuilderPipelineName(rel);
        hideBuilderTreeDropdown();
        builderLoaded = false;
        await loadBuilderSource();
      });
      // Avoid auto-select on ready: selecting during rapid destroy/recreate
      // can race and trigger jsTree internals on a torn-down instance.
    }
    async function refreshBuilderTreeFiles(){
      const msg = document.getElementById("builder_msg");
      const res = await fetch(`/api/builder/files`);
      if(!res.ok){
        msg.textContent = await readMessage(res);
        return;
      }
      const payload = await res.json();
      const files = Array.isArray(payload.files) ? payload.files : [];
      builderTreeFiles = files;
      applyBuilderTreeSelectionFromPipeline(document.getElementById("b_pipeline_path").value.trim());
      renderBuilderJsTree(files);
    }
    function showBuilderTreeDropdown(){
      const combo = document.getElementById("b_pipeline_combo");
      if(!combo) return;
      combo.classList.add("open");
    }
    function hideBuilderTreeDropdown(){
      const combo = document.getElementById("b_pipeline_combo");
      if(!combo) return;
      combo.classList.remove("open");
    }
    function filterBuilderTreeByInput(){
      const input = document.getElementById("b_pipeline_path");
      const holder = document.getElementById("b_pipeline_tree");
      const $ = window.jQuery;
      if(!input || !holder || !$ || !$.fn || !$.fn.jstree) return;
      const inst = $(holder).jstree(true);
      if(!inst) return;
      const term = String(input.value || "").trim();
      try {
        inst.search(term);
      } catch {}
    }
    async function initBuilderTreeComboBehavior(){
      const input = document.getElementById("b_pipeline_path");
      const combo = document.getElementById("b_pipeline_combo");
      if(!input || !combo) return;
      input.addEventListener("focus", async () => {
        if(!builderTreeFiles.length){
          await refreshBuilderTreeFiles();
        }
        showBuilderTreeDropdown();
        filterBuilderTreeByInput();
      });
      input.addEventListener("input", async () => {
        if(!builderTreeFiles.length){
          await refreshBuilderTreeFiles();
        }
        showBuilderTreeDropdown();
        filterBuilderTreeByInput();
      });
      document.addEventListener("mousedown", (ev) => {
        const t = ev.target;
        if(!(t instanceof Node)) return;
        if(combo.contains(t)) return;
        hideBuilderTreeDropdown();
      });
    }
    function openBuilderFilePicker(){
      const picker = document.getElementById("b_file_picker");
      picker.value = "";
      picker.click();
    }
    async function loadBuilderSourceFromFilePicker(){
      const picker = document.getElementById("b_file_picker");
      const file = (picker.files || [])[0];
      if(!file) return;
      let chosen = String(file.webkitRelativePath || file.name || "").replaceAll("\\\\","/");
      const marker = "/pipelines/";
      const markerIdx = chosen.toLowerCase().lastIndexOf(marker);
      if(markerIdx >= 0){
        chosen = chosen.slice(markerIdx + marker.length);
      }
      const v = normalizeBuilderPipelineName(chosen || document.getElementById("b_pipeline_path").value);
      if(!v){
        document.getElementById("builder_msg").textContent = "No pipeline selected.";
        return;
      }
      document.getElementById("b_pipeline_path").value = v;
      applyBuilderTreeSelectionFromPipeline(v);
      renderBuilderJsTree(builderTreeFiles);
      builderLoaded = false;
      await loadBuilderSource();
    }
    async function loadBuilderSource(){
      if(!isBuilderView || builderLoaded) return;
      let pipeline = normalizeBuilderPipelineName(document.getElementById("b_pipeline_path").value.trim());
      if(!pipeline){
        pipeline = await suggestNewPipelineName();
        document.getElementById("b_pipeline_path").value = pipeline;
        builderModel = ensureBuilderDefaultDirs({ vars: {}, dirs: {}, requires_pipelines: [], steps: [] });
        builderRunSeed = null;
        await loadBuilderPlugins();
        renderBuilderModel();
        syncYamlPreview();
        builderLoaded = true;
        return;
      }
      document.getElementById("b_pipeline_path").value = pipeline;
      applyBuilderTreeSelectionFromPipeline(pipeline);
      renderBuilderJsTree(builderTreeFiles);
      const res = await fetch(`/api/builder/source?pipeline=${encodeURIComponent(pipeline)}`);
      if(!res.ok){
        document.getElementById("builder_msg").textContent = await readMessage(res);
        builderModel = ensureBuilderDefaultDirs({ vars: {}, dirs: {}, requires_pipelines: [], steps: [] });
        builderRunSeed = null;
      } else {
        const payload = await res.json();
        builderModel = ensureBuilderDefaultDirs(payload.model || { vars: {}, dirs: {}, requires_pipelines: [], steps: [] });
        builderRunSeed = null;
      }
      await loadBuilderPlugins();
      renderBuilderModel();
      syncYamlPreview();
      builderLoaded = true;
    }
    async function saveBuilderDraft(){
      const msg = document.getElementById("builder_msg");
      const out = document.getElementById("builder_output");
      const payload = builderPayload();
      if (!payload.pipeline){
        msg.textContent = "pipeline path is required to save.";
        return;
      }
      payload.pipeline = normalizeBuilderPipelineName(payload.pipeline);
      document.getElementById("b_pipeline_path").value = payload.pipeline;
      msg.textContent = "Saving draft...";
      const encoded = encodeURIComponent(payload.pipeline);
      const update = await fetch(`/api/pipelines/${encoded}`, {
        method:"PUT",
        headers: {"Content-Type":"application/json"},
        body: JSON.stringify({ yaml_text: payload.yaml_text }),
      });
      if(update.status === 404){
        const create = await fetch(`/api/pipelines`, {
          method:"POST",
          headers: {"Content-Type":"application/json"},
          body: JSON.stringify({ pipeline: payload.pipeline, yaml_text: payload.yaml_text }),
        });
        if(!create.ok){
          msg.textContent = await readMessage(create);
          return;
        }
        const data = await create.json();
        msg.textContent = `Saved ${data.pipeline}`;
        out.textContent = JSON.stringify(data, null, 2);
        return;
      }
      if(!update.ok){
        msg.textContent = await readMessage(update);
        return;
      }
      const data = await update.json();
      msg.textContent = `Updated ${data.pipeline}`;
      out.textContent = JSON.stringify(data, null, 2);
    }
    async function runBuilderPipeline(){
      const msg = document.getElementById("builder_msg");
      const out = document.getElementById("builder_output");
      const payload = builderPayload();
      if (!payload.pipeline){
        msg.textContent = "pipeline path is required to run.";
        return;
      }
      const runMode = (document.getElementById("b_run_mode").value || "draft").trim().toLowerCase();
      payload.pipeline = normalizeBuilderPipelineName(payload.pipeline);
      document.getElementById("b_pipeline_path").value = payload.pipeline;
      const pipelineRunId = `pipelines/${payload.pipeline}`;

      builderPipelineRunning = true;
      renderBuilderPipelineStatus();
      msg.textContent = "Saving draft before run...";
      const encoded = encodeURIComponent(payload.pipeline);
      const update = await fetch(`/api/pipelines/${encoded}`, {
        method:"PUT",
        headers: {"Content-Type":"application/json"},
        body: JSON.stringify({ yaml_text: payload.yaml_text }),
      });
      if(update.status === 404){
        const create = await fetch(`/api/pipelines`, {
          method:"POST",
          headers: {"Content-Type":"application/json"},
          body: JSON.stringify({ pipeline: payload.pipeline, yaml_text: payload.yaml_text }),
        });
        if(!create.ok){
          builderPipelineRunState = "failed";
          builderPipelineRunning = false;
          renderBuilderPipelineStatus();
          msg.textContent = await readMessage(create);
          return;
        }
      } else if(!update.ok){
        builderPipelineRunState = "failed";
        builderPipelineRunning = false;
        renderBuilderPipelineStatus();
        msg.textContent = await readMessage(update);
        return;
      }

      msg.textContent = "Validating draft...";
      const validateRes = await fetch(`/api/builder/validate`, {
        method:"POST",
        headers: {"Content-Type":"application/json"},
        body: JSON.stringify(payload),
      });
      if(!validateRes.ok){
        builderValidationState = "unknown";
        builderPipelineRunState = "failed";
        builderPipelineRunning = false;
        renderBuilderPipelineStatus();
        msg.textContent = await readMessage(validateRes);
        return;
      }
      builderValidationState = "valid";
      renderBuilderPipelineStatus();

      msg.textContent = "Submitting run...";
      const runBody = {};
      if(payload.plugins_dir) runBody.plugins_dir = payload.plugins_dir;
      if(payload.workdir) runBody.workdir = payload.workdir;
      if(payload.max_retries !== undefined) runBody.max_retries = payload.max_retries;
      if(payload.retry_delay_seconds !== undefined) runBody.retry_delay_seconds = payload.retry_delay_seconds;
      runBody.dry_run = !!payload.dry_run;
      if(runMode === "repro"){
        runBody.execution_source = "auto";
        runBody.allow_workspace_source = false;
      } else {
        // Draft mode uses workspace source so iterative edits do not require
        // snapshot/bundle configuration.
        runBody.execution_source = "workspace";
        runBody.allow_workspace_source = true;
      }
      const runRes = await fetch(`/api/pipelines/${encodeURIComponent(pipelineRunId)}/run`, {
        method:"POST",
        headers: {"Content-Type":"application/json"},
        body: JSON.stringify(runBody),
      });
      if(!runRes.ok){
        builderPipelineRunState = "failed";
        builderPipelineRunning = false;
        renderBuilderPipelineStatus();
        msg.textContent = await readMessage(runRes);
        return;
      }
      const data = await runRes.json();
      builderPipelineRunState = data.success === false ? "failed" : "run_ok";
      builderPipelineRunning = false;
      renderBuilderPipelineStatus();
      msg.textContent = `Run ${data.run_id} (${data.state || "submitted"}) [${runMode}]`;
      out.textContent = JSON.stringify(data, null, 2);
    }
    async function generateBuilderDraft(){
      const msg = document.getElementById("builder_msg");
      const out = document.getElementById("builder_output");
      const payload = builderPayload();
      if (!payload.intent){
        msg.textContent = "intent is required to generate.";
        return;
      }
      msg.textContent = "Generating draft...";
      const res = await fetch(`/api/builder/generate`, {
        method:"POST",
        headers: {"Content-Type":"application/json"},
        body: JSON.stringify(payload),
      });
      if(!res.ok){
        msg.textContent = await readMessage(res);
        return;
      }
      const data = await res.json();
      builderModel = data.model || builderModel;
      builderRunSeed = null;
      renderBuilderModel();
      syncYamlPreview();
      builderValidationState = data.valid ? "valid" : "unknown";
      renderBuilderModel();
      renderBuilderPipelineStatus();
      msg.textContent = data.valid ? `Generated valid draft (${data.step_count} steps)` : "Generated draft has validation issues";
      out.textContent = JSON.stringify(data, null, 2);
    }
    async function validateBuilderDraft(opts){
      const auto = !!(opts && opts.auto);
      if(builderValidateInFlight){
        return;
      }
      builderValidateInFlight = true;
      const msg = document.getElementById("builder_msg");
      const out = document.getElementById("builder_output");
      if(!auto){
        msg.textContent = "Validating draft...";
      }
      try {
        const res = await fetch(`/api/builder/validate`, {
          method:"POST",
          headers: {"Content-Type":"application/json"},
          body: JSON.stringify(builderPayload()),
        });
        if(!res.ok){
          let raw = {};
          try { raw = await res.json(); } catch {}
          const detail = raw && Object.prototype.hasOwnProperty.call(raw, "detail") ? raw.detail : null;
          const unresolved = (detail && typeof detail === "object" && Array.isArray(detail.unresolved_inputs))
            ? detail.unresolved_inputs : [];
          setBuilderParamIssues(unresolved);
          builderValidationState = "unknown";
          builderPipelineRunState = "failed";
          let message = "";
          if(detail && typeof detail === "object"){
            message = String(detail.message || "Validation failed.");
          } else if(raw && Object.prototype.hasOwnProperty.call(raw, "detail")){
            message = String(raw.detail || "");
          }
          msg.textContent = message || "Validation failed.";
          if(unresolved.length){
            msg.textContent = `${msg.textContent} (${unresolved.length} unresolved parameter issue${unresolved.length === 1 ? "" : "s"})`;
          }
          renderBuilderModel();
          renderBuilderPipelineStatus();
          return;
        }
        const payload = await res.json();
        setBuilderParamIssues(payload.unresolved_inputs || []);
        builderValidationState = "valid";
        if(builderPipelineRunState !== "run_ok"){
          builderPipelineRunState = "not-run";
        }
        if(!auto){
          msg.textContent = `Valid draft: ${payload.step_count} steps`;
        }
        out.textContent = JSON.stringify(payload, null, 2);
        renderBuilderModel();
        renderBuilderPipelineStatus();
      } finally {
        builderValidateInFlight = false;
      }
    }
    async function testBuilderStepAt(idx){
      const msg = document.getElementById("builder_msg");
      const out = document.getElementById("builder_output");
      builderStepTesting[idx] = true;
      renderBuilderModel();
      msg.textContent = `Validating draft before step ${idx + 1} test...`;
      const prePayload = builderPayload();
      prePayload.require_dir_contract = false;
      const validateRes = await fetch(`/api/builder/validate`, {
        method:"POST",
        headers: {"Content-Type":"application/json"},
        body: JSON.stringify(prePayload),
      });
      if(!validateRes.ok){
        let raw = {};
        try { raw = await validateRes.json(); } catch {}
        const detail = raw && Object.prototype.hasOwnProperty.call(raw, "detail") ? raw.detail : null;
        const unresolved = (detail && typeof detail === "object" && Array.isArray(detail.unresolved_inputs))
          ? detail.unresolved_inputs : [];
        setBuilderParamIssues(unresolved);
        builderValidationState = "unknown";
        delete builderStepTesting[idx];
        renderBuilderModel();
        if(detail && typeof detail === "object"){
          msg.textContent = String(detail.message || "Validation failed.");
        } else if(raw && Object.prototype.hasOwnProperty.call(raw, "detail")){
          msg.textContent = String(raw.detail || "Validation failed.");
        } else {
          msg.textContent = "Validation failed.";
        }
        return;
      }
      const validatePayload = await validateRes.json();
      setBuilderParamIssues(validatePayload.unresolved_inputs || []);
      builderValidationState = "valid";
      msg.textContent = `Testing step ${idx + 1}...`;
      const payload = builderPayload();
      const seed = ensureBuilderRunSeed();
      payload.run_id = seed.run_id;
      payload.run_started_at = seed.run_started_at;
      payload.step_index = idx;
      const res = await fetch(`/api/builder/test-step`, {
        method:"POST",
        headers: {"Content-Type":"application/json"},
        body: JSON.stringify(payload),
      });
      if(!res.ok){
        builderStepStatus[idx] = "failed";
        delete builderStepTesting[idx];
        renderBuilderModel();
        msg.textContent = await readMessage(res);
        return;
      }
      const data = await res.json();
      builderStepStatus[idx] = data.success ? "run_ok" : "failed";
      builderStepOutput[idx] = JSON.stringify(data, null, 2);
      builderStepOutputCollapsed[idx] = false;
      delete builderStepTesting[idx];
      renderBuilderModel();
      msg.textContent = `Step ${data.step_name}: ${data.success ? "successful" : "failed"}`;
      out.textContent = JSON.stringify(data, null, 2);
    }
    async function setBuilderResolvedTooltip(el){
      if(!isBuilderView || !el) return;
      const raw = String(el.value || "");
      if(!raw.trim().length || raw.indexOf("{") < 0){
        el.title = raw;
        return;
      }
      const payload = builderPayload();
      payload.value = raw;
      const res = await fetch(`/api/builder/resolve-text`, {
        method: "POST",
        headers: {"Content-Type":"application/json"},
        body: JSON.stringify(payload),
      });
      if(!res.ok){
        const msg = await readMessage(res);
        el.title = `Unresolved: ${msg}`;
        return;
      }
      const out = await res.json();
      el.title = String(out.resolved || raw);
    }
    async function refreshBuilderNamespace(){
      if(!isBuilderView) return;
      const el = document.getElementById("builder_namespace");
      const treeEl = document.getElementById("builder_namespace_tree");
      if(!el) return;
      const payload = builderPayload();
      const res = await fetch(`/api/builder/namespace`, {
        method: "POST",
        headers: {"Content-Type":"application/json"},
        body: JSON.stringify(payload),
      });
      if(!res.ok){
        el.textContent = `Unavailable: ${await readMessage(res)}`;
        builderNamespaceDigest = "";
        if(treeEl){
          const $ = window.jQuery;
          if($ && $.fn && $.fn.jstree){
            try {
              const $tree = $(treeEl);
              $tree.off(".jstree");
              $tree.jstree("destroy");
            } catch {}
          }
          treeEl.innerHTML = "";
        }
        return;
      }
      const data = await res.json();
      const ns = data.namespace || {};
      const pretty = JSON.stringify(ns, null, 2);
      el.textContent = pretty;
      if(pretty !== builderNamespaceDigest){
        builderNamespaceDigest = pretty;
        renderBuilderNamespaceTree(ns);
      }
    }
    function _nsValueText(value){
      if(value === null || value === undefined) return "null";
      if(typeof value === "object"){
        if(Array.isArray(value)) return `[${value.length}]`;
        return "{...}";
      }
      const s = String(value);
      return s.length > 220 ? s.slice(0, 217) + "..." : s;
    }
    function _nsNodeText(label, value){
      const k = esc(String(label ?? ""));
      const v = esc(_nsValueText(value));
      return `<span class="ns-row"><span class="ns-key">${k}</span><span class="ns-value">${v}</span></span>`;
    }
    function isInsertableBuilderTextControl(el){
      if(!el) return false;
      if(el instanceof HTMLTextAreaElement){
        return !el.disabled && !el.readOnly;
      }
      if(el instanceof HTMLInputElement){
        if(el.disabled || el.readOnly) return false;
        const t = String(el.type || "text").toLowerCase();
        return ["text", "search", "url", "email", "tel", "password"].includes(t);
      }
      return false;
    }
    function insertAtCursor(el, text){
      if(!isInsertableBuilderTextControl(el)) return false;
      const insertText = String(text || "");
      if(!insertText) return false;
      const hasSel = typeof el.selectionStart === "number" && typeof el.selectionEnd === "number";
      if(hasSel){
        const start = Number(el.selectionStart || 0);
        const end = Number(el.selectionEnd || 0);
        const src = String(el.value || "");
        el.value = src.slice(0, start) + insertText + src.slice(end);
        const pos = start + insertText.length;
        try { el.setSelectionRange(pos, pos); } catch {}
      } else {
        el.value = String(el.value || "") + insertText;
      }
      el.dispatchEvent(new Event("input", { bubbles: true }));
      el.dispatchEvent(new Event("change", { bubbles: true }));
      try { el.focus(); } catch {}
      return true;
    }
    function isBuilderAutoValidateControl(el){
      if(!el) return false;
      if(el instanceof HTMLTextAreaElement){
        return !el.readOnly && !el.disabled;
      }
      if(el instanceof HTMLSelectElement){
        return !el.disabled;
      }
      if(el instanceof HTMLInputElement){
        if(el.readOnly || el.disabled) return false;
        const t = String(el.type || "text").toLowerCase();
        if(["button", "submit", "reset", "checkbox", "radio", "file"].includes(t)) return false;
        return true;
      }
      return false;
    }
    function scheduleBuilderAutoValidate(){
      if(!isBuilderView) return;
      if(builderAutoValidateTimer){
        clearTimeout(builderAutoValidateTimer);
      }
      builderAutoValidateTimer = setTimeout(() => {
        validateBuilderDraft({ auto: true });
      }, 220);
    }
    function buildNamespaceTreeData(namespace){
      const nodes = [];
      let seq = 0;
      const mk = (pfx) => `${pfx}_${seq++}`;
      function addObject(parentId, key, obj, pathParts){
        const id = mk("grp");
        nodes.push({ id, parent: parentId, text: String(key), icon: "ns-folder-closed", type: "group" });
        const entries = Object.entries(obj || {}).sort((a,b) => String(a[0]).localeCompare(String(b[0])));
        for(const [k, v] of entries){
          const nextPath = (pathParts || []).concat([String(k)]);
          if(v && typeof v === "object" && !Array.isArray(v)){
            addObject(id, k, v, nextPath);
          } else {
            const leafId = mk("leaf");
            nodes.push({
              id: leafId,
              parent: id,
              text: _nsNodeText(k, v),
              icon: "ns-var-leaf",
              type: "leaf",
              varPath: nextPath.join("."),
            });
          }
        }
      }
      const rootGroups = ["sys", "global", "env", "vars", "dirs"];
      const ignore = new Set(rootGroups);
      const flatEntries = Object.entries(namespace || {})
        .filter(([k]) => !ignore.has(String(k)))
        .sort((a,b) => String(a[0]).localeCompare(String(b[0])));
      const dotTree = {};
      for(const [k, v] of flatEntries){
        const key = String(k || "");
        if(!key.includes(".")){
          const leafId = mk("leaf");
          nodes.push({
            id: leafId,
            parent: "#",
            text: _nsNodeText(key, v),
            icon: "ns-var-leaf",
            type: "leaf",
            varPath: key,
          });
          continue;
        }
        const parts = key.split(".");
        let cur = dotTree;
        for(let i=0; i<parts.length - 1; i++){
          const seg = parts[i];
          cur[seg] = cur[seg] || {};
          cur = cur[seg];
        }
        cur[parts[parts.length - 1]] = v;
      }
      function addDotObject(parentId, key, obj, pathParts){
        const id = mk("grp");
        nodes.push({ id, parent: parentId, text: String(key), icon: "ns-folder-closed", type: "group" });
        const entries = Object.entries(obj || {}).sort((a,b) => String(a[0]).localeCompare(String(b[0])));
        for(const [k, v] of entries){
          const nextPath = (pathParts || []).concat([String(k)]);
          if(v && typeof v === "object" && !Array.isArray(v)){
            addDotObject(id, k, v, nextPath);
          } else {
            const leafId = mk("leaf");
            nodes.push({
              id: leafId,
              parent: id,
              text: _nsNodeText(k, v),
              icon: "ns-var-leaf",
              type: "leaf",
              varPath: nextPath.join("."),
            });
          }
        }
      }
      const dotEntries = Object.entries(dotTree).sort((a,b) => String(a[0]).localeCompare(String(b[0])));
      for(const [k, v] of dotEntries){
        addDotObject("#", k, v, [String(k)]);
      }
      for(const group of rootGroups){
        const val = namespace ? namespace[group] : null;
        if(val && typeof val === "object" && !Array.isArray(val)){
          addObject("#", group, val, [String(group)]);
        }
      }
      return nodes;
    }
    function renderBuilderNamespaceTree(namespace){
      const holder = document.getElementById("builder_namespace_tree");
      if(!holder) return;
      const $ = window.jQuery;
      if(!$ || !$.fn || !$.fn.jstree){
        holder.innerHTML = `<span class="muted">jsTree not available in this browser context.</span>`;
        return;
      }
      const data = buildNamespaceTreeData(namespace || {});
      const $holder = $(holder);
      try { $holder.jstree("destroy"); } catch {}
      $holder.off(".jstree");
      holder.innerHTML = "";
      try {
        $holder.jstree({
          core: { data, multiple: false },
          plugins: ["wholerow"],
        });
      } catch {
        return;
      }
      $holder.on("open_node.jstree", function(_ev, payload){
        const node = payload?.node;
        if(!node || node.original?.type !== "group") return;
        try { $holder.jstree(true).set_icon(node, "ns-folder-open"); } catch {}
      });
      $holder.on("close_node.jstree", function(_ev, payload){
        const node = payload?.node;
        if(!node || node.original?.type !== "group") return;
        try { $holder.jstree(true).set_icon(node, "ns-folder-closed"); } catch {}
      });
      $holder.on("select_node.jstree", function(_ev, payload){
        const node = payload?.node;
        if(!node) return;
        if(node.original?.type === "group"){
          const inst = $holder.jstree(true);
          if(!inst) return;
          try {
            if(inst.is_open(node)) inst.close_node(node);
            else inst.open_node(node);
          } catch {}
          return;
        }
        if(node.original?.type !== "leaf") return;
        const p = String(node.original?.varPath || "").trim();
        if(!p || !isInsertableBuilderTextControl(builderLastTextTarget)) return;
        insertAtCursor(builderLastTextTarget, `{${p}}`);
      });
    }
    function initBuilderTooltipResolution(){
      const root = document.getElementById("builder_panel");
      if(!root) return;
      root.addEventListener("mouseover", (ev) => {
        const t = ev.target;
        if(!(t instanceof HTMLInputElement)) return;
        if(t.type === "checkbox" || t.type === "button") return;
        setBuilderResolvedTooltip(t);
      }, true);
      root.addEventListener("focusin", (ev) => {
        const t = ev.target;
        if(!(t instanceof HTMLInputElement)) return;
        if(t.type === "checkbox" || t.type === "button") return;
        setBuilderResolvedTooltip(t);
      }, true);
      root.addEventListener("focusin", (ev) => {
        const t = ev.target;
        if(isInsertableBuilderTextControl(t)){
          builderLastTextTarget = t;
        }
      }, true);
      root.addEventListener("mousedown", (ev) => {
        const t = ev.target;
        if(isInsertableBuilderTextControl(t)){
          builderLastTextTarget = t;
        }
      }, true);
      root.addEventListener("focusout", (ev) => {
        const t = ev.target;
        if(isBuilderAutoValidateControl(t)){
          scheduleBuilderAutoValidate();
        }
      }, true);
    }
    async function loadRuns(){
      if(isDatasetsView) return;
      const res = isPipelineDetailView
        ? await fetch(`/api/pipelines/${encodeURIComponent(selectedPipeline || "")}/runs?${qp()}`)
        : await fetch(`/api/runs?${qp()}`);
      if(!res.ok){ document.getElementById("runs").innerHTML = `<tr><td colspan="4">${esc(await res.text())}</td></tr>`; return; }
      const rows = await res.json();
      const body = document.getElementById("runs");
      body.innerHTML = rows.map(r => `
        <tr data-id="${esc(r.run_id)}">
          <td>${esc(r.run_id)}</td>
          <td class="${r.success ? "ok" : "bad"}">${esc(r.status)}</td>
          <td>${esc(r.started_at)}</td>
          <td>${esc(r.pipeline)}</td>
        </tr>`).join("");
      [...body.querySelectorAll("tr")].forEach(tr => tr.onclick = () => { selected = tr.dataset.id; loadDetail(); });
    }
    async function loadPipelineSummary(){
      if(!isPipelineDetailView || !selectedPipeline) return;
      const el = document.getElementById("pipeline_summary");
      const res = await fetch(`/api/pipelines/${encodeURIComponent(selectedPipeline)}`);
      if(!res.ok){ el.innerHTML = `<div>${esc(await readMessage(res))}</div>`; return; }
      const d = await res.json();
      const p = d.latest_provenance || {};
      el.innerHTML = `
        <div><b>${esc(d.pipeline)}</b></div>
        <div>Total runs: <b>${esc(d.total_runs)}</b> | Failed: <b>${esc(d.failed_runs)}</b> | Failure rate: <b>${esc((Number(d.failure_rate || 0)*100).toFixed(1))}%</b></div>
        <div>Latest run: <b>${esc((d.latest_run || {}).run_id)}</b> (${esc((d.latest_run || {}).status)})</div>
        <div>Latest provenance: commit=${esc(p.git_commit_sha)} branch=${esc(p.git_branch)} dirty=${esc(p.git_is_dirty)}</div>
      `;
    }
    async function loadPipelineValidations(){
      if(!isPipelineDetailView || !selectedPipeline) return;
      const el = document.getElementById("pipeline_validations");
      const res = await fetch(`/api/pipelines/${encodeURIComponent(selectedPipeline)}/validations?limit=20`);
      if(!res.ok){
        el.innerHTML = `<div>${esc(await readMessage(res))}</div>`;
        return;
      }
      const rows = await res.json();
      if(!rows.length){
        el.innerHTML = `<h4>Validation History</h4><div class="muted">No validation history yet.</div>`;
        return;
      }
      const items = rows.map(v => `
        <div class="node">
          <span class="${v.valid ? "ok" : "bad"}">${v.valid ? "valid" : "invalid"}</span>
          <span class="muted"> ${esc(v.requested_at)} | source=${esc(v.source)} | steps=${esc(v.step_count)}</span>
          ${v.error ? `<div class="bad">${esc(v.error)}</div>` : ""}
        </div>
      `).join("");
      el.innerHTML = `<h4>Validation History</h4>${items}`;
    }
    async function loadPipelines(){
      if(!isPipelinesView) return;
      const res = await fetch(`/api/pipelines?${pipelineQp()}`);
      const body = document.getElementById("pipelines");
      if(!res.ok){
        body.innerHTML = `<tr><td colspan="5">${esc(await readMessage(res))}</td></tr>`;
        return;
      }
      const rows = await res.json();
      body.innerHTML = rows.map(p => `
        <tr data-pipeline="${esc(p.pipeline)}">
          <td>${esc(p.pipeline)}</td>
          <td class="${p.last_status === "succeeded" ? "ok" : p.last_status === "failed" ? "bad" : ""}">${esc(p.last_status || "")}</td>
          <td>${esc(p.last_started_at)}</td>
          <td>${esc(p.total_runs)}</td>
          <td>${esc((Number(p.failure_rate || 0) * 100).toFixed(1))}%</td>
        </tr>`).join("");
      [...body.querySelectorAll("tr")].forEach(tr => tr.onclick = () => {
        selectedPipeline = tr.dataset.pipeline;
        window.location.href = `/pipelines/${encodeURIComponent(selectedPipeline)}`;
      });
    }
    async function loadDatasets(){
      if(!isDatasetsView || isDatasetDetailView) return;
      const p = new URLSearchParams();
      const q = document.getElementById("f_q").value.trim();
      if(q) p.set("q", q);
      p.set("limit", "200");
      const res = await fetch(`/api/datasets?${p.toString()}`);
      const body = document.getElementById("runs");
      if(!res.ok){
        body.innerHTML = `<tr><td colspan="4">${esc(await readMessage(res))}</td></tr>`;
        return;
      }
      const rows = await res.json();
      body.innerHTML = rows.map(d => `
        <tr data-dataset="${esc(d.dataset_id)}">
          <td>${esc(d.dataset_id)}</td>
          <td>${esc(d.latest_version || "-")}</td>
          <td>${esc(d.status || "-")}</td>
          <td>${esc(d.data_class || "-")}</td>
        </tr>`).join("");
      [...body.querySelectorAll("tr")].forEach(tr => tr.onclick = () => {
        selectedDataset = tr.dataset.dataset;
        window.location.href = `/datasets/${encodeURIComponent(selectedDataset)}`;
      });
    }
    async function loadDatasetDetail(){
      if(!isDatasetsView) return;
      if(isDatasetDetailView){
        selectedDataset = datasetFromPath;
      }
      if(!selectedDataset) return;
      const res = await fetch(`/api/datasets/${encodeURIComponent(selectedDataset)}`);
      const detailEl = document.getElementById("detail");
      if(!res.ok){
        detailEl.textContent = `Error loading dataset ${selectedDataset}: ${await readMessage(res)}`;
        return;
      }
      const d = await res.json();
      const dict = d.dictionary_entries || [];
      detailEl.innerHTML = `
        <div><b>${esc(d.dataset_id)}</b> <span class="muted">${esc(d.status || "-")}</span></div>
        <div class="muted">class=${esc(d.data_class || "-")} owner=${esc(d.owner_user || "-")} versions=${esc((d.versions || []).length)}</div>
        <h4>Dictionary Entries (${dict.length})</h4>
        <pre>${esc(JSON.stringify(dict, null, 2))}</pre>
        <h4>Versions</h4>
        <pre>${esc(JSON.stringify(d.versions || [], null, 2))}</pre>
        <h4>Locations</h4>
        <pre>${esc(JSON.stringify(d.locations || [], null, 2))}</pre>
      `;
    }
    async function loadDetail(){
      if(isDatasetsView) return;
      if(!selected) return;
      const res = await fetch(`/api/runs/${encodeURIComponent(selected)}`);
      if(!res.ok){ document.getElementById("detail").textContent = `Error loading ${selected}`; return; }
      const d = await res.json();
      let html = `
        <div><b>${esc(d.run_id)}</b> <span class="${d.success ? "ok" : "bad"}">${esc(d.status)}</span></div>
        <div class="muted">${esc(d.pipeline)} | ${esc(d.executor)} | ${esc(d.started_at)} -> ${esc(d.ended_at)}</div>
        <h4>Steps</h4>
        <pre>${esc(JSON.stringify(d.steps, null, 2))}</pre>
        <h4>Attempts</h4>
        <pre>${esc(JSON.stringify(d.attempts, null, 2))}</pre>
        <h4>Events</h4>
        <pre>${esc(JSON.stringify(d.events, null, 2))}</pre>
        <h4>Provenance</h4>
        <pre>${esc(JSON.stringify(d.provenance, null, 2))}</pre>
        <h4>Live Log</h4>
        <div class="muted" id="live_log_path">Loading...</div>
        <pre id="live_log">Loading...</pre>
      `;
      html += `
        <h4>Artifacts</h4>
        <div class="filesplit">
          <div class="filetree" id="filetree">Loading files...</div>
          <div class="viewer"><pre id="fileview">Select a file to view content.</pre></div>
        </div>
      `;
      document.getElementById("detail").innerHTML = html;
      await loadLiveLog();
      await loadFileTree();
    }
    async function loadLive(){
      if(!isLiveRunView || !selected) return;
      const res = await fetch(`/api/runs/${encodeURIComponent(selected)}/live`);
      const el = document.getElementById("detail");
      if(!res.ok){
        el.textContent = await readMessage(res);
        return;
      }
      const d = await res.json();
      const ev = d.latest_event || {};
      el.innerHTML = `
        <div><b>${esc(d.run_id)}</b> <span class="${d.success ? "ok" : "bad"}">${esc(d.status)}</span></div>
        <div class="muted">${esc(d.pipeline)} | ${esc(d.executor)} | ${esc(d.started_at)} -> ${esc(d.ended_at)}</div>
        <div class="muted">Active attempts: ${esc(d.active_attempt_count)} | Completed steps: ${esc(d.completed_step_count)} | Failed steps: ${esc(d.failed_step_count)}</div>
        <h4>Latest Event</h4>
        <pre>${esc(JSON.stringify(ev, null, 2))}</pre>
        <h4>Active Attempts</h4>
        <pre>${esc(JSON.stringify(d.active_attempts || [], null, 2))}</pre>
        <h4>Timeline</h4>
        <pre>${esc(JSON.stringify(d.events || [], null, 2))}</pre>
        <h4>Provenance</h4>
        <pre>${esc(JSON.stringify(d.provenance || {}, null, 2))}</pre>
        <h4>Live Log</h4>
        <div class="muted" id="live_log_path">Loading...</div>
        <pre id="live_log">Loading...</pre>
      `;
      await loadLiveLog();
    }
    async function loadLiveLog(){
      if(!selected) return;
      const logEl = document.getElementById("live_log");
      const pathEl = document.getElementById("live_log_path");
      if(!logEl || !pathEl) return;
      const res = await fetch(`/api/runs/${encodeURIComponent(selected)}/live-log?limit=300`);
      if(!res.ok){
        pathEl.textContent = "";
        logEl.textContent = await readMessage(res);
        return;
      }
      const payload = await res.json();
      pathEl.textContent = payload.log_file ? `log file: ${payload.log_file}` : `state: ${payload.state || "unknown"}`;
      const lines = Array.isArray(payload.lines) ? payload.lines : [];
      logEl.textContent = lines.length ? lines.join("\\n") : "No log lines yet.";
      logEl.scrollTop = logEl.scrollHeight;
    }
    function renderTreeNode(node, depth){
      const indent = "&nbsp;".repeat(depth * 4);
      if(node.type === "dir"){
        let html = `<div class="node dir">${indent}${esc(node.name)}/</div>`;
        for(const c of (node.children || [])){ html += renderTreeNode(c, depth + 1); }
        return html;
      }
      return `<div class="node file" data-path="${esc(node.path)}">${indent}${esc(node.name)}</div>`;
    }
    async function loadFileTree(){
      if(!selected) return;
      const treeEl = document.getElementById("filetree");
      const viewEl = document.getElementById("fileview");
      const res = await fetch(`/api/runs/${encodeURIComponent(selected)}/files`);
      if(!res.ok){
        treeEl.textContent = await res.text();
        return;
      }
      const tree = await res.json();
      treeEl.innerHTML = renderTreeNode(tree, 0);
      [...treeEl.querySelectorAll(".node.file")].forEach(el => {
        el.onclick = async () => {
          const rel = el.dataset.path;
          viewEl.textContent = "Loading...";
          const fr = await fetch(`/api/runs/${encodeURIComponent(selected)}/file?path=${encodeURIComponent(rel)}`);
          if(!fr.ok){ viewEl.textContent = await fr.text(); return; }
          const payload = await fr.json();
          viewEl.textContent = payload.content;
        };
      });
    }
    async function resumeSelected(){
      if(!selected) return;
      const el = document.getElementById("resume_msg");
      el.textContent = "Resuming...";
      const body = {};
      const pluginsDir = document.getElementById("r_plugins_dir").value.trim();
      const workdir = document.getElementById("r_workdir").value.trim();
      const retries = document.getElementById("r_max_retries").value.trim();
      const delay = document.getElementById("r_retry_delay").value.trim();
      const ex = document.getElementById("r_executor").value.trim();
      if (pluginsDir) body.plugins_dir = pluginsDir;
      if (workdir) body.workdir = workdir;
      if (retries) body.max_retries = Number(retries);
      if (delay) body.retry_delay_seconds = Number(delay);
      if (ex) body.executor = ex;
      const res = await fetch(`/api/runs/${encodeURIComponent(selected)}/resume`, {
        method:"POST",
        headers: {"Content-Type":"application/json"},
        body: JSON.stringify(body),
      });
      const txt = await res.text();
      if(!res.ok){ el.textContent = txt; return; }
      try {
        const payload = JSON.parse(txt);
        el.textContent = `Created run ${payload.run_id} (${payload.state})`;
        selected = payload.run_id;
      } catch {
        el.textContent = txt;
      }
      await tick();
    }
    async function quickStop(runId){
      const res = await fetch(`/api/runs/${encodeURIComponent(runId)}/stop`, {
        method:"POST",
        headers: {"Content-Type":"application/json"},
        body: "{}",
      });
      if(!res.ok){
        return await readMessage(res);
      }
      const payload = await res.json();
      return payload.message || `Stop requested for ${payload.run_id}`;
    }
    async function stopSelected(){
      if(!selected) return;
      const el = document.getElementById("resume_msg");
      el.textContent = "Stopping...";
      const msg = await quickStop(selected);
      el.textContent = msg;
      await tick();
    }
    async function validateAction(){
      const el = document.getElementById("action_msg");
      el.textContent = "Validating...";
      const url = isPipelineDetailView
        ? `/api/pipelines/${encodeURIComponent(selectedPipeline || "")}/validate`
        : `/api/actions/validate`;
      const res = await fetch(url, {
        method:"POST",
        headers: {"Content-Type":"application/json"},
        body: JSON.stringify(actionPayload()),
      });
      if(!res.ok){ el.textContent = await readMessage(res); return; }
      const payload = await res.json();
      el.textContent = `Valid: ${payload.step_count} steps`;
    }
    async function runAction(){
      const el = document.getElementById("action_msg");
      el.textContent = "Submitting run...";
      const url = isPipelineDetailView
        ? `/api/pipelines/${encodeURIComponent(selectedPipeline || "")}/run`
        : `/api/actions/run`;
      const reqPayload = actionPayload();
      const seed = makeRunSeed();
      reqPayload.run_id = seed.run_id;
      reqPayload.run_started_at = seed.run_started_at;
      const res = await fetch(url, {
        method:"POST",
        headers: {"Content-Type":"application/json"},
        body: JSON.stringify(reqPayload),
      });
      if(!res.ok){ el.textContent = await readMessage(res); return; }
      const runResp = await res.json();
      selected = runResp.run_id;
      el.textContent = `Run ${runResp.run_id} (${runResp.state})`;
      await tick();
    }
    async function tick(){
      if(isBuilderView){
        await loadBuilderSource();
        return;
      }
      if(isPluginsView){
        await loadPluginsPage();
        return;
      }
      if(isDatasetsView){
        await loadDatasets();
        await loadDatasetDetail();
        return;
      }
      await loadOps();
      await loadPipelines();
      await loadPipelineSummary();
      await loadPipelineValidations();
      await loadRuns();
      if(isLiveRunView){
        await loadLive();
      } else {
        await loadDetail();
      }
      await loadLiveLog();
    }
    initViewMode();
    setActiveNav();
    initUserScope();
    document.getElementById("btn_apply").onclick = tick;
    document.getElementById("btn_ops_refresh").onclick = tick;
    document.getElementById("btn_pipelines").onclick = tick;
    document.getElementById("btn_validate").onclick = validateAction;
    document.getElementById("btn_run").onclick = runAction;
    document.getElementById("btn_resume").onclick = resumeSelected;
    document.getElementById("btn_stop").onclick = stopSelected;
    document.getElementById("btn_nav_live").onclick = () => {
      const runId = document.getElementById("nav_live_id").value.trim();
      if (!runId) return;
      const asUser = encodeURIComponent(currentAsUser());
      window.location.href = `/runs/${encodeURIComponent(runId)}/live?as_user=${asUser}`;
    };
    document.getElementById("btn_builder_import_local").onclick = () => { openBuilderFilePicker(); };
    document.getElementById("btn_builder_toggle_preview").onclick = () => { builderPreviewCollapsed = !builderPreviewCollapsed; renderBuilderPreviewPanel(); };
    document.getElementById("btn_builder_toggle_yaml").onclick = () => {
      builderPreviewSectionCollapsed.yaml = !builderPreviewSectionCollapsed.yaml;
      renderBuilderPreviewSections();
    };
    document.getElementById("btn_builder_toggle_output").onclick = () => {
      builderPreviewSectionCollapsed.output = !builderPreviewSectionCollapsed.output;
      renderBuilderPreviewSections();
    };
    document.getElementById("btn_builder_toggle_vars").onclick = () => {
      builderPreviewSectionCollapsed.vars = !builderPreviewSectionCollapsed.vars;
      renderBuilderPreviewSections();
    };
    document.getElementById("btn_builder_toggle_plugins").onclick = () => {
      builderPreviewSectionCollapsed.plugins = !builderPreviewSectionCollapsed.plugins;
      renderBuilderPreviewSections();
    };
    document.getElementById("btn_builder_add_req").onclick = addBuilderRequire;
    document.getElementById("btn_builder_add_var").onclick = addBuilderVar;
    document.getElementById("btn_builder_add_dir").onclick = addBuilderDir;
    document.getElementById("btn_builder_add_step").onclick = addBuilderStep;
    document.getElementById("btn_builder_save").onclick = saveBuilderDraft;
    document.getElementById("btn_builder_generate").onclick = generateBuilderDraft;
    document.getElementById("btn_builder_validate").onclick = validateBuilderDraft;
    document.getElementById("btn_builder_run").onclick = runBuilderPipeline;
    document.getElementById("btn_plugins_refresh").onclick = tick;
    document.getElementById("plugins_env").onchange = tick;
    document.getElementById("b_env_name").onchange = async () => {
      await loadBuilderPluginStats();
      renderBuilderPluginStats();
      renderBuilderModel();
    };
    document.getElementById("b_requires").addEventListener("input", handleBuilderInput);
    document.getElementById("b_vars").addEventListener("input", handleBuilderInput);
    document.getElementById("b_dirs").addEventListener("input", handleBuilderInput);
    document.getElementById("b_requires").addEventListener("change", handleBuilderInput);
    document.getElementById("b_vars").addEventListener("change", handleBuilderInput);
    document.getElementById("b_dirs").addEventListener("change", handleBuilderInput);
    document.getElementById("b_steps").addEventListener("input", handleBuilderInput);
    document.getElementById("b_steps").addEventListener("change", handleBuilderInput);
    document.getElementById("b_requires").addEventListener("click", handleBuilderClicks);
    document.getElementById("b_vars").addEventListener("click", handleBuilderClicks);
    document.getElementById("b_dirs").addEventListener("click", handleBuilderClicks);
    document.getElementById("b_steps").addEventListener("click", handleBuilderClicks);
    document.getElementById("b_file_picker").addEventListener("change", async () => { await loadBuilderSourceFromFilePicker(); });
    renderBuilderPreviewPanel();
    renderBuilderPreviewSections();
    initBuilderTreeComboBehavior();
    initBuilderTooltipResolution();
    loadPluginEnvOptions();
    loadBuilderEnvironments();
    refreshBuilderTreeFiles();
    refreshBuilderNamespace();
    tick(); setInterval(tick, 12000);
  </script>
</body>
</html>
"""


@app.get("/", response_class=HTMLResponse)
def index() -> str:
    return INDEX_HTML


@app.get("/pipelines", response_class=HTMLResponse)
def pipelines_index() -> str:
    return INDEX_HTML


@app.get("/plugins", response_class=HTMLResponse)
def plugins_index() -> str:
    return INDEX_HTML


@app.get("/datasets", response_class=HTMLResponse)
def datasets_index() -> str:
    return INDEX_HTML


@app.get("/datasets/{dataset_id:path}", response_class=HTMLResponse)
def dataset_detail_index(dataset_id: str) -> str:
    return INDEX_HTML


@app.get("/pipelines/new", response_class=HTMLResponse)
def pipelines_new_index() -> str:
    return INDEX_HTML


@app.get("/pipelines/{pipeline_id:path}/edit", response_class=HTMLResponse)
def pipeline_edit_index(pipeline_id: str) -> str:
    return INDEX_HTML


@app.get("/pipelines/{pipeline_id:path}", response_class=HTMLResponse)
def pipeline_detail_index(pipeline_id: str) -> str:
    return INDEX_HTML


@app.get("/runs/{run_id:path}/live", response_class=HTMLResponse)
def run_live_index(run_id: str) -> str:
    return INDEX_HTML


@app.get("/api/health")
def health() -> dict:
    return {"ok": True}


def _normalize_user_id(value: Optional[str]) -> str:
    raw = str(value or "").strip().lower().replace("_", "-")
    # Keep IDs stable and resilient to hidden punctuation/whitespace noise.
    raw = re.sub(r"[^a-z0-9-]+", "", raw)
    return raw or "admin"


def _is_admin_user(user_id: str) -> bool:
    normalized = _normalize_user_id(user_id)
    return normalized in {"admin", "administrator", "root"}


def _static_user_projects(user_id: str) -> set[str]:
    mapping = {
        "admin": {"land_core", "gee_lee"},
        "land-core": {"land_core"},
        "gee-lee": {"gee_lee"},
    }
    return set(mapping.get(user_id, set()))


def _load_user_projects_from_db(user_id: str) -> Optional[set[str]]:
    db_url = get_database_url()
    if not db_url:
        return None
    try:
        with psycopg.connect(db_url) as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT up.project_id
                    FROM etl_user_projects up
                    JOIN etl_projects p ON p.project_id = up.project_id
                    WHERE up.user_id = %s
                      AND p.is_active = TRUE
                    """,
                    (user_id,),
                )
                rows = cur.fetchall() or []
        return {str(r[0]) for r in rows if str(r[0]).strip()}
    except Exception:
        return None


def _resolve_user_scope(request: Request) -> UserScope:
    raw_user = request.headers.get("X-ETL-User") or request.query_params.get("as_user")
    user_id = _normalize_user_id(raw_user)
    projects = _load_user_projects_from_db(user_id)
    if projects is None:
        projects = _static_user_projects(user_id)
    if not projects:
        raise HTTPException(status_code=403, detail=f"User has no project access: {user_id}")
    return UserScope(user_id=user_id, allowed_projects=projects)


def _require_project_access(scope: UserScope, project_id: Optional[str]) -> Optional[str]:
    normalized = normalize_project_id(project_id)
    if not normalized:
        return None
    if _is_admin_user(scope.user_id):
        return normalized
    if normalized not in scope.allowed_projects:
        raise HTTPException(
            status_code=403,
            detail=f"User '{scope.user_id}' is not allowed for project '{normalized}'.",
        )
    return normalized


def _combine_project_scoped_rows(
    rows_by_project: list[list[dict[str, Any]]],
    *,
    limit: int,
    dedupe_key: str,
) -> list[dict[str, Any]]:
    merged: list[dict[str, Any]] = []
    seen: set[str] = set()
    for rows in rows_by_project:
        for row in rows:
            key = str(row.get(dedupe_key) or "")
            if key and key in seen:
                continue
            if key:
                seen.add(key)
            merged.append(row)
    merged.sort(key=lambda r: str(r.get("started_at") or r.get("last_started_at") or ""), reverse=True)
    return merged[:limit]


def _resolve_request_project_filter(
    *,
    request: Request,
    project_id: Optional[str],
    pipeline_id: Optional[str] = None,
) -> tuple[UserScope, Optional[str]]:
    scope = _resolve_user_scope(request)
    requested = normalize_project_id(project_id)
    inferred = normalize_project_id(infer_project_id_from_pipeline_path(pipeline_id)) if pipeline_id else None
    if requested and inferred and requested != inferred:
        raise HTTPException(
            status_code=400,
            detail=f"project_id '{requested}' does not match pipeline path project '{inferred}'.",
        )
    selected = _require_project_access(scope, requested or inferred)
    if selected:
        return scope, selected
    if len(scope.allowed_projects) == 1:
        return scope, sorted(scope.allowed_projects)[0]
    return scope, None


@app.get("/api/pipelines")
def api_pipelines(
    request: Request,
    limit: int = Query(default=100, ge=1, le=500),
    q: Optional[str] = Query(default=None),
    project_id: Optional[str] = Query(default=None),
) -> list[dict]:
    scope, selected_project = _resolve_request_project_filter(request=request, project_id=project_id)
    try:
        if selected_project:
            return fetch_pipelines(limit=limit, q=q, project_id=selected_project)
        rows_by_project = [
            fetch_pipelines(limit=limit, q=q, project_id=pid)
            for pid in sorted(scope.allowed_projects)
        ]
        return _combine_project_scoped_rows(rows_by_project, limit=limit, dedupe_key="pipeline")
    except WebQueryError as exc:
        raise HTTPException(status_code=503, detail=str(exc)) from exc


@app.get("/api/pipelines/{pipeline_id:path}/runs")
def api_pipeline_runs(
    request: Request,
    pipeline_id: str,
    limit: int = Query(default=50, ge=1, le=500),
    status: Optional[str] = Query(default=None),
    executor: Optional[str] = Query(default=None),
    project_id: Optional[str] = Query(default=None),
) -> list[dict]:
    scope, selected_project = _resolve_request_project_filter(
        request=request,
        project_id=project_id,
        pipeline_id=pipeline_id,
    )
    try:
        if selected_project:
            return fetch_pipeline_runs(
                pipeline_id,
                limit=limit,
                status=status,
                executor=executor,
                project_id=selected_project,
            )
        rows_by_project = [
            fetch_pipeline_runs(
                pipeline_id,
                limit=limit,
                status=status,
                executor=executor,
                project_id=pid,
            )
            for pid in sorted(scope.allowed_projects)
        ]
        return _combine_project_scoped_rows(rows_by_project, limit=limit, dedupe_key="run_id")
    except WebQueryError as exc:
        raise HTTPException(status_code=503, detail=str(exc)) from exc


@app.get("/api/pipelines/{pipeline_id:path}/validations")
def api_pipeline_validations(
    request: Request,
    pipeline_id: str,
    limit: int = Query(default=50, ge=1, le=500),
    project_id: Optional[str] = Query(default=None),
) -> list[dict]:
    scope, selected_project = _resolve_request_project_filter(
        request=request,
        project_id=project_id,
        pipeline_id=pipeline_id,
    )
    try:
        if selected_project:
            return fetch_pipeline_validations(pipeline_id, limit=limit, project_id=selected_project)
        rows_by_project = [
            fetch_pipeline_validations(pipeline_id, limit=limit, project_id=pid)
            for pid in sorted(scope.allowed_projects)
        ]
        return _combine_project_scoped_rows(rows_by_project, limit=limit, dedupe_key="validation_id")
    except WebQueryError as exc:
        raise HTTPException(status_code=503, detail=str(exc)) from exc


@app.get("/api/pipelines/{pipeline_id:path}")
def api_pipeline_detail(request: Request, pipeline_id: str, project_id: Optional[str] = Query(default=None)) -> dict:
    scope, selected_project = _resolve_request_project_filter(
        request=request,
        project_id=project_id,
        pipeline_id=pipeline_id,
    )
    try:
        payload = None
        if selected_project:
            payload = fetch_pipeline_detail(pipeline_id, project_id=selected_project)
        else:
            for pid in sorted(scope.allowed_projects):
                payload = fetch_pipeline_detail(pipeline_id, project_id=pid)
                if payload is not None:
                    break
    except WebQueryError as exc:
        raise HTTPException(status_code=503, detail=str(exc)) from exc
    if payload is None:
        raise HTTPException(status_code=404, detail=f"Pipeline not found: {pipeline_id}")
    return payload


@app.get("/api/datasets")
def api_datasets(
    request: Request,
    limit: int = Query(default=100, ge=1, le=500),
    q: Optional[str] = Query(default=None),
) -> list[dict]:
    _ = _resolve_user_scope(request)
    try:
        return fetch_datasets(limit=limit, q=q)
    except WebQueryError as exc:
        raise HTTPException(status_code=503, detail=str(exc)) from exc


@app.get("/api/datasets/{dataset_id:path}")
def api_dataset_detail(request: Request, dataset_id: str) -> dict:
    _ = _resolve_user_scope(request)
    try:
        payload = fetch_dataset_detail(dataset_id)
    except WebQueryError as exc:
        raise HTTPException(status_code=503, detail=str(exc)) from exc
    if payload is None:
        raise HTTPException(status_code=404, detail=f"Dataset not found: {dataset_id}")
    return payload


@app.get("/api/runs")
def api_runs(
    request: Request,
    limit: int = Query(default=50, ge=1, le=500),
    status: Optional[str] = Query(default=None),
    executor: Optional[str] = Query(default=None),
    q: Optional[str] = Query(default=None),
    project_id: Optional[str] = Query(default=None),
) -> list[dict]:
    scope, selected_project = _resolve_request_project_filter(request=request, project_id=project_id)
    try:
        if selected_project:
            return fetch_runs(
                limit=limit,
                status=status,
                executor=executor,
                q=q,
                project_id=selected_project,
            )
        rows_by_project = [
            fetch_runs(
                limit=limit,
                status=status,
                executor=executor,
                q=q,
                project_id=pid,
            )
            for pid in sorted(scope.allowed_projects)
        ]
        return _combine_project_scoped_rows(rows_by_project, limit=limit, dedupe_key="run_id")
    except WebQueryError as exc:
        raise HTTPException(status_code=503, detail=str(exc)) from exc


@app.get("/api/runs/{run_id}/live")
def api_run_live(run_id: str, request: Request) -> dict:
    scope = _resolve_user_scope(request)
    try:
        payload = fetch_run_detail(run_id)
    except WebQueryError as exc:
        raise HTTPException(status_code=503, detail=str(exc)) from exc
    if payload is None:
        raise HTTPException(status_code=404, detail=f"Run not found: {run_id}")
    _require_project_access(scope, payload.get("project_id"))

    attempts = payload.get("attempts") or []
    events = payload.get("events") or []
    steps = payload.get("steps") or []
    active_attempts = [
        a
        for a in attempts
        if not bool(a.get("skipped")) and not bool(a.get("success")) and not a.get("ended_at")
    ]
    failed_steps = [s for s in steps if not bool(s.get("success")) and not bool(s.get("skipped"))]
    completed_steps = [s for s in steps if bool(s.get("success")) and not bool(s.get("skipped"))]
    skipped_steps = [s for s in steps if bool(s.get("skipped"))]

    return {
        "run_id": payload.get("run_id"),
        "pipeline": payload.get("pipeline"),
        "project_id": payload.get("project_id"),
        "status": payload.get("status"),
        "success": bool(payload.get("success")),
        "executor": payload.get("executor"),
        "started_at": payload.get("started_at"),
        "ended_at": payload.get("ended_at"),
        "latest_event": events[-1] if events else None,
        "events": events,
        "active_attempt_count": len(active_attempts),
        "active_attempts": active_attempts,
        "completed_step_count": len(completed_steps),
        "failed_step_count": len(failed_steps),
        "skipped_step_count": len(skipped_steps),
        "provenance": payload.get("provenance") or {},
    }


@app.get("/api/runs/{run_id}")
def api_run_detail(run_id: str, request: Request) -> dict:
    scope = _resolve_user_scope(request)
    try:
        payload = fetch_run_detail(run_id)
    except WebQueryError as exc:
        raise HTTPException(status_code=503, detail=str(exc)) from exc
    if payload is None:
        raise HTTPException(status_code=404, detail=f"Run not found: {run_id}")
    _require_project_access(scope, payload.get("project_id"))
    return payload


def _parse_bool(value: Any, *, default: bool = False) -> bool:
    if value is None:
        return default
    if isinstance(value, bool):
        return value
    if isinstance(value, (int, float)):
        return bool(value)
    lowered = str(value).strip().lower()
    if lowered in {"1", "true", "yes", "y", "on"}:
        return True
    if lowered in {"0", "false", "no", "n", "off"}:
        return False
    return default


def _parse_optional_int(value: Any, *, field_name: str) -> Optional[int]:
    if value is None or str(value).strip() == "":
        return None
    try:
        return int(value)
    except (TypeError, ValueError) as exc:
        raise HTTPException(status_code=400, detail=f"Invalid {field_name}: must be an integer.") from exc


def _parse_optional_float(value: Any, *, field_name: str) -> Optional[float]:
    if value is None or str(value).strip() == "":
        return None
    try:
        return float(value)
    except (TypeError, ValueError) as exc:
        raise HTTPException(status_code=400, detail=f"Invalid {field_name}: must be a number.") from exc


def _parse_action_payload(payload: Optional[dict[str, Any]]) -> dict[str, Any]:
    payload = payload or {}
    pipeline_raw = str(payload.get("pipeline") or "").strip()
    if not pipeline_raw:
        raise HTTPException(status_code=400, detail="`pipeline` is required.")
    pipeline_path = Path(pipeline_raw).expanduser()
    if not pipeline_path.exists():
        raise HTTPException(status_code=400, detail=f"Pipeline path not found: {pipeline_path}")

    global_config_raw = str(payload.get("global_config") or "").strip()
    environments_config_raw = str(payload.get("environments_config") or "").strip()
    env_name_raw = str(payload.get("env") or "").strip()
    global_config_path = Path(global_config_raw).expanduser() if global_config_raw else None
    environments_config_path = Path(environments_config_raw).expanduser() if environments_config_raw else None
    env_name = env_name_raw or None

    executor = str(payload.get("executor") or "local").strip().lower()
    if executor not in {"local", "slurm"}:
        raise HTTPException(status_code=400, detail="`executor` must be one of: local, slurm.")

    max_retries = _parse_optional_int(payload.get("max_retries"), field_name="max_retries")
    retry_delay_seconds = _parse_optional_float(payload.get("retry_delay_seconds"), field_name="retry_delay_seconds")
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

    def _select_workdir_candidate(*candidates: Any) -> str:
        for candidate in candidates:
            text = str(candidate or "").strip()
            if text:
                return text
        return ".runs"

    return {
        "payload": payload,
        "pipeline_path": pipeline_path,
        "global_config_path": global_config_path,
        "environments_config_path": environments_config_path,
        "env_name": env_name,
        "executor": executor,
        "plugins_dir": Path(payload.get("plugins_dir") or "plugins"),
        "workdir_raw": workdir_raw,
        "workdir": Path(_select_workdir_candidate(workdir_raw, ".runs")),
        "dry_run": _parse_bool(payload.get("dry_run"), default=False),
        "verbose": _parse_bool(payload.get("verbose"), default=False),
        "max_retries": max_retries,
        "retry_delay_seconds": retry_delay_seconds,
        "execution_source": execution_source,
        "source_bundle": str(payload.get("source_bundle") or "").strip() or None,
        "source_snapshot": str(payload.get("source_snapshot") or "").strip() or None,
        "allow_workspace_source": _parse_bool(payload.get("allow_workspace_source"), default=False),
        "project_id": normalize_project_id(str(payload.get("project_id") or "").strip() or None),
        "run_id": run_id,
        "run_started_at": run_started_at,
    }


def _resolve_global_vars(global_config_path: Optional[Path]) -> dict[str, Any]:
    try:
        resolved = resolve_global_config_path(global_config_path)
    except ConfigError as exc:
        raise HTTPException(status_code=400, detail=f"Global config error: {exc}") from exc
    if not resolved:
        return {}
    try:
        return load_global_config(resolved)
    except ConfigError as exc:
        raise HTTPException(status_code=400, detail=f"Global config error: {exc}") from exc


def _resolve_execution_env(
    environments_config_path: Optional[Path],
    env_name: Optional[str],
    *,
    executor: str,
    global_vars: Optional[dict[str, Any]] = None,
) -> tuple[dict[str, Any], Optional[Path], Optional[str]]:
    try:
        resolved = resolve_execution_config_path(environments_config_path)
    except ExecutionConfigError as exc:
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
        envs = load_execution_config(resolved)
        env = envs.get(str(selected_env_name), {})
        if not env:
            raise HTTPException(status_code=400, detail=f"Execution env '{selected_env_name}' not found in config.")
        validate_environment_executor(str(selected_env_name), env, executor=executor)
        resolved_env = apply_execution_env_overrides(env)
        resolved_env = resolve_execution_env_templates(resolved_env, global_vars=global_vars or {})
        return resolved_env, resolved, selected_env_name
    except HTTPException:
        raise
    except ExecutionConfigError as exc:
        raise HTTPException(status_code=400, detail=f"Environments config error: {exc}") from exc


def _resolve_builder_env_vars(
    *,
    environments_config_path: Optional[Path],
    env_name: Optional[str],
    global_vars: Optional[dict[str, Any]] = None,
) -> dict[str, Any]:
    name = str(env_name or "").strip()
    if not name:
        return {}
    try:
        resolved = resolve_execution_config_path(environments_config_path)
    except ExecutionConfigError as exc:
        raise HTTPException(status_code=400, detail=f"Environments config error: {exc}") from exc
    if not resolved:
        if name == "local":
            return {}
        raise HTTPException(status_code=400, detail="Environment selected but environments config was not found.")
    try:
        envs = load_execution_config(resolved)
    except ExecutionConfigError as exc:
        raise HTTPException(status_code=400, detail=f"Environments config error: {exc}") from exc
    env = envs.get(name)
    if not isinstance(env, dict):
        if name == "local":
            return {}
        raise HTTPException(status_code=400, detail=f"Execution env '{name}' not found in config.")
    resolved_env = apply_execution_env_overrides(dict(env))
    return resolve_execution_env_templates(resolved_env, global_vars=global_vars or {})


def _parse_pipeline_from_yaml_text(
    yaml_text: str,
    *,
    global_config_path: Optional[Path],
    environments_config_path: Optional[Path] = None,
    env_name: Optional[str] = None,
) -> Pipeline:
    if not (yaml_text or "").strip():
        raise HTTPException(status_code=400, detail="`yaml_text` is required.")
    global_vars = _resolve_global_vars(global_config_path)
    env_vars = _resolve_builder_env_vars(
        environments_config_path=environments_config_path,
        env_name=env_name,
        global_vars=global_vars,
    )
    with tempfile.NamedTemporaryFile("w", suffix=".yml", delete=False, encoding="utf-8") as tmp:
        tmp.write(yaml_text)
        tmp_path = Path(tmp.name)
    try:
        return parse_pipeline(tmp_path, global_vars=global_vars, env_vars=env_vars)
    except (PipelineError, FileNotFoundError) as exc:
        raise HTTPException(status_code=400, detail=f"Invalid draft pipeline: {exc}") from exc
    finally:
        try:
            tmp_path.unlink(missing_ok=True)
        except Exception:
            pass


def _raw_vars_dirs_from_yaml_text(yaml_text: str) -> tuple[dict[str, Any], dict[str, Any]]:
    import yaml

    try:
        data = yaml.safe_load(yaml_text) or {}
    except yaml.YAMLError:
        data = {}
    if not isinstance(data, dict):
        data = {}
    vars_section = data.get("vars", {}) or {}
    dirs_section = data.get("dirs", {}) or {}
    out_vars = dict(vars_section) if isinstance(vars_section, dict) else {}
    out_dirs = dict(dirs_section) if isinstance(dirs_section, dict) else {}
    return out_vars, out_dirs


def _resolve_builder_plugins_dir(*, global_config_path: Optional[Path], plugins_dir: Optional[str]) -> Path:
    if plugins_dir and str(plugins_dir).strip():
        return Path(str(plugins_dir).strip()).expanduser()
    global_vars = _resolve_global_vars(global_config_path)
    cfg_plugins = str(global_vars.get("plugins_dir") or "").strip()
    if cfg_plugins:
        return Path(cfg_plugins).expanduser()
    return Path("plugins")


def _estimate_from_stats(mean: Optional[Any], std: Optional[Any], samples: Optional[Any], low_mult: float) -> Optional[float]:
    if mean in (None, "") or samples in (None, ""):
        return None
    try:
        mu = float(mean)
        n = int(samples)
    except (TypeError, ValueError):
        return None
    if n <= 0:
        return None
    if n < 5:
        return mu * float(low_mult)
    try:
        sigma = float(std or 0.0)
    except (TypeError, ValueError):
        sigma = 0.0
    return mu + (3.0 * max(0.0, sigma))


def _parse_mem_text_gb(raw: Any) -> Optional[float]:
    text = str(raw or "").strip().lower()
    if not text:
        return None
    m = re.match(r"^\s*([0-9]+(?:\.[0-9]+)?)\s*([kmgt]?b?)?\s*$", text)
    if not m:
        return None
    num = float(m.group(1))
    unit = str(m.group(2) or "").strip().lower()
    if unit.startswith("k"):
        return num / (1024.0 * 1024.0)
    if unit.startswith("m"):
        return num / 1024.0
    if unit.startswith("t"):
        return num * 1024.0
    return num


def _parse_slurm_time_minutes(raw: Any) -> Optional[float]:
    text = str(raw or "").strip()
    if not text:
        return None
    days = 0
    rest = text
    if "-" in text:
        dpart, rest = text.split("-", 1)
        try:
            days = int(dpart)
        except ValueError:
            return None
    parts = rest.split(":")
    try:
        if len(parts) == 3:
            h, m, s = int(parts[0]), int(parts[1]), int(parts[2])
        elif len(parts) == 2:
            h, m, s = 0, int(parts[0]), int(parts[1])
        elif len(parts) == 1:
            h, m, s = 0, int(parts[0]), 0
        else:
            return None
    except ValueError:
        return None
    return float(days * 24 * 60 + h * 60 + m + (s / 60.0))


def _parse_step_script_for_builder(script: str) -> tuple[str, dict[str, Any]]:
    try:
        tokens = shlex.split(str(script or ""))
    except Exception:
        tokens = str(script or "").split()
    if not tokens:
        return "", {}
    plugin_ref = tokens[0]
    params: dict[str, Any] = {}
    for tok in tokens[1:]:
        if "=" in tok:
            k, v = tok.split("=", 1)
            params[k] = v
    return plugin_ref, params


def _pipeline_to_builder_model_from_yaml(yaml_text: str) -> dict[str, Any]:
    import yaml

    try:
        data = yaml.safe_load(yaml_text) or {}
    except yaml.YAMLError:
        data = {}
    if not isinstance(data, dict):
        data = {}
    vars_section = data.get("vars", {}) or {}
    dirs_section = data.get("dirs", {}) or {}
    reqs = data.get("requires_pipelines", []) or []
    steps_section = data.get("steps", []) or []

    model_steps: list[dict[str, Any]] = []
    if isinstance(steps_section, list):
        for raw in steps_section:
            if not isinstance(raw, dict):
                continue
            step_map = raw.get("step") if isinstance(raw.get("step"), dict) else raw
            if not isinstance(step_map, dict):
                continue
            plugin_ref = str(step_map.get("plugin") or "").strip()
            params: dict[str, Any] = {}
            args_raw = step_map.get("args")
            if isinstance(args_raw, dict):
                for k, v in args_raw.items():
                    params[str(k)] = v
            if not plugin_ref:
                plugin_ref, script_params = _parse_step_script_for_builder(str(step_map.get("script") or ""))
                params = script_params
            resources: dict[str, Any] = {}
            resources_raw = step_map.get("resources")
            if isinstance(resources_raw, dict):
                for k, v in resources_raw.items():
                    resources[str(k)] = v
            stype = "sequential"
            foreach_raw = str(step_map.get("foreach") or "")
            foreach_glob_raw = str(step_map.get("foreach_glob") or "")
            foreach_kind_raw = str(step_map.get("foreach_kind") or "")
            foreach_mode = "var"
            if foreach_glob_raw.strip():
                foreach_mode = "glob"
            if step_map.get("foreach") or step_map.get("foreach_glob"):
                stype = "foreach"
            elif step_map.get("parallel_with"):
                stype = "parallel"
            model_steps.append(
                {
                    "type": stype,
                    "plugin": plugin_ref,
                    "params": params,
                    "resources": resources,
                    "output_var": str(step_map.get("output_var") or ""),
                    "when": str(step_map.get("when") or ""),
                    "parallel_with": str(step_map.get("parallel_with") or ""),
                    "foreach": foreach_raw,
                    "foreach_mode": foreach_mode,
                    "foreach_glob": foreach_glob_raw,
                    "foreach_kind": foreach_kind_raw or "dirs",
                }
            )
    return {
        "vars": vars_section if isinstance(vars_section, dict) else {},
        "dirs": dirs_section if isinstance(dirs_section, dict) else {},
        "requires_pipelines": [str(x) for x in reqs if str(x).strip()] if isinstance(reqs, list) else [],
        "steps": model_steps,
    }


def _validate_draft_yaml(yaml_text: str) -> tuple[Optional[Pipeline], Optional[str]]:
    try:
        pipeline = _parse_pipeline_from_yaml_text(yaml_text, global_config_path=None)
        return pipeline, None
    except HTTPException as exc:
        return None, str(exc.detail)


def _lookup_ctx_path(ctx: dict[str, Any], dotted: str) -> tuple[Any, bool]:
    cur: Any = ctx
    for part in str(dotted or "").split("."):
        if isinstance(cur, dict) and part in cur:
            cur = cur[part]
            continue
        return None, False
    return cur, True


def _resolve_text_with_ctx(value: str, ctx: dict[str, Any]) -> str:
    text = str(value or "")

    def _repl(match: re.Match[str]) -> str:
        key = str(match.group(1) or "")
        found, ok = _lookup_ctx_path(ctx, key)
        if not ok or isinstance(found, (dict, list)):
            return match.group(0)
        return str(found)

    return _TPL_RE.sub(_repl, text)


def _resolve_text_with_ctx_iterative(value: str, ctx: dict[str, Any], *, max_passes: int = DEFAULT_RESOLVE_MAX_PASSES) -> str:
    cur = str(value or "")
    for _ in range(max_passes):
        nxt = _resolve_text_with_ctx(cur, ctx)
        if nxt == cur:
            return cur
        cur = nxt
    return cur


def _build_builder_namespace(
    *,
    pipeline: Pipeline,
    global_vars: dict[str, Any],
    env_vars: dict[str, Any],
    raw_vars: Optional[dict[str, Any]] = None,
    raw_dirs: Optional[dict[str, Any]] = None,
    preview_run_id: Optional[str] = None,
    preview_run_started: Optional[datetime] = None,
) -> dict[str, Any]:
    def _step_arg_map(script: str) -> dict[str, str]:
        out: dict[str, str] = {}
        try:
            tokens = shlex.split(str(script or ""))
        except Exception:
            tokens = str(script or "").split()
        for tok in tokens[1:]:
            text = str(tok or "")
            if "=" not in text:
                continue
            k, v = text.split("=", 1)
            key = str(k or "").strip()
            if key:
                out[key] = v
        return out

    def _output_var_placeholders(p: Pipeline) -> dict[str, dict[str, Any]]:
        placeholders: dict[str, dict[str, Any]] = {}
        for idx, step in enumerate(p.steps or []):
            output_var = str(step.output_var or "").strip()
            if not output_var:
                continue
            args_map = _step_arg_map(str(step.script or ""))
            ph: dict[str, Any] = {
                "_runtime": True,
                "_producer_step": str(step.name or f"step_{idx}"),
                "_producer_index": idx,
            }
            out_val = str(args_map.get("out") or "").strip()
            if out_val:
                # Common plugin convention for file-producing steps.
                ph["output_dir"] = out_val
                ph["out"] = out_val
            placeholders[output_var] = ph
        return placeholders

    max_passes = resolve_max_passes_setting(global_vars=global_vars, env_vars=env_vars)

    def _resolve_preview_flat(
        flat_map: dict[str, Any],
        base_ctx: dict[str, Any],
        *,
        seed_flat: dict[str, Any],
        max_passes: int,
    ) -> tuple[dict[str, Any], int, bool]:
        reserved = {"sys", "global", "globals", "env", "vars", "dirs", "resolution"}
        current = dict(flat_map or {})
        for i in range(max_passes):
            ctx = dict(base_ctx)
            for k, v in current.items():
                if str(k) not in reserved:
                    ctx[str(k)] = v
            nxt: dict[str, Any] = {}
            for k, v in current.items():
                if isinstance(v, str):
                    ktxt = str(k)
                    temp_ctx = dict(ctx)
                    if ktxt in temp_ctx:
                        temp_ctx.pop(ktxt, None)
                    fallback = seed_flat.get(ktxt)
                    if fallback is not None and not isinstance(fallback, (dict, list)):
                        temp_ctx[ktxt] = fallback
                    resolved = VariableSolver.resolve_iterative(v, temp_ctx, max_passes=1)
                    if isinstance(resolved, (dict, list)):
                        nxt[ktxt] = v
                    else:
                        nxt[ktxt] = str(resolved)
                else:
                    nxt[str(k)] = v
            if nxt == current:
                return current, i + 1, True
            current = nxt
        return current, max_passes, False

    now = preview_run_started or datetime.now(timezone.utc)
    if now.tzinfo is None:
        now = now.replace(tzinfo=timezone.utc)
    else:
        now = now.astimezone(timezone.utc)
    run_id_preview = str(preview_run_id or "")
    if not run_id_preview:
        run_id_preview = "run_abcd0123"
    run_short_preview = run_id_preview[:8] if preview_run_id else "abcd0123"
    sys_ns: dict[str, Any] = {
        "run": {
            # Runtime-populated on real execution paths; placeholder values are used in builder preview.
            "id": run_id_preview,
            "short_id": run_short_preview,
        },
        "job": {
            "id": run_id_preview,
            "name": str((pipeline.vars or {}).get("jobname") or ""),
        },
        "step": {
            # Step-specific values are only known during step execution; placeholders provide preview resolution.
            "id": "step_abcd0123",
            "name": "preview_step",
            "index": "0",
        },
        "now": {
            "iso_utc": now.isoformat().replace("+00:00", "Z"),
            "yymmdd": now.strftime("%y%m%d"),
            "hhmmss": now.strftime("%H%M%S"),
            "yymmdd_hhmmss": now.strftime("%y%m%d-%H%M%S"),
        },
    }
    global_ns = dict(global_vars or {})
    env_ns = dict(env_vars or {})
    vars_ns = dict(pipeline.vars or {})
    dirs_ns = dict(pipeline.dirs or {})
    vars_preview = dict(raw_vars or vars_ns)
    dirs_preview = dict(raw_dirs or dirs_ns)
    seed_flat: dict[str, Any] = {}
    for source in (global_ns, env_ns, vars_preview):
        for k, v in source.items():
            seed_flat[str(k)] = v
    flat_ns: dict[str, Any] = {}
    for source in (seed_flat, dirs_preview):
        for k, v in source.items():
            flat_ns[str(k)] = v
    flat_ns, preview_passes_used, preview_stable = _resolve_preview_flat(
        flat_ns,
        {
            "sys": sys_ns,
            "global": global_ns,
            "globals": global_ns,
            "env": env_ns,
            "pipe": vars_ns,
            "vars": vars_ns,
            "dirs": dirs_ns,
        },
        seed_flat=seed_flat,
        max_passes=max_passes,
    )
    ns: dict[str, Any] = {
        "sys": sys_ns,
        "global": global_ns,
        "env": env_ns,
        "vars": vars_ns,
        "dirs": dirs_ns,
        "resolution": {
            # vars/dirs are produced by parse_pipeline, which already uses the
            # runtime iterative resolver with this cap.
            "stable": bool(preview_stable),
            "max_passes": int(max_passes),
            "passes_used": int(preview_passes_used),
            "source": "pipeline.parse+preview",
        },
    }
    ns.update({str(k): v for k, v in flat_ns.items()})
    output_ns = _output_var_placeholders(pipeline)
    ns["outputs"] = output_ns
    for name, value in output_ns.items():
        ns[str(name)] = value
    return ns


def _dir_category(key: str) -> Optional[str]:
    k = str(key or "").strip().lower()
    if not k:
        return None
    if k in {"work", "workdir", "work_dir"}:
        return "work"
    if k in {"log", "logdir", "log_dir"}:
        return "log"
    if k in {"artifact", "artifactdir", "artifact_dir"}:
        return "artifact"
    if k in {"stage", "stagedir", "stage_dir"}:
        return "stage"
    if k in {"tmp", "temp", "tmpdir", "tmp_dir", "tempdir", "temp_dir"}:
        return "tmp"
    return None


def _validate_pipeline_dir_contract(pipeline: Pipeline) -> None:
    counts = {"work": 0, "log": 0, "artifact": 0, "stage": 0, "tmp": 0}
    for key in (pipeline.dirs or {}).keys():
        cat = _dir_category(str(key))
        if cat:
            counts[cat] += 1
    errors: list[str] = []
    if counts["work"] != 1:
        errors.append(f"Exactly one work directory is required (found {counts['work']}).")
    if counts["log"] != 1:
        errors.append(f"Exactly one log directory is required (found {counts['log']}).")
    for optional_name in ("artifact", "stage", "tmp"):
        if counts[optional_name] > 1:
            errors.append(f"At most one {optional_name} directory is allowed (found {counts[optional_name]}).")
    if errors:
        raise HTTPException(status_code=400, detail=" ".join(errors))


def _extract_unresolved_tokens(value: Any) -> list[str]:
    if not isinstance(value, str):
        return []
    seen: set[str] = set()
    out: list[str] = []
    for token in _TPL_RE.findall(value):
        tok = str(token or "").strip()
        if not tok or tok in seen:
            continue
        seen.add(tok)
        out.append(tok)
    return out


def _collect_unresolved_step_inputs(pipeline: Pipeline) -> list[dict[str, Any]]:
    issues: list[dict[str, Any]] = []
    for idx, step in enumerate(pipeline.steps or []):
        try:
            tokens = shlex.split(str(step.script or ""))
        except Exception:
            tokens = str(step.script or "").split()
        if tokens:
            plugin_ref = str(tokens[0] or "")
            plugin_tokens = _extract_unresolved_tokens(plugin_ref)
            if plugin_tokens:
                issues.append(
                    {
                        "step_index": idx,
                        "step_name": step.name,
                        "field": "plugin",
                        "value": plugin_ref,
                        "tokens": plugin_tokens,
                    }
                )
            positional_idx = 0
            for tok in tokens[1:]:
                text = str(tok or "")
                if "=" in text:
                    key, val = text.split("=", 1)
                    field_name = f"args.{str(key or '').strip()}"
                    field_value = val
                else:
                    field_name = f"arg_list[{positional_idx}]"
                    field_value = text
                    positional_idx += 1
                unresolved = _extract_unresolved_tokens(field_value)
                if unresolved:
                    issues.append(
                        {
                            "step_index": idx,
                            "step_name": step.name,
                            "field": field_name,
                            "value": field_value,
                            "tokens": unresolved,
                        }
                    )
        for env_key, env_val in (step.env or {}).items():
            unresolved = _extract_unresolved_tokens(str(env_val or ""))
            if unresolved:
                issues.append(
                    {
                        "step_index": idx,
                        "step_name": step.name,
                        "field": f"env.{env_key}",
                        "value": str(env_val or ""),
                        "tokens": unresolved,
                    }
                )
        for field_name, field_val in (
            ("when", step.when),
            ("parallel_with", step.parallel_with),
            ("foreach", step.foreach),
            ("foreach_glob", getattr(step, "foreach_glob", None)),
            ("foreach_kind", getattr(step, "foreach_kind", None)),
        ):
            unresolved = _extract_unresolved_tokens(str(field_val or ""))
            if unresolved:
                issues.append(
                    {
                        "step_index": idx,
                        "step_name": step.name,
                        "field": field_name,
                        "value": str(field_val or ""),
                        "tokens": unresolved,
                    }
                )
    return issues


def _filter_builder_unresolved_issues(issues: list[dict[str, Any]], pipeline: Pipeline) -> list[dict[str, Any]]:
    prior_output_vars_by_step: dict[int, set[str]] = {}
    seen: set[str] = set()
    for idx, step in enumerate(pipeline.steps or []):
        prior_output_vars_by_step[idx] = set(seen)
        output_var = str(step.output_var or "").strip()
        if output_var:
            seen.add(output_var)

    def _keep_token(tok: Any, *, step_index: int) -> bool:
        text = str(tok or "").strip()
        if not text:
            return False
        # Runtime sys placeholders are valid in builder drafts; they are
        # populated during execution.
        root = text.split(".", 1)[0]
        if root == "sys":
            return False
        if root in {"item", "item_index", "item_name", "item_stem"}:
            return False
        if root in prior_output_vars_by_step.get(int(step_index), set()):
            return False
        return True

    out: list[dict[str, Any]] = []
    for issue in issues or []:
        step_index = int(issue.get("step_index") or 0)
        tokens = [t for t in (issue.get("tokens") or []) if _keep_token(t, step_index=step_index)]
        if not tokens:
            continue
        next_issue = dict(issue)
        next_issue["tokens"] = tokens
        out.append(next_issue)
    return out


def _record_pipeline_validation(
    *,
    pipeline: str,
    project_id: Optional[str] = None,
    valid: bool,
    step_count: int = 0,
    step_names: Optional[list[str]] = None,
    error: Optional[str] = None,
    source: str = "web_validate",
) -> None:
    db_url = get_database_url()
    if not db_url:
        return
    try:
        with psycopg.connect(db_url) as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    INSERT INTO etl_pipeline_validations (
                        pipeline, project_id, valid, step_count, step_names_json, error, source
                    )
                    VALUES (%s, %s, %s, %s, %s::jsonb, %s, %s)
                    """,
                    (
                        pipeline,
                        project_id,
                        bool(valid),
                        int(step_count),
                        json.dumps(step_names or []),
                        error,
                        source,
                    ),
                )
            conn.commit()
    except Exception:
        # Validation history recording should be best-effort and non-blocking.
        pass


def _resolve_repo_relative_pipeline_path(pipeline: str) -> Path:
    raw = (pipeline or "").strip()
    if not raw:
        raise HTTPException(status_code=400, detail="`pipeline` is required.")
    repo_root = Path(".").resolve()
    pipelines_root = (repo_root / "pipelines").resolve()
    candidate = Path(raw).expanduser()
    if not candidate.is_absolute():
        parts = list(candidate.parts)
        if parts and parts[0].lower() == "pipelines":
            candidate = Path(*parts[1:]) if len(parts) > 1 else Path("")
        if candidate.suffix.lower() not in {".yml", ".yaml"}:
            candidate = candidate.with_suffix(".yml")
        candidate = pipelines_root / candidate
    resolved = candidate.resolve()
    try:
        resolved.relative_to(pipelines_root)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=f"Pipeline path must be inside pipelines/: {raw}") from exc
    if resolved.suffix.lower() not in {".yml", ".yaml"}:
        resolved = resolved.with_suffix(".yml")
    return resolved


@app.post("/api/pipelines")
def api_pipelines_create(request: Request, payload: Optional[dict[str, Any]] = Body(default=None)) -> dict:
    scope = _resolve_user_scope(request)
    payload = payload or {}
    pipeline_path = _resolve_repo_relative_pipeline_path(str(payload.get("pipeline") or ""))
    req_project_id = normalize_project_id(str(payload.get("project_id") or "").strip() or None)
    path_project_id = infer_project_id_from_pipeline_path(pipeline_path)
    if req_project_id and path_project_id and req_project_id != path_project_id:
        raise HTTPException(
            status_code=400,
            detail=f"Requested project_id '{req_project_id}' does not match pipeline path project '{path_project_id}'.",
        )
    _require_project_access(scope, req_project_id or path_project_id)
    yaml_text = str(payload.get("yaml_text") or "")
    _ = _parse_pipeline_from_yaml_text(yaml_text, global_config_path=None)
    overwrite = _parse_bool(payload.get("overwrite"), default=False)
    if pipeline_path.exists() and not overwrite:
        raise HTTPException(status_code=409, detail=f"Pipeline already exists: {pipeline_path}")
    pipeline_path.parent.mkdir(parents=True, exist_ok=True)
    pipeline_path.write_text(yaml_text.strip() + "\n", encoding="utf-8")
    return {"pipeline": str(pipeline_path), "saved": True, "created": True}


@app.put("/api/pipelines/{pipeline_id:path}")
def api_pipelines_update(request: Request, pipeline_id: str, payload: Optional[dict[str, Any]] = Body(default=None)) -> dict:
    scope = _resolve_user_scope(request)
    payload = payload or {}
    if "pipeline" in payload and str(payload.get("pipeline") or "").strip() not in {"", pipeline_id}:
        raise HTTPException(status_code=400, detail="Payload pipeline does not match URL pipeline_id.")
    pipeline_path = _resolve_repo_relative_pipeline_path(pipeline_id)
    req_project_id = normalize_project_id(str(payload.get("project_id") or "").strip() or None)
    path_project_id = infer_project_id_from_pipeline_path(pipeline_path)
    if req_project_id and path_project_id and req_project_id != path_project_id:
        raise HTTPException(
            status_code=400,
            detail=f"Requested project_id '{req_project_id}' does not match pipeline path project '{path_project_id}'.",
        )
    _require_project_access(scope, req_project_id or path_project_id)
    if not pipeline_path.exists():
        raise HTTPException(status_code=404, detail=f"Pipeline file not found: {pipeline_path}")
    yaml_text = str(payload.get("yaml_text") or "")
    _ = _parse_pipeline_from_yaml_text(yaml_text, global_config_path=None)
    pipeline_path.parent.mkdir(parents=True, exist_ok=True)
    pipeline_path.write_text(yaml_text.strip() + "\n", encoding="utf-8")
    return {"pipeline": str(pipeline_path), "saved": True, "updated": True}


@app.get("/api/builder/source")
def api_builder_source(pipeline: str = Query(default="")) -> dict:
    raw = (pipeline or "").strip()
    if not raw:
        raise HTTPException(status_code=400, detail="`pipeline` query param is required.")
    repo_root = Path(".").resolve()
    pipelines_root = (repo_root / "pipelines").resolve()
    path = Path(raw).expanduser()
    if not path.is_absolute():
        # First pass: direct resolution under pipelines root.
        parts = list(path.parts)
        if parts and parts[0].lower() == "pipelines":
            path = Path(*parts[1:]) if len(parts) > 1 else Path("")
        if path.suffix.lower() not in {".yml", ".yaml"}:
            path = path.with_suffix(".yml")
        path = (pipelines_root / path).resolve()
        # Fallback: if caller passed a bare filename (e.g. download.yml),
        # search recursively under pipelines/**.
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
    if not path.exists() or not path.is_file():
        raise HTTPException(status_code=404, detail=f"Pipeline file not found: {path}")
    try:
        text = path.read_text(encoding="utf-8")
    except Exception as exc:  # noqa: BLE001
        raise HTTPException(status_code=500, detail=f"Failed to read pipeline file: {exc}") from exc
    return {"pipeline": str(path), "yaml_text": text, "model": _pipeline_to_builder_model_from_yaml(text)}


@app.get("/api/builder/files")
def api_builder_files() -> dict:
    repo_root = Path(".").resolve()
    pipelines_root = (repo_root / "pipelines").resolve()
    pipelines_root.mkdir(parents=True, exist_ok=True)
    files = sorted(p.relative_to(pipelines_root).as_posix() for p in pipelines_root.rglob("*.yml"))
    return {"pipelines_root": str(pipelines_root), "files": files}


@app.get("/api/builder/plugins")
def api_builder_plugins(
    global_config: Optional[str] = Query(default=None),
    plugins_dir: Optional[str] = Query(default=None),
) -> dict:
    global_config_path = Path(global_config).expanduser() if (global_config or "").strip() else None
    global_vars = _resolve_global_vars(global_config_path)
    root = _resolve_builder_plugins_dir(global_config_path=global_config_path, plugins_dir=plugins_dir)
    if not root.exists() or not root.is_dir():
        raise HTTPException(status_code=400, detail=f"Plugins directory not found: {root}")
    entries: list[dict[str, Any]] = []
    for f in sorted(root.rglob("*.py")):
        if f.name.startswith("_"):
            continue
        rel = f.relative_to(root).as_posix()
        try:
            pd = load_plugin(f)
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
        except PluginLoadError:
            entries.append(
                {"path": rel, "name": rel, "version": "", "description": "unloadable plugin", "params": {}, "resources": {}}
            )
    return {"plugins_dir": str(root), "plugins": entries}


@app.get("/api/plugins/stats")
def api_plugins_stats(
    global_config: Optional[str] = Query(default=None),
    plugins_dir: Optional[str] = Query(default=None),
    environments_config: Optional[str] = Query(default=None),
    env: Optional[str] = Query(default=None),
    low_sample_multiplier: float = Query(default=1.5, ge=1.0, le=10.0),
    limit: int = Query(default=200, ge=10, le=5000),
) -> dict:
    global_config_path = Path(global_config).expanduser() if (global_config or "").strip() else None
    root = _resolve_builder_plugins_dir(global_config_path=global_config_path, plugins_dir=plugins_dir)
    if not root.exists() or not root.is_dir():
        raise HTTPException(status_code=400, detail=f"Plugins directory not found: {root}")

    environments_config_path = Path(environments_config).expanduser() if (environments_config or "").strip() else None
    env_name = str(env or "").strip() or None
    env_vars = _resolve_builder_env_vars(
        environments_config_path=environments_config_path,
        env_name=env_name,
    )
    max_cpu = env_vars.get("max_cpus_per_task")
    try:
        max_cpu = int(max_cpu) if max_cpu not in (None, "") else None
    except (TypeError, ValueError):
        max_cpu = None
    max_mem_gb = _parse_mem_text_gb(env_vars.get("max_mem"))
    max_wall_minutes = _parse_slurm_time_minutes(env_vars.get("max_time"))

    entries: list[dict[str, Any]] = []
    for f in sorted(root.rglob("*.py")):
        if f.name.startswith("_"):
            continue
        rel = f.relative_to(root).as_posix()
        try:
            pd = load_plugin(f)
        except PluginLoadError:
            entries.append(
                {
                    "path": rel,
                    "name": rel,
                    "version": "",
                    "description": "unloadable plugin",
                    "resources": {},
                    "stats": {},
                    "recommendation": {},
                }
            )
            continue

        plugin_name = str(pd.meta.name or "").strip()
        plugin_version = str(pd.meta.version or "").strip()
        stats = fetch_plugin_resource_stats(
            plugin_name=plugin_name,
            plugin_version=plugin_version,
            plugin_refs=[rel],
            executor="slurm",
            limit=limit,
        )
        rec_cpu = _estimate_from_stats(
            stats.get("cpu_cores_mean"),
            stats.get("cpu_cores_std"),
            stats.get("cpu_cores_samples", stats.get("samples")),
            low_sample_multiplier,
        )
        rec_mem_gb = _estimate_from_stats(
            stats.get("memory_gb_mean"),
            stats.get("memory_gb_std"),
            stats.get("memory_gb_samples", stats.get("samples")),
            low_sample_multiplier,
        )
        rec_wall = _estimate_from_stats(
            stats.get("wall_minutes_mean"),
            stats.get("wall_minutes_std"),
            stats.get("wall_minutes_samples", stats.get("samples")),
            low_sample_multiplier,
        )

        if max_cpu is not None and rec_cpu is not None:
            rec_cpu = min(rec_cpu, float(max_cpu))
        if max_mem_gb is not None and rec_mem_gb is not None:
            rec_mem_gb = min(rec_mem_gb, float(max_mem_gb))
        if max_wall_minutes is not None and rec_wall is not None:
            rec_wall = min(rec_wall, float(max_wall_minutes))

        recommendation = {
            "cpu_cores": rec_cpu,
            "memory_gb": rec_mem_gb,
            "wall_minutes": rec_wall,
            "samples": int(stats.get("samples", 0) or 0),
            "low_sample_multiplier": float(low_sample_multiplier),
        }
        entries.append(
            {
                "path": rel,
                "name": plugin_name or rel,
                "version": plugin_version,
                "description": pd.meta.description or "",
                "resources": pd.meta.resources or {},
                "stats": stats or {},
                "recommendation": recommendation,
            }
        )
    return {
        "plugins_dir": str(root),
        "env": env_name,
        "caps": {
            "max_cpus_per_task": max_cpu,
            "max_mem_gb": max_mem_gb,
            "max_wall_minutes": max_wall_minutes,
        },
        "plugins": entries,
    }


@app.get("/api/builder/environments")
def api_builder_environments(
    environments_config: Optional[str] = Query(default=None),
) -> dict:
    raw = str(environments_config or "").strip()
    cfg_path = Path(raw).expanduser() if raw else None
    try:
        resolved = resolve_execution_config_path(cfg_path)
    except ExecutionConfigError as exc:
        raise HTTPException(status_code=400, detail=f"Environments config error: {exc}") from exc
    if not resolved:
        return {"environments_config": None, "environments": []}
    try:
        envs = load_execution_config(resolved)
    except ExecutionConfigError as exc:
        raise HTTPException(status_code=400, detail=f"Environments config error: {exc}") from exc
    return {
        "environments_config": str(resolved),
        "environments": sorted(str(k) for k in envs.keys()),
    }


@app.post("/api/builder/resolve-text")
def api_builder_resolve_text(payload: Optional[dict[str, Any]] = Body(default=None)) -> dict:
    payload = payload or {}
    value = str(payload.get("value") or "")
    global_config_raw = str(payload.get("global_config") or "").strip()
    global_config_path = Path(global_config_raw).expanduser() if global_config_raw else None
    environments_config_raw = str(payload.get("environments_config") or "").strip()
    environments_config_path = Path(environments_config_raw).expanduser() if environments_config_raw else None
    env_name = str(payload.get("env") or "").strip() or None
    global_vars = _resolve_global_vars(global_config_path)
    env_vars = _resolve_builder_env_vars(
        environments_config_path=environments_config_path,
        env_name=env_name,
        global_vars=global_vars,
    )
    raw_vars, raw_dirs = _raw_vars_dirs_from_yaml_text(str(payload.get("yaml_text") or ""))
    pipeline = _parse_pipeline_from_yaml_text(
        str(payload.get("yaml_text") or ""),
        global_config_path=global_config_path,
        environments_config_path=environments_config_path,
        env_name=env_name,
    )
    ctx = _build_builder_namespace(
        pipeline=pipeline,
        global_vars=global_vars,
        env_vars=env_vars,
        raw_vars=raw_vars,
        raw_dirs=raw_dirs,
    )
    max_passes = resolve_max_passes_setting(global_vars=global_vars, env_vars=env_vars)
    resolved_raw = VariableSolver.resolve_iterative(value, ctx, max_passes=max_passes)
    resolved = str(resolved_raw) if not isinstance(resolved_raw, (dict, list)) else value
    return {"value": value, "resolved": resolved, "changed": resolved != value}


@app.post("/api/builder/namespace")
def api_builder_namespace(payload: Optional[dict[str, Any]] = Body(default=None)) -> dict:
    payload = payload or {}
    global_config_raw = str(payload.get("global_config") or "").strip()
    global_config_path = Path(global_config_raw).expanduser() if global_config_raw else None
    environments_config_raw = str(payload.get("environments_config") or "").strip()
    environments_config_path = Path(environments_config_raw).expanduser() if environments_config_raw else None
    env_name = str(payload.get("env") or "").strip() or None
    global_vars = _resolve_global_vars(global_config_path)
    env_vars = _resolve_builder_env_vars(
        environments_config_path=environments_config_path,
        env_name=env_name,
        global_vars=global_vars,
    )
    raw_vars, raw_dirs = _raw_vars_dirs_from_yaml_text(str(payload.get("yaml_text") or ""))
    pipeline = _parse_pipeline_from_yaml_text(
        str(payload.get("yaml_text") or ""),
        global_config_path=global_config_path,
        environments_config_path=environments_config_path,
        env_name=env_name,
    )
    namespace = _build_builder_namespace(
        pipeline=pipeline,
        global_vars=global_vars,
        env_vars=env_vars,
        raw_vars=raw_vars,
        raw_dirs=raw_dirs,
    )
    return {
        "namespace": namespace,
        "counts": {
            "sys": len(dict(namespace.get("sys") or {})),
            "global": len(global_vars),
            "env": len(env_vars),
            "vars": len(dict(pipeline.vars or {})),
            "dirs": len(dict(pipeline.dirs or {})),
            "flat": len(namespace),
        },
    }


@app.post("/api/builder/validate")
def api_builder_validate(payload: Optional[dict[str, Any]] = Body(default=None)) -> dict:
    payload = payload or {}
    pipeline_label = str(payload.get("pipeline") or "<builder:draft>")
    global_config_raw = str(payload.get("global_config") or "").strip()
    global_config_path = Path(global_config_raw).expanduser() if global_config_raw else None
    environments_config_raw = str(payload.get("environments_config") or "").strip()
    environments_config_path = Path(environments_config_raw).expanduser() if environments_config_raw else None
    env_name = str(payload.get("env") or "").strip() or None
    require_dir_contract = _parse_bool(payload.get("require_dir_contract"), default=True)
    require_resolved_inputs = _parse_bool(payload.get("require_resolved_inputs"), default=True)
    unresolved_inputs: list[dict[str, Any]] = []
    try:
        pipeline = _parse_pipeline_from_yaml_text(
            str(payload.get("yaml_text") or ""),
            global_config_path=global_config_path,
            environments_config_path=environments_config_path,
            env_name=env_name,
        )
        if require_dir_contract:
            _validate_pipeline_dir_contract(pipeline)
        unresolved_inputs = _filter_builder_unresolved_issues(_collect_unresolved_step_inputs(pipeline), pipeline)
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
        _record_pipeline_validation(
            pipeline=pipeline_label,
            valid=False,
            step_count=0,
            step_names=[],
            error=str(exc.detail),
            source="builder_validate",
        )
        raise
    _record_pipeline_validation(
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


@app.post("/api/builder/generate")
def api_builder_generate(payload: Optional[dict[str, Any]] = Body(default=None)) -> dict:
    payload = payload or {}
    intent = str(payload.get("intent") or "").strip()
    constraints = str(payload.get("constraints") or "").strip() or None
    existing_yaml = str(payload.get("yaml_text") or "").strip() or None
    model = str(payload.get("model") or "").strip() or None
    auto_repair = _parse_bool(payload.get("auto_repair"), default=True)
    if not intent:
        raise HTTPException(status_code=400, detail="`intent` is required.")
    try:
        yaml_text = generate_pipeline_draft(
            intent=intent,
            constraints=constraints,
            existing_yaml=existing_yaml,
            model=model,
        )
    except AIPipelineError as exc:
        raise HTTPException(status_code=500, detail=str(exc)) from exc

    attempts = 1
    repaired = False
    repair_error = None
    pipeline, validation_error = _validate_draft_yaml(yaml_text)
    if validation_error and auto_repair:
        repaired = True
        attempts = 2
        repair_constraints = (
            (constraints + "\n\n") if constraints else ""
        ) + f"Fix these validation errors exactly:\n{validation_error}"
        try:
            yaml_text = generate_pipeline_draft(
                intent=intent,
                constraints=repair_constraints,
                existing_yaml=yaml_text,
                model=model,
            )
            pipeline, validation_error = _validate_draft_yaml(yaml_text)
        except AIPipelineError as exc:
            repair_error = str(exc)

    if pipeline is not None:
        model_payload = _pipeline_to_builder_model_from_yaml(yaml_text)
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
        "model": _pipeline_to_builder_model_from_yaml(yaml_text),
        "valid": False,
        "repaired": repaired,
        "attempts": attempts,
        "validation_error": validation_error or "Unknown validation error.",
        "repair_error": repair_error,
        "step_count": 0,
        "step_names": [],
    }


@app.post("/api/builder/test-step")
def api_builder_test_step(payload: Optional[dict[str, Any]] = Body(default=None)) -> dict:
    payload = payload or {}
    run_id_seed = str(payload.get("run_id") or "").strip() or None
    run_started_seed = str(payload.get("run_started_at") or "").strip() or None
    run_started_dt = None
    if run_started_seed:
        try:
            run_started_dt = datetime.fromisoformat(run_started_seed.replace("Z", "+00:00")).replace(tzinfo=None)
        except Exception:
            run_started_dt = None

    global_config_raw = str(payload.get("global_config") or "").strip()
    global_config_path = Path(global_config_raw).expanduser() if global_config_raw else None
    environments_config_raw = str(payload.get("environments_config") or "").strip()
    environments_config_path = Path(environments_config_raw).expanduser() if environments_config_raw else None
    env_name_raw = str(payload.get("env") or "").strip()
    env_name = env_name_raw or "local"
    yaml_text = str(payload.get("yaml_text") or "")
    global_vars = _resolve_global_vars(global_config_path)
    env_vars = _resolve_builder_env_vars(
        environments_config_path=environments_config_path,
        env_name=env_name,
        global_vars=global_vars,
    )
    pipeline = _parse_pipeline_from_yaml_text(
        yaml_text,
        global_config_path=global_config_path,
        environments_config_path=environments_config_path,
        env_name=env_name,
    )
    raw_vars, raw_dirs = _raw_vars_dirs_from_yaml_text(yaml_text)
    namespace = _build_builder_namespace(
        pipeline=pipeline,
        global_vars=global_vars,
        env_vars=env_vars,
        raw_vars=raw_vars,
        raw_dirs=raw_dirs,
        preview_run_id=run_id_seed,
        preview_run_started=run_started_dt,
    )
    resolve_max_passes = resolve_max_passes_setting(global_vars=global_vars, env_vars=env_vars)
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
            raise HTTPException(
                status_code=400,
                detail=f"Step index out of range: {step_index} (step_count={len(pipeline.steps)}).",
            )
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
    workdir_raw = str(payload.get("workdir") or "").strip()
    if not workdir_raw:
        for key in ("work", "workdir", "work_dir"):
            candidate = str(pipeline.dirs.get(key) or "").strip()
            if candidate:
                workdir_raw = candidate
                break
    workdir_resolved = (
        _resolve_text_with_ctx_iterative(workdir_raw, namespace, max_passes=resolve_max_passes)
        if workdir_raw
        else ""
    )
    if _extract_unresolved_tokens(workdir_resolved):
        workdir_resolved = ""
    workdir = Path(workdir_resolved or ".runs/builder")
    logdir_raw = ""
    for key in ("log", "logdir", "log_dir"):
        candidate = str(pipeline.dirs.get(key) or "").strip()
        if candidate:
            logdir_raw = candidate
            break
    logdir_resolved = (
        _resolve_text_with_ctx_iterative(logdir_raw, namespace, max_passes=resolve_max_passes)
        if logdir_raw
        else ""
    )
    logdir = Path(logdir_resolved) if logdir_resolved else None
    dry_run = _parse_bool(payload.get("dry_run"), default=False)
    max_retries = int(payload.get("max_retries", 0) or 0)
    retry_delay_seconds = float(payload.get("retry_delay_seconds", 0.0) or 0.0)
    if max_retries < 0 or retry_delay_seconds < 0:
        raise HTTPException(status_code=400, detail="Retry settings must be >= 0.")

    mini = Pipeline(
        vars=dict(pipeline.vars),
        dirs=dict(pipeline.dirs),
        resolve_max_passes=int(getattr(pipeline, "resolve_max_passes", resolve_max_passes) or resolve_max_passes),
        steps=[target_step],
    )
    try:
        result = run_pipeline(
            mini,
            plugin_dir=plugins_dir,
            workdir=workdir,
            logdir=logdir,
            run_id=run_id_seed,
            run_started=run_started_dt,
            dry_run=dry_run,
            max_retries=max_retries,
            retry_delay_seconds=retry_delay_seconds,
        )
    except Exception as exc:  # noqa: BLE001
        raise HTTPException(status_code=500, detail=f"Step test failed: {exc}") from exc

    step_result = result.steps[0] if result.steps else None
    return {
        "run_id": result.run_id,
        "artifact_dir": result.artifact_dir,
        "step_name": target_step.name,
        "step_index": target_step_index,
        "success": bool(step_result.success if step_result else False),
        "skipped": bool(step_result.skipped if step_result else False),
        "error": step_result.error if step_result else "No step result produced.",
        "outputs": step_result.outputs if step_result else {},
        "attempts": step_result.attempts if step_result else [],
    }


def _payload_with_pipeline(payload: Optional[dict[str, Any]], pipeline_id: str) -> dict[str, Any]:
    out = dict(payload or {})
    supplied = str(out.get("pipeline") or "").strip()
    if supplied and supplied != pipeline_id:
        raise HTTPException(
            status_code=400,
            detail=f"Payload pipeline '{supplied}' does not match URL pipeline '{pipeline_id}'.",
        )
    out["pipeline"] = pipeline_id
    return out


@app.post("/api/actions/validate")
def api_action_validate(request: Request, payload: Optional[dict[str, Any]] = Body(default=None)) -> dict:
    scope = _resolve_user_scope(request)
    args = _parse_action_payload(payload)
    requested_project_id = (
        normalize_project_id(args.get("project_id"))
        or normalize_project_id(infer_project_id_from_pipeline_path(args["pipeline_path"]))
    )
    _require_project_access(scope, requested_project_id)
    global_vars = _resolve_global_vars(args["global_config_path"])
    execution_env, environments_config_path, env_name = _resolve_execution_env(
        args["environments_config_path"],
        args["env_name"],
        executor=args["executor"],
        global_vars=global_vars,
    )
    try:
        pipeline = parse_pipeline(args["pipeline_path"], global_vars=global_vars, env_vars=execution_env)
    except (PipelineError, FileNotFoundError) as exc:
        _record_pipeline_validation(
            pipeline=str(args["pipeline_path"]),
            project_id=requested_project_id,
            valid=False,
            step_count=0,
            step_names=[],
            error=str(exc),
            source="api_validate",
        )
        raise HTTPException(status_code=400, detail=f"Invalid pipeline: {exc}") from exc
    project_id = resolve_project_id(
        explicit_project_id=requested_project_id,
        pipeline_project_id=getattr(pipeline, "project_id", None),
        pipeline_path=args["pipeline_path"],
    )
    _record_pipeline_validation(
        pipeline=str(args["pipeline_path"]),
        project_id=project_id,
        valid=True,
        step_count=len(pipeline.steps),
        step_names=[s.name for s in pipeline.steps],
        error=None,
        source="api_validate",
    )
    return {
        "valid": True,
        "pipeline": str(args["pipeline_path"]),
        "steps": [s.name for s in pipeline.steps],
        "step_count": len(pipeline.steps),
    }


@app.post("/api/actions/run")
def api_action_run(request: Request, payload: Optional[dict[str, Any]] = Body(default=None)) -> dict:
    scope = _resolve_user_scope(request)
    args = _parse_action_payload(payload)
    requested_project_id = (
        normalize_project_id(args.get("project_id"))
        or normalize_project_id(infer_project_id_from_pipeline_path(args["pipeline_path"]))
    )
    _require_project_access(scope, requested_project_id)
    global_vars = _resolve_global_vars(args["global_config_path"])
    execution_env, environments_config_path, env_name = _resolve_execution_env(
        args["environments_config_path"],
        args["env_name"],
        executor=args["executor"],
        global_vars=global_vars,
    )
    max_retries = (
        args["max_retries"] if args["max_retries"] is not None else int(execution_env.get("step_max_retries", 0) or 0)
    )
    retry_delay_seconds = (
        args["retry_delay_seconds"]
        if args["retry_delay_seconds"] is not None
        else float(execution_env.get("step_retry_delay_seconds", 0.0) or 0.0)
    )
    execution_env["step_max_retries"] = max_retries
    execution_env["step_retry_delay_seconds"] = retry_delay_seconds
    execution_source = args["execution_source"] or str(execution_env.get("execution_source") or "auto")
    source_bundle = args["source_bundle"] or execution_env.get("source_bundle")
    source_snapshot = args["source_snapshot"] or execution_env.get("source_snapshot")
    allow_workspace_source = bool(args["allow_workspace_source"] or execution_env.get("allow_workspace_source", False))
    try:
        pipeline = parse_pipeline(args["pipeline_path"], global_vars=global_vars, env_vars=execution_env)
    except (PipelineError, FileNotFoundError) as exc:
        raise HTTPException(status_code=400, detail=f"Invalid pipeline: {exc}") from exc
    project_id = resolve_project_id(
        explicit_project_id=requested_project_id,
        pipeline_project_id=getattr(pipeline, "project_id", None),
        pipeline_path=args["pipeline_path"],
    )
    workdir_candidates = [
        str(args.get("workdir_raw") or "").strip(),
        str(getattr(pipeline, "workdir", None) or "").strip(),
        str(execution_env.get("workdir") or "").strip(),
        str(global_vars.get("workdir") or "").strip(),
    ]
    resolved_workdir_text = ".runs"
    for candidate in workdir_candidates:
        if not candidate:
            continue
        if _extract_unresolved_tokens(candidate):
            continue
        resolved_workdir_text = candidate
        break

    repo_root = Path(".").resolve()
    provenance = collect_run_provenance(
        repo_root=repo_root,
        pipeline_path=args["pipeline_path"],
        global_config_path=args["global_config_path"],
        environments_config_path=environments_config_path,
        plugin_dir=args["plugins_dir"],
        pipeline=pipeline,
        cli_command=f"etl web run {args['pipeline_path']}",
    )
    if args["executor"] == "slurm":
        ex = SlurmExecutor(
            env_config=execution_env,
            repo_root=repo_root,
            plugins_dir=args["plugins_dir"],
            workdir=Path(resolved_workdir_text),
            global_config=args["global_config_path"],
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
    else:
        run_id = str(args.get("run_id") or "").strip() or uuid.uuid4().hex
        run_started_at = str(args.get("run_started_at") or "").strip() or (datetime.utcnow().isoformat() + "Z")
        pipeline_path_resolved = Path(args["pipeline_path"]).resolve()
        dedupe_key = _local_submission_key(
            pipeline_path=pipeline_path_resolved,
            project_id=project_id,
            env_name=env_name,
            execution_source=execution_source,
        )
        with _LOCAL_RUN_LOCK:
            existing = _ACTIVE_LOCAL_RUN_KEYS.get(dedupe_key)
            if existing:
                snap = dict(_LOCAL_RUN_SNAPSHOT.get(existing) or {})
                raise HTTPException(
                    status_code=409,
                    detail={
                        "message": "A local run for this pipeline/environment is already active.",
                        "run_id": existing,
                        "state": str(snap.get("state") or "running"),
                    },
                )
            _ACTIVE_LOCAL_RUN_KEYS[dedupe_key] = run_id
            _LOCAL_RUN_SNAPSHOT[run_id] = {
                "state": "queued",
                "pipeline": str(args["pipeline_path"]),
                "executor": "local",
                "project_id": project_id,
                "env_name": env_name,
            }

        ex = LocalExecutor(
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
            "execution_source": execution_source,
            "source_bundle": source_bundle,
            "source_snapshot": source_snapshot,
            "allow_workspace_source": allow_workspace_source,
            "project_id": project_id,
        }
        _submit_local_run_async(
            run_id=run_id,
            dedupe_key=dedupe_key,
            executor=ex,
            pipeline_path=Path(args["pipeline_path"]),
            context=context,
            project_id=project_id,
        )
        return {
            "run_id": run_id,
            "state": "queued",
            "pipeline": str(args["pipeline_path"]),
            "executor": args["executor"],
            "project_id": project_id,
            "job_ids": [],
            "message": "Run accepted and queued for local execution.",
        }

    try:
        submit = ex.submit(
            str(args["pipeline_path"]),
            context={
                "pipeline": pipeline,
                "execution_env": execution_env,
                "provenance": provenance,
                "repo_root": repo_root,
                "global_vars": global_vars,
                "execution_source": execution_source,
                "source_bundle": source_bundle,
                "source_snapshot": source_snapshot,
                "allow_workspace_source": allow_workspace_source,
                "project_id": project_id,
            },
        )
        st = ex.status(submit.run_id)
    except Exception as exc:  # noqa: BLE001
        raise HTTPException(status_code=500, detail=f"Run failed: {exc}") from exc
    state = st.state.value if hasattr(st.state, "value") else str(st.state)
    return {
        "run_id": submit.run_id,
        "state": state,
        "pipeline": str(args["pipeline_path"]),
        "executor": args["executor"],
        "project_id": project_id,
        "job_ids": submit.job_ids or [],
        "message": st.message or submit.message or "",
    }


@app.post("/api/pipelines/{pipeline_id:path}/validate")
def api_pipeline_validate(
    request: Request,
    pipeline_id: str,
    payload: Optional[dict[str, Any]] = Body(default=None),
) -> dict:
    return api_action_validate(request, _payload_with_pipeline(payload, pipeline_id))


@app.post("/api/pipelines/{pipeline_id:path}/run")
def api_pipeline_run(
    request: Request,
    pipeline_id: str,
    payload: Optional[dict[str, Any]] = Body(default=None),
) -> dict:
    return api_action_run(request, _payload_with_pipeline(payload, pipeline_id))


def _resolve_run_header(run_id: str, *, request: Optional[Request] = None) -> dict:
    try:
        hdr = fetch_run_header(run_id)
    except WebQueryError as exc:
        raise HTTPException(status_code=503, detail=str(exc)) from exc
    if hdr is None:
        raise HTTPException(status_code=404, detail=f"Run not found: {run_id}")
    if request is not None:
        scope = _resolve_user_scope(request)
        _require_project_access(scope, hdr.get("project_id"))
    return hdr


def _artifact_executor_for(hdr: dict):
    executor_name = str(hdr.get("executor") or "local").strip().lower()
    if executor_name == "slurm":
        return SlurmExecutor(env_config={}, repo_root=Path(".").resolve(), dry_run=True)
    return LocalExecutor()


def _resolve_artifact_dir(hdr: dict) -> str:
    raw = (hdr.get("artifact_dir") or "").strip()
    if not raw:
        raise HTTPException(status_code=400, detail="Run has no artifact_dir recorded.")
    return raw


@app.get("/api/runs/{run_id}/files")
def api_run_files(run_id: str, request: Request) -> dict:
    hdr = _resolve_run_header(run_id, request=request)
    artifact_dir = _resolve_artifact_dir(hdr)
    ex = _artifact_executor_for(hdr)
    try:
        return ex.artifact_tree(artifact_dir)
    except HTTPException:
        raise
    except Exception as exc:  # noqa: BLE001
        raise HTTPException(status_code=500, detail=f"Failed to build file tree: {exc}") from exc


@app.get("/api/runs/{run_id}/file")
def api_run_file(run_id: str, request: Request, path: str = Query(default="")) -> dict:
    hdr = _resolve_run_header(run_id, request=request)
    artifact_dir = _resolve_artifact_dir(hdr)
    ex = _artifact_executor_for(hdr)
    try:
        return ex.artifact_file(artifact_dir, path, max_bytes=MAX_FILE_VIEW_BYTES)
    except Exception as exc:  # noqa: BLE001
        detail = str(exc)
        if "not found" in detail.lower():
            raise HTTPException(status_code=404, detail=detail) from exc
        if "invalid" in detail.lower():
            raise HTTPException(status_code=400, detail=detail) from exc
        raise HTTPException(status_code=500, detail=f"Failed to read file: {exc}") from exc


@app.get("/api/runs/{run_id}/live-log")
def api_run_live_log(run_id: str, request: Request, limit: int = Query(default=200, ge=1, le=2000)) -> dict:
    hdr = None
    try:
        hdr = fetch_run_header(run_id)
    except WebQueryError:
        hdr = None

    scope = _resolve_user_scope(request)
    if hdr is not None:
        _require_project_access(scope, hdr.get("project_id"))
    else:
        with _LOCAL_RUN_LOCK:
            snap = dict(_LOCAL_RUN_SNAPSHOT.get(run_id) or {})
        if not snap:
            raise HTTPException(status_code=404, detail=f"Run not found: {run_id}")
        _require_project_access(scope, snap.get("project_id"))

    with _LOCAL_RUN_LOCK:
        snap = dict(_LOCAL_RUN_SNAPSHOT.get(run_id) or {})
        ring_lines = list(_LOCAL_RUN_LOG_RING.get(run_id) or [])
        state = str(snap.get("state") or "")
        log_file = str(snap.get("log_file") or "").strip()
    lines = ring_lines[-limit:] if ring_lines else []
    if log_file:
        file_lines = _tail_text_lines(Path(log_file), limit=limit)
        if file_lines:
            lines = file_lines

    return {
        "run_id": run_id,
        "state": state,
        "log_file": log_file or None,
        "lines": lines,
    }


@app.post("/api/runs/{run_id}/stop")
def api_stop_run(run_id: str, request: Request, payload: Optional[dict[str, Any]] = Body(default=None)) -> dict:
    scope = _resolve_user_scope(request)
    payload = payload or {}
    try:
        hdr = fetch_run_header(run_id)
    except WebQueryError as exc:
        raise HTTPException(status_code=503, detail=str(exc)) from exc
    if hdr is None:
        raise HTTPException(status_code=404, detail=f"Run not found: {run_id}")
    _require_project_access(scope, hdr.get("project_id"))

    executor_name = str(payload.get("executor") or hdr.get("executor") or "local").strip().lower()
    if executor_name != "local":
        raise HTTPException(
            status_code=400,
            detail="UI stop currently supports executor=local only.",
        )

    pipeline = str(hdr.get("pipeline") or "")
    project_id = hdr.get("project_id")
    with _LOCAL_RUN_LOCK:
        snap = dict(_LOCAL_RUN_SNAPSHOT.get(run_id) or {})
        future = _LOCAL_RUN_FUTURES.get(run_id)
        state = str(snap.get("state") or "").strip().lower()

    if not state:
        raise HTTPException(
            status_code=409,
            detail="Run is not managed by this web process (or already finished); stop unavailable.",
        )
    if state in {"succeeded", "failed", "cancelled"}:
        return {"run_id": run_id, "state": state, "message": f"Run already terminal: {state}."}

    if state == "queued":
        with _LOCAL_RUN_LOCK:
            _LOCAL_RUN_CANCEL_REQUESTED.add(run_id)
        cancelled_now = bool(future.cancel()) if future is not None else False
        if cancelled_now:
            _set_local_run_snapshot(run_id, state="cancelled", message="Cancelled before execution started.")
            _append_local_run_log(run_id, "Run cancelled before execution started.", "WARN")
            _release_local_run(run_id)
            try:
                upsert_run_status(
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

        # Future already started and will observe cancel flag at worker start.
        _set_local_run_snapshot(run_id, state="cancel_requested", message="Cancel requested; waiting for worker handoff.")
        _append_local_run_log(run_id, "Cancel requested while queued.", "WARN")
        return {"run_id": run_id, "state": "cancel_requested", "message": "Cancel requested."}

    # running/cancel_requested: cannot safely interrupt in-process execution yet.
    with _LOCAL_RUN_LOCK:
        _LOCAL_RUN_CANCEL_REQUESTED.add(run_id)
    _set_local_run_snapshot(run_id, state="cancel_requested", message="Stop requested; local in-process run cannot be force-killed yet.")
    _append_local_run_log(run_id, "Stop requested while running. Waiting for cooperative stop support.", "WARN")
    try:
        upsert_run_status(
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


@app.post("/api/runs/{run_id}/resume")
def api_resume_run(run_id: str, request: Request, payload: Optional[dict[str, Any]] = Body(default=None)) -> dict:
    scope = _resolve_user_scope(request)
    payload = payload or {}
    try:
        hdr = fetch_run_header(run_id)
    except WebQueryError as exc:
        raise HTTPException(status_code=503, detail=str(exc)) from exc
    if hdr is None:
        raise HTTPException(status_code=404, detail=f"Run not found: {run_id}")
    _require_project_access(scope, hdr.get("project_id"))

    executor_name = (payload.get("executor") or hdr.get("executor") or "local").strip().lower()
    if executor_name != "local":
        raise HTTPException(
            status_code=400,
            detail="UI resume currently supports executor=local only. Use CLI for SLURM resume.",
        )

    pipeline_path = Path(hdr["pipeline"])
    if not pipeline_path.exists():
        raise HTTPException(status_code=400, detail=f"Pipeline path not found: {pipeline_path}")

    plugins_dir = Path(payload.get("plugins_dir") or "plugins")
    workdir = Path(payload.get("workdir") or ".runs")
    max_retries = int(payload.get("max_retries", 0) or 0)
    retry_delay_seconds = float(payload.get("retry_delay_seconds", 0.0) or 0.0)
    execution_source = str(payload.get("execution_source") or "auto").strip().lower()
    source_bundle = str(payload.get("source_bundle") or "").strip() or None
    source_snapshot = str(payload.get("source_snapshot") or "").strip() or None
    allow_workspace_source = _parse_bool(payload.get("allow_workspace_source"), default=False)

    try:
        pipeline = parse_pipeline(pipeline_path, global_vars={}, env_vars={})
    except (PipelineError, FileNotFoundError) as exc:
        raise HTTPException(status_code=400, detail=f"Invalid pipeline for resume: {exc}") from exc

    provenance = collect_run_provenance(
        repo_root=Path(".").resolve(),
        pipeline_path=pipeline_path,
        global_config_path=None,
        environments_config_path=None,
        plugin_dir=plugins_dir,
        pipeline=pipeline,
        cli_command=f"etl web resume {run_id}",
    )
    ex = LocalExecutor(
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

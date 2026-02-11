from __future__ import annotations

import tempfile
import json
import shlex
from pathlib import Path
from typing import Any, Optional

import psycopg
from fastapi import Body, FastAPI, HTTPException, Query
from fastapi.responses import HTMLResponse

from .config import ConfigError, load_global_config
from .ai_pipeline import AIPipelineError, generate_pipeline_draft
from .db import get_database_url
from .execution_config import (
    ExecutionConfigError,
    apply_execution_env_overrides,
    load_execution_config,
    resolve_execution_config_path,
    validate_environment_executor,
)
from .executors.local import LocalExecutor
from .executors.slurm import SlurmExecutor
from .pipeline import Pipeline, Step, parse_pipeline, PipelineError
from .provenance import collect_run_provenance
from .plugins.base import PluginLoadError, load_plugin
from .runner import run_pipeline
from .web_queries import (
    WebQueryError,
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


INDEX_HTML = """<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8" />
  <meta name="viewport" content="width=device-width, initial-scale=1" />
  <title>Research ETL UI</title>
  <style>
    :root { --bg:#f5f7fb; --panel:#ffffff; --ink:#13223a; --muted:#5f6e86; --ok:#0a8f57; --bad:#b42318; --line:#dbe2ef; }
    body { margin:0; font-family:"Segoe UI",Tahoma,sans-serif; color:var(--ink); background:linear-gradient(160deg,#eef3ff,#f9fbff); }
    .wrap { max-width:1200px; margin:24px auto; padding:0 16px; }
    .topnav { display:flex; align-items:center; justify-content:space-between; gap:8px; margin-bottom:8px; padding:6px 8px; background:#f7f9ff; border:1px solid var(--line); border-radius:8px; }
    .topnav .links { display:flex; gap:6px; flex-wrap:wrap; }
    .topnav a { text-decoration:none; color:#274066; border:1px solid var(--line); border-radius:999px; padding:3px 8px; font-size:12px; line-height:1.2; background:#fff; }
    .topnav a.active { background:#0d3b8e; color:#fff; border-color:#0d3b8e; }
    .topnav a.context { background:#eef3ff; border-style:dashed; }
    .topnav .jump { display:flex; gap:6px; align-items:center; }
    .topnav .jump input { width:180px; padding:4px 6px; font-size:12px; }
    .topnav .jump button { padding:4px 8px; font-size:12px; }
    .head { display:flex; justify-content:space-between; align-items:center; margin-bottom:14px; gap:10px; flex-wrap:wrap; }
    h1 { margin:0; font-size:24px; }
    .muted { color:var(--muted); font-size:13px; }
    .grid { display:grid; grid-template-columns: 1fr 1fr; gap:14px; }
    body.builder-mode .grid { grid-template-columns: 1fr; }
    body.builder-mode .grid > section:first-child { display:none; }
    body.builder-mode .grid > section:last-child { max-width:980px; width:100%; margin:0 auto; }
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
    .builder-surface { display:grid; grid-template-columns: 3fr 2fr; gap:10px; }
    .builder-card { border:1px solid var(--line); border-radius:8px; background:#fafcff; padding:10px; }
    .builder-card h4 { margin:0 0 8px 0; font-size:14px; }
    .builder-head { display:flex; align-items:center; justify-content:space-between; gap:8px; margin:0 0 8px 0; }
    .builder-head h4 { margin:0; }
    .builder-list { display:grid; gap:8px; margin-bottom:10px; }
    .builder-item { border:1px dashed var(--line); border-radius:8px; padding:8px; background:#fff; }
    .builder-item .controls { margin-bottom:6px; }
    .builder-item h5 { margin:0 0 6px 0; font-size:13px; color:#334e73; }
    .step-head { display:flex; align-items:center; justify-content:space-between; gap:8px; margin-bottom:6px; }
    .status-pill { display:inline-block; border-radius:999px; padding:2px 8px; font-size:11px; border:1px solid var(--line); background:#f4f7ff; color:#3b4f70; text-transform:lowercase; }
    .status-pill.not-run { background:#f3f4f6; color:#4b5563; border-color:#d1d5db; }
    .status-pill.valid { background:#ecfdf3; color:#0a8f57; border-color:#b7ebcf; }
    .status-pill.failed { background:#fff1f1; color:#b42318; border-color:#f3c6c6; }
    .status-pill.successful { background:#e7f6ff; color:#0b6fb3; border-color:#bfe3fa; }
    .spin-btn { position:relative; min-width:96px; }
    .spin-btn.loading { color:transparent; }
    .spin-btn .spin { display:none; position:absolute; right:10px; top:50%; width:13px; height:13px; margin-top:-6.5px; border:2px solid rgba(255,255,255,.55); border-top-color:#fff; border-radius:50%; animation:spin .8s linear infinite; }
    .spin-btn.loading .spin { display:block; }
    @keyframes spin { to { transform:rotate(360deg); } }
    @media (max-width: 1100px) { .builder-surface { grid-template-columns: 1fr; } }
    @media (max-width: 960px) { .grid { grid-template-columns: 1fr; } }
  </style>
</head>
<body>
  <div class="wrap">
    <nav class="topnav">
      <div class="links">
        <a id="nav_ops" href="/">Operations</a>
        <a id="nav_pipelines" href="/pipelines">Pipelines</a>
        <a id="nav_new_pipeline" href="/pipelines/new">New Pipeline</a>
        <a id="nav_context_back" class="context" href="#" style="display:none;">Back</a>
      </div>
      <div class="jump">
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
        <div id="builder_panel" style="display:none;">
          <div class="builder-surface">
            <div class="builder-card">
              <div class="builder-head">
                <h4>Pipeline Config</h4>
                <span id="builder_pipeline_status" class="status-pill not-run">not run</span>
              </div>
              <div class="controls">
                <input id="b_pipeline_path" placeholder="pipeline name (stored under pipelines/)" />
                <button id="btn_builder_load">Load</button>
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
                <input id="b_global_config" placeholder="global_config (optional)" />
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
              <h4>YAML Preview (read-only)</h4>
              <textarea id="b_yaml" readonly style="width:100%; min-height:420px; font-family:Consolas,monospace; font-size:12px;"></textarea>
              <h4>Builder Output</h4>
              <pre id="builder_output">No draft action yet.</pre>
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
  <script>
    let selected = null;
    let selectedPipeline = null;
    const isPipelinesView = window.location.pathname.startsWith("/pipelines");
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
    let builderLoaded = false;
    let builderModel = { vars: {}, dirs: {}, requires_pipelines: [], steps: [] };
    let builderPlugins = [];
    let builderPluginMeta = {};
    let builderValidationState = "unknown";
    let builderStepStatus = {};
    let builderStepTesting = {};
    let builderPipelineRunState = "not-run";
    let builderPipelineRunning = false;
    function defaultBuilderDirs(){
      return {
        workdir: "{workdir}/{pipe.name}/{run_id}",
        logdir: "{logdir}/{pipe.name}/{run_id}",
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
      const newp = document.getElementById("nav_new_pipeline");
      const back = document.getElementById("nav_context_back");
      [ops, pipes, newp].forEach(el => el.classList.remove("active"));
      back.style.display = "none";
      if (path === "/") {
        ops.classList.add("active");
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
      if(isPipelinesView){
        document.getElementById("page_title").textContent = "Research ETL Pipelines";
        document.getElementById("ops_panel").style.display = "none";
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
        return `
          <div class="node file" data-op="view" data-id="${esc(r.run_id)}">
            <div><b>${esc(r.run_id)}</b> <span class="${r.success ? "ok" : "bad"}">${esc(r.status)}</span></div>
            <div class="muted">${esc(r.pipeline)} | ${esc(r.executor)}</div>
            <div class="controls">
              <button data-op="view" data-id="${esc(r.run_id)}">View</button>
              ${resumeBtn}
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
        return payload.detail || payload.message || txt;
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
        lines.push("  - script: echo.py");
      } else {
        for(const st of steps){
          lines.push(`  - script: ${_yamlEsc(_scriptFromStep(st))}`);
          if ((st.type || "sequential") === "parallel" && (st.parallel_with || "").trim()) {
            lines.push(`    parallel_with: ${_yamlEsc(st.parallel_with)}`);
          }
          if ((st.type || "sequential") === "foreach" && (st.foreach || "").trim()) {
            lines.push(`    foreach: ${_yamlEsc(st.foreach)}`);
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
        builderValidationState = "unknown";
        builderStepStatus = {};
        builderStepTesting = {};
        builderPipelineRunState = "not-run";
      }
      renderBuilderPipelineStatus();
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
    function builderPayload(){
      const body = { yaml_text: document.getElementById("b_yaml").value || "" };
      const pipeline = normalizeBuilderPipelineName(document.getElementById("b_pipeline_path").value.trim());
      const intent = document.getElementById("b_intent").value.trim();
      const constraints = document.getElementById("b_constraints").value.trim();
      const globalConfig = document.getElementById("b_global_config").value.trim();
      const workdir = deriveBuilderWorkdir();
      const retries = document.getElementById("b_max_retries").value.trim();
      const delay = document.getElementById("b_retry_delay").value.trim();
      if (pipeline) body.pipeline = pipeline;
      if (intent) body.intent = intent;
      if (constraints) body.constraints = constraints;
      if (globalConfig) body.global_config = globalConfig;
      if (workdir) body.workdir = workdir;
      if (retries) body.max_retries = Number(retries);
      if (delay) body.retry_delay_seconds = Number(delay);
      body.dry_run = document.getElementById("b_dry_run").checked;
      return body;
    }
    async function loadBuilderPlugins(){
      if(!isBuilderView) return;
      const gc = document.getElementById("b_global_config").value.trim();
      const qp = new URLSearchParams();
      if(gc) qp.set("global_config", gc);
      const res = await fetch(`/api/builder/plugins?${qp.toString()}`);
      if(!res.ok){
        document.getElementById("builder_msg").textContent = await readMessage(res);
        return;
      }
      const payload = await res.json();
      builderPlugins = payload.plugins || [];
      builderPluginMeta = {};
      for(const p of builderPlugins){ builderPluginMeta[p.path] = p; }
      renderBuilderModel();
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
      builderModel.steps.push({ type:"sequential", plugin:firstPlugin, params:{}, output_var:"", when:"", parallel_with:"", foreach:"" });
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
      steps.forEach((st, idx) => {
        const meta = builderPluginMeta[st.plugin] || {params:{}};
        const pluginOptions = builderPlugins.map(p => `<option value="${esc(p.path)}" ${p.path===st.plugin?"selected":""}>${esc(p.path)}</option>`).join("");
        const type = st.type || "sequential";
        let paramsHtml = "";
        for(const [pk, pspec] of Object.entries(meta.params || {})){
          const ptype = (pspec && (pspec.type || pspec["type"])) || "str";
          const pval = (st.params || {})[pk] ?? "";
          if(String(ptype).toLowerCase() === "bool"){
            paramsHtml += `<label class="muted"><input type="checkbox" data-kind="step-param-bool" data-idx="${idx}" data-param="${esc(pk)}" ${String(pval).toLowerCase()==="true"||pval===true?"checked":""} /> ${esc(pk)}</label>`;
          } else {
            paramsHtml += `<input data-kind="step-param" data-idx="${idx}" data-param="${esc(pk)}" value="${esc(pval)}" placeholder="${esc(pk)} (${esc(ptype)})" />`;
          }
        }
        const card = document.createElement("div");
        card.className = "builder-item";
        const badge = stepStatusMeta(idx);
        const loading = !!builderStepTesting[idx];
        let typeSpecificHtml = "";
        if(type === "parallel"){
          typeSpecificHtml = `<div class="controls"><input data-kind="step-parallel" data-idx="${idx}" value="${esc(st.parallel_with || "")}" placeholder="parallel_with group key" /></div>`;
        } else if (type === "foreach"){
          typeSpecificHtml = `<div class="controls"><input data-kind="step-foreach" data-idx="${idx}" value="${esc(st.foreach || "")}" placeholder="foreach var name" /></div>`;
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
            <button data-del-step="${idx}">Remove Step</button>
          </div>
          <div class="controls">${paramsHtml || '<span class="muted">No plugin params</span>'}</div>
          <div class="controls">
            <input data-kind="step-output" data-idx="${idx}" value="${esc(st.output_var || "")}" placeholder="output_var (optional)" />
            <input data-kind="step-when" data-idx="${idx}" value="${esc(st.when || "")}" placeholder="when (optional)" />
          </div>
          ${typeSpecificHtml}
        `;
        stepsEl.appendChild(card);
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
          renderBuilderModel();
          changed = true;
        }
        if(kind === "step-plugin"){ st.plugin = t.value; st.params = {}; renderBuilderModel(); changed = true; }
        if(kind === "step-output"){ st.output_var = t.value; changed = true; }
        if(kind === "step-when"){ st.when = t.value; changed = true; }
        if(kind === "step-parallel"){ st.parallel_with = t.value; changed = true; }
        if(kind === "step-foreach"){ st.foreach = t.value; changed = true; }
        if(kind === "step-param"){
          st.params = st.params || {};
          st.params[t.getAttribute("data-param")] = t.value;
          changed = true;
        }
        if(kind === "step-param-bool"){
          st.params = st.params || {};
          st.params[t.getAttribute("data-param")] = t.checked ? "true" : "false";
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
      const delReq = t.getAttribute("data-del-req");
      if (delReq !== null){ builderModel.requires_pipelines.splice(Number(delReq),1); renderBuilderModel(); syncYamlPreview(); return; }
      const delVar = t.getAttribute("data-del-var");
      if (delVar !== null){ delete builderModel.vars[delVar]; renderBuilderModel(); syncYamlPreview(); return; }
      const delDir = t.getAttribute("data-del-dir");
      if (delDir !== null){ delete builderModel.dirs[delDir]; renderBuilderModel(); syncYamlPreview(); return; }
      const delStep = t.getAttribute("data-del-step");
      if (delStep !== null){ builderModel.steps.splice(Number(delStep),1); renderBuilderModel(); syncYamlPreview(); return; }
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
      builderLoaded = false;
      await loadBuilderSource();
    }
    async function loadBuilderSource(){
      if(!isBuilderView || builderLoaded) return;
      let pipeline = normalizeBuilderPipelineName(document.getElementById("b_pipeline_path").value.trim());
      if(!pipeline){
        pipeline = await suggestNewPipelineName();
        document.getElementById("b_pipeline_path").value = pipeline;
        builderModel = ensureBuilderDefaultDirs({ vars: { env: "{env.name}" }, dirs: {}, requires_pipelines: [], steps: [] });
        await loadBuilderPlugins();
        renderBuilderModel();
        syncYamlPreview();
        builderLoaded = true;
        return;
      }
      document.getElementById("b_pipeline_path").value = pipeline;
      const res = await fetch(`/api/builder/source?pipeline=${encodeURIComponent(pipeline)}`);
      if(!res.ok){
        document.getElementById("builder_msg").textContent = await readMessage(res);
        builderModel = ensureBuilderDefaultDirs({ vars: { env: "{env.name}" }, dirs: {}, requires_pipelines: [], steps: [] });
      } else {
        const payload = await res.json();
        builderModel = ensureBuilderDefaultDirs(payload.model || { vars: {}, dirs: {}, requires_pipelines: [], steps: [] });
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
      if(payload.global_config) runBody.global_config = payload.global_config;
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
      renderBuilderModel();
      syncYamlPreview();
      builderValidationState = data.valid ? "valid" : "unknown";
      renderBuilderModel();
      renderBuilderPipelineStatus();
      msg.textContent = data.valid ? `Generated valid draft (${data.step_count} steps)` : "Generated draft has validation issues";
      out.textContent = JSON.stringify(data, null, 2);
    }
    async function validateBuilderDraft(){
      const msg = document.getElementById("builder_msg");
      const out = document.getElementById("builder_output");
      msg.textContent = "Validating draft...";
      const res = await fetch(`/api/builder/validate`, {
        method:"POST",
        headers: {"Content-Type":"application/json"},
        body: JSON.stringify(builderPayload()),
      });
      if(!res.ok){
        builderValidationState = "unknown";
        builderPipelineRunState = "failed";
        msg.textContent = await readMessage(res);
        renderBuilderModel();
        renderBuilderPipelineStatus();
        return;
      }
      const payload = await res.json();
      builderValidationState = "valid";
      if(builderPipelineRunState !== "run_ok"){
        builderPipelineRunState = "not-run";
      }
      msg.textContent = `Valid draft: ${payload.step_count} steps`;
      out.textContent = JSON.stringify(payload, null, 2);
      renderBuilderModel();
      renderBuilderPipelineStatus();
    }
    async function testBuilderStepAt(idx){
      const msg = document.getElementById("builder_msg");
      const out = document.getElementById("builder_output");
      builderStepTesting[idx] = true;
      renderBuilderModel();
      msg.textContent = `Validating draft before step ${idx + 1} test...`;
      const validateRes = await fetch(`/api/builder/validate`, {
        method:"POST",
        headers: {"Content-Type":"application/json"},
        body: JSON.stringify(builderPayload()),
      });
      if(!validateRes.ok){
        builderValidationState = "unknown";
        delete builderStepTesting[idx];
        renderBuilderModel();
        msg.textContent = await readMessage(validateRes);
        return;
      }
      builderValidationState = "valid";
      msg.textContent = `Testing step ${idx + 1}...`;
      const payload = builderPayload();
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
      delete builderStepTesting[idx];
      renderBuilderModel();
      msg.textContent = `Step ${data.step_name}: ${data.success ? "successful" : "failed"}`;
      out.textContent = JSON.stringify(data, null, 2);
    }
    async function loadRuns(){
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
    async function loadDetail(){
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
      `;
      html += `
        <h4>Artifacts</h4>
        <div class="filesplit">
          <div class="filetree" id="filetree">Loading files...</div>
          <div class="viewer"><pre id="fileview">Select a file to view content.</pre></div>
        </div>
      `;
      document.getElementById("detail").innerHTML = html;
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
      `;
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
      const res = await fetch(url, {
        method:"POST",
        headers: {"Content-Type":"application/json"},
        body: JSON.stringify(actionPayload()),
      });
      if(!res.ok){ el.textContent = await readMessage(res); return; }
      const payload = await res.json();
      selected = payload.run_id;
      el.textContent = `Run ${payload.run_id} (${payload.state})`;
      await tick();
    }
    async function tick(){
      if(isBuilderView){
        await loadBuilderSource();
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
    }
    initViewMode();
    setActiveNav();
    document.getElementById("btn_apply").onclick = tick;
    document.getElementById("btn_ops_refresh").onclick = tick;
    document.getElementById("btn_pipelines").onclick = tick;
    document.getElementById("btn_validate").onclick = validateAction;
    document.getElementById("btn_run").onclick = runAction;
    document.getElementById("btn_resume").onclick = resumeSelected;
    document.getElementById("btn_nav_live").onclick = () => {
      const runId = document.getElementById("nav_live_id").value.trim();
      if (!runId) return;
      window.location.href = `/runs/${encodeURIComponent(runId)}/live`;
    };
    document.getElementById("btn_builder_load").onclick = () => { openBuilderFilePicker(); };
    document.getElementById("btn_builder_add_req").onclick = addBuilderRequire;
    document.getElementById("btn_builder_add_var").onclick = addBuilderVar;
    document.getElementById("btn_builder_add_dir").onclick = addBuilderDir;
    document.getElementById("btn_builder_add_step").onclick = addBuilderStep;
    document.getElementById("btn_builder_save").onclick = saveBuilderDraft;
    document.getElementById("btn_builder_generate").onclick = generateBuilderDraft;
    document.getElementById("btn_builder_validate").onclick = validateBuilderDraft;
    document.getElementById("btn_builder_run").onclick = runBuilderPipeline;
    document.getElementById("b_requires").addEventListener("input", handleBuilderInput);
    document.getElementById("b_vars").addEventListener("input", handleBuilderInput);
    document.getElementById("b_dirs").addEventListener("input", handleBuilderInput);
    document.getElementById("b_vars").addEventListener("change", handleBuilderInput);
    document.getElementById("b_dirs").addEventListener("change", handleBuilderInput);
    document.getElementById("b_steps").addEventListener("input", handleBuilderInput);
    document.getElementById("b_steps").addEventListener("change", handleBuilderInput);
    document.getElementById("b_requires").addEventListener("click", handleBuilderClicks);
    document.getElementById("b_vars").addEventListener("click", handleBuilderClicks);
    document.getElementById("b_dirs").addEventListener("click", handleBuilderClicks);
    document.getElementById("b_steps").addEventListener("click", handleBuilderClicks);
    document.getElementById("b_global_config").addEventListener("change", async () => { await loadBuilderPlugins(); });
    document.getElementById("b_file_picker").addEventListener("change", async () => { await loadBuilderSourceFromFilePicker(); });
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


@app.get("/api/pipelines")
def api_pipelines(
    limit: int = Query(default=100, ge=1, le=500),
    q: Optional[str] = Query(default=None),
) -> list[dict]:
    try:
        return fetch_pipelines(limit=limit, q=q)
    except WebQueryError as exc:
        raise HTTPException(status_code=503, detail=str(exc)) from exc


@app.get("/api/pipelines/{pipeline_id:path}/runs")
def api_pipeline_runs(
    pipeline_id: str,
    limit: int = Query(default=50, ge=1, le=500),
    status: Optional[str] = Query(default=None),
    executor: Optional[str] = Query(default=None),
) -> list[dict]:
    try:
        return fetch_pipeline_runs(pipeline_id, limit=limit, status=status, executor=executor)
    except WebQueryError as exc:
        raise HTTPException(status_code=503, detail=str(exc)) from exc


@app.get("/api/pipelines/{pipeline_id:path}/validations")
def api_pipeline_validations(
    pipeline_id: str,
    limit: int = Query(default=50, ge=1, le=500),
) -> list[dict]:
    try:
        return fetch_pipeline_validations(pipeline_id, limit=limit)
    except WebQueryError as exc:
        raise HTTPException(status_code=503, detail=str(exc)) from exc


@app.get("/api/pipelines/{pipeline_id:path}")
def api_pipeline_detail(pipeline_id: str) -> dict:
    try:
        payload = fetch_pipeline_detail(pipeline_id)
    except WebQueryError as exc:
        raise HTTPException(status_code=503, detail=str(exc)) from exc
    if payload is None:
        raise HTTPException(status_code=404, detail=f"Pipeline not found: {pipeline_id}")
    return payload


@app.get("/api/runs")
def api_runs(
    limit: int = Query(default=50, ge=1, le=500),
    status: Optional[str] = Query(default=None),
    executor: Optional[str] = Query(default=None),
    q: Optional[str] = Query(default=None),
) -> list[dict]:
    try:
        return fetch_runs(limit=limit, status=status, executor=executor, q=q)
    except WebQueryError as exc:
        raise HTTPException(status_code=503, detail=str(exc)) from exc


@app.get("/api/runs/{run_id}/live")
def api_run_live(run_id: str) -> dict:
    try:
        payload = fetch_run_detail(run_id)
    except WebQueryError as exc:
        raise HTTPException(status_code=503, detail=str(exc)) from exc
    if payload is None:
        raise HTTPException(status_code=404, detail=f"Run not found: {run_id}")

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
def api_run_detail(run_id: str) -> dict:
    try:
        payload = fetch_run_detail(run_id)
    except WebQueryError as exc:
        raise HTTPException(status_code=503, detail=str(exc)) from exc
    if payload is None:
        raise HTTPException(status_code=404, detail=f"Run not found: {run_id}")
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

    return {
        "payload": payload,
        "pipeline_path": pipeline_path,
        "global_config_path": global_config_path,
        "environments_config_path": environments_config_path,
        "env_name": env_name,
        "executor": executor,
        "plugins_dir": Path(payload.get("plugins_dir") or "plugins"),
        "workdir": Path(payload.get("workdir") or ".runs"),
        "dry_run": _parse_bool(payload.get("dry_run"), default=False),
        "verbose": _parse_bool(payload.get("verbose"), default=False),
        "max_retries": max_retries,
        "retry_delay_seconds": retry_delay_seconds,
        "execution_source": execution_source,
        "source_bundle": str(payload.get("source_bundle") or "").strip() or None,
        "source_snapshot": str(payload.get("source_snapshot") or "").strip() or None,
        "allow_workspace_source": _parse_bool(payload.get("allow_workspace_source"), default=False),
    }


def _resolve_global_vars(global_config_path: Optional[Path]) -> dict[str, Any]:
    if not global_config_path:
        return {}
    try:
        return load_global_config(global_config_path)
    except ConfigError as exc:
        raise HTTPException(status_code=400, detail=f"Global config error: {exc}") from exc


def _resolve_execution_env(
    environments_config_path: Optional[Path],
    env_name: Optional[str],
    *,
    executor: str,
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
        return apply_execution_env_overrides(env), resolved, selected_env_name
    except HTTPException:
        raise
    except ExecutionConfigError as exc:
        raise HTTPException(status_code=400, detail=f"Environments config error: {exc}") from exc


def _parse_pipeline_from_yaml_text(
    yaml_text: str,
    *,
    global_config_path: Optional[Path],
) -> Pipeline:
    if not (yaml_text or "").strip():
        raise HTTPException(status_code=400, detail="`yaml_text` is required.")
    global_vars = _resolve_global_vars(global_config_path)
    with tempfile.NamedTemporaryFile("w", suffix=".yml", delete=False, encoding="utf-8") as tmp:
        tmp.write(yaml_text)
        tmp_path = Path(tmp.name)
    try:
        return parse_pipeline(tmp_path, global_vars=global_vars)
    except (PipelineError, FileNotFoundError) as exc:
        raise HTTPException(status_code=400, detail=f"Invalid draft pipeline: {exc}") from exc
    finally:
        try:
            tmp_path.unlink(missing_ok=True)
        except Exception:
            pass


def _resolve_builder_plugins_dir(*, global_config_path: Optional[Path], plugins_dir: Optional[str]) -> Path:
    if plugins_dir and str(plugins_dir).strip():
        return Path(str(plugins_dir).strip()).expanduser()
    global_vars = _resolve_global_vars(global_config_path)
    cfg_plugins = str(global_vars.get("plugins_dir") or "").strip()
    if cfg_plugins:
        return Path(cfg_plugins).expanduser()
    return Path("plugins")


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
            plugin_ref, params = _parse_step_script_for_builder(str(step_map.get("script") or ""))
            stype = "sequential"
            if step_map.get("foreach"):
                stype = "foreach"
            elif step_map.get("parallel_with"):
                stype = "parallel"
            model_steps.append(
                {
                    "type": stype,
                    "plugin": plugin_ref,
                    "params": params,
                    "output_var": str(step_map.get("output_var") or ""),
                    "when": str(step_map.get("when") or ""),
                    "parallel_with": str(step_map.get("parallel_with") or ""),
                    "foreach": str(step_map.get("foreach") or ""),
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


def _record_pipeline_validation(
    *,
    pipeline: str,
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
                        pipeline, valid, step_count, step_names_json, error, source
                    )
                    VALUES (%s, %s, %s, %s::jsonb, %s, %s)
                    """,
                    (
                        pipeline,
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
def api_pipelines_create(payload: Optional[dict[str, Any]] = Body(default=None)) -> dict:
    payload = payload or {}
    pipeline_path = _resolve_repo_relative_pipeline_path(str(payload.get("pipeline") or ""))
    yaml_text = str(payload.get("yaml_text") or "")
    _ = _parse_pipeline_from_yaml_text(yaml_text, global_config_path=None)
    overwrite = _parse_bool(payload.get("overwrite"), default=False)
    if pipeline_path.exists() and not overwrite:
        raise HTTPException(status_code=409, detail=f"Pipeline already exists: {pipeline_path}")
    pipeline_path.parent.mkdir(parents=True, exist_ok=True)
    pipeline_path.write_text(yaml_text.strip() + "\n", encoding="utf-8")
    return {"pipeline": str(pipeline_path), "saved": True, "created": True}


@app.put("/api/pipelines/{pipeline_id:path}")
def api_pipelines_update(pipeline_id: str, payload: Optional[dict[str, Any]] = Body(default=None)) -> dict:
    payload = payload or {}
    if "pipeline" in payload and str(payload.get("pipeline") or "").strip() not in {"", pipeline_id}:
        raise HTTPException(status_code=400, detail="Payload pipeline does not match URL pipeline_id.")
    pipeline_path = _resolve_repo_relative_pipeline_path(pipeline_id)
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
        parts = list(path.parts)
        if parts and parts[0].lower() == "pipelines":
            path = Path(*parts[1:]) if len(parts) > 1 else Path("")
        if path.suffix.lower() not in {".yml", ".yaml"}:
            path = path.with_suffix(".yml")
        path = (pipelines_root / path).resolve()
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
                    "description": pd.meta.description,
                    "params": pd.meta.params or {},
                }
            )
        except PluginLoadError:
            entries.append({"path": rel, "name": rel, "description": "unloadable plugin", "params": {}})
    return {"plugins_dir": str(root), "plugins": entries}


@app.post("/api/builder/validate")
def api_builder_validate(payload: Optional[dict[str, Any]] = Body(default=None)) -> dict:
    payload = payload or {}
    pipeline_label = str(payload.get("pipeline") or "<builder:draft>")
    global_config_raw = str(payload.get("global_config") or "").strip()
    global_config_path = Path(global_config_raw).expanduser() if global_config_raw else None
    try:
        pipeline = _parse_pipeline_from_yaml_text(str(payload.get("yaml_text") or ""), global_config_path=global_config_path)
        _validate_pipeline_dir_contract(pipeline)
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
    global_config_raw = str(payload.get("global_config") or "").strip()
    global_config_path = Path(global_config_raw).expanduser() if global_config_raw else None
    pipeline = _parse_pipeline_from_yaml_text(str(payload.get("yaml_text") or ""), global_config_path=global_config_path)
    _validate_pipeline_dir_contract(pipeline)
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
    workdir = Path(workdir_raw or ".runs/builder")
    dry_run = _parse_bool(payload.get("dry_run"), default=False)
    max_retries = int(payload.get("max_retries", 0) or 0)
    retry_delay_seconds = float(payload.get("retry_delay_seconds", 0.0) or 0.0)
    if max_retries < 0 or retry_delay_seconds < 0:
        raise HTTPException(status_code=400, detail="Retry settings must be >= 0.")

    mini = Pipeline(vars=dict(pipeline.vars), dirs=dict(pipeline.dirs), steps=[target_step])
    try:
        result = run_pipeline(
            mini,
            plugin_dir=plugins_dir,
            workdir=workdir,
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
def api_action_validate(payload: Optional[dict[str, Any]] = Body(default=None)) -> dict:
    args = _parse_action_payload(payload)
    global_vars = _resolve_global_vars(args["global_config_path"])
    execution_env, environments_config_path, env_name = _resolve_execution_env(
        args["environments_config_path"],
        args["env_name"],
        executor=args["executor"],
    )
    try:
        pipeline = parse_pipeline(args["pipeline_path"], global_vars=global_vars, env_vars=execution_env)
    except (PipelineError, FileNotFoundError) as exc:
        _record_pipeline_validation(
            pipeline=str(args["pipeline_path"]),
            valid=False,
            step_count=0,
            step_names=[],
            error=str(exc),
            source="api_validate",
        )
        raise HTTPException(status_code=400, detail=f"Invalid pipeline: {exc}") from exc
    _record_pipeline_validation(
        pipeline=str(args["pipeline_path"]),
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
def api_action_run(payload: Optional[dict[str, Any]] = Body(default=None)) -> dict:
    args = _parse_action_payload(payload)
    global_vars = _resolve_global_vars(args["global_config_path"])
    execution_env, environments_config_path, env_name = _resolve_execution_env(
        args["environments_config_path"],
        args["env_name"],
        executor=args["executor"],
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
            workdir=args["workdir"],
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
        ex = LocalExecutor(
            plugin_dir=args["plugins_dir"],
            workdir=args["workdir"],
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
        "job_ids": submit.job_ids or [],
        "message": st.message or submit.message or "",
    }


@app.post("/api/pipelines/{pipeline_id:path}/validate")
def api_pipeline_validate(
    pipeline_id: str,
    payload: Optional[dict[str, Any]] = Body(default=None),
) -> dict:
    return api_action_validate(_payload_with_pipeline(payload, pipeline_id))


@app.post("/api/pipelines/{pipeline_id:path}/run")
def api_pipeline_run(
    pipeline_id: str,
    payload: Optional[dict[str, Any]] = Body(default=None),
) -> dict:
    return api_action_run(_payload_with_pipeline(payload, pipeline_id))


def _resolve_run_header(run_id: str) -> dict:
    try:
        hdr = fetch_run_header(run_id)
    except WebQueryError as exc:
        raise HTTPException(status_code=503, detail=str(exc)) from exc
    if hdr is None:
        raise HTTPException(status_code=404, detail=f"Run not found: {run_id}")
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
def api_run_files(run_id: str) -> dict:
    hdr = _resolve_run_header(run_id)
    artifact_dir = _resolve_artifact_dir(hdr)
    ex = _artifact_executor_for(hdr)
    try:
        return ex.artifact_tree(artifact_dir)
    except HTTPException:
        raise
    except Exception as exc:  # noqa: BLE001
        raise HTTPException(status_code=500, detail=f"Failed to build file tree: {exc}") from exc


@app.get("/api/runs/{run_id}/file")
def api_run_file(run_id: str, path: str = Query(default="")) -> dict:
    hdr = _resolve_run_header(run_id)
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


@app.post("/api/runs/{run_id}/resume")
def api_resume_run(run_id: str, payload: Optional[dict[str, Any]] = Body(default=None)) -> dict:
    payload = payload or {}
    try:
        hdr = fetch_run_header(run_id)
    except WebQueryError as exc:
        raise HTTPException(status_code=503, detail=str(exc)) from exc
    if hdr is None:
        raise HTTPException(status_code=404, detail=f"Run not found: {run_id}")

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

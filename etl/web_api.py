from __future__ import annotations

import tempfile
from pathlib import Path
from typing import Any, Optional

from fastapi import Body, FastAPI, HTTPException, Query
from fastapi.responses import HTMLResponse

from .config import ConfigError, load_global_config
from .execution_config import ExecutionConfigError, apply_execution_env_overrides, load_execution_config
from .executors.local import LocalExecutor
from .executors.slurm import SlurmExecutor
from .pipeline import Pipeline, Step, parse_pipeline, PipelineError
from .provenance import collect_run_provenance
from .runner import run_pipeline
from .web_queries import (
    WebQueryError,
    fetch_pipeline_detail,
    fetch_pipeline_runs,
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
        <div id="builder_panel" style="display:none;">
          <div class="controls">
            <input id="b_pipeline_path" placeholder="pipeline file path (for edit/new context)" />
            <button id="btn_builder_load">Load</button>
            <button id="btn_builder_validate">Validate Draft</button>
            <button id="btn_builder_test_step">Test Step</button>
            <span id="builder_msg" class="muted"></span>
          </div>
          <div class="controls">
            <input id="b_step_name" placeholder="step name to test (default: first step)" />
            <input id="b_global_config" placeholder="global_config (optional)" />
            <input id="b_plugins_dir" placeholder="plugins_dir (default: plugins)" />
            <input id="b_workdir" placeholder="workdir (default: .runs/builder)" />
            <label class="muted"><input type="checkbox" id="b_dry_run" /> dry_run</label>
          </div>
          <div class="controls">
            <input id="b_max_retries" placeholder="max_retries (default: 0)" />
            <input id="b_retry_delay" placeholder="retry_delay_seconds (default: 0.0)" />
          </div>
          <textarea id="b_yaml" style="width:100%; min-height:260px; font-family:Consolas,monospace; font-size:12px;"></textarea>
          <h4>Builder Output</h4>
          <pre id="builder_output">No draft action yet.</pre>
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
          <input id="a_execution_config" placeholder="execution_config (optional)" />
          <input id="a_env" placeholder="env name (when execution_config set)" />
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
        document.getElementById("page_title").textContent = isBuilderNewView ? "Research ETL Pipeline Builder" : "Research ETL Pipeline Editor";
        document.getElementById("left_title").textContent = "Builder Context";
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
      const executionConfig = document.getElementById("a_execution_config").value.trim();
      const env = document.getElementById("a_env").value.trim();
      const pluginsDir = document.getElementById("a_plugins_dir").value.trim();
      const workdir = document.getElementById("a_workdir").value.trim();
      const retries = document.getElementById("a_max_retries").value.trim();
      const delay = document.getElementById("a_retry_delay").value.trim();
      if (globalConfig) body.global_config = globalConfig;
      if (executionConfig) body.execution_config = executionConfig;
      if (env) body.env = env;
      if (pluginsDir) body.plugins_dir = pluginsDir;
      if (workdir) body.workdir = workdir;
      if (retries) body.max_retries = Number(retries);
      if (delay) body.retry_delay_seconds = Number(delay);
      body.dry_run = document.getElementById("a_dry_run").checked;
      body.verbose = document.getElementById("a_verbose").checked;
      return body;
    }
    function builderPayload(){
      const body = {
        yaml_text: document.getElementById("b_yaml").value || "",
      };
      const pipeline = document.getElementById("b_pipeline_path").value.trim();
      const stepName = document.getElementById("b_step_name").value.trim();
      const globalConfig = document.getElementById("b_global_config").value.trim();
      const pluginsDir = document.getElementById("b_plugins_dir").value.trim();
      const workdir = document.getElementById("b_workdir").value.trim();
      const retries = document.getElementById("b_max_retries").value.trim();
      const delay = document.getElementById("b_retry_delay").value.trim();
      if (pipeline) body.pipeline = pipeline;
      if (stepName) body.step_name = stepName;
      if (globalConfig) body.global_config = globalConfig;
      if (pluginsDir) body.plugins_dir = pluginsDir;
      if (workdir) body.workdir = workdir;
      if (retries) body.max_retries = Number(retries);
      if (delay) body.retry_delay_seconds = Number(delay);
      body.dry_run = document.getElementById("b_dry_run").checked;
      return body;
    }
    async function loadBuilderSource(){
      if(!isBuilderView || builderLoaded) return;
      const pipeline = document.getElementById("b_pipeline_path").value.trim();
      if(!pipeline){
        document.getElementById("b_yaml").value = "vars: {}\\ndirs: {}\\nsteps:\\n  - name: step1\\n    script: echo.py\\n";
        builderLoaded = true;
        return;
      }
      const res = await fetch(`/api/builder/source?pipeline=${encodeURIComponent(pipeline)}`);
      if(!res.ok){
        document.getElementById("builder_msg").textContent = await readMessage(res);
        document.getElementById("b_yaml").value = "vars: {}\\ndirs: {}\\nsteps:\\n  - name: step1\\n    script: echo.py\\n";
      } else {
        const payload = await res.json();
        document.getElementById("b_yaml").value = payload.yaml_text || "";
      }
      builderLoaded = true;
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
        msg.textContent = await readMessage(res);
        return;
      }
      const payload = await res.json();
      msg.textContent = `Valid draft: ${payload.step_count} steps`;
      out.textContent = JSON.stringify(payload, null, 2);
    }
    async function testBuilderStep(){
      const msg = document.getElementById("builder_msg");
      const out = document.getElementById("builder_output");
      msg.textContent = "Testing step...";
      const res = await fetch(`/api/builder/test-step`, {
        method:"POST",
        headers: {"Content-Type":"application/json"},
        body: JSON.stringify(builderPayload()),
      });
      if(!res.ok){
        msg.textContent = await readMessage(res);
        return;
      }
      const payload = await res.json();
      msg.textContent = `Step ${payload.step_name}: ${payload.success ? "ok" : "failed"}`;
      out.textContent = JSON.stringify(payload, null, 2);
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
    document.getElementById("btn_builder_load").onclick = async () => { builderLoaded = false; await tick(); };
    document.getElementById("btn_builder_validate").onclick = validateBuilderDraft;
    document.getElementById("btn_builder_test_step").onclick = testBuilderStep;
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
    execution_config_raw = str(payload.get("execution_config") or "").strip()
    env_name_raw = str(payload.get("env") or "").strip()
    global_config_path = Path(global_config_raw).expanduser() if global_config_raw else None
    execution_config_path = Path(execution_config_raw).expanduser() if execution_config_raw else None
    env_name = env_name_raw or None

    if execution_config_path and not env_name:
        raise HTTPException(status_code=400, detail="`env` is required when `execution_config` is provided.")
    if env_name and not execution_config_path:
        raise HTTPException(status_code=400, detail="`execution_config` is required when `env` is provided.")

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
        "execution_config_path": execution_config_path,
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


def _resolve_execution_env(execution_config_path: Optional[Path], env_name: Optional[str]) -> dict[str, Any]:
    if not execution_config_path and not env_name:
        return {}
    try:
        envs = load_execution_config(execution_config_path)  # type: ignore[arg-type]
        env = envs.get(str(env_name), {})
        if not env:
            raise HTTPException(status_code=400, detail=f"Execution env '{env_name}' not found in config.")
        return apply_execution_env_overrides(env)
    except HTTPException:
        raise
    except ExecutionConfigError as exc:
        raise HTTPException(status_code=400, detail=f"Execution config error: {exc}") from exc


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


@app.get("/api/builder/source")
def api_builder_source(pipeline: str = Query(default="")) -> dict:
    path = Path((pipeline or "").strip()).expanduser()
    if not pipeline:
        raise HTTPException(status_code=400, detail="`pipeline` query param is required.")
    if not path.exists() or not path.is_file():
        raise HTTPException(status_code=404, detail=f"Pipeline file not found: {path}")
    try:
        text = path.read_text(encoding="utf-8")
    except Exception as exc:  # noqa: BLE001
        raise HTTPException(status_code=500, detail=f"Failed to read pipeline file: {exc}") from exc
    return {"pipeline": str(path), "yaml_text": text}


@app.post("/api/builder/validate")
def api_builder_validate(payload: Optional[dict[str, Any]] = Body(default=None)) -> dict:
    payload = payload or {}
    global_config_raw = str(payload.get("global_config") or "").strip()
    global_config_path = Path(global_config_raw).expanduser() if global_config_raw else None
    pipeline = _parse_pipeline_from_yaml_text(str(payload.get("yaml_text") or ""), global_config_path=global_config_path)
    return {
        "valid": True,
        "step_count": len(pipeline.steps),
        "step_names": [s.name for s in pipeline.steps],
        "vars": pipeline.vars,
        "dirs": pipeline.dirs,
    }


@app.post("/api/builder/test-step")
def api_builder_test_step(payload: Optional[dict[str, Any]] = Body(default=None)) -> dict:
    payload = payload or {}
    global_config_raw = str(payload.get("global_config") or "").strip()
    global_config_path = Path(global_config_raw).expanduser() if global_config_raw else None
    pipeline = _parse_pipeline_from_yaml_text(str(payload.get("yaml_text") or ""), global_config_path=global_config_path)
    step_name = str(payload.get("step_name") or "").strip()
    target_step: Optional[Step] = None
    if step_name:
        for s in pipeline.steps:
            if s.name == step_name:
                target_step = s
                break
        if target_step is None:
            raise HTTPException(status_code=400, detail=f"Step not found in draft: {step_name}")
    else:
        target_step = pipeline.steps[0] if pipeline.steps else None
    if target_step is None:
        raise HTTPException(status_code=400, detail="Draft has no steps to test.")

    plugins_dir = Path(payload.get("plugins_dir") or "plugins")
    workdir = Path(payload.get("workdir") or ".runs/builder")
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
    try:
        pipeline = parse_pipeline(args["pipeline_path"], global_vars=global_vars)
    except (PipelineError, FileNotFoundError) as exc:
        raise HTTPException(status_code=400, detail=f"Invalid pipeline: {exc}") from exc
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
    execution_env = _resolve_execution_env(args["execution_config_path"], args["env_name"])
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
        pipeline = parse_pipeline(args["pipeline_path"], global_vars=global_vars)
    except (PipelineError, FileNotFoundError) as exc:
        raise HTTPException(status_code=400, detail=f"Invalid pipeline: {exc}") from exc

    repo_root = Path(".").resolve()
    provenance = collect_run_provenance(
        repo_root=repo_root,
        pipeline_path=args["pipeline_path"],
        global_config_path=args["global_config_path"],
        execution_config_path=args["execution_config_path"],
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
            execution_config=args["execution_config_path"],
            env_name=args["env_name"],
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
        pipeline = parse_pipeline(pipeline_path)
    except (PipelineError, FileNotFoundError) as exc:
        raise HTTPException(status_code=400, detail=f"Invalid pipeline for resume: {exc}") from exc

    provenance = collect_run_provenance(
        repo_root=Path(".").resolve(),
        pipeline_path=pipeline_path,
        global_config_path=None,
        execution_config_path=None,
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

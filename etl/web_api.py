from __future__ import annotations

from pathlib import Path
from typing import Any, Optional

from fastapi import Body, FastAPI, HTTPException, Query
from fastapi.responses import HTMLResponse

from .executors.local import LocalExecutor
from .executors.slurm import SlurmExecutor
from .pipeline import parse_pipeline, PipelineError
from .provenance import collect_run_provenance
from .web_queries import WebQueryError, fetch_run_detail, fetch_run_header, fetch_runs


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
    <div class="head">
      <h1>Research ETL Runs</h1>
      <div class="muted">Auto-refresh every 12s</div>
    </div>
    <div class="grid">
      <section class="panel">
        <h3>Recent Runs</h3>
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
        <h3>Run Detail</h3>
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
    function qp(){
      const p = new URLSearchParams();
      const s = document.getElementById("f_status").value;
      const e = document.getElementById("f_executor").value;
      const q = document.getElementById("f_q").value.trim();
      if(s) p.set("status", s);
      if(e) p.set("executor", e);
      if(q) p.set("q", q);
      p.set("limit", "100");
      return p.toString();
    }
    function esc(v){return String(v ?? "").replaceAll("&","&amp;").replaceAll("<","&lt;").replaceAll(">","&gt;")}
    async function loadRuns(){
      const res = await fetch(`/api/runs?${qp()}`);
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
    async function tick(){ await loadRuns(); await loadDetail(); }
    document.getElementById("btn_apply").onclick = tick;
    document.getElementById("btn_resume").onclick = resumeSelected;
    tick(); setInterval(tick, 12000);
  </script>
</body>
</html>
"""


@app.get("/", response_class=HTMLResponse)
def index() -> str:
    return INDEX_HTML


@app.get("/api/health")
def health() -> dict:
    return {"ok": True}


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


@app.get("/api/runs/{run_id}")
def api_run_detail(run_id: str) -> dict:
    try:
        payload = fetch_run_detail(run_id)
    except WebQueryError as exc:
        raise HTTPException(status_code=503, detail=str(exc)) from exc
    if payload is None:
        raise HTTPException(status_code=404, detail=f"Run not found: {run_id}")
    return payload


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
    )
    try:
        submit = ex.submit(
            str(pipeline_path),
            context={
                "pipeline": pipeline,
                "resume_run_id": run_id,
                "provenance": provenance,
            },
        )
        st = ex.status(submit.run_id)
    except Exception as exc:  # noqa: BLE001
        raise HTTPException(status_code=500, detail=f"Resume failed: {exc}") from exc
    state = st.state.value if hasattr(st.state, "value") else str(st.state)
    return {"run_id": submit.run_id, "state": state, "pipeline": str(pipeline_path)}

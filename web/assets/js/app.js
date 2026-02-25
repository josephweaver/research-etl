let selected = null;
    let selectedPipeline = null;
    let selectedDataset = null;
    const isPipelinesView = window.location.pathname.startsWith("/pipelines");
    const isDatasetsView = window.location.pathname.startsWith("/datasets");
    const isPluginsView = window.location.pathname === "/plugins";
    const isProjectDagBaseView = window.location.pathname === "/project-dag";
    const projectDagMatch = window.location.pathname.match(/^\/projects\/(.+)\/dag$/);
    const isProjectDagView = isProjectDagBaseView || !!projectDagMatch;
    const projectDagFromPath = projectDagMatch ? decodeURIComponent(projectDagMatch[1]) : "";
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
    let builderProjectsReady = false;
    let builderRestoredProjectId = "";
    let builderModel = { project_id: "", vars: {}, var_types: {}, dirs: {}, requires_pipelines: [], steps: [] };
    let builderPlugins = [];
    let builderPluginMeta = {};
    let builderPluginStats = {};
    let builderValidationState = "unknown";
    let builderStepStatus = {};
    let builderStepTesting = {};
    let builderStepOutput = {};
    let builderStepOutputCollapsed = {};
    let builderStepLastLog = {};
    let builderStepTestJob = {};
    let builderParamIssues = {};
    let builderPipelineRunState = "not-run";
    let builderPipelineRunning = false;
    let builderPreviewCollapsed = false;
    let builderPreviewSectionCollapsed = { yaml: true, output: true, vars: true };
    let projectDagPageData = { nodes: [], edges: [], warnings: [] };
    let builderTreeFiles = [];
    let builderTreeDirs = [];
    let builderTreeFileSelection = "";
    let builderSelectedPipelineSource = "";
    let builderPipelineSources = [];
    let builderCreateMode = false;
    let builderProjectInjectedVarValues = {};
    let builderNamespaceTimer = null;
    let builderLastTextTarget = null;
    let builderAutoValidateTimer = null;
    let builderValidateInFlight = false;
    let builderNamespaceDigest = "";
    let builderRunSeed = null;
    let builderLastRunId = "";
    let builderLastRunExecutor = "";
    let builderEnvironmentsConfig = "";
    let builderEnvExecutorMap = {};
    const USER_STORAGE_KEY = "etl_ui_user";
    const PROJECT_STORAGE_KEY = "etl_ui_project";
    const ENV_STORAGE_KEY = "etl_ui_env";
    const BUILDER_LAST_PIPELINE_KEY = "etl_builder_last_pipeline";
    const VALID_UI_USERS = new Set(["admin", "land-core", "gee-lee"]);
    const _nativeFetch = window.fetch.bind(window);
    function currentAsUser(){
      const el = document.getElementById("nav_user");
      const raw = el ? String(el.value || "").trim() : "";
      const val = raw || "admin";
      return VALID_UI_USERS.has(val) ? val : "admin";
    }
    function currentEnvName(){
      const el = document.getElementById("nav_env");
      return el ? String(el.value || "").trim() : "";
    }
    function currentProjectId(){
      const el = document.getElementById("nav_project");
      return el ? String(el.value || "").trim() : "";
    }
    function projectDagHrefForProject(projectId){
      const pid = String(projectId || "").trim();
      return pid ? `/projects/${encodeURIComponent(pid)}/dag` : "/project-dag";
    }
    function updateProjectDagNavHref(){
      const el = document.getElementById("nav_project_dag");
      if(!el) return;
      el.href = projectDagHrefForProject(currentProjectId());
    }
    function loadBuilderLastPipeline(){
      try {
        const raw = String(localStorage.getItem(BUILDER_LAST_PIPELINE_KEY) || "").trim();
        if(!raw) return null;
        const parsed = JSON.parse(raw);
        if(!parsed || typeof parsed !== "object") return null;
        const pipeline = normalizeBuilderPipelineName(String(parsed.pipeline || ""));
        if(!pipeline) return null;
        const source = String(parsed.pipeline_source || "").trim();
        const projectId = String(parsed.project_id || "").trim();
        return { pipeline, pipeline_source: source, project_id: projectId };
      } catch {
        return null;
      }
    }
    function saveBuilderLastPipeline(pipeline, pipelineSource, projectId){
      const normalized = normalizeBuilderPipelineName(String(pipeline || ""));
      if(!normalized) return;
      const payload = {
        pipeline: normalized,
        pipeline_source: String(pipelineSource || "").trim(),
        project_id: String(projectId || "").trim(),
      };
      try {
        localStorage.setItem(BUILDER_LAST_PIPELINE_KEY, JSON.stringify(payload));
      } catch {}
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
      if(!u.searchParams.get("project_id")){
        const pid = currentProjectId();
        if(pid){
          u.searchParams.set("project_id", pid);
        }
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
    function initEnvScope(){
      const sel = document.getElementById("nav_env");
      if(!sel) return;
      const stored = String(localStorage.getItem(ENV_STORAGE_KEY) || "").trim();
      if(stored){
        sel.setAttribute("data-pref", stored);
      }
      sel.onchange = async () => {
        const next = String(sel.value || "").trim();
        localStorage.setItem(ENV_STORAGE_KEY, next);
        if(isBuilderView){
          renderBuilderModel();
          if(!builderPreviewSectionCollapsed.vars){
            await refreshBuilderNamespace();
          }
        }
      };
    }
    async function loadNavProjects(){
      const sel = document.getElementById("nav_project");
      if(!sel) return;
      let fromQuery = "";
      try {
        const qp = new URLSearchParams(window.location.search);
        fromQuery = String(qp.get("project_id") || "").trim();
      } catch {}
      const stored = String(localStorage.getItem(PROJECT_STORAGE_KEY) || "").trim();
      const preferred = projectDagFromPath || fromQuery || stored;
      const res = await fetch(`/api/builder/projects`);
      if(!res.ok){
        sel.innerHTML = `<option value="">project (all)</option>`;
        return;
      }
      const payload = await res.json();
      const projects = Array.isArray(payload.projects) ? payload.projects : [];
      sel.innerHTML = `<option value="">project (all)</option>` + projects.map(p => `<option value="${esc(p)}">${esc(p)}</option>`).join("");
      if(preferred && projects.includes(preferred)){
        sel.value = preferred;
      } else {
        sel.value = "";
      }
      localStorage.setItem(PROJECT_STORAGE_KEY, String(sel.value || "").trim());
      updateProjectDagNavHref();
      sel.onchange = async () => {
        const next = String(sel.value || "").trim();
        localStorage.setItem(PROJECT_STORAGE_KEY, next);
        updateProjectDagNavHref();
        if(isBuilderView){
          const bSel = document.getElementById("b_project_id");
          if(bSel){
            bSel.value = next;
          }
          builderModel.project_id = next;
          syncYamlPreview();
          await refreshBuilderProjectVars(next);
          await refreshBuilderTreeFiles();
        }
        if(isProjectDagView){
          await loadProjectDagPage();
        }
        await tick();
      };
    }
    function defaultBuilderDirs(){
      return {
        workdir: "{env.workdir}/{sys.now.yymmdd}/{sys.now.hhmmss}-{sys.run.short_id}",
        logdir: "{workdir}/logs",
      };
    }
    function _builderNameFromPipelinePath(raw){
      const normalized = normalizeBuilderPipelineName(String(raw || "").trim());
      if(!normalized) return "new_pipeline";
      let stem = normalized.split("/").pop() || "new_pipeline";
      stem = stem.replace(/\.ya?ml$/i, "");
      stem = stem.replace(/[^A-Za-z0-9_]+/g, "_").replace(/^_+|_+$/g, "");
      return stem || "new_pipeline";
    }
    function defaultBuilderVars(nameHint){
      const nm = _builderNameFromPipelinePath(nameHint);
      return {
        name: nm,
        workdir: "{env.workdir}/{name}/{sys.now.yymmdd}/{sys.now.hhmmss}-{sys.run.short_id}",
        logdir: "{workdir}/logs",
      };
    }
    function ensureBuilderCreateDefaults(model, pipelineHint){
      const out = model || {};
      out.vars = (out.vars && typeof out.vars === "object") ? out.vars : {};
      out.var_types = (out.var_types && typeof out.var_types === "object") ? out.var_types : {};
      const defs = defaultBuilderVars(pipelineHint);
      for(const [k, v] of Object.entries(defs)){
        const cur = String(out.vars[k] ?? "").trim();
        if(!cur){
          out.vars[k] = v;
        }
      }
      out.var_types.name = _builderCanonicalType(out.var_types.name || "string");
      out.var_types.workdir = _builderCanonicalType(out.var_types.workdir || "path");
      out.var_types.logdir = _builderCanonicalType(out.var_types.logdir || "path");
      return out;
    }
    function ensureBuilderDefaultDirs(model){
      const out = model || {};
      out.vars = (out.vars && typeof out.vars === "object") ? out.vars : {};
      out.var_types = (out.var_types && typeof out.var_types === "object") ? out.var_types : {};
      out.dirs = out.dirs || {};
      if(Object.keys(out.dirs).length){
        return out;
      }
      out.dirs = { ...defaultBuilderDirs() };
      return out;
    }
    function normalizeBuilderModelPlugins(model){
      const out = model || {};
      out.steps = Array.isArray(out.steps) ? out.steps : [];
      out.steps = out.steps.map((step) => {
        const st = step || {};
        st.plugin = normalizePluginRef(st.plugin || "");
        return st;
      });
      return out;
    }
    function deriveBuilderWorkdir(){
      const varsMap = (builderModel && builderModel.vars) || {};
      const dirs = (builderModel && builderModel.dirs) || {};
      const candidates = ["workdir", "work", "work_dir"];
      for(const k of candidates){
        const v = String(varsMap[k] ?? "").trim();
        if(v) return v;
      }
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
      const dag = document.getElementById("nav_project_dag");
      const newp = document.getElementById("nav_new_pipeline");
      const back = document.getElementById("nav_context_back");
      [ops, pipes, datasets, plugins, dag, newp].forEach(el => el.classList.remove("active"));
      back.style.display = "none";
      if (path === "/") {
        ops.classList.add("active");
      } else if (path === "/plugins") {
        plugins.classList.add("active");
      } else if (path === "/project-dag" || path.startsWith("/projects/")) {
        dag.classList.add("active");
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
        document.getElementById("right_title").textContent = "Dataset Detail + Create";
        document.getElementById("ops_panel").style.display = "none";
        document.getElementById("pipelines_panel").style.display = "none";
        document.getElementById("pipeline_summary").style.display = "none";
        document.getElementById("pipeline_validations").style.display = "none";
        document.getElementById("f_q").placeholder = "search datasets";
        if(isDatasetDetailView){
          selectedDataset = datasetFromPath;
          document.getElementById("f_q").value = datasetFromPath || "";
        } else {
          renderDatasetCreateForm();
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
      if(isProjectDagView){
        document.body.classList.add("project-dag-mode");
        document.getElementById("page_title").textContent = "Research ETL Project DAG";
        document.getElementById("left_title").textContent = "Project DAG";
        document.getElementById("right_title").textContent = "Dependency Graph";
        document.getElementById("ops_panel").style.display = "none";
        document.getElementById("pipelines_panel").style.display = "none";
        document.getElementById("pipeline_summary").style.display = "none";
        document.getElementById("pipeline_validations").style.display = "none";
        document.getElementById("plugins_controls").style.display = "none";
        document.getElementById("builder_panel").style.display = "none";
        document.getElementById("run_actions_panel").style.display = "none";
        document.getElementById("project_dag_page_panel").style.display = "block";
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
      const environmentsConfig = document.getElementById("a_environments_config").value.trim() || builderEnvironmentsConfig;
      const env = document.getElementById("a_env").value.trim() || currentEnvName();
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
    function _yamlKey(k){
      const s = String(k ?? "");
      return /^[A-Za-z_][A-Za-z0-9_.-]*$/.test(s) ? s : _yamlEsc(s);
    }
    function _builderVarTypeForValue(v){
      if(Array.isArray(v)) return "list";
      if(v && typeof v === "object") return "dict";
      if(typeof v === "boolean") return "bool";
      if(typeof v === "number" && Number.isFinite(v)) return "number";
      return "string";
    }
    function _builderCanonicalType(raw){
      const t = String(raw || "").trim().toLowerCase();
      if(t === "list" || t === "dict" || t === "bool" || t === "number" || t === "path") return t;
      return "string";
    }
    function _builderVarValueDisplay(value, type){
      const t = _builderCanonicalType(type);
      if(t === "list" || t === "dict"){
        try {
          return JSON.stringify(value === undefined ? (t === "list" ? [] : {}) : value);
        } catch {
          return t === "list" ? "[]" : "{}";
        }
      }
      if(t === "bool"){
        if(typeof value === "boolean") return value ? "true" : "false";
        const s = String(value ?? "").trim().toLowerCase();
        return (s === "true" || s === "1" || s === "yes" || s === "on") ? "true" : "false";
      }
      return String(value ?? "");
    }
    function _builderVarValueToken(value){
      try {
        if(value && typeof value === "object"){
          return JSON.stringify(value);
        }
      } catch {}
      return String(value ?? "");
    }
    function _parseBuilderVarValue(rawValue, type){
      const t = _builderCanonicalType(type);
      const raw = String(rawValue ?? "");
      if(t === "string" || t === "path"){
        return { ok: true, value: raw };
      }
      if(t === "number"){
        const text = raw.trim();
        if(!text.length){
          return { ok: true, value: "" };
        }
        const n = Number(text);
        if(Number.isNaN(n)){
          return { ok: false, error: "Invalid number literal." };
        }
        return { ok: true, value: n };
      }
      if(t === "bool"){
        const s = raw.trim().toLowerCase();
        if(!s.length) return { ok: true, value: false };
        if(s === "true" || s === "1" || s === "yes" || s === "on") return { ok: true, value: true };
        if(s === "false" || s === "0" || s === "no" || s === "off") return { ok: true, value: false };
        return { ok: false, error: "Invalid boolean literal; use true/false." };
      }
      const text = raw.trim();
      if(!text.length){
        return { ok: true, value: t === "list" ? [] : {} };
      }
      let parsed = null;
      try {
        parsed = JSON.parse(text);
      } catch {
        return { ok: false, error: `Invalid ${t} JSON.` };
      }
      if(t === "list" && !Array.isArray(parsed)){
        return { ok: false, error: "List value must be a JSON array." };
      }
      if(t === "dict" && (!parsed || typeof parsed !== "object" || Array.isArray(parsed))){
        return { ok: false, error: "Dict value must be a JSON object." };
      }
      return { ok: true, value: parsed };
    }
    function _yamlPushNode(lines, indent, node){
      if(Array.isArray(node)){
        if(!node.length){
          lines.push(`${indent}[]`);
          return;
        }
        for(const item of node){
          if(item && typeof item === "object"){
            lines.push(`${indent}-`);
            _yamlPushNode(lines, `${indent}  `, item);
          } else {
            lines.push(`${indent}- ${_yamlArgVal(item)}`);
          }
        }
        return;
      }
      if(node && typeof node === "object"){
        const keys = Object.keys(node);
        if(!keys.length){
          lines.push(`${indent}{}`);
          return;
        }
        for(const k of keys){
          const v = node[k];
          if(v && typeof v === "object"){
            lines.push(`${indent}${_yamlKey(k)}:`);
            _yamlPushNode(lines, `${indent}  `, v);
          } else {
            lines.push(`${indent}${_yamlKey(k)}: ${_yamlArgVal(v)}`);
          }
        }
        return;
      }
      lines.push(`${indent}${_yamlArgVal(node)}`);
    }
    function _yamlPushTypedKey(lines, indent, key, value, type){
      const t = _builderCanonicalType(type || _builderVarTypeForValue(value));
      if(t === "list"){
        const arr = Array.isArray(value) ? value : [];
        if(!arr.length){
          lines.push(`${indent}${_yamlKey(key)}: []`);
          return;
        }
        lines.push(`${indent}${_yamlKey(key)}:`);
        _yamlPushNode(lines, `${indent}  `, arr);
        return;
      }
      if(t === "dict"){
        const obj = (value && typeof value === "object" && !Array.isArray(value)) ? value : {};
        const keys = Object.keys(obj);
        if(!keys.length){
          lines.push(`${indent}${_yamlKey(key)}: {}`);
          return;
        }
        lines.push(`${indent}${_yamlKey(key)}:`);
        _yamlPushNode(lines, `${indent}  `, obj);
        return;
      }
      if(t === "bool"){
        lines.push(`${indent}${_yamlKey(key)}: ${value ? "true" : "false"}`);
        return;
      }
      if(t === "number"){
        if(typeof value === "number" && Number.isFinite(value)){
          lines.push(`${indent}${_yamlKey(key)}: ${String(value)}`);
          return;
        }
      }
      lines.push(`${indent}${_yamlKey(key)}: ${_yamlEsc(value)}`);
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
      const m = builderModel || { project_id:"", vars:{}, var_types:{}, dirs:{}, requires_pipelines:[], steps:[] };
      const lines = [];
      if ((m.project_id || "").trim()){
        lines.push(`project_id: ${_yamlEsc(m.project_id)}`);
      }
      if ((m.requires_pipelines || []).length){
        lines.push("requires_pipelines:");
        for(const r of m.requires_pipelines){ lines.push(`  - ${_yamlEsc(r)}`); }
      }
      lines.push("vars:");
      const vars = m.vars || {};
      const varTypes = m.var_types || {};
      const vkeys = Object.keys(vars);
      if(!vkeys.length){ lines.push("  {}"); } else {
        for(const k of vkeys){
          _yamlPushTypedKey(lines, "  ", k, vars[k], varTypes[k]);
        }
      }
      // Builder now derives directory contract from typed vars.
      // `workdir`/`logdir` live in vars (typically type=path).
      lines.push("dirs:");
      const varsWorkdir = String(vars.workdir ?? "").trim();
      const varsLogdir = String(vars.logdir ?? "").trim();
      const derivedWorkdir = varsWorkdir || "{env.workdir}/{sys.now.yymmdd}/{sys.now.hhmmss}-{sys.run.short_id}";
      const derivedLogdir = varsLogdir || "{workdir}/logs";
      lines.push(`  workdir: ${_yamlEsc(derivedWorkdir)}`);
      lines.push(`  logdir: ${_yamlEsc(derivedLogdir)}`);
      lines.push("steps:");
      const steps = m.steps || [];
      if(!steps.length){
        lines.push("  - plugin: echo.py");
      } else {
        for(const st of steps){
          if ((st.name || "").trim()){
            lines.push(`  - name: ${_yamlEsc(st.name)}`);
            lines.push(`    plugin: ${_yamlEsc(st.plugin || "echo.py")}`);
          } else {
            lines.push(`  - plugin: ${_yamlEsc(st.plugin || "echo.py")}`);
          }
          if (st.enabled === false) {
            lines.push("    enabled: false");
          }
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
          if ((st.type || "sequential") === "sequential_foreach" && (st.sequential_foreach || "").trim()) {
            lines.push(`    sequential_foreach: ${_yamlEsc(st.sequential_foreach)}`);
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
        builderLastRunId = "";
        builderLastRunExecutor = "";
        builderValidationState = "unknown";
        builderStepStatus = {};
        builderStepTesting = {};
        builderStepOutput = {};
        builderStepOutputCollapsed = {};
        builderStepLastLog = {};
        builderStepTestJob = {};
        builderParamIssues = {};
        builderPipelineRunState = "not-run";
      }
      renderBuilderPipelineStatus();
      if (builderNamespaceTimer) {
        clearTimeout(builderNamespaceTimer);
      }
      if(!builderPreviewSectionCollapsed.vars){
        builderNamespaceTimer = setTimeout(() => { refreshBuilderNamespace(); }, 220);
      }
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
        runBtn.disabled = !!builderPipelineRunning || !!builderCreateMode;
      }
      const termBtn = document.getElementById("btn_builder_terminate");
      if(termBtn){
        termBtn.disabled = !builderLastRunId || !!builderPipelineRunning;
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
      const projectId = document.getElementById("b_project_id").value.trim();
      const sourceSel = document.getElementById("b_pipeline_source");
      const pipelineSource = String(sourceSel && sourceSel.value ? sourceSel.value : builderSelectedPipelineSource).trim();
      const envName = currentEnvName();
      const workdir = deriveBuilderWorkdir();
      const retries = document.getElementById("b_max_retries").value.trim();
      const delay = document.getElementById("b_retry_delay").value.trim();
      const gitBranch = document.getElementById("b_git_branch").value.trim();
      if (pipeline) body.pipeline = pipeline;
      if (intent) body.intent = intent;
      if (constraints) body.constraints = constraints;
      if (projectId) body.project_id = projectId;
      if (pipelineSource) body.pipeline_source = pipelineSource;
      if (envName) body.env = envName;
      if (builderEnvironmentsConfig) body.environments_config = builderEnvironmentsConfig;
      if (workdir) body.workdir = workdir;
      if (retries) body.max_retries = Number(retries);
      if (delay) body.retry_delay_seconds = Number(delay);
      body.dry_run = document.getElementById("b_dry_run").checked;
      body.verbose = document.getElementById("b_verbose").checked;
      body.git_sync = document.getElementById("b_git_sync").checked;
      if (gitBranch) body.git_branch = gitBranch;
      return body;
    }
    async function loadBuilderProjects(){
      if(!isBuilderView) return;
      const sel = document.getElementById("b_project_id");
      const current = String(sel.value || "").trim();
      const res = await fetch(`/api/builder/projects`);
      if(!res.ok){
        document.getElementById("builder_msg").textContent = await readMessage(res);
        sel.innerHTML = `<option value="">project (optional)</option>`;
        return;
      }
      const payload = await res.json();
      const projects = Array.isArray(payload.projects) ? payload.projects : [];
      sel.innerHTML = `<option value="">project (optional)</option>` + projects.map(p => `<option value="${esc(p)}">${esc(p)}</option>`).join("");
      const modelProject = String((builderModel && builderModel.project_id) || "").trim();
      const navProject = currentProjectId();
      const restoredProject = String(builderRestoredProjectId || "").trim();
      if(current && projects.includes(current)){
        sel.value = current;
      } else if(restoredProject && projects.includes(restoredProject)){
        sel.value = restoredProject;
      } else if(navProject && projects.includes(navProject)){
        sel.value = navProject;
      } else if(modelProject && projects.includes(modelProject)){
        sel.value = modelProject;
      }
      builderRestoredProjectId = "";
      builderModel.project_id = String(sel.value || "").trim();
    }
    function builderDagStatusClass(rawStatus){
      const s = String(rawStatus || "").trim().toLowerCase();
      if(s === "succeeded" || s === "successful" || s === "completed") return "successful";
      if(s === "running") return "running";
      if(s === "queued") return "queued";
      if(s === "failed" || s === "error") return "failed";
      if(s === "missing") return "missing";
      return "not-run";
    }
    function renderDagSvg(svgId, dagData, onNodeClick){
      const svg = document.getElementById(svgId);
      if(!svg) return;
      const nodes = Array.isArray((dagData || {}).nodes) ? dagData.nodes : [];
      const edges = Array.isArray((dagData || {}).edges) ? dagData.edges : [];
      while(svg.firstChild){ svg.removeChild(svg.firstChild); }
      if(!nodes.length){
        svg.setAttribute("viewBox", "0 0 960 220");
        const t = document.createElementNS("http://www.w3.org/2000/svg", "text");
        t.setAttribute("x", "24");
        t.setAttribute("y", "36");
        t.setAttribute("font-size", "13");
        t.setAttribute("fill", "#4b5f80");
        t.textContent = "No project pipelines found.";
        svg.appendChild(t);
        return;
      }
      const byId = {};
      const indegree = {};
      const out = {};
      for(const n of nodes){
        const id = String(n.id || "");
        byId[id] = n;
        indegree[id] = 0;
        out[id] = [];
      }
      for(const e of edges){
        const from = String(e.from || "");
        const to = String(e.to || "");
        if(!from || !to || !byId[from] || !byId[to]) continue;
        out[from].push(to);
        indegree[to] = (indegree[to] || 0) + 1;
      }
      const queue = [];
      for(const n of nodes){
        const id = String(n.id || "");
        if((indegree[id] || 0) === 0) queue.push(id);
      }
      const level = {};
      for(const id of queue){ level[id] = 0; }
      while(queue.length){
        const cur = queue.shift();
        const base = Number(level[cur] || 0);
        for(const nxt of (out[cur] || [])){
          level[nxt] = Math.max(Number(level[nxt] || 0), base + 1);
          indegree[nxt] = Number(indegree[nxt] || 0) - 1;
          if(indegree[nxt] === 0){
            queue.push(nxt);
          }
        }
      }
      for(const n of nodes){
        const id = String(n.id || "");
        if(level[id] === undefined) level[id] = 0;
      }
      const columns = {};
      let maxLevel = 0;
      for(const n of nodes){
        const id = String(n.id || "");
        const lv = Number(level[id] || 0);
        if(!columns[lv]) columns[lv] = [];
        columns[lv].push(n);
        maxLevel = Math.max(maxLevel, lv);
      }
      for(const arr of Object.values(columns)){
        arr.sort((a, b) => String(a.label || a.pipeline || a.id || "").localeCompare(String(b.label || b.pipeline || b.id || "")));
      }
      const layout = {};
      const laneCounts = Object.values(columns).map((arr) => arr.length);
      const maxRows = laneCounts.length ? Math.max(...laneCounts) : 1;
      const nodeW = 220;
      const nodeH = 64;
      const gapX = 44;
      const gapY = 28;
      const pad = 16;
      const width = Math.max(960, pad * 2 + (maxLevel + 1) * nodeW + maxLevel * gapX);
      const height = Math.max(220, pad * 2 + maxRows * nodeH + Math.max(0, maxRows - 1) * gapY);
      svg.setAttribute("viewBox", `0 0 ${width} ${height}`);

      const defs = document.createElementNS("http://www.w3.org/2000/svg", "defs");
      const marker = document.createElementNS("http://www.w3.org/2000/svg", "marker");
      const markerId = `${String(svgId || "dag")}-arrow`;
      marker.setAttribute("id", markerId);
      marker.setAttribute("viewBox", "0 0 10 10");
      marker.setAttribute("refX", "8");
      marker.setAttribute("refY", "5");
      marker.setAttribute("markerWidth", "7");
      marker.setAttribute("markerHeight", "7");
      marker.setAttribute("orient", "auto-start-reverse");
      const arrowPath = document.createElementNS("http://www.w3.org/2000/svg", "path");
      arrowPath.setAttribute("d", "M 0 0 L 10 5 L 0 10 z");
      arrowPath.setAttribute("fill", "#9ab0cf");
      marker.appendChild(arrowPath);
      defs.appendChild(marker);
      svg.appendChild(defs);

      for(let lv = 0; lv <= maxLevel; lv++){
        const col = columns[lv] || [];
        col.forEach((n, idx) => {
          const id = String(n.id || "");
          const x = pad + lv * (nodeW + gapX);
          const y = pad + idx * (nodeH + gapY);
          layout[id] = { x, y, nodeW, nodeH };
        });
      }
      const edgeLayer = document.createElementNS("http://www.w3.org/2000/svg", "g");
      for(const e of edges){
        const from = String(e.from || "");
        const to = String(e.to || "");
        if(!layout[from] || !layout[to]) continue;
        const fromNode = byId[from] || {};
        const toNode = byId[to] || {};
        const a = layout[from];
        const b = layout[to];
        const x1 = a.x + a.nodeW;
        const y1 = a.y + (a.nodeH / 2);
        const x2 = b.x;
        const y2 = b.y + (b.nodeH / 2);
        const path = document.createElementNS("http://www.w3.org/2000/svg", "path");
        const midX = x1 + Math.max(18, (x2 - x1) * 0.45);
        path.setAttribute("d", `M ${x1} ${y1} C ${midX} ${y1}, ${Math.max(x1 + 16, x2 - 18)} ${y2}, ${x2} ${y2}`);
        path.setAttribute("fill", "none");
        path.setAttribute("stroke", e.missing ? "#d78a8a" : "#9ab0cf");
        path.setAttribute("stroke-width", "1.6");
        path.setAttribute("marker-end", `url(#${markerId})`);
        const edgeTitle = document.createElementNS("http://www.w3.org/2000/svg", "title");
        edgeTitle.textContent = `requires=${String(e.ref || "")} | from=${String(fromNode.pipeline || from)} (${String(fromNode.status || "unknown")}${fromNode.run_id ? `, run=${fromNode.run_id}` : ""}) -> to=${String(toNode.pipeline || to)} (${String(toNode.status || "unknown")}${toNode.run_id ? `, run=${toNode.run_id}` : ""})`;
        path.appendChild(edgeTitle);
        edgeLayer.appendChild(path);
        if(e.ref){
          const lbl = document.createElementNS("http://www.w3.org/2000/svg", "text");
          lbl.setAttribute("x", String((x1 + x2) / 2));
          lbl.setAttribute("y", String((y1 + y2) / 2 - 5));
          lbl.setAttribute("font-size", "10");
          lbl.setAttribute("fill", e.missing ? "#b05d5d" : "#6683ab");
          lbl.setAttribute("text-anchor", "middle");
          lbl.textContent = String(e.ref).length > 24 ? `${String(e.ref).slice(0, 21)}...` : String(e.ref);
          edgeLayer.appendChild(lbl);
        }
      }
      svg.appendChild(edgeLayer);

      const nodeLayer = document.createElementNS("http://www.w3.org/2000/svg", "g");
      for(const n of nodes){
        const id = String(n.id || "");
        const box = layout[id];
        if(!box) continue;
        const statusClass = builderDagStatusClass(n.status);
        const stale = !!n.stale;
        const statusColor = statusClass === "successful" ? "#0b6fb3"
          : statusClass === "running" || statusClass === "queued" ? "#9a6700"
          : statusClass === "failed" || statusClass === "missing" ? "#b42318"
          : "#4b5563";
        const exists = !!n.exists;
        const rect = document.createElementNS("http://www.w3.org/2000/svg", "rect");
        rect.setAttribute("x", String(box.x));
        rect.setAttribute("y", String(box.y));
        rect.setAttribute("width", String(box.nodeW));
        rect.setAttribute("height", String(box.nodeH));
        rect.setAttribute("rx", "8");
        rect.setAttribute("fill", exists ? (stale ? "#fffaf0" : "#f8fbff") : "#fff5f5");
        rect.setAttribute("stroke", stale ? "#9a4a00" : statusColor);
        rect.setAttribute("stroke-width", "1.2");
        nodeLayer.appendChild(rect);

        const labelText = String(n.pipeline || n.label || id || "");
        const txt = document.createElementNS("http://www.w3.org/2000/svg", "text");
        txt.setAttribute("x", String(box.x + 8));
        txt.setAttribute("y", String(box.y + 18));
        txt.setAttribute("font-size", "12");
        txt.setAttribute("fill", "#23395b");
        txt.textContent = labelText.length > 34 ? `${labelText.slice(0, 31)}...` : labelText;
        nodeLayer.appendChild(txt);

        const st = document.createElementNS("http://www.w3.org/2000/svg", "text");
        st.setAttribute("x", String(box.x + 8));
        st.setAttribute("y", String(box.y + 35));
        st.setAttribute("font-size", "11");
        st.setAttribute("fill", statusColor);
        st.textContent = stale ? `${String(n.status || "not-run")} (stale)` : String(n.status || "not-run");
        nodeLayer.appendChild(st);
        const runText = document.createElementNS("http://www.w3.org/2000/svg", "text");
        runText.setAttribute("x", String(box.x + 8));
        runText.setAttribute("y", String(box.y + 50));
        runText.setAttribute("font-size", "10");
        runText.setAttribute("fill", "#5f7396");
        const runId = String(n.run_id || "").trim();
        runText.textContent = runId ? `run: ${runId}` : (exists ? "run: none" : "missing pipeline");
        nodeLayer.appendChild(runText);

        if(exists){
          const link = document.createElementNS("http://www.w3.org/2000/svg", "title");
          const staleDeps = Array.isArray(n.stale_dependencies) && n.stale_dependencies.length
            ? ` stale_from=${n.stale_dependencies.join(",")}`
            : "";
          link.textContent = `${labelText} (${String(n.status || "not-run")}${stale ? ", stale" : ""}${runId ? `, run=${runId}` : ""})${staleDeps}`;
          rect.appendChild(link);
          rect.style.cursor = "pointer";
          if(typeof onNodeClick === "function"){
            rect.addEventListener("click", () => onNodeClick(n));
          }
        }
      }
      svg.appendChild(nodeLayer);
    }
    function renderProjectDagPage(){
      renderDagSvg("project_dag_svg", projectDagPageData, (n) => {
        const pipeline = String((n || {}).pipeline || "").trim();
        if(!pipeline) return;
        const pid = currentProjectId();
        const qp = new URLSearchParams();
        if(pid) qp.set("project_id", pid);
        const qtxt = qp.toString();
        window.location.href = `/pipelines/${encodeURIComponent(pipeline)}/edit${qtxt ? `?${qtxt}` : ""}`;
      });
    }
    async function loadProjectDagPage(){
      if(!isProjectDagView) return;
      const msgEl = document.getElementById("project_dag_msg");
      const pid = String(currentProjectId() || "").trim();
      if(!pid){
        projectDagPageData = { nodes: [], edges: [], warnings: [] };
        if(msgEl) msgEl.textContent = "Select a project from the nav bar to view its DAG.";
        renderProjectDagPage();
        return;
      }
      if(msgEl) msgEl.textContent = `Loading DAG for ${pid}...`;
      const res = await fetch(`/api/projects/${encodeURIComponent(pid)}/dag`);
      if(!res.ok){
        projectDagPageData = { nodes: [], edges: [], warnings: [] };
        if(msgEl) msgEl.textContent = await readMessage(res);
        renderProjectDagPage();
        return;
      }
      const payload = await res.json();
      projectDagPageData = {
        nodes: Array.isArray(payload.nodes) ? payload.nodes : [],
        edges: Array.isArray(payload.edges) ? payload.edges : [],
        warnings: Array.isArray(payload.warnings) ? payload.warnings : [],
      };
      if(msgEl){
        const warnings = projectDagPageData.warnings.length;
        const staleCount = projectDagPageData.nodes.filter(n => !!n.stale).length;
        msgEl.textContent = `project=${pid}, nodes=${projectDagPageData.nodes.length}, edges=${projectDagPageData.edges.length}, stale=${staleCount}${warnings ? `, warnings=${warnings}` : ""}`;
      }
      renderProjectDagPage();
    }
    function flattenBuilderProjectVars(value, prefix = ""){
      const out = {};
      if(!value || typeof value !== "object" || Array.isArray(value)){
        return out;
      }
      for(const [k, v] of Object.entries(value)){
        const key = String(k || "").trim();
        if(!key) continue;
        const dotted = prefix ? `${prefix}.${key}` : key;
        if(v && typeof v === "object" && !Array.isArray(v)){
          Object.assign(out, flattenBuilderProjectVars(v, dotted));
        } else {
          out[dotted] = v;
        }
      }
      return out;
    }
    async function refreshBuilderProjectVars(projectId){
      if(!isBuilderView) return;
      const pid = String(projectId || "").trim();
      if(!builderModel || typeof builderModel !== "object"){
        return;
      }
      builderModel.vars = builderModel.vars || {};
      builderModel.var_types = builderModel.var_types || {};
      // Remove previously injected project vars only when unchanged by user edits.
      for(const [k, injectedVal] of Object.entries(builderProjectInjectedVarValues || {})){
        if(
          Object.prototype.hasOwnProperty.call(builderModel.vars, k) &&
          _builderVarValueToken(builderModel.vars[k]) === String(injectedVal)
        ){
          delete builderModel.vars[k];
          delete builderModel.var_types[k];
        }
      }
      builderProjectInjectedVarValues = {};
      if(!pid){
        renderBuilderModel();
        syncYamlPreview();
        return;
      }
      const res = await fetch(`/api/builder/project-vars?project_id=${encodeURIComponent(pid)}`);
      if(!res.ok){
        document.getElementById("builder_msg").textContent = await readMessage(res);
        renderBuilderModel();
        syncYamlPreview();
        return;
      }
      const payload = await res.json();
      const projectVars = flattenBuilderProjectVars(payload.project_vars || {});
      for(const [k, rawVal] of Object.entries(projectVars)){
        const value = rawVal === null || rawVal === undefined ? "" : rawVal;
        const vtype = _builderVarTypeForValue(value);
        builderModel.vars[k] = value;
        builderModel.var_types[k] = vtype;
        builderProjectInjectedVarValues[k] = _builderVarValueToken(value);
      }
      renderBuilderModel();
      syncYamlPreview();
    }
    async function refreshBuilderGitStatus(){
      if(!isBuilderView) return;
      const el = document.getElementById("b_git_repo_status");
      if(!el) return;
      const res = await fetch(`/api/builder/git-status`);
      if(!res.ok){
        el.textContent = `git: ${await readMessage(res)}`;
        return;
      }
      const g = await res.json();
      if(g.sync_repo_configured === false){
        el.textContent = "git_sync disabled (set ETL_BUILDER_GIT_SYNC_REPO)";
        return;
      }
      const name = String(g.repo_name || "repo");
      const branch = String(g.branch || "");
      const commit = String(g.commit || "").slice(0, 8);
      const dirty = !!g.dirty;
      el.textContent = `${name}@${branch}:${commit}${dirty ? " *dirty" : ""}`;
    }
    async function loadBuilderEnvironments(){
      const sel = document.getElementById("nav_env");
      if(!sel) return;
      const current = String(sel.value || "").trim();
      const preferred = String(sel.getAttribute("data-pref") || localStorage.getItem(ENV_STORAGE_KEY) || "").trim();
      const qp = new URLSearchParams();
      const res = await fetch(`/api/builder/environments?${qp.toString()}`);
      if(!res.ok){
        sel.innerHTML = `<option value="">env (optional)</option>`;
        builderEnvironmentsConfig = "";
        builderEnvExecutorMap = {};
        return;
      }
      const payload = await res.json();
      const envs = Array.isArray(payload.environments) ? payload.environments : [];
      builderEnvironmentsConfig = String(payload.environments_config || "").trim();
      const specs = Array.isArray(payload.environment_specs) ? payload.environment_specs : [];
      const envExecutorMap = {};
      for(const spec of specs){
        const envNameSpec = String((spec && spec.name) || "").trim();
        const execName = String((spec && spec.executor) || "").trim().toLowerCase();
        if(!envNameSpec) continue;
        if(execName) envExecutorMap[envNameSpec] = execName;
      }
      builderEnvExecutorMap = envExecutorMap;
      sel.innerHTML = `<option value="">env (optional)</option>` + envs.map(e => `<option value="${esc(e)}">${esc(e)}</option>`).join("");
      if(current && envs.includes(current)){
        sel.value = current;
      } else if(preferred && envs.includes(preferred)){
        sel.value = preferred;
      } else if(envs.includes("local")){
        sel.value = "local";
      }
      localStorage.setItem(ENV_STORAGE_KEY, String(sel.value || "").trim());
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
      renderBuilderModel();
    }
    function stripPySuffix(path){
      const text = String(path || "").trim();
      return text.toLowerCase().endsWith(".py") ? text.slice(0, -3) : text;
    }
    function pluginPathVariants(path){
      const raw = String(path || "").trim();
      if(!raw) return [];
      const out = [];
      const add = (v) => {
        const t = String(v || "").trim();
        if(!t) return;
        if(!out.includes(t)) out.push(t);
      };
      add(raw);
      if(raw.toLowerCase().endsWith(".py")){
        add(raw.slice(0, -3));
      } else {
        add(`${raw}.py`);
      }
      return out;
    }
    function canonicalPluginPath(path){
      const variants = pluginPathVariants(path);
      for(const v of variants){
        if(builderPluginMeta[v]) return v;
      }
      return String(path || "").trim();
    }
    function normalizePluginRef(path){
      return stripPySuffix(path);
    }
    function builderMetaForPlugin(path){
      const canonical = canonicalPluginPath(path);
      if(builderPluginMeta[canonical]) return builderPluginMeta[canonical];
      return { params: {} };
    }
    function pluginRecommendation(path){
      const canonical = canonicalPluginPath(path);
      const st = builderPluginStats[String(canonical || "")] || {};
      return st.recommendation || {};
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
    function addBuilderRequire(){
      builderModel.requires_pipelines = builderModel.requires_pipelines || [];
      builderModel.requires_pipelines.push("");
      renderBuilderModel();
      syncYamlPreview();
    }
    function addBuilderDir(){
      document.getElementById("builder_msg").textContent = "Directory section is deprecated. Use vars `workdir`/`logdir` with type `path`.";
    }
    function addBuilderVar(){
      builderModel.vars = builderModel.vars || {};
      builderModel.var_types = builderModel.var_types || {};
      let i = 1;
      while(Object.prototype.hasOwnProperty.call(builderModel.vars, `var_${i}`)) i++;
      builderModel.vars[`var_${i}`] = "";
      builderModel.var_types[`var_${i}`] = "string";
      renderBuilderModel();
      syncYamlPreview();
    }
    function addBuilderStep(){
      const firstPlugin = normalizePluginRef(builderPlugins.length ? builderPlugins[0].path : "echo.py");
      builderModel.steps = builderModel.steps || [];
      builderModel.steps.push({
        name:"{sys.step.NN}_Step",
        type:"sequential",
        plugin:firstPlugin,
        enabled:true,
        params:{},
        resources:{},
        output_var:"",
        when:"",
        parallel_with:"",
        foreach:"",
        sequential_foreach:"",
        foreach_mode:"var",
        foreach_glob:"",
        foreach_kind:"dirs",
      });
      renderBuilderModel();
      syncYamlPreview();
    }
    function insertBuilderStepAt(index){
      const firstPlugin = normalizePluginRef(builderPlugins.length ? builderPlugins[0].path : "echo.py");
      const steps = builderModel.steps || [];
      const idx = Math.max(0, Math.min(Number(index || 0), steps.length));
      steps.splice(idx, 0, {
        name:"{sys.step.NN}_Step",
        type:"sequential",
        plugin:firstPlugin,
        enabled:true,
        params:{},
        resources:{},
        output_var:"",
        when:"",
        parallel_with:"",
        foreach:"",
        sequential_foreach:"",
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

      const varTypes = builderModel.var_types || {};
      Object.entries(builderModel.vars || {}).forEach(([k,v]) => {
        const vtype = _builderCanonicalType(varTypes[k] || _builderVarTypeForValue(v));
        const vdisp = _builderVarValueDisplay(v, vtype);
        const valueControl =
          vtype === "bool"
            ? `<select data-kind="var-val" data-key="${esc(k)}">
                 <option value="true" ${String(vdisp) === "true" ? "selected" : ""}>true</option>
                 <option value="false" ${String(vdisp) === "false" ? "selected" : ""}>false</option>
               </select>`
            : (vtype === "number"
                ? `<input data-kind="var-val" data-key="${esc(k)}" type="number" step="any" value="${esc(vdisp)}" placeholder="number" />`
                : ((vtype === "list" || vtype === "dict")
                    ? `<textarea data-kind="var-val" data-key="${esc(k)}" rows="2" placeholder='${vtype === "list" ? "[&quot;A&quot;,&quot;B&quot;]" : "{&quot;k&quot;:&quot;v&quot;}"}'>${esc(vdisp)}</textarea>`
                    : `<input data-kind="var-val" data-key="${esc(k)}" value="${esc(vdisp)}" placeholder="${vtype === "path" ? "path template/value" : "value"}" />`));
        const row = document.createElement("div");
        row.className = "builder-item";
        row.innerHTML = `<div class="controls"><input data-kind="var-key" data-key="${esc(k)}" value="${esc(k)}" placeholder="var key" /><select data-kind="var-type" data-key="${esc(k)}"><option value="string" ${vtype==="string"?"selected":""}>string</option><option value="path" ${vtype==="path"?"selected":""}>path</option><option value="number" ${vtype==="number"?"selected":""}>number</option><option value="bool" ${vtype==="bool"?"selected":""}>bool</option><option value="list" ${vtype==="list"?"selected":""}>list</option><option value="dict" ${vtype==="dict"?"selected":""}>dict</option></select>${valueControl}<button data-del-var="${esc(k)}">Remove</button></div>`;
        varEl.appendChild(row);
      });

      Object.entries(builderModel.dirs || {}).forEach(([k,v]) => {
        const row = document.createElement("div");
        row.className = "builder-item";
        row.innerHTML = `<div class="controls"><input data-kind="dir-key" data-key="${esc(k)}" value="${esc(k)}" placeholder="dir key" /><input data-kind="dir-val" data-key="${esc(k)}" value="${esc(v)}" placeholder="path/value" /><button data-del-dir="${esc(k)}">Remove</button></div>`;
        dirEl.appendChild(row);
      });

      const steps = builderModel.steps || [];
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
        const meta = builderMetaForPlugin(st.plugin);
        const pluginPath = normalizePluginRef(st.plugin);
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
        const lastLogLine = String(builderStepLastLog[idx] || "").trim();
        const stepInlineStatus = loading ? "Running..." : lastLogLine;
        const rec = pluginRecommendation(st.plugin);
        const recCpu = rec.cpu_cores === null || rec.cpu_cores === undefined ? "" : Number(rec.cpu_cores).toFixed(2);
        const recMem = rec.memory_gb === null || rec.memory_gb === undefined ? "" : Number(rec.memory_gb).toFixed(2);
        const recWall = rec.wall_minutes === null || rec.wall_minutes === undefined ? "" : Number(rec.wall_minutes).toFixed(2);
        const hasRec = !!(recCpu || recMem || recWall);
        st.resources = st.resources || {};
        let typeSpecificHtml = "";
        const foreachMaxConcurrencyRaw = st.resources?.foreach_max_concurrency;
        const foreachMaxConcurrency =
          foreachMaxConcurrencyRaw === null || foreachMaxConcurrencyRaw === undefined
            ? ""
            : String(foreachMaxConcurrencyRaw);
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
        } else if (type === "sequential_foreach"){
          typeSpecificHtml = `
            <div class="controls">
              <input data-kind="step-sequential-foreach" data-idx="${idx}" value="${esc(st.sequential_foreach || "")}" placeholder="sequential_foreach var name (e.g. days)" />
            </div>
          `;
        }
        card.innerHTML = `
          <div class="step-head">
            <h5>Step ${idx+1}</h5>
            <span class="status-pill ${badge.klass}">${badge.text}</span>
          </div>
          <div class="controls">
            <label class="muted">Step name</label>
            <input data-kind="step-name" data-idx="${idx}" value="${esc(st.name || "")}" placeholder="{sys.step.NN}_Step" />
          </div>
          <div class="controls">
            <select data-kind="step-type" data-idx="${idx}">
              <option value="sequential" ${type==="sequential"?"selected":""}>sequential</option>
              <option value="parallel" ${type==="parallel"?"selected":""}>parallel</option>
              <option value="foreach" ${type==="foreach"?"selected":""}>foreach</option>
              <option value="sequential_foreach" ${type==="sequential_foreach"?"selected":""}>sequential_foreach</option>
            </select>
            <div class="combo-picker step-plugin-picker" data-idx="${idx}">
              <input data-kind="step-plugin-input" data-idx="${idx}" value="${esc(pluginPath)}" placeholder="plugin path (browse tree or type exact path)" autocomplete="off" />
              <div class="combo-dropdown">
                <div id="b_step_plugin_tree_${idx}" class="builder-step-plugin-tree"></div>
              </div>
            </div>
            <label class="muted"><input type="checkbox" data-kind="step-enabled" data-idx="${idx}" ${st.enabled === false ? "" : "checked"} /> enabled</label>
            <button class="spin-btn ${loading ? "loading" : ""}" data-test-step="${idx}" ${loading ? "disabled" : ""}>
              <span>Test Step</span><span class="spin"></span>
            </button>
            ${loading ? `<button data-stop-step-test="${idx}">Stop</button>` : ``}
            <button data-apply-step-rec="${idx}" ${hasRec ? "" : "disabled"}>Apply Recommended</button>
            <button data-del-step="${idx}">Remove Step</button>
          </div>
          <div class="muted">${esc(stepInlineStatus || "")}</div>
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
              ${type === "foreach"
                ? `<div class="param-row"><div class="param-label">foreach_max_concurrency</div><div class="param-value"><input data-kind="step-res-foreach-max-concurrency" data-idx="${idx}" value="${esc(foreachMaxConcurrency)}" placeholder="e.g. 20 (SLURM array %N cap)" /></div></div>`
                : ``
              }
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
      renderBuilderStepPluginTrees();
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
          const oldType = (builderModel.var_types || {})[oldKey];
          delete builderModel.vars[oldKey];
          if(builderModel.var_types){ delete builderModel.var_types[oldKey]; }
          builderModel.vars[newKey] = val;
          builderModel.var_types = builderModel.var_types || {};
          builderModel.var_types[newKey] = _builderCanonicalType(oldType || _builderVarTypeForValue(val));
          renderBuilderModel();
          changed = true;
        }
      } else if (kind === "var-type"){
        const key = t.getAttribute("data-key") || "";
        if(key){
          builderModel.var_types = builderModel.var_types || {};
          const nextType = _builderCanonicalType(t.value);
          builderModel.var_types[key] = nextType;
          const parsed = _parseBuilderVarValue(_builderVarValueDisplay(builderModel.vars[key], nextType), nextType);
          if(parsed.ok){
            builderModel.vars[key] = parsed.value;
            document.getElementById("builder_msg").textContent = "";
          }
          renderBuilderModel();
          changed = true;
        }
      } else if (kind === "var-val"){
        const key = t.getAttribute("data-key") || "";
        if(key){
          if (eventType === "input" && !(t instanceof HTMLTextAreaElement)) return;
          builderModel.var_types = builderModel.var_types || {};
          const vtype = _builderCanonicalType(builderModel.var_types[key] || _builderVarTypeForValue(builderModel.vars[key]));
          const parsed = _parseBuilderVarValue(t.value, vtype);
          if(!parsed.ok){
            document.getElementById("builder_msg").textContent = `Var '${key}': ${parsed.error}`;
            return;
          }
          builderModel.vars[key] = parsed.value;
          document.getElementById("builder_msg").textContent = "";
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
        if(kind === "step-plugin-input"){
          const raw = String(t.value || "").trim();
          if(eventType === "input"){
            showBuilderStepPluginPicker(idx);
            filterBuilderStepPluginTree(idx, raw);
            return;
          }
          if(eventType === "change"){
            const canonical = canonicalPluginPath(raw);
            if(builderPluginMeta[canonical]){
              applyBuilderStepPluginSelection(idx, canonical);
            }
            return;
          }
          return;
        }
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
          if(st.type === "sequential_foreach" && !String(st.sequential_foreach || "").trim()){
            st.sequential_foreach = "items";
          }
          renderBuilderModel();
          changed = true;
        }
        if(kind === "step-name"){ st.name = t.value; changed = true; }
        if(kind === "step-enabled"){
          st.enabled = !!(t instanceof HTMLInputElement ? t.checked : true);
          changed = true;
        }
        if(kind === "step-output"){ st.output_var = t.value; changed = true; }
        if(kind === "step-when"){ st.when = t.value; changed = true; }
        if(kind === "step-parallel"){ st.parallel_with = t.value; changed = true; }
        if(kind === "step-foreach"){ st.foreach = t.value; changed = true; }
        if(kind === "step-sequential-foreach"){ st.sequential_foreach = t.value; changed = true; }
        if(kind === "step-foreach-mode"){
          st.foreach_mode = String(t.value || "var").trim().toLowerCase() === "glob" ? "glob" : "var";
          renderBuilderModel();
          changed = true;
        }
        if(kind === "step-foreach-glob"){ st.foreach_glob = t.value; changed = true; }
        if(kind === "step-foreach-kind"){ st.foreach_kind = t.value; changed = true; }
        if(
          kind === "step-res-cpu" ||
          kind === "step-res-mem" ||
          kind === "step-res-wall" ||
          kind === "step-res-foreach-max-concurrency"
        ){
          st.resources = st.resources || {};
          let key = "wall_minutes";
          if(kind === "step-res-cpu") key = "cpu_cores";
          if(kind === "step-res-mem") key = "memory_gb";
          if(kind === "step-res-foreach-max-concurrency") key = "foreach_max_concurrency";
          const raw = String(t.value ?? "").trim();
          if(!raw.length){
            delete st.resources[key];
          } else {
            const n = Number(raw);
            if(key === "foreach_max_concurrency"){
              st.resources[key] = Number.isNaN(n) ? raw : Math.max(1, Math.trunc(n));
            } else {
              st.resources[key] = Number.isNaN(n) ? raw : n;
            }
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
      const stopStepBtn = t.closest("[data-stop-step-test]");
      if(stopStepBtn){
        const idx = Number(stopStepBtn.getAttribute("data-stop-step-test") || "-1");
        if(idx >= 0){
          await stopBuilderStepTestAt(idx);
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
      if (delVar !== null){
        delete builderModel.vars[delVar];
        if(builderModel.var_types){ delete builderModel.var_types[delVar]; }
        renderBuilderModel();
        syncYamlPreview();
        return;
      }
      const delDir = t.getAttribute("data-del-dir");
      if (delDir !== null){ delete builderModel.dirs[delDir]; renderBuilderModel(); syncYamlPreview(); return; }
      const delStep = t.getAttribute("data-del-step");
      if (delStep !== null){
        const didx = Number(delStep);
        builderModel.steps.splice(didx,1);
        delete builderStepOutput[didx];
        delete builderStepOutputCollapsed[didx];
        delete builderStepLastLog[didx];
        delete builderStepTestJob[didx];
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
    function buildBuilderPluginJsTreeData(paths){
      const nodes = [];
      const seenDirs = new Set();
      const all = Array.isArray(paths) ? paths.map(x => String(x || "").replaceAll("\\\\", "/").trim()).filter(Boolean) : [];
      for(const rel of all){
        const parts = rel.split("/").filter(Boolean);
        if(!parts.length) continue;
        let parentId = "#";
        for(let i=0; i<parts.length - 1; i++){
          const seg = parts[i];
          const dpath = parts.slice(0, i + 1).join("/");
          const did = `pd:${dpath}`;
          if(!seenDirs.has(did)){
            seenDirs.add(did);
            nodes.push({ id: did, parent: parentId, text: seg, type: "dir", icon: "jstree-folder" });
          }
          parentId = did;
        }
        const fname = parts[parts.length - 1];
        const displayName = fname.toLowerCase().endsWith(".py") ? fname.slice(0, -3) : fname;
        nodes.push({ id: `pf:${rel}`, parent: parentId, text: displayName, type: "file", icon: "jstree-file", relpath: rel });
      }
      return nodes;
    }
    function hideBuilderStepPluginPickers(exceptIdx = null){
      const pickers = Array.from(document.querySelectorAll(".step-plugin-picker.open"));
      for(const picker of pickers){
        const idx = Number((picker instanceof HTMLElement ? picker.getAttribute("data-idx") : "") || "-1");
        if(exceptIdx !== null && idx === Number(exceptIdx)) continue;
        picker.classList.remove("open");
      }
    }
    function showBuilderStepPluginPicker(idx){
      const picker = document.querySelector(`.step-plugin-picker[data-idx="${idx}"]`);
      if(!(picker instanceof HTMLElement)) return;
      hideBuilderStepPluginPickers(idx);
      picker.classList.add("open");
    }
    function filterBuilderStepPluginTree(idx, query){
      const holder = document.getElementById(`b_step_plugin_tree_${idx}`);
      const $ = window.jQuery;
      if(!holder || !$ || !$.fn || !$.fn.jstree) return;
      const inst = $(holder).jstree(true);
      if(!inst) return;
      try {
        inst.search(String(query || "").trim());
      } catch {}
    }
    function applyBuilderStepPluginSelection(idx, pluginPath){
      const steps = builderModel.steps || [];
      if(idx < 0 || idx >= steps.length) return;
      const chosen = normalizePluginRef(pluginPath);
      if(!chosen) return;
      const st = steps[idx];
      if(String(st.plugin || "") === chosen){
        hideBuilderStepPluginPickers();
        return;
      }
      st.plugin = chosen;
      st.params = {};
      renderBuilderModel();
      syncYamlPreview();
      hideBuilderStepPluginPickers();
    }
    function renderBuilderStepPluginTrees(){
      const $ = window.jQuery;
      if(!$ || !$.fn || !$.fn.jstree) return;
      const pluginPaths = (builderPlugins || []).map(p => String((p || {}).path || "").trim()).filter(Boolean);
      (builderModel.steps || []).forEach((st, idx) => {
        const holder = document.getElementById(`b_step_plugin_tree_${idx}`);
        if(!holder) return;
        const data = buildBuilderPluginJsTreeData(pluginPaths);
        const $holder = $(holder);
        try { $holder.jstree("destroy"); } catch {}
        $holder.off(".jstree");
        holder.innerHTML = "";
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
        $holder.on("select_node.jstree", function(_ev, payload){
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
          const rel = String(node.original?.relpath || "").trim();
          if(!rel) return;
          applyBuilderStepPluginSelection(idx, rel);
        });
        $holder.on("ready.jstree", function(){
          const inst = $holder.jstree(true);
          if(!inst) return;
          const selectedPath = canonicalPluginPath(st.plugin);
          if(!selectedPath) return;
          const nodeId = `pf:${selectedPath}`;
          if(!inst.get_node(nodeId)) return;
          try {
            inst.deselect_all(true);
            inst.select_node(nodeId, false, true);
            let parent = inst.get_parent(nodeId);
            while(parent && parent !== "#"){
              inst.open_node(parent);
              parent = inst.get_parent(parent);
            }
          } catch {}
        });
      });
    }
    function handleBuilderStepPluginPickerFocus(ev){
      const t = ev.target;
      if(!(t instanceof HTMLInputElement)) return;
      if(t.getAttribute("data-kind") !== "step-plugin-input") return;
      const idx = Number(t.getAttribute("data-idx") || "-1");
      if(idx < 0) return;
      showBuilderStepPluginPicker(idx);
      filterBuilderStepPluginTree(idx, "");
    }
    function handleBuilderStepPluginPickerInput(ev){
      const t = ev.target;
      if(!(t instanceof HTMLInputElement)) return;
      if(t.getAttribute("data-kind") !== "step-plugin-input") return;
      const idx = Number(t.getAttribute("data-idx") || "-1");
      if(idx < 0) return;
      showBuilderStepPluginPicker(idx);
      filterBuilderStepPluginTree(idx, t.value);
    }
    function handleBuilderStepPluginPickerOutsideMouseDown(ev){
      const t = ev.target;
      if(!(t instanceof Node)) return;
      const inPicker = (t instanceof Element) ? t.closest(".step-plugin-picker") : null;
      if(inPicker) return;
      hideBuilderStepPluginPickers();
    }
    function buildBuilderJsTreeData(files, dirs){
      const nodes = [];
      const seenDirs = new Set();
      const onlyDirs = Array.isArray(dirs) ? dirs.map(x => String(x || "").replaceAll("\\\\", "/").trim()).filter(Boolean) : [];
      for(const drel of onlyDirs){
        const dparts = drel.split("/").filter(Boolean);
        if(!dparts.length) continue;
        let parentId = "#";
        for(let i=0; i<dparts.length; i++){
          const seg = dparts[i];
          const dpath = dparts.slice(0, i + 1).join("/");
          const did = `d:${dpath}`;
          if(!seenDirs.has(did)){
            seenDirs.add(did);
            nodes.push({ id: did, parent: parentId, text: seg, type: "dir", icon: "jstree-folder" });
          }
          parentId = did;
        }
      }
      const all = Array.isArray(files) ? files : [];
      for(const rawEntry of all){
        let treePath = "";
        let pipelinePath = "";
        let sourceLabel = "";
        if(typeof rawEntry === "string"){
          treePath = String(rawEntry || "").replaceAll("\\\\", "/").trim();
          pipelinePath = treePath;
        } else if(rawEntry && typeof rawEntry === "object"){
          treePath = String(rawEntry.tree_path || rawEntry.path || "").replaceAll("\\\\", "/").trim();
          pipelinePath = String(rawEntry.pipeline || rawEntry.relpath || treePath).replaceAll("\\\\", "/").trim();
          sourceLabel = String(rawEntry.source || "").trim();
        }
        if(!treePath) continue;
        if(!pipelinePath) pipelinePath = treePath;
        const parts = treePath.split("/").filter(Boolean);
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
        nodes.push({ id: `f:${treePath}`, parent: parentId, text: fname, type: "file", icon: "jstree-file", relpath: pipelinePath, treepath: treePath, source: sourceLabel });
      }
      nodes.push({
        id: "new:pipeline",
        parent: "#",
        text: "[new pipeline]",
        type: "new",
        icon: "jstree-file new-pipeline-icon",
      });
      return nodes;
    }
    function builderKnownPipelineExists(pipelinePath){
      const target = normalizeBuilderPipelineName(String(pipelinePath || "").trim());
      if(!target) return false;
      const all = Array.isArray(builderTreeFiles) ? builderTreeFiles : [];
      for(const rawEntry of all){
        let rel = "";
        if(typeof rawEntry === "string"){
          rel = normalizeBuilderPipelineName(String(rawEntry || ""));
        } else if(rawEntry && typeof rawEntry === "object"){
          rel = normalizeBuilderPipelineName(String(rawEntry.pipeline || rawEntry.relpath || rawEntry.path || ""));
        }
        if(rel && rel === target){
          return true;
        }
      }
      return false;
    }
    function renderBuilderCreateMode(){
      const createBtn = document.getElementById("btn_builder_create");
      const saveBtn = document.getElementById("btn_builder_save");
      const runBtn = document.getElementById("btn_builder_run");
      const validateBtn = document.getElementById("btn_builder_validate");
      const pathInput = document.getElementById("b_pipeline_path");
      if(createBtn){
        createBtn.style.display = builderCreateMode ? "" : "none";
      }
      if(saveBtn){
        saveBtn.disabled = !!builderCreateMode;
      }
      if(runBtn){
        runBtn.disabled = !!builderCreateMode || !!builderPipelineRunning;
      }
      if(validateBtn){
        validateBtn.disabled = !!builderCreateMode;
      }
      if(pathInput){
        // Keep browser history suggestions off; enable only curated dir suggestions in create mode.
        pathInput.setAttribute("autocomplete", "off");
        if(builderCreateMode){
          pathInput.setAttribute("list", "b_pipeline_path_suggestions");
        } else {
          pathInput.removeAttribute("list");
        }
      }
      updateBuilderPipelinePathSuggestions();
    }
    function updateBuilderPipelinePathSuggestions(){
      const dl = document.getElementById("b_pipeline_path_suggestions");
      if(!dl) return;
      if(!builderCreateMode){
        dl.innerHTML = "";
        return;
      }
      const sourceSel = document.getElementById("b_pipeline_source");
      const selectedSource = String(sourceSel && sourceSel.value ? sourceSel.value : builderSelectedPipelineSource).trim();
      const sources = Array.isArray(builderPipelineSources) ? builderPipelineSources.map(s => String(s || "").trim()).filter(Boolean) : [];
      const dirs = Array.isArray(builderTreeDirs) ? builderTreeDirs.map(d => String(d || "").replaceAll("\\\\","/").trim()).filter(Boolean) : [];
      const opts = new Set();
      for(const raw of dirs){
        const parts = raw.split("/").filter(Boolean);
        if(!parts.length) continue;
        if(sources.length > 1){
          // External multi-source tree prefixes dirs with source label.
          const label = parts[0];
          if(selectedSource && label !== selectedSource) continue;
          const rel = parts.slice(1).join("/");
          if(rel) opts.add(rel.endsWith("/") ? rel : `${rel}/`);
          continue;
        }
        opts.add(raw.endsWith("/") ? raw : `${raw}/`);
      }
      const sorted = Array.from(opts).sort((a, b) => a.localeCompare(b));
      dl.innerHTML = sorted.map(v => `<option value="${esc(v)}"></option>`).join("");
    }
    function applyBuilderPathSuggestionFromPrefix(){
      const input = document.getElementById("b_pipeline_path");
      const dl = document.getElementById("b_pipeline_path_suggestions");
      if(!input || !dl) return;
      const raw = String(input.value || "");
      const prefix = raw.trim().toLowerCase();
      if(!prefix) return;
      const options = Array.from(dl.querySelectorAll("option"))
        .map(o => String(o.getAttribute("value") || "").trim())
        .filter(Boolean);
      const exact = options.find(v => v.toLowerCase() === prefix);
      if(exact){
        input.value = exact;
        return;
      }
      const match = options.find(v => v.toLowerCase().startsWith(prefix));
      if(match){
        input.value = match;
      }
    }
    function enterBuilderCreateMode(){
      builderCreateMode = true;
      const projectSel = document.getElementById("b_project_id");
      const pathInput = document.getElementById("b_pipeline_path");
      const selectedProjectId = String(projectSel && projectSel.value ? projectSel.value : "").trim();
      const pathHint = String(pathInput && pathInput.value ? pathInput.value : "").trim();
      builderModel = normalizeBuilderModelPlugins(
        ensureBuilderCreateDefaults(
          {
            project_id: selectedProjectId,
            vars: {},
            var_types: {},
            dirs: {},
            requires_pipelines: [],
            steps: [],
          },
          pathHint,
        )
      );
      builderRunSeed = null;
      renderBuilderModel();
      syncYamlPreview();
      renderBuilderCreateMode();
      const input = document.getElementById("b_pipeline_path");
      if(input){
        input.value = "";
        input.focus();
      }
      builderTreeFileSelection = "";
      builderLoaded = true;
      document.getElementById("builder_msg").textContent = "New pipeline mode: enter pipeline path, then click Create.";
      hideBuilderTreeDropdown();
    }
    function exitBuilderCreateMode(){
      builderCreateMode = false;
      renderBuilderCreateMode();
      document.getElementById("builder_msg").textContent = "";
    }
    function applyBuilderTreeSelectionFromPipeline(pipeline){
      const p = normalizeBuilderPipelineName(String(pipeline || "").trim());
      if(!p){
        builderTreeFileSelection = "";
        builderSelectedPipelineSource = "";
        return;
      }
      builderTreeFileSelection = p;
    }
    function syncBuilderPipelineSourceSelect(sources, preferred){
      const sel = document.getElementById("b_pipeline_source");
      if(!sel) return;
      const list = Array.isArray(sources) ? sources.map(s => String(s || "").trim()).filter(Boolean) : [];
      const incoming = Array.from(new Set(list));
      if(incoming.length){
        builderPipelineSources = incoming.slice();
      }
      const current = String(sel.value || "").trim();
      const target = String(preferred || "").trim() || builderSelectedPipelineSource || current;
      const renderList = incoming.length ? incoming : (Array.isArray(builderPipelineSources) ? builderPipelineSources : []);
      if(renderList.length){
        sel.innerHTML = `<option value="">repo (auto)</option>` + renderList.map(s => `<option value="${esc(s)}">${esc(s)}</option>`).join("");
      }
      const available = Array.from(sel.options || [])
        .map(o => String(o.value || "").trim())
        .filter(Boolean);
      if(target && available.includes(target)){
        sel.value = target;
      } else if(current && available.includes(current)){
        sel.value = current;
      } else if(available.length === 1){
        sel.value = available[0];
      } else {
        sel.value = "";
      }
      builderSelectedPipelineSource = String(sel.value || "").trim();
    }
    function setBuilderPipelineSourceValue(value){
      const sel = document.getElementById("b_pipeline_source");
      if(!sel) return;
      const target = String(value || "").trim();
      if(!target){
        sel.value = "";
        builderSelectedPipelineSource = "";
        return;
      }
      const has = Array.from(sel.options || []).some(o => String(o.value || "").trim() === target);
      if(has){
        sel.value = target;
      }
      builderSelectedPipelineSource = String(sel.value || target).trim();
    }
    function renderBuilderJsTree(files, dirs){
      const holder = document.getElementById("b_pipeline_tree");
      if(!holder) return;
      const $ = window.jQuery;
      if(!$ || !$.fn || !$.fn.jstree){
        holder.innerHTML = `<span class="muted">jsTree not available in this browser context.</span>`;
        return;
      }
      const data = buildBuilderJsTreeData(files, dirs);
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
          if(ta === "new" && tb !== "new") return 1;
          if(tb === "new" && ta !== "new") return -1;
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
        if(ntype === "new"){
          enterBuilderCreateMode();
          return;
        }
        if(ntype !== "file") return;
        const rel = String(node.original.relpath || "").trim();
        if(!rel) return;
        exitBuilderCreateMode();
        builderSelectedPipelineSource = String(node.original.source || "").trim();
        setBuilderPipelineSourceValue(builderSelectedPipelineSource);
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
      const dirs = Array.isArray(payload.dirs) ? payload.dirs : [];
      const sources = Array.isArray(payload.sources) ? payload.sources : [];
      builderTreeFiles = files;
      builderTreeDirs = dirs;
      builderPipelineSources = sources.map(s => String(s || "").trim()).filter(Boolean);
      syncBuilderPipelineSourceSelect(sources, builderSelectedPipelineSource);
      updateBuilderPipelinePathSuggestions();
      applyBuilderTreeSelectionFromPipeline(document.getElementById("b_pipeline_path").value.trim());
      renderBuilderJsTree(files, dirs);
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
        if(builderCreateMode){
          hideBuilderTreeDropdown();
          return;
        }
        if(!builderTreeFiles.length){
          await refreshBuilderTreeFiles();
        }
        showBuilderTreeDropdown();
      });
      input.addEventListener("input", async () => {
        if(builderCreateMode){
          hideBuilderTreeDropdown();
          return;
        }
      });
      input.addEventListener("keydown", async (ev) => {
        if(!builderCreateMode) return;
        if(ev.key === "Tab"){
          applyBuilderPathSuggestionFromPrefix();
          return;
        }
        if(ev.key === "Enter"){
          ev.preventDefault();
          await createBuilderPipeline();
        }
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
      renderBuilderJsTree(builderTreeFiles, builderTreeDirs);
      builderLoaded = false;
      await loadBuilderSource();
    }
    async function loadBuilderSource(){
      if(!isBuilderView || builderLoaded) return;
      let pipeline = normalizeBuilderPipelineName(document.getElementById("b_pipeline_path").value.trim());
      if(!pipeline){
        exitBuilderCreateMode();
        document.getElementById("b_pipeline_path").value = "";
        builderModel = normalizeBuilderModelPlugins(
          ensureBuilderDefaultDirs({ project_id: "", vars: {}, var_types: {}, dirs: {}, requires_pipelines: [], steps: [] })
        );
        const projectSel = document.getElementById("b_project_id");
        if(projectSel){
          builderModel.project_id = String(projectSel.value || "").trim();
        }
        builderRunSeed = null;
        await loadBuilderPlugins();
        renderBuilderModel();
        syncYamlPreview();
        document.getElementById("builder_msg").textContent = "Select a pipeline from the tree, or choose [new pipeline].";
        builderLoaded = true;
        return;
      }
      exitBuilderCreateMode();
      document.getElementById("b_pipeline_path").value = pipeline;
      applyBuilderTreeSelectionFromPipeline(pipeline);
      renderBuilderJsTree(builderTreeFiles, builderTreeDirs);
      const projectSel = document.getElementById("b_project_id");
      const sourceSel = document.getElementById("b_pipeline_source");
      const qp = new URLSearchParams();
      qp.set("pipeline", pipeline);
      const pid = String(projectSel && projectSel.value ? projectSel.value : currentProjectId()).trim();
      if(pid) qp.set("project_id", pid);
      const source = String(sourceSel && sourceSel.value ? sourceSel.value : builderSelectedPipelineSource).trim();
      if(source) qp.set("pipeline_source", source);
      const res = await fetch(`/api/builder/source?${qp.toString()}`);
      if(!res.ok){
        document.getElementById("builder_msg").textContent = await readMessage(res);
        builderModel = normalizeBuilderModelPlugins(
          ensureBuilderDefaultDirs({ project_id: "", vars: {}, var_types: {}, dirs: {}, requires_pipelines: [], steps: [] })
        );
        if(projectSel){
          builderModel.project_id = String(projectSel.value || "").trim();
        }
        builderRunSeed = null;
        builderLoaded = false;
        return;
      } else {
        const payload = await res.json();
        builderSelectedPipelineSource = String(payload.pipeline_source || builderSelectedPipelineSource || "").trim();
        setBuilderPipelineSourceValue(builderSelectedPipelineSource);
        builderModel = normalizeBuilderModelPlugins(
          ensureBuilderDefaultDirs(payload.model || { project_id: "", vars: {}, var_types: {}, dirs: {}, requires_pipelines: [], steps: [] })
        );
        if(projectSel){
          const pid = String((builderModel && builderModel.project_id) || "").trim();
          if(pid){
            projectSel.value = pid;
          } else {
            builderModel.project_id = String(projectSel.value || "").trim();
          }
        }
        builderRunSeed = null;
        saveBuilderLastPipeline(pipeline, builderSelectedPipelineSource, builderModel.project_id || (projectSel ? projectSel.value : ""));
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
      if(builderCreateMode){
        msg.textContent = "Use Create to create a new pipeline first.";
        return;
      }
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
        body: JSON.stringify({
          yaml_text: payload.yaml_text,
          project_id: payload.project_id || "",
          pipeline_source: payload.pipeline_source || "",
        }),
      });
      if(update.status === 404){
        msg.textContent = "Pipeline does not exist yet. Click Create first.";
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
    async function createBuilderPipeline(){
      const msg = document.getElementById("builder_msg");
      const out = document.getElementById("builder_output");
      const pathEl = document.getElementById("b_pipeline_path");
      const pipeline = normalizeBuilderPipelineName(pathEl ? pathEl.value : "");
      if(!pipeline){
        msg.textContent = "pipeline path is required to create.";
        if(pathEl) pathEl.focus();
        return;
      }
      const projectId = String(document.getElementById("b_project_id").value || "").trim();
      if(!projectId){
        msg.textContent = "project_id is required before creating a new pipeline.";
        document.getElementById("b_project_id").focus();
        return;
      }
      if(builderKnownPipelineExists(pipeline)){
        if(pathEl){
          pathEl.value = pipeline;
        }
        builderLoaded = false;
        exitBuilderCreateMode();
        await loadBuilderSource();
        msg.textContent = `Pipeline already exists: ${pipeline}`;
        return;
      }
      msg.textContent = "Creating pipeline...";
      builderModel = normalizeBuilderModelPlugins(ensureBuilderCreateDefaults(builderModel, pipeline));
      syncYamlPreview();
      const payload = builderPayload();
      const sourceSel = document.getElementById("b_pipeline_source");
      const pipelineSource = String(sourceSel && sourceSel.value ? sourceSel.value : builderSelectedPipelineSource).trim();
      const create = await fetch(`/api/pipelines`, {
        method:"POST",
        headers: {"Content-Type":"application/json"},
        body: JSON.stringify({
          pipeline,
          yaml_text: payload.yaml_text,
          project_id: projectId,
          pipeline_source: pipelineSource || "",
        }),
      });
      if(!create.ok){
        msg.textContent = await readMessage(create);
        return;
      }
      const data = await create.json();
      if(pathEl){
        pathEl.value = normalizeBuilderPipelineName(data.pipeline || pipeline);
      }
      builderLoaded = false;
      await refreshBuilderTreeFiles();
      await loadBuilderSource();
      exitBuilderCreateMode();
      msg.textContent = `Created ${data.pipeline}`;
      out.textContent = JSON.stringify(data, null, 2);
    }
    async function runBuilderPipeline(){
      const msg = document.getElementById("builder_msg");
      const out = document.getElementById("builder_output");
      const payload = builderPayload();
      if(builderCreateMode){
        msg.textContent = "Use Create to create a new pipeline first.";
        return;
      }
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

      const hintedExecutor = String(payload.executor || "").trim().toLowerCase();
      const envExecutor = payload.env && builderEnvExecutorMap[payload.env]
        ? String(builderEnvExecutorMap[payload.env] || "").trim().toLowerCase()
        : "";
      const effectiveExecutor = hintedExecutor || envExecutor || "local";
      const remoteExecutor = effectiveExecutor === "slurm" || effectiveExecutor === "hpcc_direct";
      const requiresMainCheck = remoteExecutor || runMode === "repro";
      if(requiresMainCheck){
        msg.textContent = "Checking git repo is clean on main...";
        const qp = new URLSearchParams();
        if(payload.project_id) qp.set("project_id", payload.project_id);
        if(payload.projects_config) qp.set("projects_config", payload.projects_config);
        if(payload.pipeline_source) qp.set("pipeline_source", payload.pipeline_source);
        const checkRes = await fetch(`/api/builder/git-main-check?${qp.toString()}`);
        if(!checkRes.ok){
          builderPipelineRunState = "failed";
          builderPipelineRunning = false;
          renderBuilderPipelineStatus();
          msg.textContent = await readMessage(checkRes);
          return;
        }
        const check = await checkRes.json();
        if(!check.ok){
          builderPipelineRunState = "failed";
          builderPipelineRunning = false;
          renderBuilderPipelineStatus();
          msg.textContent = check.detail || "Repo must be clean on main before run.";
          out.textContent = JSON.stringify(check, null, 2);
          return;
        }
      }

      msg.textContent = "Submitting run...";
      const runBody = {};
      if(payload.env) runBody.env = payload.env;
      if(payload.environments_config) runBody.environments_config = payload.environments_config;
      if(payload.env && builderEnvExecutorMap[payload.env]){
        runBody.executor = builderEnvExecutorMap[payload.env];
      }
      if(payload.executor) runBody.executor = payload.executor;
      if(payload.plugins_dir) runBody.plugins_dir = payload.plugins_dir;
      if(payload.workdir) runBody.workdir = payload.workdir;
      if(payload.project_id) runBody.project_id = payload.project_id;
      if(payload.pipeline_source) runBody.pipeline_source = payload.pipeline_source;
      if(payload.max_retries !== undefined) runBody.max_retries = payload.max_retries;
      if(payload.retry_delay_seconds !== undefined) runBody.retry_delay_seconds = payload.retry_delay_seconds;
      runBody.dry_run = !!payload.dry_run;
      runBody.verbose = !!payload.verbose;
      if(remoteExecutor){
        runBody.execution_source = "git_remote";
        runBody.allow_workspace_source = false;
      } else if(runMode === "repro"){
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
      builderLastRunId = String(data.run_id || "").trim();
      builderLastRunExecutor = String(data.executor || runBody.executor || "").trim().toLowerCase();
      builderPipelineRunning = false;
      renderBuilderPipelineStatus();
      msg.textContent = `Run ${data.run_id} (${data.state || "submitted"}) [${runMode}]`;
      out.textContent = JSON.stringify(data, null, 2);
    }
    async function publishBuilderPipeline(){
      const msg = document.getElementById("builder_msg");
      const out = document.getElementById("builder_output");
      const payload = builderPayload();
      if(builderCreateMode){
        msg.textContent = "Use Create to create a new pipeline first.";
        return;
      }
      if (!payload.pipeline){
        msg.textContent = "pipeline path is required to publish.";
        return;
      }
      payload.pipeline = normalizeBuilderPipelineName(payload.pipeline);
      document.getElementById("b_pipeline_path").value = payload.pipeline;

      msg.textContent = "Saving draft before publish...";
      const encoded = encodeURIComponent(payload.pipeline);
      const update = await fetch(`/api/pipelines/${encoded}`, {
        method:"PUT",
        headers: {"Content-Type":"application/json"},
        body: JSON.stringify({
          yaml_text: payload.yaml_text,
          project_id: payload.project_id || "",
          pipeline_source: payload.pipeline_source || "",
        }),
      });
      if(update.status === 404){
        msg.textContent = "Pipeline does not exist yet. Click Create first.";
        return;
      }
      if(!update.ok){
        msg.textContent = await readMessage(update);
        return;
      }

      msg.textContent = "Validating draft before publish...";
      const validateRes = await fetch(`/api/builder/validate`, {
        method:"POST",
        headers: {"Content-Type":"application/json"},
        body: JSON.stringify(payload),
      });
      if(!validateRes.ok){
        msg.textContent = await readMessage(validateRes);
        return;
      }

      msg.textContent = "Publishing branch -> main...";
      const syncRes = await fetch(`/api/builder/git-sync`, {
        method:"POST",
        headers: {"Content-Type":"application/json"},
        body: JSON.stringify({
          pipeline: payload.pipeline,
          branch: payload.git_branch,
          push: true,
          create_branch: true,
          project_id: payload.project_id || "",
          projects_config: payload.projects_config || "",
          pipeline_source: payload.pipeline_source || "",
          publish_to_main: true,
          checkout_main_after_publish: true,
        }),
      });
      if(!syncRes.ok){
        msg.textContent = await readMessage(syncRes);
        return;
      }
      const data = await syncRes.json();
      await refreshBuilderGitStatus();
      msg.textContent = `Published ${payload.pipeline} to main via ${data.branch || "builder branch"}.`;
      out.textContent = JSON.stringify(data, null, 2);
    }
    async function terminateBuilderPipeline(){
      const msg = document.getElementById("builder_msg");
      const out = document.getElementById("builder_output");
      const runId = String(builderLastRunId || "").trim();
      if(!runId){
        msg.textContent = "No run selected to terminate.";
        return;
      }
      msg.textContent = `Terminating ${runId}...`;
      const body = {};
      if(builderLastRunExecutor){
        body.executor = builderLastRunExecutor;
      }
      const res = await fetch(`/api/runs/${encodeURIComponent(runId)}/stop`, {
        method:"POST",
        headers: {"Content-Type":"application/json"},
        body: JSON.stringify(body),
      });
      if(!res.ok){
        msg.textContent = await readMessage(res);
        return;
      }
      const data = await res.json();
      msg.textContent = data.message || `Terminate requested for ${runId}`;
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
      builderModel = normalizeBuilderModelPlugins(ensureBuilderDefaultDirs(data.model || builderModel));
      const projectSel = document.getElementById("b_project_id");
      if(projectSel){
        const pid = String((builderModel && builderModel.project_id) || "").trim();
        if(pid){
          projectSel.value = pid;
        } else {
          builderModel.project_id = String(projectSel.value || "").trim();
        }
      }
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
    function _sleep(ms){
      return new Promise((resolve) => setTimeout(resolve, ms));
    }
    async function stopBuilderStepTestAt(idx){
      const msg = document.getElementById("builder_msg");
      const testId = String(builderStepTestJob[idx] || "").trim();
      if(!testId){
        msg.textContent = "No running step test to stop.";
        return;
      }
      const res = await fetch(`/api/builder/test-step/stop`, {
        method:"POST",
        headers: {"Content-Type":"application/json"},
        body: JSON.stringify({ test_id: testId }),
      });
      if(!res.ok){
        msg.textContent = await readMessage(res);
        return;
      }
      builderStepLastLog[idx] = "Stop requested...";
      renderBuilderModel();
      msg.textContent = `Stop requested for step ${idx + 1}.`;
    }
    async function testBuilderStepAt(idx){
      const msg = document.getElementById("builder_msg");
      const out = document.getElementById("builder_output");
      builderStepTesting[idx] = true;
      builderStepLastLog[idx] = "Running...";
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
        builderStepLastLog[idx] = "Validation failed before step test.";
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
      const runMode = String((document.getElementById("b_run_mode") || {}).value || "draft").trim().toLowerCase();
      payload.allow_dirty_git = runMode !== "repro";
      const hintedExecutor = String(payload.executor || "").trim().toLowerCase();
      const envExecutor = payload.env && builderEnvExecutorMap[payload.env]
        ? String(builderEnvExecutorMap[payload.env] || "").trim().toLowerCase()
        : "";
      const effectiveExecutor = hintedExecutor || envExecutor || "local";
      const remoteExecutor = effectiveExecutor === "slurm" || effectiveExecutor === "hpcc_direct";
      const shouldGitSync = !!payload.git_sync || remoteExecutor;

      if(shouldGitSync){
        if(!payload.pipeline){
          builderStepStatus[idx] = "failed";
          delete builderStepTesting[idx];
          builderStepLastLog[idx] = "git_sync requires a pipeline path.";
          renderBuilderModel();
          msg.textContent = "git_sync requires a pipeline path.";
          return;
        }
        msg.textContent = payload.git_sync
          ? `Syncing git branch before step ${idx + 1} test...`
          : `Remote executor selected; syncing git branch before step ${idx + 1} test...`;
        const syncRes = await fetch(`/api/builder/git-sync`, {
          method:"POST",
          headers: {"Content-Type":"application/json"},
          body: JSON.stringify({
            pipeline: payload.pipeline,
            branch: payload.git_branch,
            push: true,
            create_branch: true,
            project_id: payload.project_id || "",
            projects_config: payload.projects_config || "",
            pipeline_source: payload.pipeline_source || "",
          }),
        });
        if(!syncRes.ok){
          builderStepStatus[idx] = "failed";
          delete builderStepTesting[idx];
          const errText = await readMessage(syncRes);
          builderStepLastLog[idx] = errText || "git sync failed.";
          renderBuilderModel();
          msg.textContent = errText;
          return;
        }
        await refreshBuilderGitStatus();
      }
      const startRes = await fetch(`/api/builder/test-step/start`, {
        method:"POST",
        headers: {"Content-Type":"application/json"},
        body: JSON.stringify(payload),
      });
      if(!startRes.ok){
        builderStepStatus[idx] = "failed";
        delete builderStepTesting[idx];
        const errText = await readMessage(startRes);
        builderStepLastLog[idx] = errText || "Step test failed.";
        renderBuilderModel();
        msg.textContent = errText;
        return;
      }
      const startData = await startRes.json();
      const testId = String(startData.test_id || "").trim();
      if(!testId){
        builderStepStatus[idx] = "failed";
        delete builderStepTesting[idx];
        builderStepLastLog[idx] = "Failed to start step test.";
        renderBuilderModel();
        msg.textContent = "Failed to start step test.";
        return;
      }
      builderStepTestJob[idx] = testId;

      let data = null;
      while(true){
        await _sleep(700);
        const statusRes = await fetch(`/api/builder/test-step/status?test_id=${encodeURIComponent(testId)}`);
        if(!statusRes.ok){
          builderStepStatus[idx] = "failed";
          delete builderStepTesting[idx];
          delete builderStepTestJob[idx];
          const errText = await readMessage(statusRes);
          builderStepLastLog[idx] = errText || "Step test failed.";
          renderBuilderModel();
          msg.textContent = errText;
          return;
        }
        const statusPayload = await statusRes.json();
        const state = String(statusPayload.state || "").trim().toLowerCase();
        if(state === "running" || state === "queued"){
          continue;
        }
        if(state === "cancelled"){
          builderStepStatus[idx] = "failed";
          delete builderStepTesting[idx];
          delete builderStepTestJob[idx];
          builderStepLastLog[idx] = "Step test cancelled.";
          renderBuilderModel();
          msg.textContent = `Step ${idx + 1} cancelled.`;
          return;
        }
        if(state !== "completed"){
          builderStepStatus[idx] = "failed";
          delete builderStepTesting[idx];
          delete builderStepTestJob[idx];
          const errText = String(statusPayload.error || statusPayload.detail || "Step test failed.");
          builderStepLastLog[idx] = errText;
          renderBuilderModel();
          msg.textContent = errText;
          return;
        }
        data = statusPayload.result || null;
        break;
      }
      if(!data){
        builderStepStatus[idx] = "failed";
        delete builderStepTesting[idx];
        delete builderStepTestJob[idx];
        builderStepLastLog[idx] = "Step test failed.";
        renderBuilderModel();
        msg.textContent = "Step test failed.";
        return;
      }
      builderStepStatus[idx] = data.success ? "run_ok" : "failed";
      builderStepOutput[idx] = JSON.stringify(data, null, 2);
      builderStepOutputCollapsed[idx] = false;
      builderStepLastLog[idx] = String(data.last_log_line || (data.success ? "Step test completed." : data.error || "Step test failed."));
      delete builderStepTesting[idx];
      delete builderStepTestJob[idx];
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
      const rootGroups = ["sys", "global", "env", "project", "vars", "dirs"];
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
    function renderDatasetCreateForm(msgText = "", isError = false){
      if(!isDatasetsView || isDatasetDetailView) return;
      const detailEl = document.getElementById("detail");
      detailEl.innerHTML = `
        <div><b>Add Dataset</b></div>
        <div class="muted">Create or update a dataset record in the registry.</div>
        <div class="controls">
          <input id="ds_dataset_id" placeholder="dataset_id (required, e.g. serve.demo_v1)" />
          <input id="ds_data_class" placeholder="data_class (optional, e.g. SERVE)" />
        </div>
        <div class="controls">
          <input id="ds_owner_user" placeholder="owner_user (optional)" />
          <input id="ds_status" placeholder="status (optional, default: active)" />
        </div>
        <div class="controls">
          <button id="btn_dataset_add">Add Dataset</button>
          <span id="dataset_add_msg" class="${isError ? "bad" : "muted"}">${esc(msgText)}</span>
        </div>
      `;
      const btn = document.getElementById("btn_dataset_add");
      if(btn){
        btn.onclick = createDatasetFromForm;
      }
    }
    async function createDatasetFromForm(){
      const msgEl = document.getElementById("dataset_add_msg");
      const datasetId = document.getElementById("ds_dataset_id").value.trim();
      const dataClass = document.getElementById("ds_data_class").value.trim();
      const ownerUser = document.getElementById("ds_owner_user").value.trim();
      const status = document.getElementById("ds_status").value.trim();
      if(!datasetId){
        if(msgEl){
          msgEl.className = "bad";
          msgEl.textContent = "dataset_id is required.";
        }
        return;
      }
      if(msgEl){
        msgEl.className = "muted";
        msgEl.textContent = "Saving...";
      }
      const payload = { dataset_id: datasetId };
      if(dataClass) payload.data_class = dataClass;
      if(ownerUser) payload.owner_user = ownerUser;
      if(status) payload.status = status;
      const res = await fetch("/api/datasets", {
        method: "POST",
        headers: {"Content-Type":"application/json"},
        body: JSON.stringify(payload),
      });
      if(!res.ok){
        const msg = await readMessage(res);
        if(msgEl){
          msgEl.className = "bad";
          msgEl.textContent = msg;
        }
        return;
      }
      const out = await res.json();
      if(msgEl){
        msgEl.className = "ok";
        msgEl.textContent = out.created ? `Created ${out.dataset_id}` : `Updated ${out.dataset_id}`;
      }
      await loadDatasets();
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
    async function quickStop(runId, body=null){
      const res = await fetch(`/api/runs/${encodeURIComponent(runId)}/stop`, {
        method:"POST",
        headers: {"Content-Type":"application/json"},
        body: JSON.stringify(body || {}),
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
        if(!builderProjectsReady){
          return;
        }
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
      if(isProjectDagView){
        await loadProjectDagPage();
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


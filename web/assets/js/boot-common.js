function etlBootCommon(){
  initViewMode();
  setActiveNav();
  initUserScope();
  initEnvScope();

  function bindClick(id, handler){
    const el = document.getElementById(id);
    if(el){ el.onclick = handler; }
  }

  bindClick("btn_nav_live", () => {
    const runEl = document.getElementById("nav_live_id");
    if(!runEl) return;
    const runId = String(runEl.value || "").trim();
    if(!runId) return;
    const asUser = encodeURIComponent(currentAsUser());
    window.location.href = `/runs/${encodeURIComponent(runId)}/live?as_user=${asUser}`;
  });

  loadPluginEnvOptions();
  loadNavProjects().then(async () => {
    await loadBuilderProjects();
    builderProjectsReady = true;
    if(isBuilderView){
      await refreshBuilderProjectVars(builderModel.project_id || currentProjectId());
      await refreshBuilderTreeFiles();
    }
    if(isProjectDagView){
      await loadProjectDagPage();
    }
  }).catch(() => {
    builderProjectsReady = true;
  });

  loadBuilderEnvironments().then(async () => {
    if(isBuilderView){
      await refreshBuilderGitStatus();
      renderBuilderModel();
    }
  });

  if(!isBuilderView){
    refreshBuilderTreeFiles();
  }
}

function etlStartPolling(){
  tick();
  if(isBuilderView){
    setTimeout(() => {
      if(!builderPreviewSectionCollapsed.vars){
        refreshBuilderNamespace();
      }
    }, 0);
  }
  setInterval(tick, 12000);
}

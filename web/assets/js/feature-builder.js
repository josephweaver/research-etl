etlBootCommon();

(function bindBuilder(){
  const bind = (id, fn) => { const el = document.getElementById(id); if(el) el.onclick = fn; };

  bind("btn_builder_import_local", () => { openBuilderFilePicker(); });
  bind("btn_builder_toggle_preview", () => { builderPreviewCollapsed = !builderPreviewCollapsed; renderBuilderPreviewPanel(); });
  bind("btn_builder_toggle_yaml", () => {
    builderPreviewSectionCollapsed.yaml = !builderPreviewSectionCollapsed.yaml;
    renderBuilderPreviewSections();
  });
  bind("btn_builder_toggle_output", () => {
    builderPreviewSectionCollapsed.output = !builderPreviewSectionCollapsed.output;
    renderBuilderPreviewSections();
  });
  bind("btn_builder_toggle_vars", () => {
    builderPreviewSectionCollapsed.vars = !builderPreviewSectionCollapsed.vars;
    renderBuilderPreviewSections();
    if(!builderPreviewSectionCollapsed.vars){
      refreshBuilderNamespace();
    }
  });
  bind("btn_builder_add_req", addBuilderRequire);
  bind("btn_builder_add_var", addBuilderVar);
  bind("btn_builder_add_step", addBuilderStep);
  bind("btn_builder_create", createBuilderPipeline);
  bind("btn_builder_save", saveBuilderDraft);
  bind("btn_builder_generate", generateBuilderDraft);
  bind("btn_builder_validate", validateBuilderDraft);
  bind("btn_builder_run", runBuilderPipeline);
  bind("btn_builder_new_session", async () => { await createBuilderSessionFromUi(); });
  bind("btn_builder_import_run_state", async () => { await importBuilderRunStateToSession(); });
  bind("btn_builder_publish", publishBuilderPipeline);
  bind("btn_builder_terminate", terminateBuilderPipeline);

  const addDirBtn = document.getElementById("btn_builder_add_dir");
  if(addDirBtn){ addDirBtn.onclick = addBuilderDir; }

  const sourceSel = document.getElementById("b_pipeline_source");
  if(sourceSel){
    sourceSel.onchange = () => {
      builderSelectedPipelineSource = String(sourceSel.value || "").trim();
      saveBuilderLastPipeline(
        document.getElementById("b_pipeline_path")?.value,
        builderSelectedPipelineSource,
        document.getElementById("b_project_id")?.value,
      );
      updateBuilderPipelinePathSuggestions();
    };
  }

  const pipelinePath = document.getElementById("b_pipeline_path");
  if(pipelinePath){
    pipelinePath.addEventListener("input", () => {
      if(!builderCreateMode) return;
      renderBuilderCreateMode();
    });
  }

  const projectSel = document.getElementById("b_project_id");
  if(projectSel){
    projectSel.onchange = async () => {
      const next = String(projectSel.value || "").trim();
      builderModel.project_id = next;
      const navSel = document.getElementById("nav_project");
      if(navSel && String(navSel.value || "").trim() !== next){
        navSel.value = next;
        localStorage.setItem(PROJECT_STORAGE_KEY, next);
        updateProjectDagNavHref();
      }
      saveBuilderLastPipeline(
        document.getElementById("b_pipeline_path")?.value,
        builderSelectedPipelineSource,
        next,
      );
      syncYamlPreview();
      await refreshBuilderProjectVars(next);
      await refreshBuilderTreeFiles();
      await refreshBuilderSessions();
    };
  }

  const sessionSel = document.getElementById("b_session_id");
  if(sessionSel){
    sessionSel.onchange = async () => {
      const next = String(sessionSel.value || "").trim();
      await onBuilderSessionSelect(next);
    };
  }

  const reqEl = document.getElementById("b_requires");
  const varEl = document.getElementById("b_vars");
  const dirsEl = document.getElementById("b_dirs");
  const stepsEl = document.getElementById("b_steps");
  const filePickEl = document.getElementById("b_file_picker");

  if(reqEl){ reqEl.addEventListener("input", handleBuilderInput); reqEl.addEventListener("change", handleBuilderInput); reqEl.addEventListener("click", handleBuilderClicks); }
  if(varEl){ varEl.addEventListener("input", handleBuilderInput); varEl.addEventListener("change", handleBuilderInput); varEl.addEventListener("click", handleBuilderClicks); }
  if(dirsEl){ dirsEl.addEventListener("input", handleBuilderInput); dirsEl.addEventListener("change", handleBuilderInput); dirsEl.addEventListener("click", handleBuilderClicks); }
  if(stepsEl){
    stepsEl.addEventListener("input", handleBuilderInput);
    stepsEl.addEventListener("change", handleBuilderInput);
    stepsEl.addEventListener("focusin", handleBuilderStepPluginPickerFocus, true);
    stepsEl.addEventListener("input", handleBuilderStepPluginPickerInput, true);
    stepsEl.addEventListener("click", handleBuilderClicks);
  }
  document.addEventListener("mousedown", handleBuilderStepPluginPickerOutsideMouseDown);
  if(filePickEl){
    filePickEl.addEventListener("change", async () => { await loadBuilderSourceFromFilePicker(); });
  }

  renderBuilderPreviewPanel();
  renderBuilderPreviewSections();
  initBuilderTreeComboBehavior();
  initBuilderTooltipResolution();
})();

etlStartPolling();

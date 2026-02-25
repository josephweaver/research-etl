etlBootCommon();

(function bindOps(){
  const bind = (id, fn) => { const el = document.getElementById(id); if(el) el.onclick = fn; };
  bind("btn_apply", tick);
  bind("btn_ops_refresh", tick);
  bind("btn_pipelines", tick);
  bind("btn_validate", validateAction);
  bind("btn_run", runAction);
  bind("btn_resume", resumeSelected);
  bind("btn_stop", stopSelected);
})();

etlStartPolling();

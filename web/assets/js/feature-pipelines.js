etlBootCommon();

(function bindPipelines(){
  const bind = (id, fn) => { const el = document.getElementById(id); if(el) el.onclick = fn; };
  bind("btn_pipelines", tick);
  bind("btn_validate", validateAction);
  bind("btn_run", runAction);
})();

etlStartPolling();

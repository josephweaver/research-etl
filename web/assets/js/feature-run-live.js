etlBootCommon();

(function bindRunLive(){
  const bind = (id, fn) => { const el = document.getElementById(id); if(el) el.onclick = fn; };
  bind("btn_resume", resumeSelected);
  bind("btn_stop", stopSelected);
})();

etlStartPolling();

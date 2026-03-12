etlBootCommon();

(function bindQuery(){
  const bind = (id, fn) => { const el = document.getElementById(id); if(el) el.onclick = fn; };
  bind("btn_query_preview", runQueryPreview);
})();

etlStartPolling();

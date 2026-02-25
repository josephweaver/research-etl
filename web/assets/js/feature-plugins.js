etlBootCommon();
(function bindPlugins(){
  const el = document.getElementById("btn_plugins_refresh");
  if(el) el.onclick = tick;
  const env = document.getElementById("plugins_env");
  if(env) env.onchange = tick;
})();
etlStartPolling();

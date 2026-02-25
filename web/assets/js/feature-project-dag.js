etlBootCommon();
(function bindDag(){
  const el = document.getElementById("btn_project_dag_refresh");
  if(el){
    el.onclick = () => { loadProjectDagPage(); };
  }
})();
etlStartPolling();

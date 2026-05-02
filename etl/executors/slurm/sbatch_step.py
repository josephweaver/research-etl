from __future__ import annotations

from pathlib import Path
import shlex
from ...permissions import shell_permissions_prelude
from .template_engine import render_template_file


def write_step_sbatch(destination: Path, script_text: str) -> Path:
    destination.parent.mkdir(parents=True, exist_ok=True)
    destination.write_text(str(script_text or "").replace("\r\n", "\n"), encoding="utf-8", newline="\n")
    return destination


def _render_chunk(name: str, lines: list[str]) -> str:
    if not lines:
        return ""
    return "\n".join([f"# --- chunk: {name} ---", *lines])


def _with_chunk_logs(lines: list[str], name: str, verbose: bool) -> list[str]:
    if not lines:
        return []
    out: list[str] = []
    if verbose:
        out.append(f"log_step 'chunk:{name}:start'")
    out.extend(lines)
    if verbose:
        out.append(f"log_step 'chunk:{name}:end'")
    return out


def render_step_script(
    *,
    executor,
    run_id: str,
    checkout_root: str,
    pipeline_path: str,
    steps: list,
    step_indices: list[int],
    context_file: str,
    workdir: str,
    plugins_dir: str,
    logdir: str,
    venv_path: str,
    req_path: str,  # kept for future parity/simplification work
    python_bin: str,
    project_id: str | None = None,
    resume_run_id: str | None = None,
    run_started_at: str | None = None,
    global_config_path: str | None = None,
    projects_config_path: str | None = None,
    environments_config_path: str | None = None,
    commandline_vars: dict | None = None,
    child_jobs_file: str | None = None,
    sbatch_time: str | None = None,
    sbatch_cpus_per_task: int | None = None,
    sbatch_mem: str | None = None,
    array_index: bool = False,
    array_count: int | None = None,
    array_max_parallel: int | None = None,
    foreach_from_array: bool = False,
    foreach_item_offset: int = 0,
    flatten_vars_for_cli=None,
) -> str:
    logdir = logdir or (Path(workdir) / "slurm_logs").as_posix()
    sbatch_lines: list[str] = []
    chunk_runtime_flags: list[str] = []
    chunk_permissions: list[str] = shell_permissions_prelude(getattr(executor, "env_config", {}))
    chunk_runtime_bootstrap: list[str] = []
    chunk_modules: list[str] = []
    chunk_runtime_ready: list[str] = []
    chunk_step_scope: list[str] = []
    chunk_run_batch: list[str] = []
    eff_time = str(sbatch_time or executor.env.time or "").strip() or None
    eff_cpus = sbatch_cpus_per_task if sbatch_cpus_per_task not in (None, 0) else executor.env.cpus_per_task
    eff_mem = str(sbatch_mem or executor.env.mem or "").strip() or None
    if executor.env.partition:
        sbatch_lines.append(f"#SBATCH -p {executor.env.partition}")
    if executor.env.account:
        sbatch_lines.append(f"#SBATCH -A {executor.env.account}")
    if eff_time:
        sbatch_lines.append(f"#SBATCH -t {eff_time}")
    if eff_cpus:
        sbatch_lines.append(f"#SBATCH -c {int(eff_cpus)}")
    if eff_mem:
        sbatch_lines.append(f"#SBATCH --mem={eff_mem}")
    sbatch_lines.append(f"#SBATCH -J etl-{run_id[:8]}")
    sbatch_lines.append(f"#SBATCH -o {logdir}/etl-{run_id}-%j.%a.out" if array_index else f"#SBATCH -o {logdir}/etl-{run_id}-%j.out")
    if array_index:
        array_n = int(array_count or len(steps))
        array_upper = max(0, array_n - 1)
        if array_max_parallel not in (None, ""):
            try:
                arr_cap = int(array_max_parallel)
            except (TypeError, ValueError):
                arr_cap = 0
            if arr_cap > 0:
                sbatch_lines.append(f"#SBATCH --array=0-{array_upper}%{arr_cap}")
            else:
                sbatch_lines.append(f"#SBATCH --array=0-{array_upper}")
        else:
            sbatch_lines.append(f"#SBATCH --array=0-{array_upper}")
    if executor.env.sbatch_extra:
        for extra in executor.env.sbatch_extra:
            sbatch_lines.append(f"#SBATCH {extra}")
    if executor.verbose:
        chunk_runtime_flags.append("ETL_VERBOSE=1")
        chunk_runtime_flags.append("log_step(){ [ \"$ETL_VERBOSE\" = \"1\" ] && echo \"[etl][$(date -u +%Y-%m-%dT%H:%M:%SZ)] $1\"; }")
        chunk_runtime_flags.append("log_step 'batch bootstrap started'")
    if executor.verbose:
        chunk_runtime_bootstrap.append("log_step 'creating log and work directories'")
    chunk_runtime_bootstrap.append(f"mkdir -p {logdir}")
    chunk_runtime_bootstrap.append(f"etl_fix_permissions {shlex.quote(logdir)}")
    chunk_runtime_bootstrap.append(f"cd {checkout_root}")
    if executor.verbose:
        chunk_runtime_bootstrap.append("log_step 'activating runtime environment'")
    chunk_runtime_bootstrap.append(f"PYTHON={python_bin}")
    chunk_runtime_bootstrap.append(f"VENV={venv_path}")
    chunk_runtime_bootstrap.append("VENV_BASE=\"$VENV\"")
    chunk_runtime_bootstrap.append(f"export ETL_REPO_ROOT={checkout_root}")
    chunk_runtime_bootstrap.append(
        "if [ -z \"${ETL_PIPELINE_ASSET_CACHE_ROOT:-}\" ]; then export ETL_PIPELINE_ASSET_CACHE_ROOT=\"$(dirname \"$ETL_REPO_ROOT\")\"; fi"
    )
    chunk_runtime_bootstrap.append("export ETL_PIPELINE_ASSET_SYNC_MODE=cache_only")
    executor._append_db_tunnel_lines(chunk_runtime_bootstrap)
    if child_jobs_file:
        chunk_runtime_bootstrap.append(f"export ETL_CHILD_JOBS_FILE={child_jobs_file}")
    if executor.load_secrets_file:
        if executor.verbose:
            chunk_runtime_bootstrap.append("log_step 'loading optional secrets file (values hidden)'")
        chunk_runtime_bootstrap.append("if [ -f \"$HOME/.secrets/etl\" ]; then source \"$HOME/.secrets/etl\"; fi")
    chunk_runtime_bootstrap.append("ETL_HOSTNAME=\"$(hostname 2>/dev/null || echo unknown-host)\"")
    chunk_runtime_bootstrap.append("ETL_NODE_FAMILY=\"$(printf '%s' \"$ETL_HOSTNAME\" | awk -F- '{print $1}')\"")
    chunk_runtime_bootstrap.append("ETL_ARCH=\"$(uname -m 2>/dev/null || echo unknown-arch)\"")
    chunk_runtime_bootstrap.append("ETL_CPU_MODEL=\"$(lscpu 2>/dev/null | awk -F: '/Model name/ {gsub(/^ +/, \"\", $2); print $2; exit}' || echo unknown-cpu)\"")
    chunk_runtime_bootstrap.append("ETL_CPU_FLAGS=\"$(lscpu 2>/dev/null | awk -F: '/Flags/ {gsub(/^ +/, \"\", $2); print $2; exit}' || echo unknown-flags)\"")
    chunk_runtime_bootstrap.append("if [ -z \"$ETL_NODE_FAMILY\" ]; then ETL_NODE_FAMILY=\"$ETL_ARCH\"; fi")
    chunk_runtime_bootstrap.append("VENV=\"$VENV_BASE-$ETL_NODE_FAMILY\"")
    chunk_runtime_bootstrap.append("ETL_VENV_INFO=\"$VENV/.etl_venv_build_info\"")

    if executor.env.modules:
        for mod in executor.env.modules:
            if executor.verbose:
                chunk_modules.append(f"log_step {shlex.quote(f'loading module: {mod}')}")
            chunk_modules.append(f"module load {mod}")
    if executor.env.conda_env:
        if executor.verbose:
            chunk_modules.append("log_step 'activating conda environment'")
        chunk_modules.append(f"source activate {executor.env.conda_env}")
    chunk_runtime_ready.append(f"REQ_PATH={shlex.quote(req_path)}")
    chunk_runtime_ready.append("ETL_VENV_LOCKDIR=\"$VENV.lockdir\"")
    chunk_runtime_ready.append("acquire_venv_lock(){ while ! mkdir \"$ETL_VENV_LOCKDIR\" 2>/dev/null; do sleep 2; done; }")
    chunk_runtime_ready.append("release_venv_lock(){ rmdir \"$ETL_VENV_LOCKDIR\" 2>/dev/null || true; }")
    chunk_runtime_ready.append("rebuild_venv(){")
    chunk_runtime_ready.append("  if [ -d \"$VENV\" ]; then")
    chunk_runtime_ready.append("    case \"$VENV\" in")
    chunk_runtime_ready.append("      \"$ETL_REPO_ROOT\"/*) rm -rf \"$VENV\" ;;")
    chunk_runtime_ready.append("      *) echo \"[etl][step] refusing to remove venv outside ETL_REPO_ROOT: $VENV\" >&2; exit 1 ;;")
    chunk_runtime_ready.append("    esac")
    chunk_runtime_ready.append("  fi")
    chunk_runtime_ready.append("  $PYTHON -m venv --copies \"$VENV\"")
    chunk_runtime_ready.append("}")
    chunk_runtime_ready.append("acquire_venv_lock")
    if executor.verbose:
        chunk_runtime_ready.append("log_step 'ensuring family-specific runtime venv exists'")
    chunk_runtime_ready.append("if [ ! -f \"$VENV/bin/activate\" ]; then rebuild_venv; fi")
    chunk_runtime_ready.append("if [ -x \"$VENV/bin/python\" ] && ! \"$VENV/bin/python\" -c 'import sys' >/dev/null 2>&1; then")
    chunk_runtime_ready.append("  echo \"[etl][step] existing family venv interpreter failed smoke test; rebuilding: $VENV\" >&2")
    chunk_runtime_ready.append("  rebuild_venv")
    chunk_runtime_ready.append("fi")
    chunk_runtime_ready.append("if [ ! -f \"$VENV/bin/activate\" ]; then echo \"[etl][step] venv activation script missing: $VENV/bin/activate\" >&2; release_venv_lock; exit 1; fi")
    chunk_runtime_ready.append("if [ -f \"$REQ_PATH\" ]; then \"$VENV/bin/python\" -m pip install -r \"$REQ_PATH\"; fi")
    chunk_runtime_ready.append(f"export PYTHONPATH={checkout_root}:${{PYTHONPATH:-}}")
    chunk_runtime_ready.append("if ! \"$VENV/bin/python\" -c 'import etl.run_batch' >/dev/null 2>&1; then")
    chunk_runtime_ready.append("  \"$VENV/bin/python\" -m pip install --no-deps -e \"$ETL_REPO_ROOT\"")
    chunk_runtime_ready.append("fi")
    chunk_runtime_ready.append("ETL_SETUP_HOSTNAME=\"$ETL_HOSTNAME\"")
    chunk_runtime_ready.append("ETL_SETUP_ARCH=\"$ETL_ARCH\"")
    chunk_runtime_ready.append("ETL_SETUP_CPU_MODEL=\"$ETL_CPU_MODEL\"")
    chunk_runtime_ready.append("ETL_SETUP_CPU_FLAGS=\"$ETL_CPU_FLAGS\"")
    chunk_runtime_ready.append("printf 'setup_hostname=%q\\nsetup_arch=%q\\nsetup_cpu_model=%q\\nsetup_cpu_flags=%q\\nvenv_path=%q\\nrepo_root=%q\\n' \"$ETL_SETUP_HOSTNAME\" \"$ETL_SETUP_ARCH\" \"$ETL_SETUP_CPU_MODEL\" \"$ETL_SETUP_CPU_FLAGS\" \"$VENV\" \"$ETL_REPO_ROOT\" > \"$ETL_VENV_INFO\"")
    chunk_runtime_ready.append('etl_fix_permissions "$VENV"')
    chunk_runtime_ready.append("release_venv_lock")
    chunk_runtime_ready.append("if [ -f \"$ETL_VENV_INFO\" ]; then source \"$ETL_VENV_INFO\"; fi")
    if executor.verbose:
        chunk_runtime_ready.append("log_step 'activating venv after module setup'")
    chunk_runtime_ready.append("source \"$VENV/bin/activate\"")
    executor._append_db_tunnel_database_url_rewrite_lines(chunk_runtime_ready, python_expr="\"$VENV/bin/python\"")

    env_workdir = Path(workdir).as_posix()
    if executor.verbose:
        chunk_step_scope.append("log_step 'ensuring step workdir exists'")
    chunk_step_scope.append(f"mkdir -p {env_workdir}")
    chunk_step_scope.append(f"etl_fix_permissions {shlex.quote(env_workdir)}")
    if executor.verbose:
        chunk_step_scope.append("log_step 'switching to step workdir'")
    chunk_step_scope.append(f"cd {env_workdir}")
    foreach_arg: str | None = None
    if array_index and foreach_from_array:
        step_arg = ",".join(str(i) for i in step_indices)
        if int(foreach_item_offset or 0) > 0:
            foreach_arg = f"$((SLURM_ARRAY_TASK_ID+{int(foreach_item_offset)}))"
        else:
            foreach_arg = "${SLURM_ARRAY_TASK_ID}"
    elif array_index:
        indices_str = " ".join(str(i) for i in step_indices)
        chunk_step_scope.append(f"step_indices=({indices_str})")
        step_arg = "${step_indices[$SLURM_ARRAY_TASK_ID]}"
    else:
        step_arg = ",".join(str(i) for i in step_indices)
    run_started_expr = ""
    if run_started_at:
        chunk_run_batch.append("RUN_STARTED_OPT=''")
        chunk_run_batch.append("RUN_STARTED_VAL=''")
        chunk_run_batch.append("if \"$VENV/bin/python\" -m etl.run_batch -h 2>&1 | grep -q -- '--run-started-at'; then")
        chunk_run_batch.append("  RUN_STARTED_OPT='--run-started-at'")
        chunk_run_batch.append(f"  RUN_STARTED_VAL={shlex.quote(str(run_started_at))}")
        chunk_run_batch.append("fi")
        run_started_expr = "${RUN_STARTED_OPT:+$RUN_STARTED_OPT $RUN_STARTED_VAL}"

    cmd = [
        "$VENV/bin/python",
        "-m",
        "etl.run_batch",
        pipeline_path,
        "--steps",
        step_arg,
        "--plugins-dir",
        plugins_dir,
        "--workdir",
        env_workdir,
    ]
    cmd += ["--context-file", context_file]
    cmd += ["--run-id", run_id]
    if project_id:
        cmd += ["--project-id", str(project_id)]
    if resume_run_id:
        cmd += ["--resume-run-id", str(resume_run_id)]
    if run_started_expr:
        cmd += [run_started_expr]
    cmd += ["--max-retries", str(executor.step_max_retries)]
    cmd += ["--retry-delay-seconds", str(executor.step_retry_delay_seconds)]
    if global_config_path:
        chunk_runtime_bootstrap.append(f"export ETL_GLOBAL_CONFIG={global_config_path}")
        cmd += ["--global-config", global_config_path]
    if projects_config_path:
        chunk_runtime_bootstrap.append(f"export ETL_PROJECTS_CONFIG={projects_config_path}")
        cmd += ["--projects-config", projects_config_path]
    if environments_config_path and executor.env_name:
        chunk_runtime_bootstrap.append(f"export ETL_ENVIRONMENTS_CONFIG={environments_config_path}")
        chunk_runtime_bootstrap.append(f"export ETL_ENV_NAME={executor.env_name}")
        cmd += ["--environments-config", environments_config_path, "--env", executor.env_name]
    if project_id:
        chunk_runtime_bootstrap.append(f"export ETL_PROJECT_ID={project_id}")
    if foreach_arg:
        cmd += ["--foreach-item-index", foreach_arg]
    flatten_fn = flatten_vars_for_cli
    if flatten_fn is None:
        flatten_fn = lambda d: []  # noqa: E731
    for key, value in flatten_fn(dict(commandline_vars or {})):
        cmd += ["--var", f"{key}={value}"]
    if executor.verbose:
        cmd += ["--verbose"]

    if executor.verbose:
        chunk_run_batch.append("log_step 'running etl.run_batch'")
    chunk_run_batch.append(" ".join(cmd))
    template_path = Path(__file__).resolve().parent / "templates" / "step.sbatch.tmpl"
    return render_template_file(
        template_path,
        {
            "sbatch_lines": "\n".join(sbatch_lines),
            "chunk_runtime_flags": _render_chunk("runtime_flags", chunk_runtime_flags),
            "chunk_permissions": _render_chunk("permissions", chunk_permissions),
            "chunk_runtime_bootstrap": _render_chunk("runtime_bootstrap", _with_chunk_logs(chunk_runtime_bootstrap, "runtime_bootstrap", executor.verbose)),
            "chunk_modules": _render_chunk("modules", _with_chunk_logs(chunk_modules, "modules", executor.verbose)),
            "chunk_runtime_ready": _render_chunk("runtime_ready", _with_chunk_logs(chunk_runtime_ready, "runtime_ready", executor.verbose)),
            "chunk_step_scope": _render_chunk("step_scope", _with_chunk_logs(chunk_step_scope, "step_scope", executor.verbose)),
            "chunk_run_batch": _render_chunk("run_batch", _with_chunk_logs(chunk_run_batch, "run_batch", executor.verbose)),
        },
    )

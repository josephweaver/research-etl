from __future__ import annotations

from pathlib import Path
import shlex
from ...common.parsing import parse_bool
from .template_engine import render_template_file
from ...source_control import CheckoutSpec, checkout


def write_setup_sbatch(destination: Path, script_text: str) -> Path:
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

def render_setup_script(
    *,
    executor,
    run_id: str,
    checkout_root: str,
    workdir: str,
    logdir: str,
    venv_path: str,
    req_path: str,
    python_bin: str,
    workdirs_to_create: list[str],
    logdirs_to_create: list[str],
    execution_source: str = "auto",
    git_origin_url: str | None = None,
    git_commit_sha: str | None = None,
    source_bundle_path: str | None = None,
    source_snapshot_path: str | None = None,
    allow_workspace_source: bool = False,
    pipeline_asset_overlays: list[dict[str, str]] | None = None,
    parse_slurm_time_to_minutes,
    parse_mem_to_mb,
    format_minutes_as_slurm_time,
    format_mb_as_slurm_mem,
) -> str:
    logdir = logdir or (Path(workdir) / "slurm_logs").as_posix()
    sbatch_lines: list[str] = []
    chunk_runtime_flags: list[str] = []
    chunk_dirs: list[str] = []
    chunk_modules: list[str] = []
    chunk_source_checkout: list[str] = []
    chunk_asset_overlays: list[str] = []
    chunk_venv_bootstrap: list[str] = []
    chunk_install: list[str] = []
    chunk_finalize: list[str] = []
    setup_time = str(executor.env.setup_time or executor.env.time or "00:10:00").strip() or "00:10:00"
    setup_cpus = int(executor.env.cpus_per_task or 0) or None
    setup_mem = str(executor.env.mem or "").strip() or None
    max_time_min = parse_slurm_time_to_minutes(str(executor.env.max_time or ""))
    max_cpus = int(executor.env.max_cpus_per_task or 0) or None
    max_mem_mb = parse_mem_to_mb(str(executor.env.max_mem or ""))
    if setup_time and max_time_min is not None:
        cur_min = parse_slurm_time_to_minutes(setup_time)
        if cur_min is not None:
            setup_time = format_minutes_as_slurm_time(min(cur_min, max_time_min))
    if setup_cpus and max_cpus is not None:
        setup_cpus = min(setup_cpus, max_cpus)
    if setup_mem and max_mem_mb is not None:
        cur_mem_mb = parse_mem_to_mb(setup_mem)
        if cur_mem_mb is not None:
            setup_mem = format_mb_as_slurm_mem(min(cur_mem_mb, max_mem_mb))
    if executor.env.partition:
        sbatch_lines.append(f"#SBATCH -p {executor.env.partition}")
    if executor.env.account:
        sbatch_lines.append(f"#SBATCH -A {executor.env.account}")
    if setup_time:
        sbatch_lines.append(f"#SBATCH -t {setup_time}")
    if setup_cpus:
        sbatch_lines.append(f"#SBATCH -c {setup_cpus}")
    if setup_mem:
        sbatch_lines.append(f"#SBATCH --mem={setup_mem}")
    sbatch_lines.append(f"#SBATCH -J etl-setup-{run_id[:6]}")
    sbatch_lines.append(f"#SBATCH -o {logdir}/etl-setup-{run_id}-%j.out")
    if executor.env.sbatch_extra:
        for extra in executor.env.sbatch_extra:
            sbatch_lines.append(f"#SBATCH {extra}")
    if executor.verbose:
        chunk_runtime_flags.append("ETL_VERBOSE=1")
        chunk_runtime_flags.append("log_step(){ [ \"$ETL_VERBOSE\" = \"1\" ] && echo \"[etl][$(date -u +%Y-%m-%dT%H:%M:%SZ)] $1\"; }")
        chunk_runtime_flags.append("log_step 'setup bootstrap started'")
    if executor.verbose:
        chunk_dirs.append("log_step 'creating setup directories'")
    chunk_dirs.append(f"echo \"[etl][setup][paths] workdir={workdir}\"")
    chunk_dirs.append(f"echo \"[etl][setup][paths] setup_logdir={logdir}\"")
    chunk_dirs.append(f"mkdir -p {logdir}")
    chunk_dirs.append(f"mkdir -p {workdir}")
    for d in workdirs_to_create:
        chunk_dirs.append(f"mkdir -p {d}")
    for d in logdirs_to_create:
        chunk_dirs.append(f"mkdir -p {d}")
    if executor.env.modules:
        for mod in executor.env.modules:
            if executor.verbose:
                chunk_modules.append(f"log_step {shlex.quote(f'loading module: {mod}')}")
            chunk_modules.append(f"module load {mod}")
    if executor.env.conda_env:
        if executor.verbose:
            chunk_modules.append("log_step 'activating conda environment'")
        chunk_modules.append(f"source activate {executor.env.conda_env}")
    if executor.verbose:
        chunk_source_checkout.append("log_step 'preparing execution source'")
    mode = str(execution_source or "auto").strip().lower() or "auto"
    allow_non_git_source = parse_bool(getattr(executor, "env_config", {}).get("allow_non_git_source"), default=False)
    repo_url_q = shlex.quote(str(git_origin_url or "").strip())
    repo_sha_q = shlex.quote(str(git_commit_sha or "").strip())
    source_bundle_q = shlex.quote(str(source_bundle_path or "").strip())
    source_snapshot_q = shlex.quote(str(source_snapshot_path or "").strip())
    chunk_source_checkout.append(f"CHECKOUT_ROOT={shlex.quote(checkout_root)}")
    chunk_source_checkout.append(f"REPO_URL={repo_url_q}")
    chunk_source_checkout.append(f"REPO_SHA={repo_sha_q}")
    chunk_source_checkout.append(f"SOURCE_BUNDLE={source_bundle_q}")
    chunk_source_checkout.append(f"SOURCE_SNAPSHOT={source_snapshot_q}")
    # Pre-1.0 simplified source path: default to linear git_remote flow.
    if mode in {"git_remote", "auto"}:
        chunk_source_checkout.append(f"[ -n {repo_url_q} ] && [ -n {repo_sha_q} ] || {{ echo \"[etl][setup][source] missing repo_url or repo_sha for git_remote\" >&2; exit 1; }}")
        chunk_source_checkout.extend(
            checkout(
                CheckoutSpec(
                    repo_url=repo_url_q,
                    revision=repo_sha_q,
                    checkout_root=shlex.quote(checkout_root),
                    mode="detached",
                )
            )
        )
    elif mode == "workspace":
        if allow_workspace_source and allow_non_git_source:
            chunk_source_checkout.append("mkdir -p \"$(dirname \\\"$CHECKOUT_ROOT\\\")\"")
            chunk_source_checkout.append("[ -d \"$CHECKOUT_ROOT\" ] || { echo \"[etl][setup][source] workspace checkout missing: $CHECKOUT_ROOT\" >&2; exit 1; }")
            chunk_source_checkout.append("cd \"$CHECKOUT_ROOT\" || { echo \"[etl][setup][source] cannot cd checkout root: $CHECKOUT_ROOT\" >&2; exit 1; }")
        else:
            chunk_source_checkout.append("echo \"[etl][setup][source] workspace mode disabled (set allow_non_git_source=true and allow_workspace_source=true to enable)\" >&2")
            chunk_source_checkout.append("exit 1")
    elif mode == "git_bundle":
        if allow_non_git_source:
            chunk_source_checkout.append(f"[ -n {source_bundle_q} ] && [ -n {repo_sha_q} ] || {{ echo \"[etl][setup][source] missing source_bundle or repo_sha for git_bundle\" >&2; exit 1; }}")
            chunk_source_checkout.append(f"if [ ! -f {source_bundle_q} ]; then echo \"[etl][setup][source] source bundle not found\" >&2; exit 1; fi")
            chunk_source_checkout.append("if [ -d \"$CHECKOUT_ROOT\" ] && [ ! -d \"$CHECKOUT_ROOT/.git\" ]; then rm -rf \"$CHECKOUT_ROOT\"; fi")
            chunk_source_checkout.append(f"if [ ! -d \"$CHECKOUT_ROOT/.git\" ]; then git clone --no-checkout {source_bundle_q} \"$CHECKOUT_ROOT\" || {{ echo \"[etl][setup][source] git clone from bundle failed\" >&2; exit 1; }}; fi")
            chunk_source_checkout.append("cd \"$CHECKOUT_ROOT\" || { echo \"[etl][setup][source] cannot cd checkout root: $CHECKOUT_ROOT\" >&2; exit 1; }")
            chunk_source_checkout.append(f"git fetch {source_bundle_q} --tags || {{ echo \"[etl][setup][source] git fetch from bundle failed\" >&2; exit 1; }}")
            chunk_source_checkout.append(f"git checkout --detach {repo_sha_q} || {{ echo \"[etl][setup][source] git checkout failed for requested SHA\" >&2; exit 1; }}")
            chunk_source_checkout.append(f"git reset --hard {repo_sha_q} || {{ echo \"[etl][setup][source] git reset failed for requested SHA\" >&2; exit 1; }}")
        else:
            chunk_source_checkout.append("echo \"[etl][setup][source] git_bundle mode disabled (set allow_non_git_source=true to enable)\" >&2")
            chunk_source_checkout.append("exit 1")
    elif mode == "snapshot":
        if allow_non_git_source:
            chunk_source_checkout.append(f"[ -n {source_snapshot_q} ] || {{ echo \"[etl][setup][source] missing source_snapshot for snapshot mode\" >&2; exit 1; }}")
            chunk_source_checkout.append(f"if [ ! -f {source_snapshot_q} ]; then echo \"[etl][setup][source] source snapshot not found\" >&2; exit 1; fi")
            chunk_source_checkout.append("rm -rf \"$CHECKOUT_ROOT\"")
            chunk_source_checkout.append("mkdir -p \"$CHECKOUT_ROOT\"")
            chunk_source_checkout.append(f"case {source_snapshot_q} in")
            chunk_source_checkout.append(f"  *.zip) unzip -q {source_snapshot_q} -d \"$CHECKOUT_ROOT\" || exit 1 ;;")
            chunk_source_checkout.append(f"  *.tar|*.tar.gz|*.tgz|*.tar.bz2|*.tbz2|*.tar.xz|*.txz) tar -xf {source_snapshot_q} -C \"$CHECKOUT_ROOT\" || exit 1 ;;")
            chunk_source_checkout.append("  *) echo \"[etl][setup][source] unsupported snapshot extension\" >&2; exit 1 ;;")
            chunk_source_checkout.append("esac")
            chunk_source_checkout.append("cd \"$CHECKOUT_ROOT\" || { echo \"[etl][setup][source] cannot cd checkout root: $CHECKOUT_ROOT\" >&2; exit 1; }")
        else:
            chunk_source_checkout.append("echo \"[etl][setup][source] snapshot mode disabled (set allow_non_git_source=true to enable)\" >&2")
            chunk_source_checkout.append("exit 1")
    else:
        chunk_source_checkout.append(f"echo \"Unsupported execution_source: {shlex.quote(mode)}\" >&2")
        chunk_source_checkout.append("exit 1")
    overlays = list(pipeline_asset_overlays or [])
    if overlays:
        chunk_asset_overlays.append("ASSET_PIPELINES_LINKED=0")
        chunk_asset_overlays.append("ASSET_SCRIPTS_LINKED=0")
    for idx, overlay in enumerate(overlays):
        repo_url = str((overlay or {}).get("repo_url") or "").strip()
        if not repo_url:
            continue
        ref = str((overlay or {}).get("ref") or "main").strip() or "main"
        pipelines_dir = str((overlay or {}).get("pipelines_dir") or "pipelines").strip() or "pipelines"
        scripts_dir = str((overlay or {}).get("scripts_dir") or "scripts").strip() or "scripts"
        # Keep external pipeline assets in a sibling cache root (shared across checkouts)
        # instead of nesting clones under the ETL checkout directory.
        asset_dir_var = f"ASSET_DIR_{idx}"
        if executor.verbose:
            chunk_asset_overlays.append(f"log_step {shlex.quote(f'syncing pipeline asset source {idx + 1}: {repo_url} ({ref})')}")
        chunk_asset_overlays.append(f"ASSET_URL_{idx}={shlex.quote(repo_url)}")
        chunk_asset_overlays.append(f"ASSET_REF_{idx}={shlex.quote(ref)}")
        chunk_asset_overlays.append("ASSET_CACHE_ROOT=${ETL_PIPELINE_ASSET_CACHE_ROOT:-$(dirname \"$CHECKOUT_ROOT\")}")
        chunk_asset_overlays.append("mkdir -p \"$ASSET_CACHE_ROOT\"")
        chunk_asset_overlays.append(f"ASSET_REPO_NAME_{idx}=\"$(basename \"$ASSET_URL_{idx}\")\"")
        chunk_asset_overlays.append(f"ASSET_REPO_NAME_{idx}=\"${{ASSET_REPO_NAME_{idx}%.git}}\"")
        chunk_asset_overlays.append(f"ASSET_REPO_NAME_{idx}=\"$(printf '%s' \"$ASSET_REPO_NAME_{idx}\" | sed -E 's/[^A-Za-z0-9._-]+/-/g; s/^-+//; s/-+$//')\"")
        chunk_asset_overlays.append(f"ASSET_COMMIT_{idx}=\"$(git ls-remote \"$ASSET_URL_{idx}\" \"$ASSET_REF_{idx}\" | awk 'NR==1 {{print $1}}')\"")
        chunk_asset_overlays.append(f"if [ -z \"$ASSET_COMMIT_{idx}\" ]; then echo \"[etl][setup][assets] could not resolve commit for $ASSET_URL_{idx} ref=$ASSET_REF_{idx}\" >&2; exit 1; fi")
        chunk_asset_overlays.append(f"ASSET_SHORT_SHA_{idx}=\"$(printf '%s' \"$ASSET_COMMIT_{idx}\" | cut -c1-12)\"")
        chunk_asset_overlays.append(f"{asset_dir_var}=\"$ASSET_CACHE_ROOT/${{ASSET_REPO_NAME_{idx}}}-${{ASSET_SHORT_SHA_{idx}}}\"")
        chunk_asset_overlays.extend(
            checkout(
                CheckoutSpec(
                    repo_url=f"$ASSET_URL_{idx}",
                    revision=f"$ASSET_COMMIT_{idx}",
                    checkout_root=f"${asset_dir_var}",
                    mode="detached",
                    clean=True,
                )
            )
        )
        chunk_asset_overlays.append(
            "python3 - <<PY\n"
            "import json\n"
            "from pathlib import Path\n"
            "cache_root = Path(\"${ASSET_CACHE_ROOT}\").resolve()\n"
            f"repo_url = \"${{ASSET_URL_{idx}}}\".strip()\n"
            f"ref = \"${{ASSET_REF_{idx}}}\".strip()\n"
            f"repo_dir = Path(\"${{{asset_dir_var}}}\").resolve()\n"
            "index_path = cache_root / '.asset_ref_index.json'\n"
            "try:\n"
            "    index = json.loads(index_path.read_text(encoding='utf-8')) if index_path.exists() else {}\n"
            "    if not isinstance(index, dict):\n"
            "        index = {}\n"
            "except Exception:\n"
            "    index = {}\n"
            "index[f'{repo_url}|{ref}'] = repo_dir.name\n"
            "index_path.write_text(json.dumps(index, indent=2, sort_keys=True) + '\\n', encoding='utf-8')\n"
            "PY"
        )
        chunk_asset_overlays.append(
            f"if [ \"$ASSET_PIPELINES_LINKED\" = \"0\" ] && [ -d \"${asset_dir_var}/{pipelines_dir}\" ]; then "
            "rm -rf \"$CHECKOUT_ROOT/pipelines\"; "
            f"ln -sfn \"${asset_dir_var}/{pipelines_dir}\" \"$CHECKOUT_ROOT/pipelines\"; "
            "ASSET_PIPELINES_LINKED=1; "
            "fi"
        )
        chunk_asset_overlays.append(
            f"if [ \"$ASSET_SCRIPTS_LINKED\" = \"0\" ] && [ -d \"${asset_dir_var}/{scripts_dir}\" ]; then "
            "rm -rf \"$CHECKOUT_ROOT/scripts\"; "
            f"ln -sfn \"${asset_dir_var}/{scripts_dir}\" \"$CHECKOUT_ROOT/scripts\"; "
            "ASSET_SCRIPTS_LINKED=1; "
            "fi"
        )
    if executor.verbose:
        chunk_venv_bootstrap.append("log_step 'bootstrapping venv'")
    chunk_venv_bootstrap.append(f"PYTHON={python_bin}")
    chunk_venv_bootstrap.append(f"VENV={venv_path}")
    chunk_venv_bootstrap.append(f"export ETL_REPO_ROOT={checkout_root}")
    executor._append_db_tunnel_lines(chunk_venv_bootstrap)
    if executor.load_secrets_file:
        if executor.verbose:
            chunk_venv_bootstrap.append("log_step 'loading optional secrets file (values hidden)'")
        chunk_venv_bootstrap.append("if [ -f \"$HOME/.secrets/etl\" ]; then source \"$HOME/.secrets/etl\"; fi")
    chunk_venv_bootstrap.append("if [ ! -f \"$VENV/bin/activate\" ]; then")
    chunk_venv_bootstrap.append("  $PYTHON -m venv \"$VENV\"")
    chunk_venv_bootstrap.append("fi")
    chunk_venv_bootstrap.append("if [ ! -f \"$VENV/bin/activate\" ]; then echo \"[etl][setup] venv activation script missing: $VENV/bin/activate\" >&2; exit 1; fi")
    if executor.verbose:
        chunk_install.append("log_step 'installing requirements if present'")
    chunk_install.append(f"if [ -f \"{req_path}\" ]; then \"$VENV/bin/python\" -m pip install -r \"{req_path}\"; fi")
    chunk_install.append(f"export PYTHONPATH={checkout_root}:${{PYTHONPATH:-}}")
    chunk_install.append("if ! \"$VENV/bin/python\" -c 'import etl.run_batch' >/dev/null 2>&1; then")
    chunk_install.append("  \"$VENV/bin/python\" -m pip install --no-deps -e \"$ETL_REPO_ROOT\"")
    chunk_install.append("fi")
    if executor.verbose:
        chunk_finalize.append("log_step 'setup complete'")
    chunk_finalize.append("echo setup complete")
    template_path = Path(__file__).resolve().parent / "templates" / "setup.sbatch.tmpl"
    return render_template_file(
        template_path,
        {
            "sbatch_lines": "\n".join(sbatch_lines),
            "chunk_runtime_flags": _render_chunk("runtime_flags", chunk_runtime_flags),
            "chunk_dirs": _render_chunk("dirs", _with_chunk_logs(chunk_dirs, "dirs", executor.verbose)),
            "chunk_modules": _render_chunk("modules", _with_chunk_logs(chunk_modules, "modules", executor.verbose)),
            "chunk_source_checkout": _render_chunk("source_checkout", _with_chunk_logs(chunk_source_checkout, "source_checkout", executor.verbose)),
            "chunk_asset_overlays": _render_chunk("asset_overlays", _with_chunk_logs(chunk_asset_overlays, "asset_overlays", executor.verbose)),
            "chunk_venv_bootstrap": _render_chunk("venv_bootstrap", _with_chunk_logs(chunk_venv_bootstrap, "venv_bootstrap", executor.verbose)),
            "chunk_install": _render_chunk("install", _with_chunk_logs(chunk_install, "install", executor.verbose)),
            "chunk_finalize": _render_chunk("finalize", _with_chunk_logs(chunk_finalize, "finalize", executor.verbose)),
        },
    )

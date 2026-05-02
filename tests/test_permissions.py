from __future__ import annotations

from types import SimpleNamespace

from etl.permissions import shell_permissions_prelude, shared_umask_from_config
from etl.executors.slurm.sbatch_controller import render_controller_script
from etl.executors.slurm.sbatch_setup import render_setup_script
from etl.executors.slurm.sbatch_step import render_step_script


def _executor():
    env_config = {
        "shared_umask": "0002",
        "shared_group": "Viens_AgroEco_Lab",
        "chmod_group_writable": True,
        "setgid_dirs": True,
    }
    env = SimpleNamespace(
        partition=None,
        account=None,
        time="00:10:00",
        cpus_per_task=1,
        mem="1G",
        setup_time=None,
        max_time=None,
        max_cpus_per_task=None,
        max_mem=None,
        sbatch_extra=None,
        modules=[],
        conda_env=None,
    )
    return SimpleNamespace(
        env_config=env_config,
        env=env,
        verbose=False,
        step_max_retries=0,
        step_retry_delay_seconds=0.0,
        load_secrets_file=False,
        _append_db_tunnel_lines=lambda lines: None,
        _append_db_tunnel_database_url_rewrite_lines=lambda lines, python_expr: None,
    )


def test_shared_umask_normalizes_octal_text() -> None:
    assert shared_umask_from_config({"shared_umask": "002"}) == "0002"
    assert shared_umask_from_config({"shared_umask": "0o007"}) == "0007"


def test_shell_permissions_prelude_sets_umask_and_group_repair() -> None:
    text = "\n".join(shell_permissions_prelude(_executor().env_config))

    assert 'umask "$ETL_SHARED_UMASK"' in text
    assert "ETL_SHARED_GROUP=Viens_AgroEco_Lab" in text
    assert "chmod -R g+rwX" in text
    assert "chmod g+s" in text


def test_slurm_setup_and_step_scripts_include_permission_policy() -> None:
    ex = _executor()

    setup = render_setup_script(
        executor=ex,
        run_id="abc123456",
        checkout_root="/shared/research-etl",
        workdir="/shared/work/run",
        logdir="/shared/work/run/logs/setup",
        venv_path="/shared/research-etl/.venv",
        req_path="/shared/research-etl/requirements.txt",
        python_bin="python3",
        workdirs_to_create=["/shared/work/run/step_0"],
        logdirs_to_create=["/shared/work/run/logs/step_0"],
        execution_source="workspace",
        allow_workspace_source=True,
        pipeline_asset_overlays=[],
        parse_slurm_time_to_minutes=lambda value: 10.0,
        parse_mem_to_mb=lambda value: 1024,
        format_minutes_as_slurm_time=lambda value: "00:10:00",
        format_mb_as_slurm_mem=lambda value: "1G",
    )
    step = render_step_script(
        executor=ex,
        run_id="abc123456",
        checkout_root="/shared/research-etl",
        pipeline_path="/shared/research-etl/pipelines/sample.yml",
        steps=[],
        step_indices=[0],
        context_file="/shared/work/run/context.json",
        workdir="/shared/work/run",
        plugins_dir="/shared/research-etl/plugins",
        logdir="/shared/work/run/logs/step_0",
        venv_path="/shared/research-etl/.venv",
        req_path="/shared/research-etl/requirements.txt",
        python_bin="python3",
    )

    assert "ETL_SHARED_UMASK=0002" in setup
    assert "etl_fix_permissions /shared/work/run" in setup
    assert "ETL_SHARED_UMASK=0002" in step
    assert "etl_fix_permissions /shared/work/run" in step


def test_slurm_controller_script_includes_permission_policy() -> None:
    script = render_controller_script(
        executor=_executor(),
        run_id="abc123456",
        workdir="/shared/work/run",
        logdir="/shared/work/run/logs/controller",
        child_jobs_file="/shared/work/run/child_jobs.txt",
        wait_children=True,
        poll_seconds=10,
    )

    assert "ETL_SHARED_UMASK=0002" in script
    assert "etl_fix_permissions /shared/work/run" in script

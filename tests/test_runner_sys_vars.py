# research-etl
# Copyright (c) 2026 Joseph Weaver
# This file is part of the research-etl project and is licensed under the MIT License.
# You may not use this file except in compliance with the License.
# See https://github.com/josephweaver/research-etl for details.

from __future__ import annotations

from pathlib import Path

from etl.pipeline import Pipeline, Step
from etl.runner import run_pipeline


def _write_capture_plugin(path: Path) -> None:
    path.write_text(
        "\n".join(
            [
                "meta = {'name': 'capture', 'version': '0.1.0', 'description': 'capture args/env'}",
                "def run(args, ctx):",
                "    return {'args': args, 'env': args.get('env', {})}",
            ]
        ),
        encoding="utf-8",
    )


def test_runner_resolves_sys_placeholders_in_step_args(tmp_path: Path) -> None:
    plugin_dir = tmp_path / "plugins"
    plugin_dir.mkdir(parents=True, exist_ok=True)
    _write_capture_plugin(plugin_dir / "capture.py")

    pipeline = Pipeline(
        vars={"jobname": "yanroy"},
        steps=[
            Step(
                name="s1",
                script="capture.py tag={sys.now.yymmdd}-{sys.run.short_id} rid={sys.run.id} sname={sys.step.name} sid={sys.step.id}",
                output_var="out",
            )
        ],
    )

    run_id = "1234567890abcdef1234567890abcdef"
    result = run_pipeline(
        pipeline,
        plugin_dir=plugin_dir,
        workdir=tmp_path / ".runs",
        run_id=run_id,
    )
    assert result.success is True
    out = result.steps[0].outputs["args"]
    assert out["rid"] == run_id
    assert out["sname"] == "s1"
    assert len(str(out["sid"])) == 32
    assert out["tag"].endswith("-12345678")
    assert "{" not in out["tag"]


def test_runner_exposes_sys_apppath_from_etl_repo_root(tmp_path: Path, monkeypatch) -> None:
    plugin_dir = tmp_path / "plugins"
    plugin_dir.mkdir(parents=True, exist_ok=True)
    _write_capture_plugin(plugin_dir / "capture.py")
    app_root = tmp_path / "remote-checkout"
    app_root.mkdir(parents=True, exist_ok=True)
    monkeypatch.setenv("ETL_REPO_ROOT", str(app_root))

    pipeline = Pipeline(
        vars={"jobname": "yanroy"},
        steps=[Step(name="s1", script="capture.py app={sys.apppath}", output_var="out")],
    )

    result = run_pipeline(
        pipeline,
        plugin_dir=plugin_dir,
        workdir=tmp_path / ".runs",
        run_id="1234567890abcdef1234567890abcdef",
    )
    assert result.success is True
    out = result.steps[0].outputs["args"]
    assert out["app"] == app_root.resolve().as_posix()


def test_runner_exposes_project_execution_paths(tmp_path: Path, monkeypatch) -> None:
    plugin_dir = tmp_path / "plugins"
    plugin_dir.mkdir(parents=True, exist_ok=True)
    _write_capture_plugin(plugin_dir / "capture.py")
    app_root = tmp_path / "src" / "research-etl-abc123"
    project_root = tmp_path / "src" / "landcore-etl-pipelines-abc123"
    pipeline_file = project_root / "pipelines" / "yanroy" / "db_fields.yml"
    (project_root / ".git").mkdir(parents=True, exist_ok=True)
    pipeline_file.parent.mkdir(parents=True, exist_ok=True)
    pipeline_file.write_text("steps: []\n", encoding="utf-8")
    monkeypatch.setenv("ETL_REPO_ROOT", str(app_root))
    monkeypatch.setenv("ETL_PROJECTS_DIR", str((tmp_path / "src").resolve()))
    monkeypatch.setenv("ETL_PROJECT_DIR", str(project_root))
    monkeypatch.setenv("ETL_PIPELINE_FILE", str(pipeline_file))

    pipeline = Pipeline(
        vars={"jobname": "yanroy"},
        steps=[
            Step(
                name="s1",
                script="capture.py projects={sys.projectsdir} project={sys.projectdir} pipe={sys.pipelinefile}",
                output_var="out",
            )
        ],
    )

    result = run_pipeline(
        pipeline,
        plugin_dir=plugin_dir,
        workdir=tmp_path / ".runs",
        run_id="fedcba9876543210fedcba9876543210",
    )
    assert result.success is True
    out = result.steps[0].outputs["args"]
    assert out["projects"] == (tmp_path / "src").resolve().as_posix()
    assert out["project"] == project_root.resolve().as_posix()
    assert out["pipe"] == pipeline_file.resolve().as_posix()


def test_runner_resolves_sys_step_nn_for_parallel_steps(tmp_path: Path) -> None:
    plugin_dir = tmp_path / "plugins"
    plugin_dir.mkdir(parents=True, exist_ok=True)
    _write_capture_plugin(plugin_dir / "capture.py")

    pipeline = Pipeline(
        vars={"jobname": "yanroy"},
        steps=[
            Step(name="left", script="capture.py nn={sys.step.NN}", output_var="l", parallel_with="g1"),
            Step(name="right", script="capture.py nn={sys.step.NN}", output_var="r", parallel_with="g1"),
            Step(name="done", script="capture.py nn={sys.step.NN}", output_var="d"),
        ],
    )

    result = run_pipeline(
        pipeline,
        plugin_dir=plugin_dir,
        workdir=tmp_path / ".runs",
        run_id="fedcba9876543210fedcba9876543210",
    )
    assert result.success is True
    by_name = {r.step.name: r for r in result.steps}
    assert by_name["left"].outputs["args"]["nn"] == "01a"
    assert by_name["right"].outputs["args"]["nn"] == "01b"
    assert by_name["done"].outputs["args"]["nn"] == "02"


def test_runner_resolves_sys_placeholders_in_step_env(tmp_path: Path) -> None:
    plugin_dir = tmp_path / "plugins"
    plugin_dir.mkdir(parents=True, exist_ok=True)
    _write_capture_plugin(plugin_dir / "capture.py")

    pipeline = Pipeline(
        vars={"jobname": "yanroy"},
        steps=[
            Step(
                name="s1",
                script="capture.py",
                env={
                    "RUN_TAG": "{sys.now.yymmdd}-{sys.run.short_id}",
                    "STEP_NAME": "{sys.step.name}",
                },
                output_var="out",
            )
        ],
    )

    run_id = "abcdef0123456789abcdef0123456789"
    result = run_pipeline(
        pipeline,
        plugin_dir=plugin_dir,
        workdir=tmp_path / ".runs",
        run_id=run_id,
    )
    assert result.success is True
    env = result.steps[0].outputs["env"]
    assert env["STEP_NAME"] == "s1"
    assert env["RUN_TAG"].endswith("-abcdef01")
    assert "{" not in env["RUN_TAG"]


def test_runner_respects_pipeline_resolve_max_passes(tmp_path: Path) -> None:
    plugin_dir = tmp_path / "plugins"
    plugin_dir.mkdir(parents=True, exist_ok=True)
    _write_capture_plugin(plugin_dir / "capture.py")

    pipeline = Pipeline(
        vars={"a": "{b}", "b": "ok"},
        resolve_max_passes=1,
        steps=[
            Step(
                name="s1",
                script="capture.py x={a}",
                output_var="out",
            )
        ],
    )
    result = run_pipeline(pipeline, plugin_dir=plugin_dir, workdir=tmp_path / ".runs")
    assert result.success is True
    out = result.steps[0].outputs["args"]
    assert out["x"] == "{b}"


def test_runner_reuses_existing_stamped_workdir_for_same_run_id_suffix(tmp_path: Path) -> None:
    plugin_dir = tmp_path / "plugins"
    plugin_dir.mkdir(parents=True, exist_ok=True)
    _write_capture_plugin(plugin_dir / "capture.py")

    run_id = "1d024133aaaaaaaaaaaaaaaaaaaaaaaa"
    pipeline = Pipeline(steps=[Step(name="s1", script="capture.py")])
    stamped_workdir = tmp_path / ".runs" / "260215" / f"232242-{run_id[:8]}"

    result = run_pipeline(
        pipeline,
        plugin_dir=plugin_dir,
        workdir=stamped_workdir,
        run_id=run_id,
    )
    assert result.success is True
    assert str(result.artifact_dir).replace("\\", "/").endswith(f"/260215/232242-{run_id[:8]}")


def test_runner_foreach_can_use_expr_range_in_vars(tmp_path: Path) -> None:
    plugin_dir = tmp_path / "plugins"
    plugin_dir.mkdir(parents=True, exist_ok=True)
    _write_capture_plugin(plugin_dir / "capture.py")

    pipeline = Pipeline(
        vars={"years": "{expr.range(2000, 2002)}"},
        steps=[
            Step(
                name="s_year",
                script="capture.py year={item}",
                foreach="years",
                output_var="out",
            )
        ],
    )
    result = run_pipeline(pipeline, plugin_dir=plugin_dir, workdir=tmp_path / ".runs")
    assert result.success is True
    years = sorted(int(r.outputs["args"]["year"]) for r in result.steps)
    assert years == [2000, 2001, 2002]


def test_runner_resolves_expr_dateformat_with_sys_now(tmp_path: Path) -> None:
    plugin_dir = tmp_path / "plugins"
    plugin_dir.mkdir(parents=True, exist_ok=True)
    _write_capture_plugin(plugin_dir / "capture.py")

    pipeline = Pipeline(
        steps=[
            Step(
                name="s1",
                script="capture.py tag={expr.dateformat(sys.now,'%Y%m%d-%H%M%S')}",
                output_var="out",
            )
        ],
    )
    result = run_pipeline(
        pipeline,
        plugin_dir=plugin_dir,
        workdir=tmp_path / ".runs",
        run_id="00112233445566778899aabbccddeeff",
    )
    assert result.success is True
    tag = result.steps[0].outputs["args"]["tag"]
    assert len(tag) == 15
    assert "-" in tag


def test_runner_foreach_can_use_expr_daterange_in_vars(tmp_path: Path) -> None:
    plugin_dir = tmp_path / "plugins"
    plugin_dir.mkdir(parents=True, exist_ok=True)
    _write_capture_plugin(plugin_dir / "capture.py")

    pipeline = Pipeline(
        vars={"days": "{expr.daterange(expr.date(2025,1,1), expr.date(2025,1,3))}"},
        steps=[
            Step(
                name="s_day",
                script="capture.py day={item}",
                foreach="days",
                output_var="out",
            )
        ],
    )
    result = run_pipeline(pipeline, plugin_dir=plugin_dir, workdir=tmp_path / ".runs")
    assert result.success is True
    days = sorted(str(r.outputs["args"]["day"]) for r in result.steps)
    assert days == ["20250101", "20250102", "20250103"]

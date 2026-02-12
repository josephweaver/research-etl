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
                script="capture.py tag={sys.now.yymmdd}-{sys.run.short_id} rid={sys.run.id} sname={sys.step.name}",
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
    assert out["tag"].endswith("-12345678")
    assert "{" not in out["tag"]


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


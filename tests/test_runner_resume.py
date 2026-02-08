from __future__ import annotations

from pathlib import Path

from etl.pipeline import Pipeline, Step
from etl.runner import run_pipeline


def test_runner_resume_skips_success_and_preserves_outputs_for_downstream_when(tmp_path: Path) -> None:
    plugin_dir = tmp_path / "plugins"
    plugin_dir.mkdir(parents=True, exist_ok=True)
    (plugin_dir / "echo.py").write_text(
        "\n".join(
            [
                "meta = {'name': 'echo', 'version': '0.1.0', 'description': 'test'}",
                "def run(args, ctx):",
                "    return {'ok': True}",
            ]
        ),
        encoding="utf-8",
    )

    pipeline = Pipeline(
        steps=[
            Step(name="step1", script="echo.py", output_var="out1"),
            Step(name="step2", script="echo.py", when="out1['x'] == 1"),
        ]
    )
    result = run_pipeline(
        pipeline,
        plugin_dir=plugin_dir,
        workdir=tmp_path / ".runs",
        resume_succeeded_steps={"step1"},
        prior_step_outputs={"step1": {"x": 1}},
    )

    by_name = {s.step.name: s for s in result.steps}
    assert by_name["step1"].skipped is True
    assert by_name["step2"].success is True
    assert by_name["step2"].skipped is False

from __future__ import annotations

from pathlib import Path

from etl.pipeline import Pipeline, Step
from etl.runner import run_pipeline


def _write_flaky_per_step_plugin(path: Path) -> None:
    path.write_text(
        "\n".join(
            [
                "meta = {'name': 'flaky_per_step', 'version': '0.1.0', 'description': 'retry test'}",
                "_COUNTS = {}",
                "def run(args, ctx):",
                "    key = ctx.workdir.name",
                "    _COUNTS[key] = _COUNTS.get(key, 0) + 1",
                "    if _COUNTS[key] == 1:",
                "        raise RuntimeError('boom-once')",
                "    return {'item': args.get('item')}",
            ]
        ),
        encoding="utf-8",
    )


def _write_echo_plugin(path: Path) -> None:
    path.write_text(
        "\n".join(
            [
                "meta = {'name': 'echo', 'version': '0.1.0', 'description': 'test'}",
                "def run(args, ctx):",
                "    return {'ok': True}",
            ]
        ),
        encoding="utf-8",
    )


def test_runner_retries_foreach_parallel_group(tmp_path: Path) -> None:
    plugin_dir = tmp_path / "plugins"
    plugin_dir.mkdir(parents=True, exist_ok=True)
    _write_flaky_per_step_plugin(plugin_dir / "flaky_per_step.py")
    _write_echo_plugin(plugin_dir / "echo.py")

    pipeline = Pipeline(
        vars={"items": ["a", "b"]},
        steps=[
            Step(
                name="fan",
                script="flaky_per_step.py item={item}",
                foreach="items",
                parallel_with="g1",
                output_var="out",
            ),
            Step(
                name="gate",
                script="echo.py",
                when="out_0['item'] == 'a' and out_1['item'] == 'b'",
            ),
        ],
    )
    result = run_pipeline(
        pipeline,
        plugin_dir=plugin_dir,
        workdir=tmp_path / ".runs",
        max_retries=1,
    )

    assert result.success is True
    by_name = {s.step.name: s for s in result.steps}
    assert by_name["fan_0"].success is True
    assert by_name["fan_1"].success is True
    assert by_name["fan_0"].attempt_no == 2
    assert by_name["fan_1"].attempt_no == 2
    assert [a["success"] for a in by_name["fan_0"].attempts] == [False, True]
    assert [a["success"] for a in by_name["fan_1"].attempts] == [False, True]
    assert by_name["gate"].success is True


def test_runner_resume_skips_foreach_parallel_and_uses_prior_outputs(tmp_path: Path) -> None:
    plugin_dir = tmp_path / "plugins"
    plugin_dir.mkdir(parents=True, exist_ok=True)
    _write_flaky_per_step_plugin(plugin_dir / "flaky_per_step.py")
    _write_echo_plugin(plugin_dir / "echo.py")

    pipeline = Pipeline(
        vars={"items": ["a", "b"]},
        steps=[
            Step(
                name="fan",
                script="flaky_per_step.py item={item}",
                foreach="items",
                parallel_with="g1",
                output_var="out",
            ),
            Step(
                name="gate",
                script="echo.py",
                when="out_0['item'] == 'a' and out_1['item'] == 'b'",
            ),
        ],
    )
    result = run_pipeline(
        pipeline,
        plugin_dir=plugin_dir,
        workdir=tmp_path / ".runs",
        resume_succeeded_steps={"fan_0", "fan_1"},
        prior_step_outputs={
            "fan_0": {"item": "a"},
            "fan_1": {"item": "b"},
        },
    )

    by_name = {s.step.name: s for s in result.steps}
    assert by_name["fan_0"].skipped is True
    assert by_name["fan_0"].attempt_no == 0
    assert by_name["fan_1"].skipped is True
    assert by_name["fan_1"].attempt_no == 0
    assert by_name["gate"].success is True
    assert by_name["gate"].skipped is False

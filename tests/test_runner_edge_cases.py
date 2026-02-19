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


def _write_make_dirs_plugin(path: Path) -> None:
    path.write_text(
        "\n".join(
            [
                "from pathlib import Path",
                "meta = {'name': 'make_dirs', 'version': '0.1.0', 'description': 'test'}",
                "def run(args, ctx):",
                "    root = Path(str(args.get('root') or ctx.workdir / 'tiles'))",
                "    root.mkdir(parents=True, exist_ok=True)",
                "    (root / 'tile_a').mkdir(parents=True, exist_ok=True)",
                "    (root / 'tile_b').mkdir(parents=True, exist_ok=True)",
                "    return {'root': root.resolve().as_posix()}",
            ]
        ),
        encoding="utf-8",
    )


def _write_item_plugin(path: Path) -> None:
    path.write_text(
        "\n".join(
            [
                "meta = {'name': 'item_echo', 'version': '0.1.0', 'description': 'test'}",
                "def run(args, ctx):",
                "    return {'item': str(args.get('item') or '')}",
            ]
        ),
        encoding="utf-8",
    )


def _write_serial_guard_plugin(path: Path) -> None:
    path.write_text(
        "\n".join(
            [
                "import time",
                "meta = {'name': 'serial_guard', 'version': '0.1.0', 'description': 'test'}",
                "_ACTIVE = 0",
                "def run(args, ctx):",
                "    global _ACTIVE",
                "    _ACTIVE += 1",
                "    if _ACTIVE > 1:",
                "        _ACTIVE -= 1",
                "        raise RuntimeError('concurrent execution detected')",
                "    try:",
                "        time.sleep(0.02)",
                "        return {'item': str(args.get('item') or '')}",
                "    finally:",
                "        _ACTIVE -= 1",
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


def test_runner_sequential_foreach_forces_serial_item_execution(tmp_path: Path) -> None:
    plugin_dir = tmp_path / "plugins"
    plugin_dir.mkdir(parents=True, exist_ok=True)
    _write_serial_guard_plugin(plugin_dir / "serial_guard.py")

    pipeline = Pipeline(
        vars={"items": ["a", "b", "c"]},
        steps=[
            Step(
                name="fan",
                script="serial_guard.py item={item}",
                sequential_foreach="items",
                # Even if set on input Step, sequential_foreach expansion should
                # clear parallel grouping for expanded item steps.
                parallel_with="g1",
                output_var="out",
            ),
        ],
    )
    result = run_pipeline(
        pipeline,
        plugin_dir=plugin_dir,
        workdir=tmp_path / ".runs",
    )
    assert result.success is True
    assert [s.step.name for s in result.steps] == ["fan_0", "fan_1", "fan_2"]


def test_runner_expands_foreach_glob_after_prior_step_outputs(tmp_path: Path) -> None:
    plugin_dir = tmp_path / "plugins"
    plugin_dir.mkdir(parents=True, exist_ok=True)
    _write_make_dirs_plugin(plugin_dir / "make_dirs.py")
    _write_item_plugin(plugin_dir / "item_echo.py")

    pipeline = Pipeline(
        steps=[
            Step(
                name="prepare",
                script=f"make_dirs.py root={(tmp_path / 'tiles').as_posix()}",
                output_var="prepared",
            ),
            Step(
                name="fan",
                script='item_echo.py item="{item}"',
                foreach_glob="{prepared.root}/*",
                foreach_kind="dirs",
                parallel_with="g1",
                output_var="out",
            ),
        ],
    )
    result = run_pipeline(
        pipeline,
        plugin_dir=plugin_dir,
        workdir=tmp_path / ".runs",
    )

    assert result.success is True
    by_name = {s.step.name: s for s in result.steps}
    assert by_name["prepare"].success is True
    assert by_name["fan_0"].success is True
    assert by_name["fan_1"].success is True
    items = sorted([by_name["fan_0"].outputs["item"], by_name["fan_1"].outputs["item"]])
    assert items[0].endswith("/tile_a")
    assert items[1].endswith("/tile_b")


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

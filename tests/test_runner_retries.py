from __future__ import annotations

from pathlib import Path

from etl.pipeline import Pipeline, Step
from etl.runner import run_pipeline


def _write_retry_plugin(path: Path, fail_count: int) -> None:
    path.write_text(
        "\n".join(
            [
                "meta = {'name': 'retry_plugin', 'version': '0.1.0', 'description': 'retry test'}",
                "_COUNT = 0",
                "def run(args, ctx):",
                "    global _COUNT",
                "    _COUNT += 1",
                f"    if _COUNT <= {fail_count}:",
                "        raise RuntimeError(f'boom-{_COUNT}')",
                "    return {'attempt': _COUNT}",
            ]
        ),
        encoding="utf-8",
    )


def test_runner_retries_until_success(tmp_path: Path) -> None:
    plugin_dir = tmp_path / "plugins"
    plugin_dir.mkdir(parents=True, exist_ok=True)
    _write_retry_plugin(plugin_dir / "retry_plugin.py", fail_count=2)

    pipeline = Pipeline(steps=[Step(name="retry_step", script="retry_plugin.py")])
    result = run_pipeline(
        pipeline,
        plugin_dir=plugin_dir,
        workdir=tmp_path / ".runs",
        max_retries=2,
    )

    assert result.success is True
    step = result.steps[0]
    assert step.attempt_no == 3
    assert len(step.attempts) == 3
    assert [a["success"] for a in step.attempts] == [False, False, True]


def test_runner_retries_exhausted(tmp_path: Path) -> None:
    plugin_dir = tmp_path / "plugins"
    plugin_dir.mkdir(parents=True, exist_ok=True)
    _write_retry_plugin(plugin_dir / "retry_plugin.py", fail_count=5)

    pipeline = Pipeline(steps=[Step(name="retry_step", script="retry_plugin.py")])
    result = run_pipeline(
        pipeline,
        plugin_dir=plugin_dir,
        workdir=tmp_path / ".runs",
        max_retries=2,
    )

    assert result.success is False
    step = result.steps[0]
    assert step.attempt_no == 3
    assert len(step.attempts) == 3
    assert all(a["success"] is False for a in step.attempts)


def test_runner_records_engine_metrics_when_plugin_omits_them(tmp_path: Path) -> None:
    plugin_dir = tmp_path / "plugins"
    plugin_dir.mkdir(parents=True, exist_ok=True)
    (plugin_dir / "metrics_free.py").write_text(
        "\n".join(
            [
                "meta = {'name': 'metrics_free', 'version': '0.1.0', 'description': 'no explicit metrics'}",
                "def run(args, ctx):",
                "    vals = [i * i for i in range(200000)]",
                "    return {'count': len(vals)}",
            ]
        ),
        encoding="utf-8",
    )

    pipeline = Pipeline(steps=[Step(name="m", script="metrics_free.py")])
    result = run_pipeline(pipeline, plugin_dir=plugin_dir, workdir=tmp_path / ".runs")

    assert result.success is True
    attempt = result.steps[0].attempts[0]
    assert attempt["memory_gb"] is not None
    assert attempt["cpu_cores"] is not None
    assert attempt["cpu_count"] is None or attempt["cpu_count"] >= 1

from __future__ import annotations

from pathlib import Path

from etl.pipeline import Pipeline, Step
from etl.runner import run_pipeline


def test_plugin_standard_logging_writes_step_log(tmp_path: Path) -> None:
    plugins = tmp_path / "plugins"
    plugins.mkdir(parents=True, exist_ok=True)
    (plugins / "loggy.py").write_text(
        "\n".join(
            [
                "meta = {'name': 'loggy', 'version': '0.1.0', 'description': 'log test'}",
                "def run(args, ctx):",
                "    ctx.log('plain')",
                "    ctx.log('warned', 'WARN')",
                "    ctx.info('infox')",
                "    ctx.warn('warnx')",
                "    ctx.error('errorx')",
                "    return {'ok': True}",
            ]
        ),
        encoding="utf-8",
    )

    pipeline = Pipeline(steps=[Step(name="s1", script="loggy.py")])
    result = run_pipeline(pipeline, plugin_dir=plugins, workdir=tmp_path / ".runs")
    assert result.success is True
    step_dir = Path(result.artifact_dir) / "s1"
    log_file = step_dir / "step.log"
    assert log_file.exists()
    text = log_file.read_text(encoding="utf-8")
    assert "[INFO] plain" in text
    assert "[WARN] warned" in text
    assert "[ERROR] errorx" in text


def test_plugin_logging_respects_pipeline_logdir(tmp_path: Path) -> None:
    plugins = tmp_path / "plugins"
    plugins.mkdir(parents=True, exist_ok=True)
    (plugins / "loggy.py").write_text(
        "\n".join(
            [
                "meta = {'name': 'loggy', 'version': '0.1.0', 'description': 'log test'}",
                "def run(args, ctx):",
                "    ctx.log('plain')",
                "    return {'ok': True}",
            ]
        ),
        encoding="utf-8",
    )

    pipeline = Pipeline(
        dirs={"logdir": str(tmp_path / "logs")},
        steps=[Step(name="s1", script="loggy.py")],
    )
    result = run_pipeline(pipeline, plugin_dir=plugins, workdir=tmp_path / ".runs")
    assert result.success is True
    log_file = tmp_path / "logs" / "s1" / "step.log"
    assert log_file.exists()
    assert "[INFO] plain" in log_file.read_text(encoding="utf-8")

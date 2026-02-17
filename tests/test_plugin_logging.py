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
    log_files = list((Path(result.artifact_dir) / "s1").glob("*/logs/step.log"))
    assert log_files
    log_file = log_files[0]
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
    log_files = list((tmp_path / "logs" / "s1").glob("*/logs/step.log"))
    assert log_files
    log_file = log_files[0]
    assert "[INFO] plain" in log_file.read_text(encoding="utf-8")


def test_plugin_logging_emits_step_log_callback(tmp_path: Path) -> None:
    plugins = tmp_path / "plugins"
    plugins.mkdir(parents=True, exist_ok=True)
    (plugins / "loggy.py").write_text(
        "\n".join(
            [
                "meta = {'name': 'loggy', 'version': '0.1.0', 'description': 'log test'}",
                "def run(args, ctx):",
                "    ctx.log('plain')",
                "    ctx.warn('warnx')",
                "    return {'ok': True}",
            ]
        ),
        encoding="utf-8",
    )

    seen = []

    def _step_log(step_name: str, message: str, level: str = "INFO") -> None:
        seen.append((step_name, level, message))

    pipeline = Pipeline(steps=[Step(name="s1", script="loggy.py")])
    result = run_pipeline(
        pipeline,
        plugin_dir=plugins,
        workdir=tmp_path / ".runs",
        step_log_func=_step_log,
    )
    assert result.success is True
    assert ("s1", "INFO", "plain") in seen
    assert ("s1", "WARN", "warnx") in seen


def test_plugin_logging_redacts_secret_values(tmp_path: Path, monkeypatch) -> None:
    monkeypatch.setenv("OPENAI_API_KEY", "sk-super-secret-123456")
    plugins = tmp_path / "plugins"
    plugins.mkdir(parents=True, exist_ok=True)
    (plugins / "loggy.py").write_text(
        "\n".join(
            [
                "meta = {'name': 'loggy', 'version': '0.1.0', 'description': 'log test'}",
                "def run(args, ctx):",
                "    ctx.log('token=' + args.get('token', ''))",
                "    return {'ok': True}",
            ]
        ),
        encoding="utf-8",
    )

    pipeline = Pipeline(
        vars={"secret": {"OPENAI_API_KEY": "sk-super-secret-123456"}},
        steps=[Step(name="s1", script="loggy.py token=sk-super-secret-123456")],
    )
    result = run_pipeline(pipeline, plugin_dir=plugins, workdir=tmp_path / ".runs")
    assert result.success is True
    log_files = list((Path(result.artifact_dir) / "s1").glob("*/logs/step.log"))
    assert log_files
    log_file = log_files[0]
    text = log_file.read_text(encoding="utf-8")
    assert "sk-super-secret-123456" not in text
    assert "[REDACTED]" in text

from __future__ import annotations

from pathlib import Path

import cli
from etl.executors.base import SubmissionResult
from etl.pipeline import Pipeline, Step


class _Status:
    state = "succeeded"


def test_resolve_workdir_precedence_order() -> None:
    p = Pipeline(workdir="pipe_workdir", steps=[Step(name="s1", script="echo.py")])
    assert (
        cli._resolve_workdir(
            cli_workdir="cli_workdir",
            pipeline=p,
            env_vars={"workdir": "env_workdir"},
            global_vars={"workdir": "global_workdir"},
        )
        == "cli_workdir"
    )
    assert (
        cli._resolve_workdir(
            cli_workdir=None,
            pipeline=p,
            env_vars={"workdir": "env_workdir"},
            global_vars={"workdir": "global_workdir"},
        )
        == "pipe_workdir"
    )
    assert (
        cli._resolve_workdir(
            cli_workdir=None,
            pipeline=Pipeline(steps=[Step(name="s1", script="echo.py")]),
            env_vars={"workdir": "env_workdir"},
            global_vars={"workdir": "global_workdir"},
        )
        == "env_workdir"
    )
    assert (
        cli._resolve_workdir(
            cli_workdir=None,
            pipeline=Pipeline(steps=[Step(name="s1", script="echo.py")]),
            env_vars={},
            global_vars={"workdir": "global_workdir"},
        )
        == "global_workdir"
    )
    assert (
        cli._resolve_workdir(
            cli_workdir=None,
            pipeline=Pipeline(steps=[Step(name="s1", script="echo.py")]),
            env_vars={},
            global_vars={},
        )
        == ".runs"
    )


def test_cmd_run_uses_pipeline_workdir_when_cli_missing(monkeypatch, tmp_path: Path) -> None:
    pipeline_path = tmp_path / "pipeline.yml"
    pipeline_path.write_text("steps:\n  - name: s1\n    script: echo.py\n", encoding="utf-8")
    monkeypatch.chdir(tmp_path)

    parser = cli.build_parser()
    args = parser.parse_args(["run", str(pipeline_path)])
    args._raw_argv = ["run", str(pipeline_path)]

    monkeypatch.setattr(cli, "resolve_global_config_path", lambda _: None)
    monkeypatch.setattr(cli, "resolve_execution_config_path", lambda _: None)
    monkeypatch.setattr(cli, "collect_run_provenance", lambda **k: {"git_commit_sha": "abc"})
    monkeypatch.setattr(cli, "load_runs", lambda store: [])

    def _fake_parse(path, global_vars=None, env_vars=None, context_vars=None):
        return Pipeline(workdir=".out/work", steps=[Step(name="s1", script="echo.py")])

    monkeypatch.setattr(cli, "parse_pipeline", _fake_parse)

    seen = {}

    class _Exec:
        def __init__(self, **kwargs):
            seen["workdir"] = kwargs.get("workdir")

        def submit(self, pipeline_path, context):
            return SubmissionResult(run_id="run1")

        def status(self, run_id):
            return _Status()

    monkeypatch.setattr(cli, "LocalExecutor", _Exec)
    rc = cli.cmd_run(args)
    assert rc == 0
    assert str(seen["workdir"]).replace("\\", "/").endswith(".out/work")


def test_cmd_run_var_workdir_overrides_pipeline_env_global(monkeypatch, tmp_path: Path) -> None:
    pipeline_path = tmp_path / "pipeline.yml"
    pipeline_path.write_text("steps:\n  - name: s1\n    script: echo.py\n", encoding="utf-8")
    monkeypatch.chdir(tmp_path)

    parser = cli.build_parser()
    args = parser.parse_args(["run", str(pipeline_path), "--var", "workdir=.out/cli_work"])
    args._raw_argv = ["run", str(pipeline_path), "--var", "workdir=.out/cli_work"]

    monkeypatch.setattr(cli, "resolve_global_config_path", lambda _: None)
    monkeypatch.setattr(cli, "resolve_execution_config_path", lambda _: None)
    monkeypatch.setattr(cli, "collect_run_provenance", lambda **k: {"git_commit_sha": "abc"})
    monkeypatch.setattr(cli, "load_runs", lambda store: [])

    def _fake_parse(path, global_vars=None, env_vars=None, context_vars=None):
        return Pipeline(workdir=".out/pipeline", steps=[Step(name="s1", script="echo.py")])

    monkeypatch.setattr(cli, "parse_pipeline", _fake_parse)

    seen = {}

    class _Exec:
        def __init__(self, **kwargs):
            seen["workdir"] = kwargs.get("workdir")

        def submit(self, pipeline_path, context):
            return SubmissionResult(run_id="run1")

        def status(self, run_id):
            return _Status()

    monkeypatch.setattr(cli, "LocalExecutor", _Exec)
    rc = cli.cmd_run(args)
    assert rc == 0
    assert str(seen["workdir"]).replace("\\", "/").endswith(".out/cli_work")

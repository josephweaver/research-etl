from __future__ import annotations

from pathlib import Path

import cli
from etl.executors.base import SubmissionResult
from etl.pipeline import Pipeline, Step


class _Status:
    state = "succeeded"


def test_cmd_run_auto_runs_missing_dependency(monkeypatch, tmp_path: Path) -> None:
    parser = cli.build_parser()
    dep = (tmp_path / "dep.yml").resolve()
    root = (tmp_path / "root.yml").resolve()
    dep.write_text("steps:\n  - name: d\n    script: echo.py\n", encoding="utf-8")
    root.write_text("steps:\n  - name: r\n    script: echo.py\n", encoding="utf-8")
    args = parser.parse_args(["run", str(root), "--workdir", str(tmp_path / ".runs")])
    args._raw_argv = ["run", str(root), "--workdir", str(tmp_path / ".runs")]

    def _fake_parse(path, global_vars=None, env_vars=None, context_vars=None):
        p = Path(path).resolve()
        if p == root:
            return Pipeline(requires_pipelines=[str(dep)], steps=[Step(name="r", script="echo.py")])
        if p == dep:
            return Pipeline(steps=[Step(name="d", script="echo.py")])
        raise AssertionError(f"unexpected path {p}")

    monkeypatch.setattr(cli, "parse_pipeline", _fake_parse)
    monkeypatch.setattr(cli, "collect_run_provenance", lambda **k: {"git_commit_sha": "abc"})
    monkeypatch.setattr(cli, "load_runs", lambda store: [])

    submitted = []

    class _Exec:
        def __init__(self, **kwargs):
            pass

        def submit(self, pipeline_path, context):
            submitted.append(Path(pipeline_path).resolve())
            return SubmissionResult(run_id=f"run_{len(submitted)}")

        def status(self, run_id):
            return _Status()

    monkeypatch.setattr(cli, "LocalExecutor", _Exec)
    rc = cli.cmd_run(args)
    assert rc == 0
    assert submitted == [dep, root]


def test_cmd_run_detects_dependency_cycle(monkeypatch, tmp_path: Path, capsys) -> None:
    parser = cli.build_parser()
    a = (tmp_path / "a.yml").resolve()
    b = (tmp_path / "b.yml").resolve()
    a.write_text("steps:\n  - name: a\n    script: echo.py\n", encoding="utf-8")
    b.write_text("steps:\n  - name: b\n    script: echo.py\n", encoding="utf-8")
    args = parser.parse_args(["run", str(a)])
    args._raw_argv = ["run", str(a)]

    def _fake_parse(path, global_vars=None, env_vars=None, context_vars=None):
        p = Path(path).resolve()
        if p == a:
            return Pipeline(requires_pipelines=[str(b)], steps=[Step(name="a", script="echo.py")])
        if p == b:
            return Pipeline(requires_pipelines=[str(a)], steps=[Step(name="b", script="echo.py")])
        raise AssertionError(f"unexpected path {p}")

    monkeypatch.setattr(cli, "parse_pipeline", _fake_parse)
    rc = cli.cmd_run(args)
    captured = capsys.readouterr()
    assert rc == 1
    assert "dependency cycle" in captured.err.lower()


def test_cmd_run_ignore_dependencies_skips_auto_dependency_runs(monkeypatch, tmp_path: Path) -> None:
    parser = cli.build_parser()
    dep = (tmp_path / "dep.yml").resolve()
    root = (tmp_path / "root.yml").resolve()
    dep.write_text("steps:\n  - name: d\n    script: echo.py\n", encoding="utf-8")
    root.write_text("steps:\n  - name: r\n    script: echo.py\n", encoding="utf-8")
    args = parser.parse_args(
        ["run", str(root), "--workdir", str(tmp_path / ".runs"), "--ignore-dependencies"]
    )
    args._raw_argv = ["run", str(root), "--workdir", str(tmp_path / ".runs"), "--ignore-dependencies"]

    def _fake_parse(path, global_vars=None, env_vars=None, context_vars=None):
        p = Path(path).resolve()
        if p == root:
            return Pipeline(requires_pipelines=[str(dep)], steps=[Step(name="r", script="echo.py")])
        if p == dep:
            return Pipeline(steps=[Step(name="d", script="echo.py")])
        raise AssertionError(f"unexpected path {p}")

    monkeypatch.setattr(cli, "parse_pipeline", _fake_parse)
    monkeypatch.setattr(cli, "collect_run_provenance", lambda **k: {"git_commit_sha": "abc"})
    monkeypatch.setattr(cli, "load_runs", lambda store: [])

    submitted = []

    class _Exec:
        def __init__(self, **kwargs):
            pass

        def submit(self, pipeline_path, context):
            submitted.append(Path(pipeline_path).resolve())
            return SubmissionResult(run_id=f"run_{len(submitted)}")

        def status(self, run_id):
            return _Status()

    monkeypatch.setattr(cli, "LocalExecutor", _Exec)
    rc = cli.cmd_run(args)
    assert rc == 0
    assert submitted == [root]

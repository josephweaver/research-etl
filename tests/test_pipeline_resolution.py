from __future__ import annotations

from pathlib import Path

import pytest

from etl.pipeline import PipelineError, parse_pipeline


def test_parse_pipeline_resolves_hierarchical_recursive_scopes(tmp_path: Path) -> None:
    p = tmp_path / "p.yml"
    p.write_text(
        "\n".join(
            [
                "vars:",
                "  pipe:",
                "    name: yanroy",
                "    datadir: \"{env.datadir}/{pipe.name}\"",
                "dirs:",
                "  outdir: \"{pipe.datadir}/out\"",
                "steps:",
                "  - name: s1",
                "    script: \"echo.py path={dirs.outdir}\"",
            ]
        ),
        encoding="utf-8",
    )

    pipeline = parse_pipeline(p, global_vars={"datadir": "/data"}, env_vars={"datadir": "{global.datadir}/dev"})

    assert pipeline.vars["pipe"]["datadir"] == "/data/dev/yanroy"
    assert pipeline.dirs["outdir"] == "/data/dev/yanroy/out"
    assert "path=/data/dev/yanroy/out" in pipeline.steps[0].script


def test_parse_pipeline_precedence_global_env_pipe_and_flat_overrides(tmp_path: Path) -> None:
    p = tmp_path / "p.yml"
    p.write_text(
        "\n".join(
            [
                "vars:",
                "  datadir: \"{env.datadir}/pipe\"",
                "  pipe:",
                "    datadir: \"{datadir}\"",
                "steps:",
                "  - name: s1",
                "    script: \"echo.py msg={datadir}|{global.datadir}|{env.datadir}|{pipe.datadir}\"",
            ]
        ),
        encoding="utf-8",
    )

    pipeline = parse_pipeline(
        p,
        global_vars={"datadir": "/global"},
        env_vars={"datadir": "/env"},
    )
    assert pipeline.vars["datadir"] == "/env/pipe"
    assert pipeline.vars["pipe"]["datadir"] == "/env/pipe"
    assert pipeline.steps[0].script == "echo.py msg=/env/pipe|/global|/env|/env/pipe"


def test_parse_pipeline_resolves_iterative_chain(tmp_path: Path) -> None:
    p = tmp_path / "p.yml"
    p.write_text(
        "\n".join(
            [
                "vars:",
                "  a: \"{b}\"",
                "  b: \"{c}\"",
                "  c: done",
                "steps:",
                "  - name: s1",
                "    script: \"echo.py msg={a}\"",
            ]
        ),
        encoding="utf-8",
    )
    pipeline = parse_pipeline(p)
    assert pipeline.vars["a"] == "done"
    assert pipeline.vars["b"] == "done"
    assert pipeline.steps[0].script == "echo.py msg=done"


def test_parse_pipeline_keeps_missing_placeholders(tmp_path: Path) -> None:
    p = tmp_path / "p.yml"
    p.write_text(
        "\n".join(
            [
                "vars:",
                "  a: \"{missing.path}/x\"",
                "steps:",
                "  - name: s1",
                "    script: \"echo.py msg={a}\"",
            ]
        ),
        encoding="utf-8",
    )
    pipeline = parse_pipeline(p)
    assert pipeline.vars["a"] == "{missing.path}/x"
    assert pipeline.steps[0].script == "echo.py msg={missing.path}/x"


def test_parse_pipeline_rejects_noncontiguous_parallel_group_tokens(tmp_path: Path) -> None:
    p = tmp_path / "p.yml"
    p.write_text(
        "\n".join(
            [
                "steps:",
                "  - name: s1",
                "    script: echo.py",
                "    parallel_with: step1",
                "  - name: s2",
                "    script: echo.py",
                "  - name: s3",
                "    script: echo.py",
                "    parallel_with: step1",
            ]
        ),
        encoding="utf-8",
    )
    with pytest.raises(PipelineError, match="out of order"):
        parse_pipeline(p)


def test_parse_pipeline_supports_plugin_and_args_mapping(tmp_path: Path) -> None:
    p = tmp_path / "p.yml"
    p.write_text(
        "\n".join(
            [
                "steps:",
                "  - name: s1",
                "    plugin: gdrive_download.py",
                "    args:",
                "      src: Data/yanroy/raw",
                "      out: \"/tmp/out dir\"",
                "      recursive: true",
            ]
        ),
        encoding="utf-8",
    )
    pipeline = parse_pipeline(p)
    script = pipeline.steps[0].script
    assert script.startswith("gdrive_download.py ")
    assert "src=Data/yanroy/raw" in script
    assert "recursive=true" in script
    assert "'out=/tmp/out dir'" in script


def test_parse_pipeline_supports_plugin_with_arg_list(tmp_path: Path) -> None:
    p = tmp_path / "p.yml"
    p.write_text(
        "\n".join(
            [
                "steps:",
                "  - name: s1",
                "    plugin: echo.py",
                "    args:",
                "      message: hello",
                "    arg_list:",
                "      - pos1",
                "      - \"pos 2\"",
            ]
        ),
        encoding="utf-8",
    )
    pipeline = parse_pipeline(p)
    script = pipeline.steps[0].script
    assert "message=hello" in script
    assert "pos1" in script
    assert "'pos 2'" in script


def test_parse_pipeline_supports_script_as_token_list(tmp_path: Path) -> None:
    p = tmp_path / "p.yml"
    p.write_text(
        "\n".join(
            [
                "steps:",
                "  - name: s1",
                "    script:",
                "      - echo.py",
                "      - message=hello world",
            ]
        ),
        encoding="utf-8",
    )
    pipeline = parse_pipeline(p)
    assert pipeline.steps[0].script == "echo.py 'message=hello world'"


def test_parse_pipeline_rejects_script_and_plugin_together(tmp_path: Path) -> None:
    p = tmp_path / "p.yml"
    p.write_text(
        "\n".join(
            [
                "steps:",
                "  - name: s1",
                "    script: echo.py",
                "    plugin: echo.py",
            ]
        ),
        encoding="utf-8",
    )
    with pytest.raises(PipelineError, match="both `script` and `plugin`"):
        parse_pipeline(p)

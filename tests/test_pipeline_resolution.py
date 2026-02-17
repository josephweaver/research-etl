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


def test_parse_pipeline_supports_globals_namespace_alias(tmp_path: Path) -> None:
    p = tmp_path / "p.yml"
    p.write_text(
        "\n".join(
            [
                "steps:",
                "  - name: s1",
                "    script: \"echo.py out={globals.basedir}/work\"",
            ]
        ),
        encoding="utf-8",
    )
    pipeline = parse_pipeline(p, global_vars={"basedir": "/tmp/out"})
    assert pipeline.steps[0].script == "echo.py out=/tmp/out/work"


def test_parse_pipeline_supports_secret_namespace_from_context(tmp_path: Path) -> None:
    p = tmp_path / "p.yml"
    p.write_text(
        "\n".join(
            [
                "steps:",
                "  - name: s1",
                "    script: \"echo.py key={secret.OPENAI_API_KEY}\"",
            ]
        ),
        encoding="utf-8",
    )
    pipeline = parse_pipeline(
        p,
        context_vars={"secret": {"OPENAI_API_KEY": "sk-test-123"}},
    )
    assert pipeline.steps[0].script == "echo.py key=sk-test-123"


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


def test_parse_pipeline_supports_step_resources_mapping(tmp_path: Path) -> None:
    p = tmp_path / "p.yml"
    p.write_text(
        "\n".join(
            [
                "steps:",
                "  - name: s1",
                "    plugin: echo.py",
                "    resources:",
                "      cpu_cores: 8",
                "      memory_gb: 32",
                "      wall_minutes: 45",
            ]
        ),
        encoding="utf-8",
    )
    pipeline = parse_pipeline(p)
    assert pipeline.steps[0].resources["cpu_cores"] == 8
    assert pipeline.steps[0].resources["memory_gb"] == 32
    assert pipeline.steps[0].resources["wall_minutes"] == 45


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


def test_parse_pipeline_accepts_top_level_workdir(tmp_path: Path) -> None:
    p = tmp_path / "p.yml"
    p.write_text(
        "\n".join(
            [
                "workdir: .out/work",
                "steps:",
                "  - name: s1",
                "    script: echo.py",
            ]
        ),
        encoding="utf-8",
    )
    pipeline = parse_pipeline(p)
    assert pipeline.workdir == ".out/work"


def test_parse_pipeline_accepts_metadata_workdir(tmp_path: Path) -> None:
    p = tmp_path / "p.yml"
    p.write_text(
        "\n".join(
            [
                "metadata:",
                "  workdir: .out/work2",
                "steps:",
                "  - name: s1",
                "    script: echo.py",
            ]
        ),
        encoding="utf-8",
    )
    pipeline = parse_pipeline(p)
    assert pipeline.workdir == ".out/work2"


def test_parse_pipeline_dirs_reference_uses_dirs_workdir_not_env_flat(tmp_path: Path) -> None:
    p = tmp_path / "p.yml"
    p.write_text(
        "\n".join(
            [
                "vars:",
                "  name: yanroy",
                "dirs:",
                "  workdir: \"{env.workdir}/{name}/{sys.now.yymmdd}/{sys.now.hhmmss}-{sys.run.short_id}\"",
                "  cachedir: \"{workdir}/raw\"",
                "steps:",
                "  - name: s1",
                "    script: \"echo.py out={cachedir}\"",
            ]
        ),
        encoding="utf-8",
    )
    pipeline = parse_pipeline(p, env_vars={"workdir": ".out/work"})
    assert pipeline.dirs["workdir"].startswith(".out/work/yanroy/")
    assert pipeline.dirs["cachedir"].startswith(".out/work/yanroy/")
    assert pipeline.dirs["cachedir"].endswith("/raw")
    assert "out=.out/work/yanroy/" in pipeline.steps[0].script


def test_parse_pipeline_dirs_workdir_self_reference_uses_base_fallback(tmp_path: Path) -> None:
    p = tmp_path / "p.yml"
    p.write_text(
        "\n".join(
            [
                "vars:",
                "  name: yanroy",
                "dirs:",
                "  workdir: \"{workdir}/{name}\"",
                "  cachedir: \"{workdir}/raw\"",
                "steps:",
                "  - name: s1",
                "    script: \"echo.py out={cachedir}\"",
            ]
        ),
        encoding="utf-8",
    )
    pipeline = parse_pipeline(p, env_vars={"workdir": ".out/work"})
    assert pipeline.dirs["workdir"] == ".out/work/yanroy"
    assert pipeline.dirs["cachedir"] == ".out/work/yanroy/raw"
    assert pipeline.steps[0].script == "echo.py out=.out/work/yanroy/raw"


def test_parse_pipeline_respects_configured_resolve_max_passes(tmp_path: Path) -> None:
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
    limited = parse_pipeline(p, global_vars={"resolve_max_passes": 1})
    full = parse_pipeline(p, global_vars={"resolve_max_passes": 10})
    capped_low = parse_pipeline(p, global_vars={"resolve_max_passes": 0})
    capped_high = parse_pipeline(p, global_vars={"resolve_max_passes": 9999})
    assert limited.resolve_max_passes == 1
    assert full.resolve_max_passes == 10
    assert capped_low.resolve_max_passes == 1
    assert capped_high.resolve_max_passes == 100


def test_parse_pipeline_supports_foreach_glob_fields(tmp_path: Path) -> None:
    p = tmp_path / "p.yml"
    p.write_text(
        "\n".join(
            [
                "dirs:",
                "  root: /tmp/data",
                "steps:",
                "  - name: s1",
                "    script: \"echo.py in={item}\"",
                "    foreach_glob: \"{dirs.root}/*\"",
                "    foreach_kind: dirs",
            ]
        ),
        encoding="utf-8",
    )
    pipeline = parse_pipeline(p)
    step = pipeline.steps[0]
    assert step.foreach_glob == "/tmp/data/*"
    assert step.foreach_kind == "dirs"


def test_parse_pipeline_rejects_both_foreach_and_foreach_glob(tmp_path: Path) -> None:
    p = tmp_path / "p.yml"
    p.write_text(
        "\n".join(
            [
                "steps:",
                "  - name: s1",
                "    script: echo.py",
                "    foreach: items",
                "    foreach_glob: /tmp/*",
            ]
        ),
        encoding="utf-8",
    )
    with pytest.raises(PipelineError, match="both `foreach` and `foreach_glob`"):
        parse_pipeline(p)


def test_parse_pipeline_rejects_foreach_kind_without_foreach_glob(tmp_path: Path) -> None:
    p = tmp_path / "p.yml"
    p.write_text(
        "\n".join(
            [
                "steps:",
                "  - name: s1",
                "    script: echo.py",
                "    foreach_kind: dirs",
            ]
        ),
        encoding="utf-8",
    )
    with pytest.raises(PipelineError, match="requires `foreach_glob`"):
        parse_pipeline(p)


def test_parse_pipeline_skips_disabled_steps_with_default_false(tmp_path: Path) -> None:
    p = tmp_path / "p.yml"
    p.write_text(
        "\n".join(
            [
                "steps:",
                "  - name: enabled_default",
                "    script: echo.py",
                "  - name: disabled_lower",
                "    script: echo.py",
                "    disabled: true",
                "  - name: disabled_upper",
                "    script: echo.py",
                "    Disabled: true",
                "  - name: enabled_explicit",
                "    script: echo.py",
                "    disabled: false",
            ]
        ),
        encoding="utf-8",
    )
    pipeline = parse_pipeline(p)
    assert [s.name for s in pipeline.steps] == ["enabled_default", "enabled_explicit"]


def test_parse_pipeline_rejects_non_boolean_disabled(tmp_path: Path) -> None:
    p = tmp_path / "p.yml"
    p.write_text(
        "\n".join(
            [
                "steps:",
                "  - name: s1",
                "    script: echo.py",
                "    disabled: \"true\"",
            ]
        ),
        encoding="utf-8",
    )
    with pytest.raises(PipelineError, match="`disabled` must be a boolean"):
        parse_pipeline(p)

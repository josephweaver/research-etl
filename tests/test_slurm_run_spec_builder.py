from __future__ import annotations

from pathlib import Path

from etl.executors.slurm import executor as slurm_mod
from etl.executors.slurm.run_spec_builder import SlurmRunSpecBuilder
from etl.executors.slurm.executor import SlurmExecutor
from etl.pipeline import Pipeline, Step
from etl.git_checkout import GitExecutionSpec


def test_slurm_run_spec_builder_resolves_sources_and_paths(monkeypatch, tmp_path: Path) -> None:
    pipeline = Pipeline(
        vars={"jobname": "sample"},
        steps=[Step(name="s1", script="echo.py")],
    )
    monkeypatch.setattr(slurm_mod, "parse_pipeline", lambda *_args, **_kwargs: pipeline)
    monkeypatch.setattr(
        "etl.executors.slurm.run_spec_builder.parse_pipeline",
        lambda *_args, **_kwargs: pipeline,
    )
    monkeypatch.setattr(
        "etl.executors.slurm.run_spec_builder._SOURCE_PROVIDER.resolve_execution_spec",
        lambda **_kwargs: GitExecutionSpec(
            commit_sha="abc123def4567890",
            origin_url="git@github.com:org/research-etl.git",
            repo_name="research-etl",
            git_is_dirty=False,
        ),
    )

    ex = SlurmExecutor(
        {"workdir": "/tmp/work", "logdir": "/tmp/logs"},
        repo_root=tmp_path,
        plugins_dir=Path("plugins"),
        dry_run=False,
        enforce_git_checkout=True,
        require_clean_git=False,
    )

    spec = SlurmRunSpecBuilder(ex).build(
        "pipelines/sample.yml",
        {
            "run_id": "runabc1234",
            "project_id": "crop_insurance",
            "project_vars": {
                "pipeline_asset_sources": [
                    {
                        "repo_url": "git@github.com:josephweaver/crop-insurance-etl-pipelines.git",
                        "local_repo_path": "../crop-insurance-etl-pipelines",
                        "pipelines_dir": "pipelines",
                        "scripts_dir": "scripts",
                        "ref": "main",
                    }
                ]
            },
            "commandline_vars": {
                "source": {
                    "pipeline": {
                        "repo_url": "git@github.com:josephweaver/crop-insurance-etl-pipelines.git",
                        "revision": "feature/runtime-sync",
                    }
                }
            },
        },
    )

    assert spec.run_id == "runabc1234"
    assert spec.project_id == "crop_insurance"
    assert spec.etl_source.revision == "abc123def4567890"
    assert spec.etl_source.mode == "auto"
    assert spec.paths is not None
    assert spec.paths.pipeline_path.endswith("/pipelines/sample.yml")
    assert spec.paths.checkout_root.endswith("/research-etl-abc123def456")
    assert spec.pipeline_assets[0].revision == "feature/runtime-sync"
    assert spec.paths.setup_logdir.endswith("/logs/setup")

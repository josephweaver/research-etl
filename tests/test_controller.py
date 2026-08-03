from __future__ import annotations

from pathlib import Path

from controler.app import ControllerApp


def _local_app() -> ControllerApp:
    app = object.__new__(ControllerApp)
    app.controller_cfg = {}
    app.worker_cfg = {}
    app.exec_env = {}
    app._is_remote_controller = lambda: False  # type: ignore[method-assign]
    return app


def test_seed_counties_ignores_directories_without_county_input(tmp_path: Path) -> None:
    valid = tmp_path / "17001"
    valid.mkdir()
    (valid / "county_data.csv").write_text("FIPS\n17001\n", encoding="utf-8")
    (tmp_path / "17003").mkdir()

    app = _local_app()
    app.controller_cfg = {"seed_county_file_name": "county_data.csv"}

    counties = app._seed_counties_from_dir(tmp_path.as_posix())

    assert [county.fips for county in counties] == ["17001"]


def test_worker_preflight_checks_inputs_and_worker_files(tmp_path: Path) -> None:
    seed_dir = tmp_path / "inputs"
    seed_dir.mkdir()
    manifest = tmp_path / "manifest.csv"
    manifest.write_text("focal_fips\n17001\n", encoding="utf-8")
    python_bin = tmp_path / "python"
    python_bin.touch()
    pipeline = tmp_path / "pipeline.yml"
    pipeline.touch()
    model_script = tmp_path / "model.R"
    model_script.touch()

    app = _local_app()
    app.controller_cfg = {
        "seed_county_dir": seed_dir.as_posix(),
        "seed_manifest_csv": manifest.as_posix(),
    }
    app.worker_cfg = {
        "python_bin": python_bin.as_posix(),
        "pipeline_path": pipeline.as_posix(),
        "required_paths": [model_script.as_posix()],
    }

    result = app._worker_preflight()

    assert result["ready"] is True
    model_script.unlink()
    assert app._worker_preflight()["ready"] is False


def test_bootstrap_uses_configured_git_refs() -> None:
    class RecordingTransport:
        script = ""

        def run_text(self, text: str, check: bool = False):
            self.script = text
            return None

    app = _local_app()
    app.transport = RecordingTransport()
    app.exec_env = {"git_remote_url": "git@example/research-etl.git"}
    app.worker_cfg = {
        "repo_root": "/work/research-etl",
        "python_bin": "/work/research-etl/.venv/bin/python",
        "git_ref": "etl-branch",
        "pipeline_repo_root": "/work/landcore-etl-pipelines",
        "pipeline_git_remote_url": "git@example/landcore-etl-pipelines.git",
        "pipeline_git_ref": "pipeline-branch",
    }

    result = app._bootstrap_submission_environment()

    assert result["prepared"] is True
    assert "GIT_REF=etl-branch" in app.transport.script
    assert "PIPELINE_GIT_REF=pipeline-branch" in app.transport.script
    assert 'git clone --branch "$ref" --single-branch' in app.transport.script

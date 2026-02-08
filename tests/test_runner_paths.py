from __future__ import annotations

import re
from pathlib import Path

from etl.pipeline import Pipeline, Step
from etl.runner import run_pipeline


def test_runner_creates_timestamped_artifact_directory(tmp_path: Path) -> None:
    plugin_dir = tmp_path / "plugins"
    plugin_dir.mkdir(parents=True, exist_ok=True)
    (plugin_dir / "echo.py").write_text(
        "\n".join(
            [
                "meta = {'name': 'echo', 'version': '0.1.0', 'description': 'test'}",
                "def run(args, ctx):",
                "    return {'ok': True}",
            ]
        ),
        encoding="utf-8",
    )

    runs_dir = tmp_path / ".runs"
    run_id = "1234567890abcdef1234567890abcdef"
    pipeline = Pipeline(steps=[Step(name="echo_step", script="echo.py")])

    result = run_pipeline(
        pipeline,
        plugin_dir=plugin_dir,
        workdir=runs_dir,
        run_id=run_id,
        dry_run=True,
    )

    assert result.artifact_dir is not None
    artifact_dir = Path(result.artifact_dir)
    rel = artifact_dir.relative_to(runs_dir)
    assert re.fullmatch(r"\d{6}", rel.parts[0])
    assert re.fullmatch(r"\d{6}-[0-9a-f]{8}", rel.parts[1])
    assert rel.parts[1].endswith("-12345678")
    assert (artifact_dir / "echo_step").exists()

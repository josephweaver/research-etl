# research-etl
# Copyright (c) 2026 Joseph Weaver
# This file is part of the research-etl project and is licensed under the MIT License.
# You may not use this file except in compliance with the License.
# See https://github.com/josephweaver/research-etl for details.

from __future__ import annotations

import subprocess
from pathlib import Path

from etl.plugins.base import PluginContext
from plugins import source_control_checkin, source_control_checkout


def _git(cmd: list[str], cwd: Path | None = None) -> str:
    proc = subprocess.run(
        ["git", *cmd],
        cwd=str(cwd) if cwd else None,
        capture_output=True,
        text=True,
        check=False,
    )
    if proc.returncode != 0:
        raise AssertionError(proc.stderr or proc.stdout or f"git command failed: {' '.join(cmd)}")
    return str(proc.stdout or "").strip()


def _ctx(tmp_path: Path) -> PluginContext:
    return PluginContext(run_id="t1", workdir=tmp_path / "work", log=lambda *_a, **_k: None)


def test_source_control_checkout_plugin_clones_named_repo(tmp_path: Path) -> None:
    remote = tmp_path / "remote.git"
    seed = tmp_path / "seed"
    checkout_dir = tmp_path / "checkout"
    config_path = tmp_path / "source_control.yml"

    _git(["init", "--bare", str(remote)])
    _git(["clone", str(remote), str(seed)])
    _git(["config", "user.email", "test@example.com"], cwd=seed)
    _git(["config", "user.name", "Test User"], cwd=seed)
    (seed / "README.md").write_text("hello\n", encoding="utf-8")
    _git(["add", "README.md"], cwd=seed)
    _git(["commit", "-m", "init"], cwd=seed)
    _git(["branch", "-M", "main"], cwd=seed)
    _git(["push", "-u", "origin", "main"], cwd=seed)

    config_path.write_text(
        "\n".join(
            [
                "repositories:",
                "  TEST_REPO:",
                "    provider: git",
                f"    local_path: {checkout_dir.as_posix()}",
                f"    repo_url: {remote.as_posix()}",
                "    default_branch: main",
            ]
        ),
        encoding="utf-8",
    )

    out = source_control_checkout.run(
        {"repo_alias": "TEST_REPO", "config_path": str(config_path)},
        _ctx(tmp_path),
    )

    assert out["repo_alias"] == "TEST_REPO"
    assert out["branch"] == "main"
    assert Path(out["local_repo_path"]).exists()
    assert (Path(out["local_repo_path"]) / "README.md").exists()


def test_source_control_checkout_resolves_repo_local_path_from_etl_repo_root(tmp_path: Path, monkeypatch) -> None:
    remote = tmp_path / "remote.git"
    seed = tmp_path / "seed"
    etl_root = tmp_path / "etlroot"
    checkout_dir = etl_root.parent / "landcore-duckdb"
    config_path = etl_root / "config" / "source_control.yml"

    _git(["init", "--bare", str(remote)])
    _git(["clone", str(remote), str(seed)])
    _git(["config", "user.email", "test@example.com"], cwd=seed)
    _git(["config", "user.name", "Test User"], cwd=seed)
    (seed / "README.md").write_text("hello\n", encoding="utf-8")
    _git(["add", "README.md"], cwd=seed)
    _git(["commit", "-m", "init"], cwd=seed)
    _git(["branch", "-M", "main"], cwd=seed)
    _git(["push", "-u", "origin", "main"], cwd=seed)

    config_path.parent.mkdir(parents=True)
    config_path.write_text(
        "\n".join(
            [
                "repositories:",
                "  LC_DUCKDB:",
                "    provider: git",
                "    local_path: ../landcore-duckdb",
                f"    repo_url: {remote.as_posix()}",
                "    default_branch: main",
            ]
        ),
        encoding="utf-8",
    )
    monkeypatch.setenv("ETL_REPO_ROOT", str(etl_root))

    out = source_control_checkout.run(
        {"repo_alias": "LC_DUCKDB", "config_path": str(config_path)},
        _ctx(tmp_path),
    )

    assert Path(out["local_repo_path"]).resolve() == checkout_dir.resolve()


def test_source_control_checkin_plugin_commits_file(tmp_path: Path) -> None:
    repo = tmp_path / "repo"
    target_file = repo / "tables" / "yanroy" / "fields.yml"

    _git(["init", str(repo)])
    _git(["-C", str(repo), "config", "user.email", "test@example.com"])
    _git(["-C", str(repo), "config", "user.name", "Test User"])
    _git(["-C", str(repo), "branch", "-M", "main"])
    (repo / "README.md").write_text("hello\n", encoding="utf-8")
    _git(["-C", str(repo), "add", "README.md"])
    _git(["-C", str(repo), "commit", "-m", "init"])

    target_file.parent.mkdir(parents=True, exist_ok=True)
    target_file.write_text("name: fields\n", encoding="utf-8")

    out = source_control_checkin.run(
        {
            "local_path": str(repo),
            "path": str(target_file),
            "commit_message": "add fields table config",
            "create_pr": False,
        },
        _ctx(tmp_path),
    )

    assert out["committed"] is True
    assert out["branch"] == "main"
    assert out["commit_sha"]

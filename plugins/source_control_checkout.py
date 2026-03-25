# research-etl
# Copyright (c) 2026 Joseph Weaver
# This file is part of the research-etl project and is licensed under the MIT License.
# You may not use this file except in compliance with the License.
# See https://github.com/josephweaver/research-etl for details.

from __future__ import annotations

import os
from pathlib import Path

from etl.source_control import (
    SourceControlConfigError,
    prepare_local_checkout,
    resolve_repo_config,
)


meta = {
    "name": "source_control_checkout",
    "version": "0.1.0",
    "description": "Prepare a local git working copy from a named source-control repo config or explicit repo settings.",
    "inputs": [],
    "outputs": [
        "repo_alias",
        "provider",
        "repo_url",
        "local_repo_path",
        "branch",
        "commit_sha",
        "config_path",
    ],
    "params": {
        "repo_alias": {"type": "str", "default": ""},
        "config_path": {"type": "str", "default": ""},
        "local_path": {"type": "str", "default": ""},
        "repo_url": {"type": "str", "default": ""},
        "branch": {"type": "str", "default": ""},
        "remote": {"type": "str", "default": "origin"},
        "pull": {"type": "bool", "default": True},
        "create_branch": {"type": "bool", "default": False},
        "set_remote_url": {"type": "bool", "default": False},
    },
    "idempotent": False,
}


def run(args, ctx):
    repo_alias = str(args.get("repo_alias") or "").strip()
    repo_cfg = {}
    if repo_alias:
        try:
            repo_cfg = resolve_repo_config(repo_alias=repo_alias, config_path=str(args.get("config_path") or "").strip() or None)
        except SourceControlConfigError as exc:
            raise ValueError(str(exc)) from exc
    provider = str(repo_cfg.get("provider") or "git").strip() or "git"
    if provider.lower() != "git":
        raise ValueError(f"Unsupported source-control provider: {provider}")
    local_path = str(args.get("local_path") or repo_cfg.get("local_path") or "").strip()
    repo_url = str(args.get("repo_url") or repo_cfg.get("repo_url") or "").strip()
    branch = str(args.get("branch") or repo_cfg.get("default_branch") or "").strip()
    if not local_path:
        raise ValueError("local_path is required, either directly or via repo_alias config")
    result = prepare_local_checkout(
        repo_url=repo_url,
        local_path=local_path,
        branch=branch,
        remote=str(args.get("remote") or "origin").strip() or "origin",
        pull=bool(args.get("pull", True)),
        create_branch=bool(args.get("create_branch", False)),
        set_remote_url=bool(args.get("set_remote_url", False)),
        env=dict(os.environ),
    )
    if not result.get("ok"):
        raise RuntimeError(result.get("reason") or "source-control checkout failed")
    out = {
        "repo_alias": repo_alias,
        "provider": provider,
        "repo_url": repo_url,
        "local_repo_path": str(Path(result["local_repo_path"]).resolve().as_posix()),
        "branch": str(result.get("branch") or ""),
        "commit_sha": str(result.get("commit_sha") or ""),
        "config_path": str(repo_cfg.get("config_path") or str(args.get("config_path") or "")).strip(),
    }
    ctx.log(f"[source_control_checkout] repo={repo_alias or out['local_repo_path']} branch={out['branch']}")
    return out

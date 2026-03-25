# research-etl
# Copyright (c) 2026 Joseph Weaver
# This file is part of the research-etl project and is licensed under the MIT License.
# You may not use this file except in compliance with the License.
# See https://github.com/josephweaver/research-etl for details.

from __future__ import annotations

import json
import os
import subprocess
from pathlib import Path

from etl.source_control import (
    GitHubPullRequestError,
    SourceControlConfigError,
    checkin_files,
    create_pull_request,
    infer_github_repo,
    resolve_repo_config,
)


meta = {
    "name": "source_control_checkin",
    "version": "0.1.0",
    "description": "Commit changed files in a configured local git repo and optionally push/open a PR.",
    "inputs": [],
    "outputs": [
        "repo_alias",
        "repo_root",
        "workspace_paths",
        "committed",
        "message",
        "commit_sha",
        "pushed",
        "pr_url",
        "pr_number",
        "branch",
        "base_branch",
    ],
    "params": {
        "repo_alias": {"type": "str", "default": ""},
        "config_path": {"type": "str", "default": ""},
        "local_path": {"type": "str", "default": ""},
        "path": {"type": "str", "default": ""},
        "paths_json": {"type": "str", "default": ""},
        "commit_message": {"type": "str", "default": ""},
        "push": {"type": "bool", "default": False},
        "remote": {"type": "str", "default": "origin"},
        "branch": {"type": "str", "default": ""},
        "create_pr": {"type": "bool", "default": True},
        "direct_push": {"type": "bool", "default": False},
        "base_branch": {"type": "str", "default": ""},
        "pr_title": {"type": "str", "default": ""},
        "pr_body": {"type": "str", "default": ""},
        "no_github_api": {"type": "bool", "default": False},
    },
    "idempotent": False,
}


def _parse_paths(args: dict) -> list[str]:
    one = str(args.get("path") or "").strip()
    many = str(args.get("paths_json") or "").strip()
    out: list[str] = []
    if one:
        out.append(one)
    if many:
        parsed = json.loads(many)
        if not isinstance(parsed, list):
            raise ValueError("paths_json must decode to a list")
        out.extend(str(item or "").strip() for item in parsed if str(item or "").strip())
    if not out:
        raise ValueError("path or paths_json is required")
    return out


def _current_branch(repo_root: Path) -> str:
    proc = subprocess.run(
        ["git", "-C", str(repo_root), "rev-parse", "--abbrev-ref", "HEAD"],
        capture_output=True,
        text=True,
        check=False,
    )
    return str(proc.stdout or "").strip()


def _origin_url(repo_root: Path) -> str:
    proc = subprocess.run(
        ["git", "-C", str(repo_root), "config", "--get", "remote.origin.url"],
        capture_output=True,
        text=True,
        check=False,
    )
    return str(proc.stdout or "").strip()


def run(args, ctx):
    repo_alias = str(args.get("repo_alias") or "").strip()
    repo_cfg = {}
    if repo_alias:
        try:
            repo_cfg = resolve_repo_config(repo_alias=repo_alias, config_path=str(args.get("config_path") or "").strip() or None)
        except SourceControlConfigError as exc:
            raise ValueError(str(exc)) from exc
    repo_root = str(args.get("local_path") or repo_cfg.get("local_path") or "").strip()
    if not repo_root:
        raise ValueError("local_path is required, either directly or via repo_alias config")
    paths = [str(Path(p).expanduser().resolve()) for p in _parse_paths(args)]
    message = str(args.get("commit_message") or "").strip()
    if not message:
        raise ValueError("commit_message is required")
    result = checkin_files(
        file_paths=paths,
        message=message,
        push=bool(args.get("push", False)),
        remote=str(args.get("remote") or "origin").strip() or "origin",
        branch=str(args.get("branch") or "").strip(),
        env=dict(os.environ),
    )
    if result.get("committed") is False and result.get("reason") not in {"no_staged_changes"}:
        raise RuntimeError(result.get("reason") or "source-control checkin failed")

    out = dict(result)
    repo_root_path = Path(str(out.get("repo_root") or repo_root)).resolve()
    branch = str(args.get("branch") or "").strip() or _current_branch(repo_root_path)
    out["repo_alias"] = repo_alias
    out["branch"] = branch
    out["base_branch"] = str(args.get("base_branch") or repo_cfg.get("default_branch") or "main").strip() or "main"
    out["pr_url"] = None
    out["pr_number"] = None
    create_pr = bool(args.get("create_pr", True))
    direct_push = bool(args.get("direct_push", False))
    if direct_push:
        create_pr = False
    if create_pr and branch == out["base_branch"]:
        raise ValueError(
            f"PR mode requires a non-base branch. Current branch '{branch}' matches base branch '{out['base_branch']}'. "
            "Use source_control_checkout with create_branch=true first, or set direct_push=true."
        )

    if create_pr and bool(out.get("committed")) and bool(out.get("pushed")):
        repo_url = str(repo_cfg.get("repo_url") or "").strip() or _origin_url(repo_root_path)
        owner = str(repo_cfg.get("github_owner") or "").strip()
        repo_name = str(repo_cfg.get("github_repo") or "").strip()
        if not owner or not repo_name:
            owner, repo_name = infer_github_repo(repo_url)
        if not owner or not repo_name:
            raise ValueError("GitHub PR creation requires github_owner/github_repo or a GitHub repo_url")
        title = str(args.get("pr_title") or f"Update {repo_alias or repo_name}").strip()
        body = str(args.get("pr_body") or f"Automated update from research-etl.").strip()
        trace: list[str] = []
        try:
            pr_url, pr_number = create_pull_request(
                owner=owner,
                repo=repo_name,
                head=branch,
                base=out["base_branch"],
                title=title,
                body=body,
                use_github_api=not bool(args.get("no_github_api", False)),
                trace=trace,
            )
        except GitHubPullRequestError as exc:
            for line in trace:
                ctx.error(f"[source_control_checkin] {line}")
            raise RuntimeError(str(exc)) from exc
        out["pr_url"] = pr_url
        out["pr_number"] = pr_number
        for line in trace:
            ctx.log(f"[source_control_checkin] {line}")

    ctx.log(
        f"[source_control_checkin] repo={repo_alias or repo_root_path.as_posix()} committed={bool(out.get('committed'))} pushed={bool(out.get('pushed'))}"
    )
    return out

# research-etl
# Copyright (c) 2026 Joseph Weaver
# This file is part of the research-etl project and is licensed under the MIT License.
# You may not use this file except in compliance with the License.
# See https://github.com/josephweaver/research-etl for details.

from __future__ import annotations

import json
import os
import re
import subprocess
from typing import Optional
from urllib import error, request


class GitHubPullRequestError(RuntimeError):
    """Raised when GitHub PR creation fails."""


def _trace(trace: Optional[list[str]], message: str) -> None:
    if trace is not None:
        trace.append(str(message))


def infer_github_repo(repo_url: str) -> tuple[Optional[str], Optional[str]]:
    text = str(repo_url or "").strip()
    if not text:
        return None, None
    m = re.search(r"github\.com[:/](?P<owner>[^/]+)/(?P<repo>[^/]+?)(?:\.git)?$", text)
    if not m:
        return None, None
    return m.group("owner"), m.group("repo")


def _github_api_create_pr(
    *,
    owner: str,
    repo: str,
    head: str,
    base: str,
    title: str,
    body: str,
    trace: Optional[list[str]] = None,
) -> tuple[str, Optional[int]]:
    token = str(os.environ.get("GITHUB_TOKEN") or "").strip()
    if not token:
        raise GitHubPullRequestError("GITHUB_TOKEN is not set for GitHub API PR creation.")
    payload = json.dumps({"title": title, "head": head, "base": base, "body": body}).encode("utf-8")
    req = request.Request(
        f"https://api.github.com/repos/{owner}/{repo}/pulls",
        data=payload,
        method="POST",
        headers={
            "Accept": "application/vnd.github+json",
            "Authorization": f"Bearer {token}",
            "X-GitHub-Api-Version": "2022-11-28",
            "Content-Type": "application/json",
            "User-Agent": "research-etl-source-control",
        },
    )
    _trace(trace, f"github_api:create_pr repo={owner}/{repo} base={base} head={head}")
    try:
        with request.urlopen(req, timeout=30) as resp:
            text = resp.read().decode("utf-8", errors="replace")
    except error.HTTPError as exc:
        body_text = exc.read().decode("utf-8", errors="replace")
        _trace(trace, f"github_api:error status={exc.code} body={body_text[:1000]}")
        raise GitHubPullRequestError(f"GitHub API PR create failed ({exc.code}).") from exc
    except Exception as exc:  # noqa: BLE001
        _trace(trace, f"github_api:error {exc}")
        raise GitHubPullRequestError(f"GitHub API PR create failed: {exc}") from exc
    try:
        parsed = json.loads(text)
    except Exception as exc:  # noqa: BLE001
        raise GitHubPullRequestError("GitHub API response parse failed.") from exc
    pr_url = str(parsed.get("html_url") or "").strip()
    pr_number = parsed.get("number")
    return pr_url, int(pr_number) if pr_number is not None else None


def _gh_cli_create_pr(
    *,
    owner: str,
    repo: str,
    head: str,
    base: str,
    title: str,
    body: str,
    trace: Optional[list[str]] = None,
) -> tuple[str, Optional[int]]:
    cmd = [
        "gh",
        "pr",
        "create",
        "--repo",
        f"{owner}/{repo}",
        "--head",
        head,
        "--base",
        base,
        "--title",
        title,
        "--body",
        body,
    ]
    _trace(trace, f"cmd: {' '.join(cmd)}")
    proc = subprocess.run(cmd, capture_output=True, text=True, check=False)
    if proc.returncode != 0:
        detail = (proc.stderr or proc.stdout or "").strip()
        _trace(trace, f"gh:error {detail[:1000]}")
        raise GitHubPullRequestError(detail or "gh pr create failed")
    text = str(proc.stdout or "").strip()
    pr_url = text.splitlines()[-1].strip() if text else ""
    pr_number = None
    if pr_url:
        m = re.search(r"/pull/(\d+)", pr_url)
        if m:
            pr_number = int(m.group(1))
    return pr_url, pr_number


def create_pull_request(
    *,
    owner: str,
    repo: str,
    head: str,
    base: str,
    title: str,
    body: str,
    use_github_api: bool = True,
    trace: Optional[list[str]] = None,
) -> tuple[str, Optional[int]]:
    if use_github_api:
        try:
            return _github_api_create_pr(
                owner=owner,
                repo=repo,
                head=head,
                base=base,
                title=title,
                body=body,
                trace=trace,
            )
        except GitHubPullRequestError as exc:
            _trace(trace, f"github_api_failed_fallback_to_gh={exc}")
    return _gh_cli_create_pr(owner=owner, repo=repo, head=head, base=base, title=title, body=body, trace=trace)


__all__ = ["GitHubPullRequestError", "create_pull_request", "infer_github_repo"]

# research-etl
# Copyright (c) 2026 Joseph Weaver
# This file is part of the research-etl project and is licensed under the MIT License.
# You may not use this file except in compliance with the License.
# See https://github.com/josephweaver/research-etl for details.

from __future__ import annotations

import logging
import re
import shutil
import subprocess
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, Optional

import psycopg

from .db import get_database_url
from .source_control.github import GitHubPullRequestError, create_pull_request
from .subprocess_logging import run_logged_subprocess


_LOG = logging.getLogger("etl.dictionary_pr")


class DictionaryPRError(RuntimeError):
    """Raised when dictionary PR automation fails."""

    def __init__(self, message: str, *, details: Optional[Dict[str, Any]] = None) -> None:
        super().__init__(message)
        self.details: Dict[str, Any] = dict(details or {})


@dataclass(frozen=True)
class DictionaryRepo:
    repo_id: int
    repo_key: str
    provider: str
    owner: str
    repo_name: str
    default_branch: str
    local_path: str
    is_active: bool


def _log(trace: list[str], message: str) -> None:
    trace.append(str(message))


def _connect() -> psycopg.Connection:
    db_url = get_database_url()
    if not db_url:
        raise DictionaryPRError("ETL_DATABASE_URL is not configured.", details={"operation_log": []})
    try:
        return psycopg.connect(db_url)
    except Exception as exc:  # noqa: BLE001
        raise DictionaryPRError(f"Could not connect to database: {exc}", details={"operation_log": []}) from exc


def _run_cmd(cmd: list[str], *, cwd: Optional[Path], trace: list[str], check: bool = True) -> subprocess.CompletedProcess:
    _log(trace, f"cmd: {' '.join(cmd)}")
    proc = run_logged_subprocess(
        cmd,
        logger=_LOG,
        action="dictionary_pr.cmd",
        cwd=cwd,
        check=False,
    )
    out = (proc.stdout or "").strip()
    err = (proc.stderr or "").strip()
    if out:
        _log(trace, f"stdout: {out[:1000]}")
    if err:
        _log(trace, f"stderr: {err[:1000]}")
    if check and proc.returncode != 0:
        raise DictionaryPRError(
            f"Command failed ({proc.returncode}): {' '.join(cmd)}",
            details={"operation_log": trace},
        )
    return proc


def _sanitize_branch_part(value: str) -> str:
    text = re.sub(r"[^a-zA-Z0-9._/-]+", "-", str(value or "").strip())
    text = text.strip("-/.")
    return text or "update"


def _load_repo(conn: psycopg.Connection, repo_key: str) -> Optional[DictionaryRepo]:
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT repo_id, repo_key, provider, owner, repo_name, default_branch, local_path, is_active
            FROM etl_dictionary_repos
            WHERE repo_key = %s
            """,
            (repo_key,),
        )
        row = cur.fetchone()
    if not row:
        return None
    return DictionaryRepo(
        repo_id=int(row[0]),
        repo_key=str(row[1]),
        provider=str(row[2] or "github"),
        owner=str(row[3]),
        repo_name=str(row[4]),
        default_branch=str(row[5] or "main"),
        local_path=str(row[6] or ""),
        is_active=bool(row[7]),
    )


def _resolve_target_path(
    conn: psycopg.Connection,
    *,
    dataset_id: str,
    repo_id: int,
    explicit_file_path: Optional[str],
) -> str:
    file_path = str(explicit_file_path or "").strip()
    if file_path:
        return file_path.replace("\\", "/")
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT file_path
            FROM etl_dataset_dictionary_entries
            WHERE dataset_id = %s AND repo_id = %s
            ORDER BY updated_at DESC, entry_id DESC
            LIMIT 1
            """,
            (dataset_id, repo_id),
        )
        row = cur.fetchone()
    if row and str(row[0] or "").strip():
        return str(row[0]).replace("\\", "/")
    return f"datasets/{dataset_id}.yml"


def _gh_cli_create_pr(
    *,
    repo_full: str,
    head: str,
    base: str,
    title: str,
    body: str,
    trace: list[str],
) -> tuple[str, Optional[int]]:
    proc = _run_cmd(
        ["gh", "pr", "create", "--repo", repo_full, "--head", head, "--base", base, "--title", title, "--body", body],
        cwd=None,
        trace=trace,
        check=True,
    )
    text = str(proc.stdout or "").strip()
    pr_url = text.splitlines()[-1].strip() if text else ""
    pr_number = None
    if pr_url:
        m = re.search(r"/pull/(\d+)", pr_url)
        if m:
            pr_number = int(m.group(1))
    return pr_url, pr_number


def _ensure_dataset_row(conn: psycopg.Connection, dataset_id: str) -> None:
    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO etl_datasets (dataset_id, status, created_at, updated_at)
            VALUES (%s, 'active', NOW(), NOW())
            ON CONFLICT (dataset_id)
            DO UPDATE SET updated_at = NOW()
            """,
            (dataset_id,),
        )


def _upsert_dictionary_entry(
    conn: psycopg.Connection,
    *,
    dataset_id: str,
    repo_id: int,
    file_path: str,
    file_sha: Optional[str],
    pr_url: Optional[str],
    pr_number: Optional[int],
    review_status: str,
) -> None:
    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO etl_dataset_dictionary_entries (
                dataset_id, repo_id, file_path, file_sha, pr_url, pr_number, review_status, last_synced_at, created_at, updated_at
            )
            VALUES (%s, %s, %s, %s, %s, %s, %s, NOW(), NOW(), NOW())
            ON CONFLICT (dataset_id, repo_id, file_path)
            DO UPDATE SET
                file_sha = COALESCE(EXCLUDED.file_sha, etl_dataset_dictionary_entries.file_sha),
                pr_url = COALESCE(EXCLUDED.pr_url, etl_dataset_dictionary_entries.pr_url),
                pr_number = COALESCE(EXCLUDED.pr_number, etl_dataset_dictionary_entries.pr_number),
                review_status = EXCLUDED.review_status,
                last_synced_at = NOW(),
                updated_at = NOW()
            """,
            (dataset_id, repo_id, file_path, file_sha, pr_url, pr_number, review_status),
        )


def create_dictionary_pr(
    *,
    dataset_id: str,
    repo_key: str,
    source_file: str,
    file_path: Optional[str] = None,
    branch_name: Optional[str] = None,
    base_branch: Optional[str] = None,
    pr_title: Optional[str] = None,
    pr_body: Optional[str] = None,
    commit_message: Optional[str] = None,
    create_pr: bool = True,
    use_github_api: bool = True,
    dry_run: bool = False,
) -> Dict[str, Any]:
    trace: list[str] = []
    ds_id = str(dataset_id or "").strip()
    repo_key_text = str(repo_key or "").strip()
    src = Path(str(source_file or "")).expanduser().resolve()
    if not ds_id:
        raise DictionaryPRError("dataset_id is required", details={"operation_log": trace})
    if not repo_key_text:
        raise DictionaryPRError("repo_key is required", details={"operation_log": trace})
    if not src.exists() or not src.is_file():
        raise DictionaryPRError(f"source_file not found: {src}", details={"operation_log": trace})
    _log(trace, f"start dataset_id={ds_id} repo_key={repo_key_text} source_file={src}")

    try:
        with _connect() as conn:
            repo = _load_repo(conn, repo_key_text)
            if not repo:
                raise DictionaryPRError(
                    f"Dictionary repo not found for repo_key='{repo_key_text}'",
                    details={"operation_log": trace},
                )
            if not repo.is_active:
                raise DictionaryPRError(
                    f"Dictionary repo '{repo_key_text}' is inactive.",
                    details={"operation_log": trace},
                )
            repo_path = Path(repo.local_path).expanduser().resolve()
            if not repo.local_path or not repo_path.exists():
                raise DictionaryPRError(
                    f"Dictionary repo local_path missing or not found: {repo.local_path}",
                    details={"operation_log": trace},
                )
            target_rel = _resolve_target_path(
                conn,
                dataset_id=ds_id,
                repo_id=repo.repo_id,
                explicit_file_path=file_path,
            )
            _log(trace, f"resolved target file_path={target_rel}")

            base = str(base_branch or repo.default_branch or "main").strip() or "main"
            branch = str(branch_name or "").strip()
            if not branch:
                stamp = datetime.now(timezone.utc).strftime("%Y%m%d-%H%M%S")
                branch = f"bot/dict-{_sanitize_branch_part(ds_id)}-{stamp}"
            title = str(pr_title or f"Update dictionary entry for {ds_id}").strip()
            body = str(
                pr_body
                or f"Automated dictionary update for `{ds_id}`.\n\n- Source file: `{src.as_posix()}`\n- Repo key: `{repo.repo_key}`"
            ).strip()
            msg = str(commit_message or f"Update dictionary entry for {ds_id}").strip()

            target_abs = (repo_path / Path(target_rel)).resolve()
            if repo_path not in target_abs.parents and target_abs != repo_path:
                raise DictionaryPRError(
                    f"Target file_path resolves outside repo root: {target_rel}",
                    details={"operation_log": trace},
                )

            _run_cmd(["git", "-C", str(repo_path), "fetch", "origin", base], cwd=None, trace=trace, check=not dry_run)
            _run_cmd(["git", "-C", str(repo_path), "checkout", base], cwd=None, trace=trace, check=not dry_run)
            _run_cmd(
                ["git", "-C", str(repo_path), "pull", "--ff-only", "origin", base],
                cwd=None,
                trace=trace,
                check=not dry_run,
            )
            _run_cmd(["git", "-C", str(repo_path), "checkout", "-B", branch], cwd=None, trace=trace, check=not dry_run)

            _log(trace, f"copy source -> target {src} -> {target_abs}")
            if not dry_run:
                target_abs.parent.mkdir(parents=True, exist_ok=True)
                shutil.copy2(src, target_abs)

            _run_cmd(["git", "-C", str(repo_path), "add", target_rel], cwd=None, trace=trace, check=not dry_run)
            diff_proc = _run_cmd(
                ["git", "-C", str(repo_path), "diff", "--cached", "--quiet"],
                cwd=None,
                trace=trace,
                check=False,
            )
            has_changes = diff_proc.returncode != 0
            _log(trace, f"staged_changes={has_changes}")

            commit_sha = None
            file_sha = None
            pr_url = None
            pr_number = None
            review_status = "none"

            if has_changes and not dry_run:
                _run_cmd(["git", "-C", str(repo_path), "commit", "-m", msg], cwd=None, trace=trace, check=True)
                push_proc = _run_cmd(
                    ["git", "-C", str(repo_path), "push", "-u", "origin", branch],
                    cwd=None,
                    trace=trace,
                    check=True,
                )
                _ = push_proc
                commit_proc = _run_cmd(["git", "-C", str(repo_path), "rev-parse", "HEAD"], cwd=None, trace=trace, check=True)
                commit_sha = str(commit_proc.stdout or "").strip() or None
                file_proc = _run_cmd(
                    ["git", "-C", str(repo_path), "rev-parse", f"HEAD:{target_rel}"],
                    cwd=None,
                    trace=trace,
                    check=False,
                )
                if file_proc.returncode == 0:
                    file_sha = str(file_proc.stdout or "").strip() or None

                if create_pr:
                    if repo.provider.lower() != "github":
                        raise DictionaryPRError(
                            f"PR automation for provider '{repo.provider}' is not implemented.",
                            details={"operation_log": trace},
                        )
                    repo_full = f"{repo.owner}/{repo.repo_name}"
                    if use_github_api:
                        try:
                            pr_url, pr_number = create_pull_request(
                                owner=repo.owner,
                                repo=repo.repo_name,
                                head=branch,
                                base=base,
                                title=title,
                                body=body,
                                trace=trace,
                                use_github_api=True,
                            )
                        except GitHubPullRequestError as api_exc:
                            _log(trace, f"github_api_failed_fallback_to_gh={api_exc}")
                            pr_url, pr_number = _gh_cli_create_pr(
                                repo_full=repo_full,
                                head=branch,
                                base=base,
                                title=title,
                                body=body,
                                trace=trace,
                            )
                    else:
                        pr_url, pr_number = create_pull_request(
                            owner=repo.owner,
                            repo=repo.repo_name,
                            head=branch,
                            base=base,
                            title=title,
                            body=body,
                            trace=trace,
                            use_github_api=False,
                        )
                    review_status = "open"

            if not dry_run:
                _ensure_dataset_row(conn, ds_id)
                _upsert_dictionary_entry(
                    conn,
                    dataset_id=ds_id,
                    repo_id=repo.repo_id,
                    file_path=target_rel,
                    file_sha=file_sha,
                    pr_url=pr_url,
                    pr_number=pr_number,
                    review_status=review_status if has_changes else "none",
                )
                conn.commit()
                _log(trace, "db_upsert_complete")

            return {
                "dataset_id": ds_id,
                "repo_key": repo.repo_key,
                "provider": repo.provider,
                "repo": f"{repo.owner}/{repo.repo_name}",
                "local_repo_path": repo_path.as_posix(),
                "file_path": target_rel,
                "base_branch": base,
                "branch_name": branch,
                "has_changes": bool(has_changes),
                "commit_sha": commit_sha,
                "pr_url": pr_url,
                "pr_number": pr_number,
                "review_status": review_status if has_changes else "none",
                "dry_run": bool(dry_run),
                "operation_log": trace,
            }
    except DictionaryPRError:
        raise
    except Exception as exc:  # noqa: BLE001
        _log(trace, f"unhandled_error={exc}")
        raise DictionaryPRError(f"Dictionary PR workflow failed: {exc}", details={"operation_log": trace}) from exc


__all__ = ["DictionaryPRError", "DictionaryRepo", "create_dictionary_pr"]

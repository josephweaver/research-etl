from __future__ import annotations

from etl.dictionary_pr import DictionaryPRError, create_dictionary_pr


meta = {
    "name": "dataset_dictionary_pr",
    "version": "0.1.0",
    "description": "Create/update a dataset dictionary file in a mapped repo and optionally open a PR.",
    "inputs": [],
    "outputs": [
        "dataset_id",
        "repo_key",
        "provider",
        "repo",
        "local_repo_path",
        "file_path",
        "base_branch",
        "branch_name",
        "has_changes",
        "commit_sha",
        "pr_url",
        "pr_number",
        "review_status",
        "dry_run",
        "operation_log",
    ],
    "params": {
        "dataset_id": {"type": "str", "default": ""},
        "repo_key": {"type": "str", "default": ""},
        "source_file": {"type": "str", "default": ""},
        "file_path": {"type": "str", "default": ""},
        "branch_name": {"type": "str", "default": ""},
        "base_branch": {"type": "str", "default": ""},
        "pr_title": {"type": "str", "default": ""},
        "pr_body": {"type": "str", "default": ""},
        "commit_message": {"type": "str", "default": ""},
        "no_pr": {"type": "bool", "default": False},
        "no_github_api": {"type": "bool", "default": False},
        "dry_run": {"type": "bool", "default": False},
    },
    "idempotent": False,
}


def run(args, ctx):
    dataset_id = str(args.get("dataset_id") or "").strip()
    repo_key = str(args.get("repo_key") or "").strip()
    source_file = str(args.get("source_file") or "").strip()
    if not dataset_id:
        raise ValueError("dataset_id is required")
    if not repo_key:
        raise ValueError("repo_key is required")
    if not source_file:
        raise ValueError("source_file is required")

    ctx.log(
        f"[dataset_dictionary_pr] dataset_id={dataset_id} repo_key={repo_key} "
        f"source_file={source_file} dry_run={bool(args.get('dry_run', False))}"
    )
    try:
        receipt = create_dictionary_pr(
            dataset_id=dataset_id,
            repo_key=repo_key,
            source_file=source_file,
            file_path=str(args.get("file_path") or "").strip() or None,
            branch_name=str(args.get("branch_name") or "").strip() or None,
            base_branch=str(args.get("base_branch") or "").strip() or None,
            pr_title=str(args.get("pr_title") or "").strip() or None,
            pr_body=str(args.get("pr_body") or "").strip() or None,
            commit_message=str(args.get("commit_message") or "").strip() or None,
            create_pr=not bool(args.get("no_pr", False)),
            use_github_api=not bool(args.get("no_github_api", False)),
            dry_run=bool(args.get("dry_run", False)),
        )
    except DictionaryPRError as exc:
        for line in list(getattr(exc, "details", {}).get("operation_log") or []):
            ctx.error(f"[dataset_dictionary_pr] {line}")
        raise

    for line in list(receipt.get("operation_log") or []):
        ctx.log(f"[dataset_dictionary_pr] {line}")
    return receipt

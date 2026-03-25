# research-etl
# Copyright (c) 2026 Joseph Weaver
# This file is part of the research-etl project and is licensed under the MIT License.
# You may not use this file except in compliance with the License.
# See https://github.com/josephweaver/research-etl for details.

from .config import (
    SourceControlConfigError,
    load_source_control_config,
    resolve_repo_config,
    resolve_source_control_config_path,
)
from .base import SourceControlError, SourceExecutionSpec, SourceProvider
from .github import GitHubPullRequestError, create_pull_request, infer_github_repo
from .git_provider import GitSourceProvider, make_git_source_provider
from .runtime import (
    CheckoutSpec,
    build_source_commandline_vars,
    checkin_files,
    checkin_single_file,
    checkout,
    merge_source_commandline_vars,
    prepare_local_checkout,
    resolve_source_override,
)

__all__ = [
    "SourceControlConfigError",
    "SourceControlError",
    "SourceExecutionSpec",
    "SourceProvider",
    "GitSourceProvider",
    "GitHubPullRequestError",
    "make_git_source_provider",
    "CheckoutSpec",
    "build_source_commandline_vars",
    "checkin_files",
    "checkin_single_file",
    "checkout",
    "create_pull_request",
    "infer_github_repo",
    "load_source_control_config",
    "merge_source_commandline_vars",
    "prepare_local_checkout",
    "resolve_repo_config",
    "resolve_source_control_config_path",
    "resolve_source_override",
]

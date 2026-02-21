# research-etl
# Copyright (c) 2026 Joseph Weaver
# This file is part of the research-etl project and is licensed under the MIT License.
# You may not use this file except in compliance with the License.
# See https://github.com/josephweaver/research-etl for details.

"""Executor package namespace."""

from .base import Executor, RunState, RunStatus, SubmissionResult
from .hpcc_direct import HpccDirectExecutor
from .local import LocalExecutor
from .slurm import SlurmExecutor

__all__ = [
    "Executor",
    "RunState",
    "RunStatus",
    "SubmissionResult",
    "LocalExecutor",
    "SlurmExecutor",
    "HpccDirectExecutor",
]

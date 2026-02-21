# research-etl
# Copyright (c) 2026 Joseph Weaver
# This file is part of the research-etl project and is licensed under the MIT License.
# You may not use this file except in compliance with the License.
# See https://github.com/josephweaver/research-etl for details.

from __future__ import annotations


meta = {
    "name": "terminate",
    "version": "0.1.0",
    "description": "Terminate the current pipeline run intentionally.",
    "inputs": [],
    "outputs": [],
    "params": {
        "reason": {"type": "str", "default": "pipeline terminated by terminate plugin"},
    },
    "idempotent": True,
}


def run(args, ctx):
    reason = str(args.get("reason") or "pipeline terminated by terminate plugin").strip()
    message = reason or "pipeline terminated by terminate plugin"
    ctx.log(f"[terminate] {message}", "WARN")
    raise RuntimeError(message)

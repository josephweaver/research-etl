# research-etl
# Copyright (c) 2026 Joseph Weaver
# This file is part of the research-etl project and is licensed under the MIT License.
# You may not use this file except in compliance with the License.
# See https://github.com/josephweaver/research-etl for details.

meta = {
    "name": "echo",
    "version": "0.1.0",
    "description": "Echo a message and return uppercase variant",
    "inputs": [],
    "outputs": ["text", "upper"],
    "params": {
        "message": {"type": "str", "default": "hello"},
        "verbose": {"type": "bool", "default": False},
    },
    "idempotent": True,
}


def run(args, ctx):
    msg = args.get("message", "hello")
    verbose = bool(args.get("verbose", False))
    ctx.log(f"[echo] message={msg}")
    if verbose:
        ctx.log(f"[echo] length={len(str(msg))}")
    return {"text": msg, "upper": str(msg).upper()}


def validate(args, outputs, ctx):
    if not outputs.get("text"):
        raise ValueError("echo produced empty text")

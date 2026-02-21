# research-etl
# Copyright (c) 2026 Joseph Weaver
# This file is part of the research-etl project and is licensed under the MIT License.
# You may not use this file except in compliance with the License.
# See https://github.com/josephweaver/research-etl for details.

meta = {
    "name": "fail_always",
    "version": "0.1.0",
    "description": "Intentionally fail for testing error paths",
    "params": {
        "message": {"type": "str", "default": "intentional failure from fail_always"},
        "verbose": {"type": "bool", "default": False},
    },
}


def run(args, ctx):
    message = args.get("message", "intentional failure from fail_always")
    ctx.log(f"[fail_always] about to fail message={message}", "ERROR")
    raise RuntimeError(str(message))

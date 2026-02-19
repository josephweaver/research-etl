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

meta = {
    "name": "fail_always",
    "version": "0.1.0",
    "description": "Intentionally fail for testing error paths",
}


def run(args, ctx):
    message = args.get("message", "intentional failure from fail_always")
    raise RuntimeError(str(message))

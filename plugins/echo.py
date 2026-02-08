meta = {
    "name": "echo",
    "version": "0.1.0",
    "description": "Echo a message and return uppercase variant",
    "inputs": [],
    "outputs": ["text", "upper"],
    "params": {"message": {"type": "str", "default": "hello"}},
    "idempotent": True,
}


def run(args, ctx):
    msg = args.get("message", "hello")
    ctx.log(f"[echo] message={msg}")
    return {"text": msg, "upper": str(msg).upper()}


def validate(args, outputs, ctx):
    if not outputs.get("text"):
        raise ValueError("echo produced empty text")

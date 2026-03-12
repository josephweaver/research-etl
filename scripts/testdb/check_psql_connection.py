#!/usr/bin/env python3
from __future__ import annotations

import os
import subprocess
import sys
from urllib.parse import ParseResult, urlparse, urlunparse


def _loopback_url(raw: str, host: str, port: int) -> str:
    parsed = urlparse(raw)
    if not parsed.scheme:
        raise ValueError("ETL_DATABASE_URL is not a valid URL")
    netloc = parsed.netloc
    if "@" in netloc:
        creds, _ = netloc.rsplit("@", 1)
        userinfo = f"{creds}@"
    else:
        userinfo = ""
    new_netloc = f"{userinfo}{host}:{int(port)}"
    rebuilt = ParseResult(
        scheme=parsed.scheme,
        netloc=new_netloc,
        path=parsed.path,
        params=parsed.params,
        query=parsed.query,
        fragment=parsed.fragment,
    )
    return urlunparse(rebuilt)


def main() -> int:
    db_url = str(os.environ.get("ETL_DATABASE_URL") or "").strip()
    if not db_url:
        print("ETL_DATABASE_URL is not set", file=sys.stderr)
        return 2

    host = str(os.environ.get("ETL_DB_TUNNEL_HOST") or "127.0.0.1").strip() or "127.0.0.1"
    port = int(str(os.environ.get("ETL_DB_TUNNEL_PORT") or "6543").strip() or "6543")
    local_url = _loopback_url(db_url, host=host, port=port)

    env = dict(os.environ)
    env.setdefault("PGCONNECT_TIMEOUT", "10")

    cmd = [
        "psql",
        local_url,
        "-v",
        "ON_ERROR_STOP=1",
        "-c",
        "select now() as ts, current_database() as db, current_user as usr;",
    ]
    print("running:", " ".join(cmd[:-1] + ["<sql omitted>"]))
    proc = subprocess.run(cmd, text=True, capture_output=True, env=env, check=False)
    if proc.stdout:
        print(proc.stdout.strip())
    if proc.stderr:
        print(proc.stderr.strip(), file=sys.stderr)
    return int(proc.returncode)


if __name__ == "__main__":
    raise SystemExit(main())

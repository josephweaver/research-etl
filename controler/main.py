from __future__ import annotations

import argparse
import json
import sys

from .app import ControllerApp


def _build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(description="Lightweight checkpoint-driven SLURM controller.")
    sub = p.add_subparsers(dest="cmd", required=True)

    p_status = sub.add_parser("status", help="Print county controller status rows as JSON.")
    p_status.add_argument("--config", required=True)
    p_status.add_argument("--fips-glob", default=None)

    p_run = sub.add_parser("run-once", help="Submit one new SLURM wave for eligible counties.")
    p_run.add_argument("--config", required=True)
    p_run.add_argument("--fips-glob", default=None)

    p_single = sub.add_parser("run-one", help="Submit exactly one county by FIPS without checkpoint discovery.")
    p_single.add_argument("--config", required=True)
    p_single.add_argument("--fips", required=True)

    p_bootstrap = sub.add_parser("bootstrap", help="Prepare remote repo checkouts and the Python venv once.")
    p_bootstrap.add_argument("--config", required=True)

    p_doctor = sub.add_parser("doctor", help="Validate checkpoint/log assumptions without submitting work.")
    p_doctor.add_argument("--config", required=True)
    p_doctor.add_argument("--fips", default=None)
    p_doctor.add_argument("--fips-glob", default=None)

    p_preview = sub.add_parser("preview", help="Show the resolved worker command for one county.")
    p_preview.add_argument("--config", required=True)
    p_preview.add_argument("--fips", required=True)

    p_item = sub.add_parser("run-item", help="Execute one manifest row worker command.")
    p_item.add_argument("--config", required=True)
    p_item.add_argument("--manifest", required=True)
    p_item.add_argument("--index", required=True, type=int)

    return p


def main(argv: list[str] | None = None) -> int:
    args = _build_parser().parse_args(argv)
    app = ControllerApp(args.config)
    if args.cmd == "status":
        print(json.dumps(app.status_rows(args.fips_glob), indent=2))
        return 0
    if args.cmd == "run-once":
        print(json.dumps(app.run_once(args.fips_glob), indent=2))
        return 0
    if args.cmd == "run-one":
        print(json.dumps(app.run_one(args.fips), indent=2))
        return 0
    if args.cmd == "bootstrap":
        print(json.dumps(app.bootstrap(), indent=2))
        return 0
    if args.cmd == "doctor":
        print(json.dumps(app.doctor(args.fips, args.fips_glob), indent=2))
        return 0
    if args.cmd == "preview":
        print(json.dumps(app.preview(args.fips), indent=2))
        return 0
    if args.cmd == "run-item":
        result = app.run_item(args.manifest, args.index)
        print(json.dumps(result, indent=2))
        return int(result["returncode"])
    raise RuntimeError(f"unknown command: {args.cmd}")


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))

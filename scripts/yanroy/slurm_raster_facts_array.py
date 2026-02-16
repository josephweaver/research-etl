from __future__ import annotations

import argparse
import ast
import glob
import os
import re
import shlex
import shutil
import subprocess
import sys
import time
import uuid
from pathlib import Path
from typing import List

from etl.plugins.base import PluginContext, load_plugin


def _norm(path_text: str) -> str:
    return str(path_text or "").replace("\\", "/")


def _sanitize_name(path_text: str) -> str:
    base = Path(path_text).name or "item"
    clean = re.sub(r"[^A-Za-z0-9._-]+", "_", base).strip("._")
    return clean or "item"


def _discover_items(pattern: str, *, kind: str) -> List[str]:
    found = sorted(set(_norm(p) for p in glob.glob(pattern, recursive=True)))
    out: List[str] = []
    for p in found:
        path = Path(p)
        if kind == "dirs" and not path.is_dir():
            continue
        if kind == "files" and not path.is_file():
            continue
        out.append(path.resolve().as_posix())
    return out


def _parse_tiles(raw: str) -> List[str]:
    text = str(raw or "").strip()
    if not text:
        return []
    try:
        val = ast.literal_eval(text)
        if isinstance(val, list):
            out = []
            for v in val:
                t = str(v or "").strip()
                if t:
                    out.append(t)
            return out
    except Exception:
        pass
    return [tok.strip() for tok in text.split(",") if tok.strip()]


def _discover_items_from_tiles(input_root: str, tiles: List[str], *, kind: str) -> List[str]:
    root = Path(str(input_root or "")).expanduser().resolve()
    out: List[str] = []
    seen: set[str] = set()
    for tile in tiles:
        pat = str((root / "**" / tile).as_posix())
        for p in sorted(set(_norm(x) for x in glob.glob(pat, recursive=True))):
            path = Path(p)
            if kind == "dirs" and not path.is_dir():
                continue
            if kind == "files" and not path.is_file():
                continue
            resolved = path.resolve().as_posix()
            if resolved in seen:
                continue
            seen.add(resolved)
            out.append(resolved)
    return out


def _append_child_job(job_id: str) -> None:
    child_file = str(os.environ.get("ETL_CHILD_JOBS_FILE") or "").strip()
    if not child_file:
        return
    path = Path(child_file)
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("a", encoding="utf-8") as f:
        f.write(f"{job_id}\n")


def _wait_for_job(job_id: str, poll_seconds: int) -> int:
    poll = max(2, int(poll_seconds or 10))
    while True:
        q = subprocess.run(["squeue", "-h", "-j", str(job_id)], capture_output=True, text=True, check=False)
        if q.returncode != 0:
            break
        if not str(q.stdout or "").strip():
            break
        time.sleep(poll)
    s = subprocess.run(
        ["sacct", "-n", "-P", "-j", str(job_id), "--format=State"],
        capture_output=True,
        text=True,
        check=False,
    )
    if s.returncode != 0:
        return 0
    lines = [ln.strip() for ln in str(s.stdout or "").splitlines() if ln.strip()]
    if not lines:
        return 0
    for state in lines:
        token = state.split("|", 1)[0].strip().upper()
        if token.startswith("COMPLETED"):
            continue
        return 1
    return 0


def _run_one_item(*, plugin_path: Path, item: str, output_root: str, index: int, run_id: str) -> None:
    out_dir = Path(output_root) / f"{_sanitize_name(item)}_{index}"
    out_dir.mkdir(parents=True, exist_ok=True)
    plugin = load_plugin(plugin_path)
    ctx = PluginContext(
        run_id=run_id,
        workdir=out_dir,
        log=lambda message, level="INFO": print(f"[slurm_raster_facts_array] [{level}] {message}"),
    )
    args = {"input_dir": item, "output_dir": out_dir.as_posix(), "probe_unknown": True}
    plugin.run(args, ctx)


def _run_worker(args: argparse.Namespace) -> int:
    item_file = Path(args.item_file).resolve()
    if not item_file.exists():
        print(f"item file not found: {item_file}")
        return 1
    items = [ln.strip() for ln in item_file.read_text(encoding="utf-8").splitlines() if ln.strip()]
    idx = int(args.index)
    if idx < 0 or idx >= len(items):
        print(f"worker index out of range: {idx} / {len(items)}")
        return 1
    checkout_root = Path(args.checkout_root).resolve()
    plugin_path = (checkout_root / args.plugins_dir / "raster_facts.py").resolve()
    if not plugin_path.exists():
        print(f"raster_facts plugin not found: {plugin_path}")
        return 1
    run_id = str(args.run_id or "").strip() or uuid.uuid4().hex
    _run_one_item(plugin_path=plugin_path, item=items[idx], output_root=args.output_root, index=idx, run_id=run_id)
    return 0


def _run_submit(args: argparse.Namespace) -> int:
    tiles = _parse_tiles(str(args.tiles or ""))
    if tiles:
        if not str(args.input_root or "").strip():
            print("--input-root is required when --tiles is provided")
            return 1
        items = _discover_items_from_tiles(str(args.input_root), tiles, kind=args.item_kind)
    else:
        items = _discover_items(args.input_glob, kind=args.item_kind)
    if not items:
        if tiles:
            print(f"no items found for tiles={tiles!r} input_root={args.input_root!r} kind={args.item_kind}")
        else:
            print(f"no items found for glob={args.input_glob!r} kind={args.item_kind}")
        return 1

    workdir = Path(args.workdir).resolve()
    workdir.mkdir(parents=True, exist_ok=True)
    item_file = (workdir / "raster_facts_items.txt").resolve()
    item_file.write_text("\n".join(items) + "\n", encoding="utf-8")
    logdir = (workdir / "slurm_child_logs").resolve()
    logdir.mkdir(parents=True, exist_ok=True)

    if not shutil.which("sbatch"):
        print("sbatch not found; running locally")
        checkout_root = Path(args.checkout_root).resolve()
        plugin_path = (checkout_root / args.plugins_dir / "raster_facts.py").resolve()
        run_id = str(args.run_id or "").strip() or uuid.uuid4().hex
        for i, item in enumerate(items):
            _run_one_item(plugin_path=plugin_path, item=item, output_root=args.output_root, index=i, run_id=run_id)
        return 0

    script_path = (workdir / "raster_facts_child_array.sbatch").resolve()
    python_bin = str(args.python_bin or sys.executable).strip() or sys.executable
    script_lines = [
        "#!/bin/bash --login",
        f"#SBATCH -J {args.job_name}",
        f"#SBATCH -o {logdir.as_posix()}/raster_facts-%A_%a.out",
        f"#SBATCH --array=0-{len(items)-1}",
        "set -euo pipefail",
        f"{shlex.quote(python_bin)} {shlex.quote(Path(__file__).resolve().as_posix())} "
        f"--worker-mode --item-file {shlex.quote(item_file.as_posix())} "
        f"--index $SLURM_ARRAY_TASK_ID "
        f"--output-root {shlex.quote(str(args.output_root))} "
        f"--checkout-root {shlex.quote(str(args.checkout_root))} "
        f"--plugins-dir {shlex.quote(str(args.plugins_dir))} "
        f"--run-id {shlex.quote(str(args.run_id or ''))}",
    ]
    script_path.write_text("\n".join(script_lines) + "\n", encoding="utf-8")

    cmd = ["sbatch", str(script_path)]
    proc = subprocess.run(cmd, capture_output=True, text=True, check=False)
    if proc.returncode != 0:
        detail = (proc.stderr or proc.stdout or "").strip()
        print(f"sbatch failed: {detail}")
        return 1
    out = str(proc.stdout or "").strip().split()
    job_id = out[-1] if out else ""
    if not job_id:
        print(f"could not parse child job id from sbatch output: {proc.stdout}")
        return 1
    print(f"submitted child array job: {job_id}")
    _append_child_job(job_id)
    if not bool(args.wait):
        return 0
    return _wait_for_job(job_id, int(args.poll_seconds))


def build_parser() -> argparse.ArgumentParser:
    ap = argparse.ArgumentParser(description="Submit raster_facts child SLURM array and optionally wait.")
    ap.add_argument("--worker-mode", action="store_true", help="Internal worker mode for array tasks")
    ap.add_argument("--input-glob", default="", help="Glob pattern for raster_facts input dirs/files")
    ap.add_argument("--input-root", default="", help="Root directory to search when using --tiles")
    ap.add_argument("--tiles", default="", help="Tile list (Python list literal or comma-separated)")
    ap.add_argument("--item-kind", default="dirs", choices=["any", "files", "dirs"], help="Type filter")
    ap.add_argument("--output-root", required=True, help="Output root dir for per-item raster_facts outputs")
    ap.add_argument("--workdir", required=True, help="Working dir for manifests and sbatch script")
    ap.add_argument("--checkout-root", default=os.environ.get("ETL_REPO_ROOT", "."), help="Repo checkout root")
    ap.add_argument("--plugins-dir", default="plugins", help="Plugins dir relative to checkout root")
    ap.add_argument("--python-bin", default=sys.executable, help="Python interpreter for worker tasks")
    ap.add_argument("--job-name", default="raster-facts", help="SLURM child array job name")
    ap.add_argument("--wait", action="store_true", default=True, help="Wait for child array completion")
    ap.add_argument("--poll-seconds", type=int, default=10, help="Polling seconds while waiting")
    ap.add_argument("--run-id", default="", help="Parent run id")
    ap.add_argument("--item-file", default="", help="Internal: item file for worker mode")
    ap.add_argument("--index", default="0", help="Internal: item index for worker mode")
    return ap


def main(argv: list[str] | None = None) -> int:
    ap = build_parser()
    args, unknown = ap.parse_known_args(argv)
    if unknown:
        print(f"[slurm_raster_facts_array][WARN] ignoring unknown args: {' '.join(unknown)}")
    if args.worker_mode:
        return _run_worker(args)
    if not str(args.tiles or "").strip() and not str(args.input_glob or "").strip():
        print("submit mode requires either --input-glob or --tiles")
        return 1
    return _run_submit(args)


if __name__ == "__main__":
    raise SystemExit(main())

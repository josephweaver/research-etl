from __future__ import annotations

from pathlib import Path
import shlex


def write_controller_sbatch(destination: Path, script_text: str) -> Path:
    destination.parent.mkdir(parents=True, exist_ok=True)
    destination.write_text(str(script_text or "").replace("\r\n", "\n"), encoding="utf-8", newline="\n")
    return destination


def render_controller_script(
    *,
    executor,
    run_id: str,
    workdir: str,
    logdir: str,
    child_jobs_file: str,
    wait_children: bool,
    poll_seconds: int,
) -> str:
    lines = ["#!/bin/bash --login"]
    if executor.env.partition:
        lines.append(f"#SBATCH -p {executor.env.partition}")
    if executor.env.account:
        lines.append(f"#SBATCH -A {executor.env.account}")
    lines.append("#SBATCH -t 00:20:00")
    lines.append("#SBATCH -c 1")
    lines.append("#SBATCH --mem=512M")
    lines.append(f"#SBATCH -J etl-ctl-{run_id[:8]}")
    lines.append(f"#SBATCH -o {logdir}/etl-controller-{run_id}-%j.out")
    if executor.env.sbatch_extra:
        for extra in executor.env.sbatch_extra:
            lines.append(f"#SBATCH {extra}")
    lines.append("set -euo pipefail")
    lines.append(f"mkdir -p {logdir}")
    lines.append(f"mkdir -p {workdir}")
    lines.append(f"CHILD_FILE={shlex.quote(child_jobs_file)}")
    lines.append(f"POLL={int(poll_seconds)}")
    lines.append(f"WAIT_CHILDREN={'1' if wait_children else '0'}")
    lines.append("echo '[controller] started'")
    lines.append("if [ \"$WAIT_CHILDREN\" != \"1\" ]; then")
    lines.append("  echo '[controller] wait_children disabled; exiting'")
    lines.append("  exit 0")
    lines.append("fi")
    lines.append("if [ ! -f \"$CHILD_FILE\" ]; then")
    lines.append("  echo '[controller] no child job file found; exiting'")
    lines.append("  exit 0")
    lines.append("fi")
    lines.append("mapfile -t RAW_IDS < <(grep -Eo '[0-9]+(_[0-9]+)?' \"$CHILD_FILE\" | awk '!seen[$0]++')")
    lines.append("if [ ${#RAW_IDS[@]} -eq 0 ]; then")
    lines.append("  echo '[controller] no child ids registered; exiting'")
    lines.append("  exit 0")
    lines.append("fi")
    lines.append("echo \"[controller] waiting on ${#RAW_IDS[@]} child job ids\"")
    lines.append("for jid in \"${RAW_IDS[@]}\"; do")
    lines.append("  while squeue -h -j \"$jid\" >/dev/null 2>&1 && [ -n \"$(squeue -h -j \"$jid\")\" ]; do")
    lines.append("    sleep \"$POLL\"")
    lines.append("  done")
    lines.append("done")
    lines.append("FAILED=0")
    lines.append("for jid in \"${RAW_IDS[@]}\"; do")
    lines.append("  if command -v sacct >/dev/null 2>&1; then")
    lines.append("    state=$(sacct -n -P -j \"$jid\" --format=State 2>/dev/null | head -n 1 | cut -d'|' -f1 | tr -d '[:space:]')")
    lines.append("    if [ -n \"$state\" ] && [[ \"$state\" != COMPLETED* ]]; then")
    lines.append("      echo \"[controller] child job $jid state=$state\"")
    lines.append("      FAILED=1")
    lines.append("    fi")
    lines.append("  fi")
    lines.append("done")
    lines.append("if [ \"$FAILED\" = \"1\" ]; then")
    lines.append("  echo '[controller] child job failures detected'")
    lines.append("  exit 1")
    lines.append("fi")
    lines.append("echo '[controller] all child jobs complete'")
    lines.append("exit 0")
    return "\n".join(lines)

from __future__ import annotations

import argparse
import json
import shlex
import shutil
import subprocess
from pathlib import Path


def build_vector_tiles_with_tippecanoe(
    input_ndjson: Path,
    output_mbtiles: Path,
    layer_name: str,
    minimum_zoom: int = 8,
    maximum_zoom: int = 14,
    tippecanoe_bin: str = "tippecanoe",
    detect_shared_borders: bool = True,
    coalesce_densest_as_needed: bool = True,
    drop_densest_as_needed: bool = True,
    read_parallel: bool = True,
    verbose: bool = False,
) -> dict:
    if not input_ndjson.exists():
        raise FileNotFoundError(f"input ndjson not found: {input_ndjson}")
    if not layer_name.strip():
        raise ValueError("layer_name is required")

    exe = shutil.which(tippecanoe_bin)
    if not exe:
        raise FileNotFoundError(
            f"tippecanoe binary not found: {tippecanoe_bin}. Install tippecanoe and ensure it is on PATH."
        )

    output_mbtiles.parent.mkdir(parents=True, exist_ok=True)

    cmd = [
        exe,
        "-o",
        output_mbtiles.resolve().as_posix(),
        "-l",
        layer_name,
        "-Z",
        str(int(minimum_zoom)),
        "-z",
        str(int(maximum_zoom)),
        "--force",
        "--no-feature-limit",
        "--no-tile-size-limit",
    ]
    if read_parallel:
        cmd.append("--read-parallel")
    if detect_shared_borders:
        cmd.append("--detect-shared-borders")
    if coalesce_densest_as_needed:
        cmd.append("--coalesce-densest-as-needed")
    if drop_densest_as_needed:
        cmd.append("--drop-densest-as-needed")
    cmd.append(input_ndjson.resolve().as_posix())

    proc = subprocess.run(cmd, capture_output=True, text=True, check=False)
    if proc.returncode != 0:
        raise RuntimeError(
            "tippecanoe failed "
            f"rc={proc.returncode} cmd={shlex.join(cmd)} "
            f"stderr={(proc.stderr or '').strip()[:2000]}"
        )

    result = {
        "tippecanoe_bin": exe,
        "command": shlex.join(cmd),
        "layer_name": layer_name,
        "minimum_zoom": int(minimum_zoom),
        "maximum_zoom": int(maximum_zoom),
        "input_ndjson": input_ndjson.resolve().as_posix(),
        "output_mbtiles": output_mbtiles.resolve().as_posix(),
        "stdout": str(proc.stdout or ""),
        "stderr": str(proc.stderr or ""),
        "size_bytes": int(output_mbtiles.stat().st_size) if output_mbtiles.exists() else 0,
    }
    if verbose:
        print(
            f"[build_vector_tiles_with_tippecanoe] mbtiles={result['output_mbtiles']} "
            f"size_bytes={result['size_bytes']}"
        )
    return result


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(description="Build MBTiles vector tiles from NDJSON features using tippecanoe.")
    ap.add_argument("--input-ndjson", required=True)
    ap.add_argument("--output-mbtiles", required=True)
    ap.add_argument("--layer-name", default="yanroy_fields")
    ap.add_argument("--minimum-zoom", type=int, default=8)
    ap.add_argument("--maximum-zoom", type=int, default=14)
    ap.add_argument("--tippecanoe-bin", default="tippecanoe")
    ap.add_argument("--summary-json", default="")
    ap.add_argument("--verbose", action="store_true")
    args, unknown = ap.parse_known_args(argv)
    if unknown:
        print(f"[build_vector_tiles_with_tippecanoe][WARN] ignoring unknown args: {' '.join(unknown)}")

    result = build_vector_tiles_with_tippecanoe(
        input_ndjson=Path(args.input_ndjson).expanduser().resolve(),
        output_mbtiles=Path(args.output_mbtiles).expanduser().resolve(),
        layer_name=str(args.layer_name),
        minimum_zoom=int(args.minimum_zoom),
        maximum_zoom=int(args.maximum_zoom),
        tippecanoe_bin=str(args.tippecanoe_bin),
        verbose=bool(args.verbose),
    )

    summary_path = Path(str(args.summary_json or "").strip()).expanduser().resolve() if str(args.summary_json or "").strip() else None
    if summary_path is not None:
        summary_path.parent.mkdir(parents=True, exist_ok=True)
        summary_path.write_text(json.dumps(result, indent=2), encoding="utf-8")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())

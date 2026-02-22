# research-etl
# Copyright (c) 2026 Joseph Weaver
# This file is part of the research-etl project and is licensed under the MIT License.
# You may not use this file except in compliance with the License.
# See https://github.com/josephweaver/research-etl for details.

from __future__ import annotations

from pathlib import Path

try:
    import rasterio  # type: ignore
    from rasterio.enums import Resampling  # type: ignore
    from rasterio.warp import calculate_default_transform, reproject  # type: ignore
except Exception:  # noqa: BLE001
    rasterio = None
    Resampling = None
    calculate_default_transform = None
    reproject = None


meta = {
    "name": "geo_raster_reproject",
    "version": "0.1.0",
    "description": "Reproject a raster dataset to a target CRS.",
    "inputs": [],
    "outputs": [
        "input_path",
        "output_path",
        "source_crs",
        "target_crs",
        "width",
        "height",
        "band_count",
    ],
    "params": {
        "input_path": {"type": "str", "default": ""},
        "output_path": {"type": "str", "default": ""},
        "target_crs": {"type": "str", "default": ""},
        "source_crs": {"type": "str", "default": ""},
        "resampling": {"type": "str", "default": "nearest"},
        "resolution": {"type": "str", "default": ""},
        "src_nodata": {"type": "str", "default": ""},
        "dst_nodata": {"type": "str", "default": ""},
        "compress": {"type": "str", "default": "deflate"},
        "overwrite": {"type": "bool", "default": True},
        "verbose": {"type": "bool", "default": False},
    },
    "idempotent": True,
}


def _resolve_path(path_text: str, ctx) -> Path:
    p = Path(str(path_text or "")).expanduser()
    if p.is_absolute():
        return p
    repo_rel = (Path(".").resolve() / p).resolve()
    if repo_rel.exists():
        return repo_rel
    text = str(path_text or "").replace("\\", "/")
    if text.startswith(".") or "/" in text:
        return repo_rel
    return (ctx.workdir / p).resolve()


def _parse_float_or_none(text: str):
    raw = str(text or "").strip()
    if not raw:
        return None
    return float(raw)


def _parse_resolution(raw: str):
    text = str(raw or "").strip()
    if not text:
        return None
    parts = [p.strip() for p in text.replace(";", ",").split(",") if p.strip()]
    if len(parts) == 1:
        value = float(parts[0])
        return (value, value)
    if len(parts) == 2:
        return (float(parts[0]), float(parts[1]))
    raise ValueError("resolution must be '<xres>' or '<xres>,<yres>'")


def _normalize_resampling(name: str):
    if Resampling is None:
        raise RuntimeError("rasterio is not available")
    token = str(name or "nearest").strip().lower().replace("-", "_")
    if not hasattr(Resampling, token):
        valid = ", ".join(sorted(k for k in dir(Resampling) if not k.startswith("_")))
        raise ValueError(f"invalid resampling '{name}'. valid: {valid}")
    return getattr(Resampling, token)


def run(args, ctx):
    if rasterio is None or Resampling is None or calculate_default_transform is None or reproject is None:
        raise RuntimeError(
            "geo_raster_reproject requires rasterio. Install requirements.txt in the active environment."
        )

    input_text = str(args.get("input_path") or "").strip()
    output_text = str(args.get("output_path") or "").strip()
    target_crs = str(args.get("target_crs") or "").strip()
    source_crs_arg = str(args.get("source_crs") or "").strip()
    if not input_text:
        raise ValueError("input_path is required")
    if not output_text:
        raise ValueError("output_path is required")
    if not target_crs:
        raise ValueError("target_crs is required")

    input_path = _resolve_path(input_text, ctx)
    output_path = _resolve_path(output_text, ctx)
    if not input_path.exists():
        raise FileNotFoundError(f"input_path not found: {input_path}")
    output_path.parent.mkdir(parents=True, exist_ok=True)
    overwrite = bool(args.get("overwrite", True))
    if output_path.exists() and not overwrite:
        raise FileExistsError(f"output_path exists and overwrite=false: {output_path}")

    resampling = _normalize_resampling(str(args.get("resampling") or "nearest"))
    resolution = _parse_resolution(str(args.get("resolution") or ""))
    src_nodata = _parse_float_or_none(str(args.get("src_nodata") or ""))
    dst_nodata = _parse_float_or_none(str(args.get("dst_nodata") or ""))
    compress = str(args.get("compress") or "deflate").strip()
    verbose = bool(args.get("verbose", False))

    with rasterio.open(input_path) as src:
        src_crs = src.crs or None
        if src_crs is None:
            if not source_crs_arg:
                raise ValueError("input raster has no CRS; provide source_crs")
            src_crs = source_crs_arg
        src_crs_text = str(src_crs.to_string() if hasattr(src_crs, "to_string") else src_crs)

        transform_kwargs = {}
        if resolution is not None:
            transform_kwargs["resolution"] = resolution
        dst_transform, dst_width, dst_height = calculate_default_transform(
            src_crs,
            target_crs,
            src.width,
            src.height,
            *src.bounds,
            **transform_kwargs,
        )

        profile = src.profile.copy()
        profile.update(
            crs=target_crs,
            transform=dst_transform,
            width=int(dst_width),
            height=int(dst_height),
            compress=compress or profile.get("compress"),
        )
        if dst_nodata is not None:
            profile.update(nodata=dst_nodata)
        if overwrite and output_path.exists():
            output_path.unlink()

        with rasterio.open(output_path, "w", **profile) as dst:
            for band_idx in range(1, src.count + 1):
                reproject(
                    source=rasterio.band(src, band_idx),
                    destination=rasterio.band(dst, band_idx),
                    src_transform=src.transform,
                    src_crs=src_crs,
                    src_nodata=src_nodata if src_nodata is not None else src.nodata,
                    dst_transform=dst_transform,
                    dst_crs=target_crs,
                    dst_nodata=dst_nodata if dst_nodata is not None else profile.get("nodata"),
                    resampling=resampling,
                )
            dst_crs_text = str(dst.crs.to_string() if hasattr(dst.crs, "to_string") else dst.crs)

    ctx.log(
        f"[geo_raster_reproject] input={input_path.as_posix()} output={output_path.as_posix()} "
        f"source_crs={src_crs_text} target_crs={target_crs} width={dst_width} height={dst_height} bands={src.count}"
    )
    if verbose:
        ctx.log(f"[geo_raster_reproject] resolution={resolution} resampling={str(args.get('resampling') or 'nearest')}")

    return {
        "input_path": input_path.resolve().as_posix(),
        "output_path": output_path.resolve().as_posix(),
        "source_crs": src_crs_text,
        "target_crs": dst_crs_text,
        "width": int(dst_width),
        "height": int(dst_height),
        "band_count": int(src.count),
    }

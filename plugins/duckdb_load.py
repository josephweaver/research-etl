# research-etl
# Copyright (c) 2026 Joseph Weaver
# This file is part of the research-etl project and is licensed under the MIT License.
# You may not use this file except in compliance with the License.
# See https://github.com/josephweaver/research-etl for details.

from __future__ import annotations

import glob
from pathlib import Path
from typing import List


meta = {
    "name": "duckdb_load",
    "version": "0.1.0",
    "description": "Load CSV/Parquet files into a DuckDB table (replace/append/create_if_missing).",
    "inputs": [],
    "outputs": [
        "db_path",
        "table",
        "mode",
        "format",
        "source_files",
        "source_count",
        "table_existed",
        "rows_before",
        "rows_after",
        "rows_loaded",
    ],
    "params": {
        "db_path": {"type": "str", "default": ""},
        "table": {"type": "str", "default": ""},
        "source": {"type": "str", "default": ""},
        "source_glob": {"type": "str", "default": ""},
        "format": {"type": "str", "default": "auto"},
        "mode": {"type": "str", "default": "replace"},  # replace | append | create_if_missing
        "csv_delim": {"type": "str", "default": ","},
        "csv_header": {"type": "bool", "default": True},
        "csv_nullstr": {"type": "str", "default": ""},
        "union_by_name": {"type": "bool", "default": True},
        "analyze": {"type": "bool", "default": True},
        "threads": {"type": "int", "default": 0},
        "allow_empty": {"type": "bool", "default": False},
        "verbose": {"type": "bool", "default": False},
    },
    "idempotent": False,
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


def _quote_ident(name: str) -> str:
    text = str(name or "").strip()
    if not text:
        raise ValueError("table is required")
    return '"' + text.replace('"', '""') + '"'


def _quote_sql_literal(text: str) -> str:
    return "'" + str(text or "").replace("'", "''") + "'"


def _parse_csv_list(raw: str) -> List[str]:
    out: List[str] = []
    seen = set()
    for token in str(raw or "").replace(";", ",").split(","):
        t = str(token or "").strip()
        if not t:
            continue
        if t in seen:
            continue
        seen.add(t)
        out.append(t)
    return out


def _detect_format(fmt: str, files: List[Path]) -> str:
    v = str(fmt or "auto").strip().lower()
    if v and v != "auto":
        return v
    if not files:
        return "csv"
    suffixes = {p.suffix.lower() for p in files}
    if suffixes.issubset({".parquet"}):
        return "parquet"
    return "csv"


def _resolve_sources(source: str, source_glob: str, ctx) -> List[Path]:
    out: List[Path] = []
    seen = set()
    patterns = _parse_csv_list(source_glob)
    explicit = _parse_csv_list(source)

    for pat in patterns:
        matches = sorted(glob.glob(pat, recursive=True))
        if not matches:
            matches = sorted(glob.glob(str((ctx.workdir / pat).as_posix()), recursive=True))
        for raw in matches:
            p = Path(raw)
            if not p.is_file():
                continue
            key = p.resolve().as_posix().lower()
            if key in seen:
                continue
            seen.add(key)
            out.append(p.resolve())

    for item in explicit:
        p = _resolve_path(item, ctx)
        if p.is_file():
            key = p.resolve().as_posix().lower()
            if key not in seen:
                seen.add(key)
                out.append(p.resolve())
            continue
        matches = sorted(glob.glob(str(p), recursive=True))
        for raw in matches:
            mp = Path(raw)
            if not mp.is_file():
                continue
            key = mp.resolve().as_posix().lower()
            if key in seen:
                continue
            seen.add(key)
            out.append(mp.resolve())
    return out


def _duckdb_source_arg(files: List[Path]) -> str:
    if len(files) == 1:
        return _quote_sql_literal(files[0].as_posix())
    return "[" + ",".join(_quote_sql_literal(p.as_posix()) for p in files) + "]"


def run(args, ctx):
    try:
        import duckdb
    except Exception as exc:  # noqa: BLE001
        raise RuntimeError("duckdb_load requires the `duckdb` package") from exc

    db_path_text = str(args.get("db_path") or "").strip()
    if not db_path_text:
        raise ValueError("db_path is required")
    table = str(args.get("table") or "").strip()
    if not table:
        raise ValueError("table is required")

    mode = str(args.get("mode") or "replace").strip().lower()
    if mode not in {"replace", "append", "create_if_missing"}:
        raise ValueError("mode must be one of: replace, append, create_if_missing")

    files = _resolve_sources(str(args.get("source") or ""), str(args.get("source_glob") or ""), ctx)
    allow_empty = bool(args.get("allow_empty", False))
    if not files and not allow_empty:
        raise FileNotFoundError("no files resolved from source/source_glob")

    fmt = _detect_format(str(args.get("format") or "auto"), files)
    if fmt not in {"csv", "parquet"}:
        raise ValueError("format must be csv, parquet, or auto")

    db_path = _resolve_path(db_path_text, ctx)
    db_path.parent.mkdir(parents=True, exist_ok=True)
    table_q = _quote_ident(table)
    source_arg = _duckdb_source_arg(files) if files else "[]"

    if fmt == "parquet":
        read_expr = f"read_parquet({source_arg}, union_by_name={str(bool(args.get('union_by_name', True))).lower()})"
    else:
        delim = str(args.get("csv_delim") or ",")
        header = bool(args.get("csv_header", True))
        nullstr = str(args.get("csv_nullstr") or "")
        read_expr = (
            f"read_csv_auto({source_arg}, delim={_quote_sql_literal(delim)}, "
            f"header={str(header).upper()}, nullstr={_quote_sql_literal(nullstr)}, "
            f"union_by_name={str(bool(args.get('union_by_name', True))).lower()})"
        )

    verbose = bool(args.get("verbose", False))
    ctx.log(
        f"[duckdb_load] start db={db_path.as_posix()} table={table} mode={mode} format={fmt} sources={len(files)}"
    )
    if verbose and files:
        ctx.log(f"[duckdb_load] source_preview={[p.as_posix() for p in files[:10]]}")

    con = duckdb.connect(str(db_path))
    try:
        threads = int(args.get("threads") or 0)
        if threads > 0:
            con.execute(f"PRAGMA threads={threads}")

        table_exists = bool(
            con.execute(
                "SELECT EXISTS (SELECT 1 FROM information_schema.tables WHERE table_schema = current_schema() AND table_name = ?)",
                [table],
            ).fetchone()[0]
        )
        rows_before = int(con.execute(f"SELECT COUNT(*) FROM {table_q}").fetchone()[0]) if table_exists else 0

        if files:
            if mode == "replace":
                con.execute(f"CREATE OR REPLACE TABLE {table_q} AS SELECT * FROM {read_expr}")
            elif mode == "append":
                if not table_exists:
                    raise RuntimeError(f"append mode requires existing table: {table}")
                con.execute(f"INSERT INTO {table_q} SELECT * FROM {read_expr}")
            else:  # create_if_missing
                if table_exists:
                    con.execute(f"INSERT INTO {table_q} SELECT * FROM {read_expr}")
                else:
                    con.execute(f"CREATE TABLE {table_q} AS SELECT * FROM {read_expr}")

        table_exists_after = bool(
            con.execute(
                "SELECT EXISTS (SELECT 1 FROM information_schema.tables WHERE table_schema = current_schema() AND table_name = ?)",
                [table],
            ).fetchone()[0]
        )
        rows_after = int(con.execute(f"SELECT COUNT(*) FROM {table_q}").fetchone()[0]) if table_exists_after else 0
        rows_loaded = rows_after if mode == "replace" else max(0, rows_after - rows_before)

        if bool(args.get("analyze", True)) and table_exists_after:
            con.execute(f"ANALYZE {table_q}")

        ctx.log(
            f"[duckdb_load] complete table={table} rows_before={rows_before} rows_after={rows_after} rows_loaded={rows_loaded}"
        )
        return {
            "db_path": db_path.as_posix(),
            "table": table,
            "mode": mode,
            "format": fmt,
            "source_files": [p.as_posix() for p in files],
            "source_count": len(files),
            "table_existed": table_exists,
            "rows_before": rows_before,
            "rows_after": rows_after,
            "rows_loaded": rows_loaded,
        }
    finally:
        con.close()

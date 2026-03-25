# research-etl
# Copyright (c) 2026 Joseph Weaver
# This file is part of the research-etl project and is licensed under the MIT License.
# You may not use this file except in compliance with the License.
# See https://github.com/josephweaver/research-etl for details.

from __future__ import annotations

import json
from pathlib import Path
from typing import Any, List


meta = {
    "name": "duckdb_sql",
    "version": "0.1.0",
    "description": "Run generic SQL against DuckDB, with optional workspace-root-aware file resolution.",
    "inputs": [],
    "outputs": [
        "db_path",
        "workspace_config_path",
        "workspace_root",
        "sql_file",
        "statements_executed",
        "row_count",
        "columns",
        "rows",
    ],
    "params": {
        "db_path": {"type": "str", "default": ":memory:"},
        "workspace_config_path": {"type": "str", "default": ""},
        "sql": {"type": "str", "default": ""},
        "sql_file": {"type": "str", "default": ""},
        "init_sql": {"type": "str", "default": ""},
        "init_sql_file": {"type": "str", "default": ""},
        "extensions": {"type": "str", "default": ""},
        "read_only": {"type": "bool", "default": False},
        "threads": {"type": "int", "default": 0},
        "fetch": {"type": "bool", "default": True},
        "row_limit": {"type": "int", "default": 200},
    },
    "idempotent": False,
}


def _resolve_path(path_text: str, *, base_dir: Path, ctx) -> Path:
    text = str(path_text or "").strip()
    if not text:
        raise ValueError("path is required")
    p = Path(text).expanduser()
    if p.is_absolute():
        return p.resolve()
    workspace_rel = (base_dir / p).resolve()
    if workspace_rel.exists():
        return workspace_rel
    repo_rel = (Path(".").resolve() / p).resolve()
    if repo_rel.exists():
        return repo_rel
    return (ctx.workdir / p).resolve()


def _quote_sql_literal(text: str) -> str:
    return "'" + str(text or "").replace("'", "''") + "'"


def _parse_csv_list(raw: str) -> List[str]:
    out: List[str] = []
    seen = set()
    for token in str(raw or "").replace(";", ",").split(","):
        t = str(token or "").strip()
        if not t or t in seen:
            continue
        seen.add(t)
        out.append(t)
    return out


def _load_text_arg(inline_text: str, file_text: str, *, base_dir: Path, ctx, label: str) -> str:
    inline = str(inline_text or "").strip()
    file_arg = str(file_text or "").strip()
    if inline and file_arg:
        raise ValueError(f"{label}: provide inline text or file path, not both")
    if inline:
        return inline
    if file_arg:
        path = _resolve_path(file_arg, base_dir=base_dir, ctx=ctx)
        if not path.exists() or not path.is_file():
            raise FileNotFoundError(f"{label} file not found: {path}")
        return path.read_text(encoding="utf-8")
    return ""


def _to_jsonable_rows(rows: list[Any], *, limit: int) -> list[list[Any]]:
    out: list[list[Any]] = []
    for row in rows[: max(0, limit)]:
        converted = []
        for value in row:
            if value is None or isinstance(value, (str, int, float, bool)):
                converted.append(value)
            else:
                converted.append(str(value))
        out.append(converted)
    return out


def run(args, ctx):
    try:
        import duckdb
    except Exception as exc:  # noqa: BLE001
        raise RuntimeError("duckdb_sql requires the `duckdb` package") from exc

    workspace_cfg_text = str(args.get("workspace_config_path") or "").strip()
    workspace_cfg = Path(workspace_cfg_text).expanduser().resolve() if workspace_cfg_text else None
    workspace_root = workspace_cfg.parent if workspace_cfg is not None else Path(".").resolve()

    db_path_text = str(args.get("db_path") or ":memory:").strip() or ":memory:"
    db_path = db_path_text if db_path_text == ":memory:" else str(_resolve_path(db_path_text, base_dir=workspace_root, ctx=ctx))
    if db_path != ":memory:":
        Path(db_path).parent.mkdir(parents=True, exist_ok=True)

    init_sql = _load_text_arg(
        str(args.get("init_sql") or ""),
        str(args.get("init_sql_file") or ""),
        base_dir=workspace_root,
        ctx=ctx,
        label="init_sql",
    )
    sql_text = _load_text_arg(
        str(args.get("sql") or ""),
        str(args.get("sql_file") or ""),
        base_dir=workspace_root,
        ctx=ctx,
        label="sql",
    )
    if not sql_text:
        raise ValueError("sql or sql_file is required")

    sql_file = ""
    if str(args.get("sql_file") or "").strip():
        sql_file = str(_resolve_path(str(args.get("sql_file") or ""), base_dir=workspace_root, ctx=ctx).as_posix())

    extensions = _parse_csv_list(str(args.get("extensions") or ""))
    fetch = bool(args.get("fetch", True))
    row_limit = max(0, int(args.get("row_limit") or 200))

    ctx.log(
        f"[duckdb_sql] start db={db_path} workspace_root={workspace_root.as_posix()} sql_file={sql_file or '<inline>'}"
    )
    con = duckdb.connect(database=db_path, read_only=bool(args.get("read_only", False)))
    try:
        threads = int(args.get("threads") or 0)
        if threads > 0:
            con.execute(f"PRAGMA threads={threads}")

        # Keep extension handling simple and explicit.
        for ext in extensions:
            con.execute(f"INSTALL {ext}")
            con.execute(f"LOAD {ext}")

        if workspace_cfg is not None:
            con.execute(f"SET home_directory = {_quote_sql_literal(workspace_root.as_posix())}")

        if init_sql:
            con.execute(init_sql)

        cur = con.execute(sql_text)
        rows: list[list[Any]] = []
        columns: list[str] = []
        row_count = 0
        if fetch and cur.description:
            raw_rows = cur.fetchall()
            columns = [str(item[0]) for item in list(cur.description or [])]
            row_count = len(raw_rows)
            rows = _to_jsonable_rows(raw_rows, limit=row_limit)

        statements_executed = max(1, len([part for part in sql_text.split(";") if str(part).strip()]))
        ctx.log(
            f"[duckdb_sql] complete statements={statements_executed} row_count={row_count} fetched={bool(columns)}"
        )
        return {
            "db_path": db_path,
            "workspace_config_path": workspace_cfg.as_posix() if workspace_cfg is not None else "",
            "workspace_root": workspace_root.as_posix(),
            "sql_file": sql_file,
            "statements_executed": statements_executed,
            "row_count": row_count,
            "columns": columns,
            "rows": rows,
        }
    finally:
        con.close()

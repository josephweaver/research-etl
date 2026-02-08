# Install Guide

This guide covers a minimal local developer install for `research-etl`.

## Prerequisites

- Python 3.10 or newer
- `git`

## Windows (PowerShell)

```powershell
# From the repo root
python -m venv .venv
.venv\Scripts\Activate.ps1
python -m pip install --upgrade pip
pip install -e ".[dev]"

# Verify CLI
etl --version
etl validate pipelines/sample.yml
```

If `etl` is not recognized, either:

- run commands as `python -m cli ...`, or
- add `%APPDATA%\Python\Python310\Scripts` (or your active Python scripts path) to `PATH`.

## Linux/macOS (bash/zsh)

```bash
# From the repo root
python3 -m venv .venv
source .venv/bin/activate
python -m pip install --upgrade pip
pip install -e ".[dev]"

# Verify CLI
etl --version
etl validate pipelines/sample.yml
```

## Run Tests

```bash
python -m pytest -q
```

## Runtime-only install (no test tooling)

```bash
pip install -e .
```

## Optional: database-backed tracking

Set `ETL_DATABASE_URL` before running commands if you want Postgres persistence for runs/events.

Windows example:

```powershell
setx ETL_DATABASE_URL "postgresql://user:pass@host:5432/dbname"
```

Linux/macOS example:

```bash
export ETL_DATABASE_URL="postgresql://user:pass@host:5432/dbname"
```

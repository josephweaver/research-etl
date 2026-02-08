# Project Status (2026-02-06)

## What's implemented
- Plugin contract and loader (`etl/plugins/base.py`), namespace exports.
- Pipeline parsing/templating/validation (`etl/pipeline.py`): supports `vars`, `dirs`, `steps`, simple `{var}` interpolation, basic checks.
- Local runner (`etl/runner.py`): shlex script parsing, param coercion from `meta.params`, per-step env, per-step workdir, optional validate hook, dry-run.
- Local executor (`etl/executors/local.py`) wired to runner; records runs.
- Run tracking (`etl/tracking.py`): JSONL store at `.runs/runs.jsonl`, CLI inspection.
- CLI (`cli.py`): `plugins list`, `validate`, `run`, `runs list/show`.
- Example plugin and pipeline: `plugins/echo.py`, `pipelines/sample.yml`.
- Parallel/foreach support: `parallel_with` batches, `foreach` fan-out, `when` skips.
- Execution env config loader and sample (`config/execution.example.yml`), CLI flags `--execution-config` and `--env` forwarded to executors.

## Known gaps / TODO
- Step `when` conditions are parsed but not evaluated/enforced.
- No retry/resume/parallelism or resource scheduling; execution is synchronous local only.
- No logging levels/structured logs; logs go to stdout via provided log_fn.
- No schema validation of `params` types beyond simple coercion; no strict pipeline schema enforcement beyond basics.
- Executors: only `local` exists; SLURM/K8s/etc. not implemented.
- Validation hook return type is loose; no standardized ValidationResult.
- No packaging/setup.py/requirements; PyYAML dependency implicit.

## Next recommended steps
1) Add `local` executor options: retries, resume-from-step, and better-controlled parallelism (bounded workers).  
2) Extend tracking: include config hash, git SHA, plugin versions, timings; add `runs export` for PR comment/status.  
3) Add schema validation (pydantic/voluptuous) for pipeline files and plugin metadata.  
4) SLURM executor: add job arrays + dependency graph + sacct/squeue status.  
5) Add tests for pipeline parsing, runner, and CLI commands.  
6) Package metadata/requirements (PyYAML) and maybe `typer`/`click` if upgrading CLI.

## Quick commands
- Validate sample: `python cli.py validate pipelines/sample.yml`
- Dry-run sample: `python cli.py run pipelines/sample.yml --dry-run`
- List runs: `python cli.py runs list`
- Show run: `python cli.py runs show <run_id>`

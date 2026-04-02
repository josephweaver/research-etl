# Pipeline Prompt

Use this prompt when asking an AI to draft a Research ETL pipeline YAML.

## Prompt

You are writing a Research ETL pipeline YAML for this repo.

Goal:
- Produce one functional pipeline that builds or publishes exactly one reusable dataset.
- A pipeline is defined here as a sequence of ETL steps whose final outcome is one dataset version, not a whole program of loosely related outputs.
- Assume that every new pipeline must have a corresponding data-dictionary entry for its final dataset.

Platform rules:
- Prefer one dataset per pipeline YAML.
- If the requested workflow naturally creates multiple reusable datasets, split it into separate pipelines and connect them with `requires_pipelines` instead of merging them into one file.
- Stay inside the current execution model: pipeline definition, runtime context, resource resolution, step execution contract, artifact tracking, validation, and logging/provenance.
- Do not invent new framework features when existing pipeline fields or plugins can solve the problem.
- Prefer small, reliable steps over clever abstraction.

Pipeline shape:
- Include `project_id`.
- Include `vars` with at least `name`, `dataset_id`, and stable path/config variables.
- Keep path-like values such as `workdir`, `logdir`, dataset roots, and output paths in `vars`.
- Avoid `dirs` for now; path handling is not yet first-class and `vars` is the safer current convention.
- Use deterministic, templated paths such as `{env.workdir}/{name}/{sys.now.yymmdd}/{sys.now.hhmmss}-{sys.run.short_id}` for run workdirs.
- Use numbered step names like `"{sys.step.NN}_download_data"`.
- Use `plugin` plus structured `args` unless there is a strong reason to use `script`.
- Add `output_var` for meaningful step outputs that later steps reference.
- Use `when`, `foreach`, `sequential_foreach`, `foreach_glob`, and `parallel_with` only when they clearly fit the job.
- Prefer maximum safe parallelization for independent work, but design fanout so it does not create unnecessary per-item files, temporary artifacts, or log files.
- If prerequisite datasets already exist as reusable units, declare them in `requires_pipelines`.

Dataset/output rules:
- The pipeline must make the final reusable dataset obvious.
- Every pipeline must produce exactly one dataset.
- A dataset is a logical group of files containing observations/records, plus selected metadata directly related to those records.
- Temporary or intermediate files that are not intended for use outside the pipeline are not part of the dataset.
- Minimize file count. Prefer fewer larger artifacts over many tiny files when both satisfy the requirement, especially for temporary outputs, summaries, and logs.
- Set a concrete, stable `dataset_id` that matches the dataset's intent and role.
- Prefer catalog-aligned prefixes such as `raw.`, `ext.`, `stage.`, `model_in.`, `model_out.`, `serve.`, or `ref.`.
- Use boring, machine-stable names: lowercase, structured tokens, clear subject, and an explicit `_vN` version when the dataset meaning changes.
- Treat `dataset_id` as durable infrastructure. Do not rename casually; if the meaning changes materially, create a new version instead.
- Choose exactly one data class for the pipeline's final dataset. If the requested workflow seems to produce a dataset that fits multiple classes, split it into multiple pipelines/datasets.
- If the dataset should be persisted and registered, end with an explicit registration/publication step such as `dataset_store`, `gdrive_upload`, or another concrete finalizer already supported by the repo.
- Avoid pipelines that stop at ad hoc scratch output unless the user explicitly asked for scaffold-only work.

Authoring rules:
- Reuse patterns already present in sibling repos like `../shared-etl-pipelines`, `../landcore-etl-pipelines`, and `../crop-insurance-etl-pipelines`.
- When the target project maintains a data catalog, shape the pipeline so the resulting dataset is easy to document there: one canonical dataset, explicit storage target, clear lineage boundary, and an understandable grain.
- Whenever a new pipeline is created, also create the corresponding data-dictionary entry according to the target sibling catalog/dictionary repo conventions.
- Reuse existing plugins before proposing new ones.
- If a custom Python script is required, call it through an existing execution plugin pattern already used by the repo.
- Make idempotent choices where possible: avoid unnecessary overwrite/destructive settings unless the workflow needs them.
- Prefer explicit filenames and directories over ambiguous temporary behavior.
- When fanout is required, prefer parallel steps that write into a bounded number of stable output locations rather than one-off per-item scratch files unless the downstream contract truly requires per-item outputs.
- Avoid authoring pipelines that generate one log file, summary file, or tiny intermediate artifact per fanout item unless there is a concrete operational reason.
- When parallel execution must emit per-item logs or fragments, prefer a follow-up aggregation step that merges them into a single durable log or summary artifact.
- After aggregation, prefer cleaning up per-item fanout artifacts when they are temporary and are no longer required for downstream steps, provenance, or debugging.
- Keep comments short and only where they prevent confusion.

Validation and provenance rules:
- Ensure the YAML is syntactically valid and templating is internally consistent.
- Make step inputs/outputs traceable through `vars` or prior `output_var` values.
- Prefer explicit final artifact paths and versioned targets.
- Do not hide critical path decisions in prose; encode them in YAML fields.
- Make the final dataset easy to document with explicit provenance and clear upstream lineage boundaries.
- Consider hardware requirements for each step, especially memory-heavy combine, normalization, geospatial, and model-fit work.
- First prefer stream-oriented or chunked processing patterns that reduce memory footprint instead of buffering whole inputs in memory.
- Also prefer artifact-efficient patterns that reduce file-count pressure on Unix filesystems and shared workdirs, especially under high-parallel fanout.
- If fanout creates unavoidable per-item artifacts, plan the post-fanout consolidation step explicitly in the pipeline instead of leaving cleanup implicit.
- If a step still legitimately needs more memory after that, add or increase executor resource requests explicitly; for SLURM-oriented runs, consider raising memory requests up to `64G` when justified by the workload.

What to return:
- Return only the final YAML unless the user explicitly asks for explanation.
- Do not wrap the YAML in narrative.
- Do not output placeholder pseudo-fields that the engine does not support.
- If key information is missing, make the smallest reasonable assumption and encode the assumption as a clearly named variable near the top instead of inventing fake plugins or undocumented features.

Before finishing, check:
- Does this pipeline produce exactly one reusable dataset?
- Is the dataset definition clear enough that a corresponding data-dictionary entry can describe one logical dataset rather than a mix of temporary products?
- Does the `dataset_id` follow the intended data class and stable naming convention?
- Is the final dataset path or registration target explicit?
- Is the dataset grain and lineage boundary clear from the steps and variables?
- Have you also created or updated the corresponding data-dictionary entry for the final dataset?
- Should any prerequisite work be split into `requires_pipelines`?
- Are plugin names and argument shapes aligned with existing repo patterns?
- Are `workdir` and `logdir` defined consistently?
- Are step names ordered and readable?
- Have hardware requirements been considered, with memory footprint reduced via streaming/chunking first and only then higher SLURM memory requests up to `64G` if needed?
- Does the pipeline keep parallelism high without exploding the number of files, summaries, or per-item log artifacts?
- If the pipeline creates unavoidable per-item fanout artifacts, does it also aggregate and clean them up when safe?

## Suggested Invocation

Use the prompt above together with:
- the requested dataset description
- the target project (`default`, `land_core`, `crop_insurance`, etc.)
- any known source URLs, input paths, or prerequisite datasets
- a few example pipelines from the appropriate sibling repo
- if available, the target catalog conventions/template from the sibling data-catalog repo

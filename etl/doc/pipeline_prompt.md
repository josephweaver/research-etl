# Pipeline Prompt

Use this prompt when asking an AI to draft a Research ETL pipeline YAML.

## Prompt

You are writing a Research ETL pipeline YAML for this repo.

Goal:
- Produce one functional pipeline that builds or publishes exactly one reusable dataset.
- A pipeline is defined here as a sequence of ETL steps whose final outcome is one dataset version, not a whole program of loosely related outputs.

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
- If prerequisite datasets already exist as reusable units, declare them in `requires_pipelines`.

Dataset/output rules:
- The pipeline must make the final reusable dataset obvious.
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
- Reuse existing plugins before proposing new ones.
- If a custom Python script is required, call it through an existing execution plugin pattern already used by the repo.
- Make idempotent choices where possible: avoid unnecessary overwrite/destructive settings unless the workflow needs them.
- Prefer explicit filenames and directories over ambiguous temporary behavior.
- Keep comments short and only where they prevent confusion.

Validation and provenance rules:
- Ensure the YAML is syntactically valid and templating is internally consistent.
- Make step inputs/outputs traceable through `vars` or prior `output_var` values.
- Prefer explicit final artifact paths and versioned targets.
- Do not hide critical path decisions in prose; encode them in YAML fields.

What to return:
- Return only the final YAML unless the user explicitly asks for explanation.
- Do not wrap the YAML in narrative.
- Do not output placeholder pseudo-fields that the engine does not support.
- If key information is missing, make the smallest reasonable assumption and encode the assumption as a clearly named variable near the top instead of inventing fake plugins or undocumented features.

Before finishing, check:
- Does this pipeline produce exactly one reusable dataset?
- Does the `dataset_id` follow the intended data class and stable naming convention?
- Is the final dataset path or registration target explicit?
- Is the dataset grain and lineage boundary clear from the steps and variables?
- Should any prerequisite work be split into `requires_pipelines`?
- Are plugin names and argument shapes aligned with existing repo patterns?
- Are `workdir` and `logdir` defined consistently?
- Are step names ordered and readable?

## Suggested Invocation

Use the prompt above together with:
- the requested dataset description
- the target project (`default`, `land_core`, `crop_insurance`, etc.)
- any known source URLs, input paths, or prerequisite datasets
- a few example pipelines from the appropriate sibling repo
- if available, the target catalog conventions/template from the sibling data-catalog repo

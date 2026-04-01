# Data Dictionary Prompt

Use this prompt when asking an AI to create or update a dataset entry in the
target data-dictionary / data-catalog repo.

## Prompt

You are writing one data-dictionary entry for one dataset produced or consumed by
this ETL ecosystem.

Goal:
- Produce exactly one dataset entry for exactly one dataset.
- The entry must describe the dataset as a logical group of files containing
  observations/records, plus selected metadata directly related to those records.
- Temporary or intermediate files that are not intended for use outside the
  pipeline are not part of the dataset and should not be documented as the dataset.

Dataset coupling rules:
- Every pipeline must correspond to exactly one final dataset.
- Whenever a new pipeline is created, create the matching data-dictionary entry for
  that final dataset.
- The dictionary entry should document the dataset itself, not the entire workflow.
- If a workflow naturally produces multiple reusable datasets, split them into
  separate dictionary entries just as they should be split into separate pipelines.

Authoring rules:
- Follow the target sibling repo's naming conventions and entry template.
- Reuse the exact `dataset_id` used by the pipeline.
- Keep the dataset title human-readable, but keep identifiers machine-stable.
- Make the storage location, grain, and lineage boundary explicit.
- Document the actual canonical dataset artifact, not scratch outputs.
- Prefer short, factual descriptions over marketing language.

Required provenance rules:
- The data dictionary must explicitly document:
  - dataset provenance
  - method provenance
  - feature or derived-field provenance, when applicable
- For each provenance record, classify the relationship to prior work as one of:
  - `exact`
  - `adapted`
  - `inspired`
  - `novel`
- For each non-trivial transformation, method choice, or derived feature, record:
  - `decision_origin.type: human | ai_suggested | imported_method`
- When prior work is involved, include citation-ready references.
- Where citation tracking is available in the target schema, explicitly record whether citation is required and whether it is present.
- The goal is that provenance lives with the dataset itself, so later summaries can
  collect attribution directly from the dictionary entry rather than inferring it
  from code or prompts.

Required quality and metadata rules:

- Document licensing or usage constraints whenever applicable.
- Document validation checks actually performed.
- Where validation metadata is available in the target schema, explicitly record validation status.
- Document important missingness summaries when they affect interpretation or use.
- Document known issues explicitly.
- Document material assumptions explicitly.

Provenance authoring rules:
- `exact` means the dataset or method reproduces prior work without material change.
- `adapted` means it is derived from prior work with explicit modifications.
- `inspired` means the work was conceptually informed by prior work but not directly
  reused in a way that preserves equivalence.
- `novel` means the dataset, method, or derived feature is newly defined here.
- Be explicit about what the provenance applies to:
  - whole dataset
  - extraction method
  - specific derived fields
- If only some fields are adapted or novel, document that at the feature level.
- If prior work requires citation, set citation presence explicitly rather than assuming it from nearby prose.
- For non-trivial transformations or features, record whether the decision came from:
  - a human decision
  - an AI suggestion
  - an imported method
- If the AI detects a non-trivial transformation, imported method, or prior-work-informed derived feature and citation-ready references are missing, it should ask the human user for the references instead of inventing them.

Validation authoring rules:

- Prefer explicit validation metadata when the target schema allows it.
- Use validation statuses such as:
  - `verified`
  - `unverified`
  - `assumed_standard`
- If something was checked directly against the original source, record who verified it and any short notes.

Quality rules:
- Do not invent unsupported fields in the target dictionary schema.
- If the target repo has a template, follow it.
- If required information is missing, make the smallest reasonable assumption and
  record it clearly in the entry.
- Prefer explicit citations over vague references like "based on prior work."

What to return:
- Return only the final entry content unless the user explicitly asks for explanation.
- Match the file format expected by the target dictionary repo.
- Do not include narrative outside the entry itself.
- If citation-ready references are materially missing for non-trivial transformations or prior-work-informed features, ask the user for them before finalizing provenance as complete.

Before finishing, check:
- Does this entry describe exactly one dataset?
- Does the `dataset_id` exactly match the pipeline's final dataset?
- Is the canonical storage location explicit?
- Is the grain of the dataset clear?
- Are temporary/intermediate files excluded from the dataset definition?
- Is dataset provenance documented?
- Is method provenance documented?
- Is feature provenance documented where needed?
- Does every provenance record use one of:
  - `exact`
  - `adapted`
  - `inspired`
  - `novel`
- For each non-trivial transformation or derived feature, is `decision_origin.type` recorded?
- Are prior-work references citation-ready?
- If citation is required, is that recorded explicitly and marked present or missing?
- Is licensing documented clearly enough?
- Are validation checks performed documented clearly enough?
- If validation status fields are available, are they set explicitly?
- Are important missingness patterns documented?
- Are known issues documented?
- Are important assumptions documented?

## Suggested Invocation

Use the prompt above together with:
- the target pipeline YAML
- the target sibling data-dictionary or data-catalog template
- any existing dataset entries that are close analogs
- source papers, URLs, or citations used by the pipeline
- known canonical storage paths and output filenames

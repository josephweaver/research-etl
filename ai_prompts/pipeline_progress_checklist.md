# Pipeline Progress Checklist

Use this checklist when the user asks:

- "what is next?"
- "where are we?"
- "what is still missing?"
- "what do we need before modeling?"

The goal is to compare the desired workflow state against the current completed
artifacts and identify the next blocking step.

## Working Rule

Do not answer only with a broad status summary.

Instead:

1. identify what the final target dataset or workflow outcome is
2. compare current completed artifacts against this checklist
3. identify the first missing dependency on the critical path
4. answer with the next concrete step

## Progress Stages

### 1. Source availability

- Has the raw or external source been identified?
- Has it been downloaded or staged?
- Are the expected years / regions / files actually present?

### 2. Reusable upstream dataset creation

- Has the raw source been normalized into a reusable dataset?
- If multiple upstream datasets are needed, which are complete and which are missing?

### 3. Intermediate staging

- Have stage datasets been created where needed?
- Are the stage outputs stored in stable dataset paths instead of run-only workdirs?

### 4. Feature or covariate assembly

- Have required joins, aggregations, or weighted summaries been built?
- Are all required modeling fields present?

### 5. Final dataset construction

- Has the one target dataset for this pipeline been produced?
- Is the output path explicit and stable?
- Is the grain correct?

### 6. Data dictionary

- Has the matching data-dictionary entry been created?
- Does it match the final `dataset_id`?
- Is provenance documented?

### 7. Validation

- Has the pipeline run successfully?
- Have obvious schema, row-count, duplicate-key, null, and coverage checks passed?

### 8. Environment verification

- Were environment-specific assumptions verified?
- For HPCC: module/runtime/library assumptions
- For Windows: interpreter and shell assumptions

## What To Report

When answering "what next?", report:

- the target dataset or workflow goal
- what is complete
- what is blocked or missing
- the next concrete step on the critical path

Prefer:

```text
The next step is X, because Y is already complete and Z is the first missing dependency.
```

Avoid:

```text
Here is a long unordered recap of everything that exists.
```

## Critical-Path Heuristics

- If raw source is missing, next step is raw ingest.
- If raw source exists but no stage dataset exists, next step is stage creation.
- If multiple upstream datasets exist and one required covariate is missing, the next step is that missing covariate.
- If the dataset exists but the data-dictionary entry does not, the next step may be documentation if the pipeline goal is otherwise complete.
- If the pipeline is blocked by environment/runtime issues, the next step is environment repair, not more downstream design.

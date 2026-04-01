# Provenance Guidelines

Use this file when writing dataset-level provenance into the data dictionary.

## Purpose

Provenance should live with the dataset itself so that later pipeline summaries,
workflow summaries, attribution reports, and citations can be assembled from the
data dictionary rather than inferred from code.

## Required Provenance Types

When applicable, document all three:

- dataset provenance
- method provenance
- feature or derived-field provenance

## Relationship Labels

Every provenance record should classify its relationship to prior work as one of:

- `exact`
- `adapted`
- `inspired`
- `novel`

## Interpretation

### `exact`

Use when:

- the dataset or method reproduces prior work without meaningful change
- the field semantics are the same as the cited upstream source

Do not use when:

- the extraction, aggregation, normalization, or field semantics changed materially

### `adapted`

Use when:

- prior work was reused but transformed, filtered, aggregated, remapped, or joined
- the method or field is clearly derived from prior work but not identical

### `inspired`

Use when:

- prior work informed the design conceptually
- the final implementation or field definition is not a direct derivative

### `novel`

Use when:

- the dataset, method, or derived field is newly defined in this project
- there is no direct antecedent that should receive an `exact`, `adapted`, or `inspired` label

## What To Document

### Dataset Provenance

Document:

- the upstream datasets or publications the dataset depends on
- whether the dataset is exact, adapted, inspired, or novel relative to prior work
- the canonical citations and source references

### Method Provenance

Document:

- the extraction, normalization, aggregation, or modeling method used to build the dataset
- whether that method is exact, adapted, inspired, or novel relative to prior work
- the decision origin for non-trivial method choices:
  - `human`
  - `ai_suggested`
  - `imported_method`
- any direct source code, paper, or workflow references when relevant

### Feature / Derived-Field Provenance

Document when:

- fields are derived rather than copied directly
- only some fields differ materially from prior work
- downstream reporting may need per-field attribution
- downstream reporting may need to know whether a feature choice came from a human, AI suggestion, or imported method

Examples:

- weighted NCCPI field averages from MUKEY overlaps
- annual tillage proportion fields derived from raster counts
- model-ready climate covariates assembled from staged monthly products

For non-trivial methods or derived features, prefer:

```yaml
decision_origin:
  type: human | ai_suggested | imported_method
```

Interpretation:

- `human`
  - explicitly decided by a human contributor
- `ai_suggested`
  - proposed by AI and adopted in the workflow
- `imported_method`
  - taken from prior work, an inherited script, paper, or established external method

## Citation Rule

When prior work is involved:

- include citation-ready references
- prefer full paper/report citations or stable URLs
- avoid vague phrases such as:
  - "based on USDA data"
  - "following prior work"

Where the target schema supports explicit citation flags, prefer:

```yaml
citation:
  required: true
  present: true
```

Working rule:

- if prior work materially informs the dataset, method, or feature, citation should usually be `required: true`
- only mark `present: true` when the actual citation is recorded in the entry
- if citation-ready references are missing and the transformation or feature is non-trivial, ask the human user for the references rather than inventing them

## Working Rule

If provenance is ambiguous, be more explicit, not less:

- name the upstream source
- name the method
- classify the relationship
- cite the source

If the AI can identify that a non-trivial method or feature likely comes from prior work but does not have a reliable citation:

- ask the user for the source paper, report, workflow, or repository reference
- do not fabricate citations
- do not silently omit the need for citation if it appears required

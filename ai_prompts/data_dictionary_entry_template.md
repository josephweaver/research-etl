# Data Dictionary Entry Template

Use this as a guidance template when creating a dataset entry in a sibling
data-dictionary or data-catalog repo.

Adapt field names to the target repo's actual schema, but preserve the structure
and coverage unless the target template requires something different.

```yaml
dataset_id: "stage.example_dataset_v1"
title: "Example dataset (v1)"
data_class: "STAGE"

status: "draft"
owner: ""
steward: ""

description: >
  Plain-English description of what the dataset represents, what it contains,
  and why it exists.

tags: []

geometry_type: ""

spatial_coverage:
  region: ""
  bbox: []
  crs: ""

temporal_coverage:
  type: ""
  start: ""
  end: ""
  values: []

grain: ""
primary_keys: []

representations:
  - kind: "file"
    uri: ""
    details:
      format: ""
      notes: ""

how_to_use:
  notes: >
    Explain intended use, non-intended use when relevant, and safe interpretation.
  examples: []

fields: []
bands: []

lineage:
  upstream: []
  transform:
    tool: ""
    ref: ""
    parameters: {}

provenance:
  dataset:
    relationship: "novel"
    decision_origin:
      type: "human"
    based_on: []
    citations: []
    citation:
      required: false
      present: false
    notes: ""
  method:
    relationship: "novel"
    decision_origin:
      type: "human"
    based_on: []
    citations: []
    citation:
      required: false
      present: false
    notes: ""
  features: []
  # Example feature entry:
  # - field: "derived_feature_name"
  #   relationship: "adapted"
  #   decision_origin:
  #     type: "ai_suggested"
  #   based_on: []
  #   citations: []
  #   citation:
  #     required: true
  #     present: true
  #   notes: ""

serve_contract: {}

quality:
  validation:
    status: "unverified"
    verified_by: ""
    notes: ""
    performed: []
    pending: []
  missingness:
    summary: []
  known_issues: []
  assumptions: []

security:
  classification: "internal"
  pii: false
  usage_constraints: ""

license:
  type: ""
  attribution: ""
  citation: ""
  url: ""

deprecation:
  replaced_by: ""
  reason: ""
  date: ""

notes: []
```

## Required Intent

When using this template:

- document exactly one dataset
- keep temporary outputs out of the dataset definition
- align `dataset_id` with the pipeline's final dataset
- make provenance explicit
- include citation-ready references when prior work exists
- fill out:
  - licensing
  - validation performed
  - missingness summary
  - known issues
  - assumptions

## Provenance Relationship Values

Allowed values:

- `exact`
- `adapted`
- `inspired`
- `novel`

## Decision Origin

For each non-trivial transformation, method choice, or derived feature, prefer:

```yaml
decision_origin:
  type: human | ai_suggested | imported_method
```

Use:

- `human`
  - when the transformation or feature definition was explicitly chosen by a human
- `ai_suggested`
  - when the transformation or feature definition was proposed by AI and adopted
- `imported_method`
  - when the transformation or feature definition comes from a prior workflow, paper, external method, or inherited implementation pattern

Working rule:

- add `decision_origin` to method provenance for non-trivial transformations
- add `decision_origin` to feature provenance for non-trivial derived fields
- use it at dataset level when the overall dataset definition itself reflects a non-trivial origin decision

## Citation Flags

Where citation tracking is relevant, prefer explicit fields such as:

```yaml
citation:
  required: true
  present: true
```

Use:

- `required: true` when attribution or prior-work citation is needed
- `present: true` only when the citation has actually been recorded in the entry

## Validation Status Values

Prefer explicit validation metadata such as:

```yaml
validation:
  status: verified | unverified | assumed_standard
  verified_by: joseph_weaver
  notes: "Checked against original paper"
```

Use:

- `verified` when the dataset detail was checked directly against the source or a trusted reference
- `unverified` when it has not yet been checked
- `assumed_standard` when a standard interpretation is being used but has not yet been directly verified for this dataset

## Working Rule

If the target sibling repo already has a stronger official template, follow that
template first and use this file as coverage guidance rather than as a schema override.

If a non-trivial transformation or derived feature appears to require citation
and no reliable citation-ready reference is available, ask the user for the
reference before marking provenance as complete.

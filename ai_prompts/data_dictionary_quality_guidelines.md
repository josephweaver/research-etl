# Data Dictionary Quality Guidelines

Use this file when writing or reviewing dataset entries to ensure that quality,
limitations, and compliance details are documented explicitly.

## Required Coverage

For each dataset entry, explicitly document the following whenever applicable:

- licensing
- validation checks performed
- missingness summary
- known issues
- assumptions

These should live with the dataset entry so later summaries and reports do not
need to infer them from code or pipeline logs.

## Licensing Documentation

Document:

- the source license or usage terms, if known
- attribution requirements
- citation requirements
- access restrictions or internal-use restrictions

Working rule:

- do not leave licensing implicit for `RAW` or `EXT` datasets if the source is external
- if the exact license is unknown, say so explicitly rather than omitting it

## Validation Checks Performed

Document the checks that were actually performed, not just the checks that would
be nice to have.

Examples:

- schema/column validation
- row-count checks
- duplicate-key checks
- null or missing-value checks
- temporal coverage checks
- spatial coverage checks
- categorical value checks
- file inventory checks

Working rule:

- distinguish between completed validation and validation still needed

Where the schema allows it, prefer explicit validation metadata such as:

```yaml
validation:
  status: verified | unverified | assumed_standard
  verified_by: joseph_weaver
  notes: "Checked against original paper"
```

Interpretation:

- `verified`
  - directly checked against a source, paper, original dataset, or trusted reference
- `unverified`
  - not yet checked directly
- `assumed_standard`
  - standard interpretation is being used, but no direct verification has been recorded yet

## Missingness Summary

Document important missingness patterns when they matter for interpretation or
downstream use.

Examples:

- null share for key covariates
- years or states with incomplete coverage
- fields intentionally blank for some records
- nodata-heavy raster-derived features

Working rule:

- include missingness when it affects usability, modeling, interpretation, or trust
- if a formal missingness analysis was not done, note that explicitly

## Known Issues

Document known limitations that users should not have to rediscover.

Examples:

- incomplete year coverage
- unverified class mappings
- environment-specific extraction limitations
- suspected source anomalies
- precision or geometry caveats

Working rule:

- be factual and specific
- prefer concrete limitations over vague warnings

## Assumptions

Document assumptions made during:

- source interpretation
- normalization
- aggregation
- joins
- feature derivation
- environment/runtime handling

Examples:

- assumed year inventory is 2005 through 2016
- assumed `Valu1` is readable from state gSSURGO geodatabases
- assumed `tile_field_id` is canonical join key
- assumed omitted tillage proportion is the baseline category

Working rule:

- if an assumption materially affects dataset meaning, document it
- if the assumption is later verified, update the entry accordingly

## Reference Collection Rule

AI is often better at collecting references from the user than inventing them correctly.

Working rule:

- if a dataset includes a non-trivial transformation, imported method, or derived
  feature and reliable references are missing, ask the human user for the
  relevant source references
- prefer collecting references explicitly from the user over guessing
- if references are still unavailable, record that the citation is required but not present

## Preferred Pattern

When possible, the dataset entry should make it easy to answer:

- what this dataset is
- where it came from
- how trustworthy it is
- what was validated
- what is still uncertain
- what limitations apply

## Review Questions

Before finishing a dataset entry, ask:

1. Is licensing/usage documented clearly enough?
2. Are completed validation checks listed?
3. Are major missingness patterns documented?
4. Are known issues explicit?
5. Are important assumptions explicit?

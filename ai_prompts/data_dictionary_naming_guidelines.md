# Data Dictionary Naming Guidelines

Use this file when creating `dataset_id` values or naming dataset entry files.

## Guiding Principles

- names are identifiers, not prose
- stability beats cleverness
- structure beats ambiguity
- machine-friendly first

## Dataset ID Format

Standard pattern:

```text
<data_class>.<subject>[_<subject>...][_v<version>]
```

Examples:

```text
raw.ssurgo_state_gdb_9state_v1
stage.lobell_tillage_field_year_v1
model_in.tillage_covariates_v1
serve.yield_county_year_v3
```

## Rules

- use lowercase
- use structured subject tokens
- use nouns, not verbs
- include `_vN` when the dataset meaning changes materially
- do not rename an existing `dataset_id`
- if meaning changes, create a new version instead

## File Naming

Dataset entry filenames should exactly match `dataset_id` plus `.yml`.

Example:

```text
datasets/stage/stage.lobell_tillage_field_year_v1.yml
```

## Column Naming

For dataset fields:

- use `snake_case`
- use lowercase
- prefer explicit names
- avoid ambiguous abbreviations unless they are standard in the domain

## Working Rule

Choose the most boring name that is still precise.

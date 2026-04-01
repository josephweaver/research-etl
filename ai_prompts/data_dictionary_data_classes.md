# Data Dictionary Data Classes

Use this file when deciding the correct `data_class` for a dataset entry or
choosing the matching `dataset_id` prefix for a pipeline.

Each dataset must belong to exactly one class.

If a dataset seems to fit multiple classes, it is usually a sign that the work
should be split into multiple datasets.

## RAW

Purpose:

- preserve source data as received

Use when:

- the dataset is essentially the original source payload
- it is immutable
- it is not yet normalized for modeling or serving

Do not use directly for:

- web serving
- final model input

## EXT

Purpose:

- document datasets produced outside the organization

Use when:

- an external researcher, collaborator, or institution produced the dataset
- licensing, citation, or attribution matters directly

## STAGE

Purpose:

- document intermediate processing outputs

Use when:

- the dataset comes from reprojection, clipping, rasterization, overlay, joins,
  normalization, or similar ETL transformations
- it is reusable, but not a final modeling or serving product

## MODEL_IN

Purpose:

- clean, versioned inputs to a model

Use when:

- the dataset is the actual feature/input table or aligned covariate product a
  model consumes

## MODEL_OUT

Purpose:

- outputs produced by a model

Use when:

- the dataset contains predictions, uncertainty, diagnostics, or similar model
  results

## SERVE

Purpose:

- fast, stable data meant for web or API consumption

Use when:

- the dataset is optimized for user-facing queries, tiling, API reads, or
  simplified web access

## REF

Purpose:

- shared reference data used across multiple datasets and workflows

Use when:

- the dataset is a stable lookup, boundary, mask, grid definition, or similar
  shared infrastructure

## Working Rule

When choosing a class, ask:

- is it source-as-received? -> `RAW`
- is it externally produced? -> `EXT`
- is it an intermediate ETL product? -> `STAGE`
- is it fed into a model? -> `MODEL_IN`
- is it produced by a model? -> `MODEL_OUT`
- is it optimized for web/API use? -> `SERVE`
- is it shared reference infrastructure? -> `REF`

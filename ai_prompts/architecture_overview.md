# Architecture Overview

Use this file when the task requires system-level reasoning across the ETL
framework, sibling pipeline repos, and sibling data-catalog repos.

This is not the detailed pipeline authoring prompt. It is the system map.

## Purpose

The ETL ecosystem is organized so that:

- datasets are the primary reusable unit
- each pipeline produces exactly one dataset
- dataset metadata and provenance live in the sibling data-catalog/dictionary repo
- the ETL framework provides execution, plugins, validation, and runtime behavior
- project-specific pipeline repos define concrete datasets and transformations

## Core Repos And Roles

### `research-etl`

Role:

- shared ETL framework

Responsibilities:

- pipeline execution engine
- plugin system
- runtime context and templating
- tracking and validation infrastructure
- prompt-engineering assets in `ai_prompts/`

### Project pipeline repos

Examples:

- `../landcore-etl-pipelines`
- `../shared-etl-pipelines`
- `../crop-insurance-etl-pipelines`

Role:

- define project-specific pipelines and source-specific scripts

Responsibilities:

- concrete pipeline YAML files
- source-specific scripts
- project-level staging/modeling workflows

### Data catalog / data dictionary repos

Current workspace example:

- `../landcore-data-catalog`

Role:

- authoritative metadata for datasets

Responsibilities:

- one canonical entry per dataset
- data class
- grain
- storage location
- lineage
- provenance
- usage guidance
- licensing / validation / assumptions / known issues

### Query / downstream repos

Example:

- `../landcore-duckdb`

Role:

- query metadata, views, and analytical interfaces over canonical datasets

## Core Concepts

### Dataset

A dataset is:

- one logical group of files, rows, records, features, or tiles
- plus selected metadata directly related to those records

A dataset is not:

- a whole workflow
- a random folder of temporary files
- an execution run directory

### Pipeline

A pipeline is:

- one ETL workflow whose final outcome is exactly one reusable dataset

If a workflow naturally produces multiple reusable datasets:

- split it into multiple pipelines
- connect them with `requires_pipelines`

### Data dictionary entry

Every reusable dataset should have:

- one canonical data-dictionary or data-catalog entry

The entry should document:

- what the dataset is
- grain
- storage location
- lineage
- provenance
- quality and limitations

## Lifecycle

Typical flow:

1. `RAW` or `EXT`
   - source-as-received or externally produced datasets
2. `STAGE`
   - intermediate reusable geospatial or normalization outputs
3. `MODEL_IN`
   - curated model inputs
4. `MODEL_OUT`
   - model outputs
5. `SERVE`
   - optimized datasets for web/API use
6. `REF`
   - shared reference infrastructure used across the system

Not every dataset passes through all classes.

## Architectural Rules

### One dataset per pipeline

This is a core rule.

Why:

- keeps lineage clear
- keeps catalog entries one-to-one with pipeline outcomes
- makes validation and reuse easier
- makes "what is next?" reasoning easier

### Metadata lives with the dataset

Do not expect later summary code to infer:

- provenance
- licensing
- validation status
- assumptions

from pipeline code alone.

That information should live in the data dictionary entry.

### Framework vs project logic

Put shared behavior in `research-etl` when it is:

- reusable across repos
- part of execution, validation, tracking, or plugin behavior

Put logic in a project pipeline repo when it is:

- source-specific
- dataset-specific
- geometry-heavy and not broadly reusable

### Prompt guidance vs code enforcement

If a mistake is recurring and machine-checkable:

- prefer validator/linter/framework enforcement in addition to prompt guidance

Prompts help with authoring.
Framework checks help with reliability.

## How To Use This Overview

Use this file when deciding:

- which repo should own a change
- whether something should be a new pipeline or a new step in an existing pipeline
- whether a reusable output deserves its own dataset and catalog entry
- whether a change belongs in prompts, plugins, validators, or scripts

Do not use this file as the only prompt for pipeline authoring.

For actual pipeline drafting, also read:

- [`pipeline_prompt.md`](/C:/Joe%20Local%20Only/College/Research/etl/ai_prompts/pipeline_prompt.md)
- [`pipeline_template.md`](/C:/Joe%20Local%20Only/College/Research/etl/ai_prompts/pipeline_template.md)

For dataset entry work, also read:

- [`data_dictionary_prompt.md`](/C:/Joe%20Local%20Only/College/Research/etl/ai_prompts/data_dictionary_prompt.md)
- [`data_dictionary_entry_template.md`](/C:/Joe%20Local%20Only/College/Research/etl/ai_prompts/data_dictionary_entry_template.md)

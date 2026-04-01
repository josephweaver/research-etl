# Data Dictionary Guidelines

Use this file when a task involves creating or updating dataset entries in a
target sibling data-dictionary or data-catalog repo.

In this workspace, the current sibling repo is:

- [`landcore-data-catalog`](/C:/Joe%20Local%20Only/College/Research/landcore-data-catalog)

Even if users say "data dictionary", follow the actual target repo structure and
template conventions.

## Core Idea

Document datasets, not files.

A dataset is:

- a logical group of files containing observations or records
- plus selected metadata directly related to those records

A dataset is not:

- scratch output
- temporary intermediates that are not meant for reuse
- a whole workflow
- an arbitrary directory of unrelated files

Every reusable dataset should have one canonical entry.

## Coupling Rule

When creating a new pipeline:

- the pipeline must produce exactly one dataset
- a matching data-dictionary entry must also be created for that final dataset

If the workflow naturally creates multiple reusable datasets:

- split them into multiple pipelines
- and create multiple dataset entries

## What A Good Entry Must Explain

At minimum, the entry should make clear:

- what the dataset represents
- what one row, cell, feature, or record means
- where it physically lives
- how it was created
- how it is intended to be used
- how it should not be used, when relevant

## What Does Not Belong

Do not treat these as data-dictionary content:

- raw data payloads
- ETL code
- modeling code
- large SQL scripts

Link to those resources instead.

## Authoring Rule

If a dataset is used in:

- production
- scheduled jobs
- modeling
- reports
- dashboards
- cross-team handoff

it should have an entry.

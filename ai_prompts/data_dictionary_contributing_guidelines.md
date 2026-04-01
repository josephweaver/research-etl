# Data Dictionary Contributing Guidelines

Use this file when adding or editing entries in a sibling data-dictionary or
data-catalog repo.

## Required Workflow

When adding a new dataset entry:

1. identify the one dataset being documented
2. choose exactly one data class
3. use the correct `dataset_id`
4. place the file under the matching `datasets/<class>/` directory
5. follow the target repo template as closely as possible

## Minimum Completeness

Before considering an entry acceptable, ensure it includes:

- `dataset_id`
- `title`
- `data_class`
- `status`
- `description`
- `geometry_type`
- `spatial_coverage`
- `temporal_coverage`
- `grain`
- at least one `representation`
- `how_to_use`
- `lineage`

If the target schema has more required fields, follow that schema.

## Writing Style

- write in plain English
- keep sentences short
- prefer factual wording
- be explicit about limitations
- document intended use and misuse risk when relevant

## Update Rule

When changing an existing entry:

- do not silently rewrite meaning
- preserve identifier stability
- if the dataset meaning changed materially, create a new version
- use deprecation fields when replacing an older dataset

## Review Priority

Prioritize:

- correct classification
- correct lineage boundary
- clear grain
- correct naming
- usable provenance

Do not over-optimize for prose style.

# `exec_script`

Use for:

- source-specific logic
- normalization
- geospatial transformations with dataset-specific rules
- one-off ETL scripts that are not broadly reusable enough to justify a new plugin

Good fit:

- complex joins
- raster/vector logic with project-specific behavior
- dataset-specific parsing and reshaping

Important notes:

- prefer `exec_script` over inventing a new plugin for logic that is narrowly tied to one source or one dataset
- keep path and config values in pipeline `vars`
- make output paths explicit
- if the script invokes module-managed external tools on HPCC, handle the module runtime environment explicitly

Common mistakes:

- creating a new plugin too early
- hiding final output paths inside the script
- assuming shell/runtime environment details instead of making them explicit

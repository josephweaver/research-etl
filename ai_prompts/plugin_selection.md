# Plugin Selection Guide

Use this file when choosing between existing plugins for a new pipeline.

The goal is to reuse existing plugins first and avoid unnecessary custom scripts
or new plugin work.

## Working Rule

Prefer this order:

1. existing plugin with a clear fit
2. existing plugin plus a small source-specific script
3. new plugin only when the behavior is broadly reusable

If a plugin is confusing, subtle, or repeatedly misused:

- capture the usage guidance in `ai_prompts/plugins/`

Treat that directory as a cache of learned plugin understanding.

## Common Choices

### `web_download_list`

Use when:

- downloading files over HTTP/HTTPS
- one or more URLs are known up front

Do not use when:

- the source is Google Drive via rclone
- the source is FTP
- the source is STAC

### `archive_extract`

Use when:

- extracting `.zip` or `.7z` archives into a stable directory

Do not use when:

- the source is not an archive
- extraction logic is source-specific and needs custom behavior the plugin does not support

### `exec_script`

Use when:

- the work is source-specific
- the transformation is too detailed or unusual for a general plugin
- geospatial or normalization logic belongs in a project script

Do not use when:

- an existing plugin already cleanly expresses the task

### `combine_files`

Use when:

- combining multiple same-shape outputs into one stable output
- especially CSV, JSON, YAML, XML, or text merges

Do not use when:

- the merge requires complex business logic better handled in a script

### `dataset_store`

Use when:

- the final dataset should be registered/persisted through the ETL dataset registry

### Geospatial plugins

Use when:

- the operation is clearly one of the supported geospatial patterns

Examples:

- raster clipping
- raster selection by polygon
- raster aggregation by polygon

Working rule:

- if the geospatial logic is highly source-specific or has many edge cases, prefer a script
- if the plugin contract is subtle, check `ai_prompts/plugins/` first

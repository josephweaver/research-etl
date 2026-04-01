# `archive_extract`

Use for:

- extracting `.zip` and `.7z` archives into a stable directory

Good fit:

- raw source staging
- stable extraction directories
- archive batches selected by a glob

Important notes:

- prefer explicit stable output directories in pipeline `vars`
- use `archive_glob` when extracting many archives of the same pattern
- keep extraction as its own step; do not hide it inside a custom script unless needed

Common mistakes:

- extracting into a transient workdir when the extracted source is meant to be reused
- mixing extraction with later transformation logic in one step

Preferred pattern:

```yaml
plugin: archive_extract
args:
  archive_glob: "{rawdir}/gSSURGO_*.zip"
  out: "{extractdir}"
  overwrite: true
```

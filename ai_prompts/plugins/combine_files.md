# `combine_files`

Use for:

- deterministic merge of multiple same-shape outputs into one final artifact

Good fit:

- per-tile CSVs to one CSV
- per-state CSVs to one CSV
- repeated outputs from `foreach` steps

Important notes:

- this is best when the files already share a compatible structure
- prefer a separate combine step rather than doing ad hoc merging in many worker scripts
- good way to make the final dataset artifact explicit

Common mistakes:

- using it when the input files need business-rule reconciliation
- relying on it for structurally incompatible files

Preferred pattern:

```yaml
plugin: combine_files
args:
  input_glob: "{per_state_glob}"
  output_file: "{combined_csv}"
  format: csv
  verbose: true
```

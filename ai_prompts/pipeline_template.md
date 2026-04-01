# Pipeline Template

Use this as a starter shape for dataset-oriented pipelines in this repo.

```yaml
project_id: default

# Add only when this dataset depends on other reusable datasets.
# requires_pipelines:
#   - ../shared/source_pipeline.yml

vars:
  name: example_dataset_pipeline
  dataset_id: stage.example_dataset_v1
  owner: example-owner
  grain: "1 row per entity per period"

  # Core runtime paths
  workdir: "{env.workdir}/{name}/{sys.now.yymmdd}/{sys.now.hhmmss}-{sys.run.short_id}"
  logdir: "{workdir}/logs"

  # Dataset-specific paths and controls
  source_url: "https://example.org/data/example.zip"
  base_dir: "{project.basedir}/data/example_dataset"
  raw_dir: "{base_dir}/raw"
  extract_dir: "{base_dir}/extract"
  output_dir: "{base_dir}/final"
  output_file: "{output_dir}/example_dataset.csv"
  register_dataset: true

steps:
  - name: "{sys.step.NN}_download_source"
    plugin: web_download_list
    args:
      urls: "{source_url}"
      out: "{raw_dir}"
      overwrite: false
      conditional_get: true
      fail_on_error: true
      verbose: true
    output_var: downloaded

  - name: "{sys.step.NN}_extract_source"
    plugin: archive_extract
    args:
      archive_glob: "{downloaded.output_dir}/*.zip"
      out: "{extract_dir}"
      overwrite: false
      verbose: true
    output_var: extracted

  - name: "{sys.step.NN}_build_dataset"
    plugin: exec_script
    args:
      script: "scripts/example/build_example_dataset.py"
      script_args: "--input-dir \"{extract_dir}\" --output-file \"{output_file}\" --verbose"
      verbose: true
    output_var: built_dataset

  - name: "{sys.step.NN}_register_dataset"
    when: register_dataset
    plugin: dataset_store
    args:
      dataset_id: "{dataset_id}"
      project_id: default
      path: "{output_file}"
      stage: staging
      version: "{sys.now.yymmdd_hhmmss}"
      runtime_context: local
      location_type: hpcc_cache
      target_uri: "{project.basedir}/datasets/{dataset_id}/{sys.now.yymmdd_hhmmss}"
      owner: "{owner}"
      data_class: STAGE
      dry_run: false
    output_var: registered_dataset
```

## Notes

- Keep one reusable dataset per file.
- Choose a `dataset_id` that matches the final data class and is stable enough to become the catalog/documentation filename.
- Move shared upstream work into separate pipelines and reference them with `requires_pipelines`.
- Keep the final dataset boundary clear enough that a catalog entry can describe one canonical dataset, one grain, and one lineage story.
- Replace `web_download_list`, `archive_extract`, `exec_script`, and `dataset_store` with the concrete plugins that match the real job.
- If the final dataset is published rather than registered, replace the last step with the actual publish/upload step and make the destination explicit.

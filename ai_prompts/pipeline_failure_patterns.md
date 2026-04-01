# Pipeline Failure Patterns

Use this document to capture recurring pipeline-authoring and pipeline-runtime
failures discovered while building ETL workflows. The goal is to convert concrete
failures into:

- prompt updates
- validation/lint rules
- reusable example patterns
- framework/plugin improvements

Prefer short, operational notes over long retrospectives.

## How To Use

For each recurring problem:

1. Add one entry using the template below.
2. Focus on the concrete symptom, root cause, and preferred fix.
3. Decide whether the right response is:
   - prompt guidance
   - validator/linter enforcement
   - plugin/framework improvement
   - example update
4. If the issue is likely to recur, add or update a rule in:
   - [`pipeline_prompt.md`](/C:/Joe%20Local%20Only/College/Research/etl/ai_prompts/pipeline_prompt.md)

## Entry Template

### Pattern: `<short_name>`

**Status**
- `active` | `resolved` | `superseded`

**Category**
- `authoring`
- `templating`
- `foreach`
- `paths`
- `runtime`
- `slurm`
- `hpcc`
- `plugin-selection`
- `data-shape`
- `catalog-alignment`
- `other`

**Symptom**
- What failed?
- Include the user-visible error or the bad output shape.

**Example Error**
```text
Paste the representative error message here.
```

**Root Cause**
- What was actually wrong?
- Keep this precise and technical.

**Preferred Fix**
- What should the AI do next time?
- What should a human do if they see this again?

**Prompt Rule**
- One short imperative rule to add to or reinforce in the authoring prompt.

**Validator/Linter Opportunity**
- Can the framework detect this automatically before runtime?
- `yes` / `no`
- If yes, describe the check.

**Example Bad Pattern**
```yaml
# Minimal bad example
```

**Example Good Pattern**
```yaml
# Minimal corrected example
```

**Applies To**
- Which repos, pipeline types, environments, or plugins are affected?

**Related Files**
- Prompt docs
- Plugins
- Example pipelines
- Tests

**Notes**
- Optional short notes only.

---

## Patterns

### Pattern: `foreach_requires_list`

**Status**
- `active`

**Category**
- `foreach`

**Symptom**
- Pipeline expansion fails before execution.
- `foreach` references a variable that is a string instead of a YAML list.

**Example Error**
```text
`foreach` expects a list; got <class 'str'> for states
```

**Root Cause**
- The pipeline defined a comma-delimited string such as `"IA,IL,IN"` and used it
  with `foreach`, but the engine expects an actual YAML list.

**Preferred Fix**
- Define `foreach` inputs as YAML lists in `vars`.
- Do not rely on comma-separated strings for list expansion.

**Prompt Rule**
- If a step uses `foreach`, the referenced variable must be a YAML list, not a comma-delimited string.

**Validator/Linter Opportunity**
- `yes`
- Validate that variables referenced by `foreach` resolve to list-like values before run execution.

**Example Bad Pattern**
```yaml
vars:
  states: "IA,IL,IN"
steps:
  - name: "{sys.step.NN}_example"
    plugin: exec_script
    foreach: states
```

**Example Good Pattern**
```yaml
vars:
  states:
    - IA
    - IL
    - IN
steps:
  - name: "{sys.step.NN}_example"
    plugin: exec_script
    foreach: states
```

**Applies To**
- All repos using `foreach`

**Related Files**
- [`pipeline_prompt.md`](/C:/Joe%20Local%20Only/College/Research/etl/ai_prompts/pipeline_prompt.md)

**Notes**
- This is a frequent AI-authored YAML mistake and should be enforced early.

---

### Pattern: `avoid_dirs_section`

**Status**
- `active`

**Category**
- `paths`

**Symptom**
- Pipeline YAML works inconsistently with current repo conventions or drifts from
  the documented authoring standard.

**Example Error**
```text
No runtime error required; this is a standards drift issue.
```

**Root Cause**
- The pipeline uses `dirs` even though current authoring guidance prefers keeping
  path-like values in `vars`.

**Preferred Fix**
- Put `workdir`, `logdir`, dataset roots, and stable output paths in `vars`.
- Avoid `dirs` until path handling is first-class in the framework.

**Prompt Rule**
- Keep path-like configuration in `vars` and avoid `dirs` unless the framework standard changes.

**Validator/Linter Opportunity**
- `yes`
- Warn when a pipeline contains `dirs`.

**Example Bad Pattern**
```yaml
vars:
  name: example
dirs:
  workdir: "{env.workdir}/{name}"
```

**Example Good Pattern**
```yaml
vars:
  name: example
  workdir: "{env.workdir}/{name}"
```

**Applies To**
- All authoring prompts and all ETL repos following the current standard

**Related Files**
- [`pipeline_prompt.md`](/C:/Joe%20Local%20Only/College/Research/etl/ai_prompts/pipeline_prompt.md)

**Notes**
- This is a style and consistency rule more than a runtime rule.

---

### Pattern: `module_dependent_binary_needs_runtime_env`

**Status**
- `active`

**Category**
- `hpcc`

**Symptom**
- A step can find a binary by absolute path, but execution fails with missing
  shared libraries.

**Example Error**
```text
error while loading shared libraries: libgdal.so.37: cannot open shared object file
```

**Root Cause**
- The ETL step invoked a module-managed binary directly without loading the module
  environment that provides its dependent shared libraries.

**Preferred Fix**
- Run the binary through a shell that sources the profile and loads the required
  module first.
- Alternatively, use a wrapper script that establishes the module environment.

**Prompt Rule**
- If a step uses an HPCC module-managed binary, load the required module in the runtime shell instead of assuming an absolute binary path is sufficient.

**Validator/Linter Opportunity**
- `yes`
- Warn when a pipeline hardcodes binaries from module-managed paths without an explicit module-load strategy.

**Example Bad Pattern**
```yaml
vars:
  ogr2ogr_bin: "/opt/software-current/.../ogr2ogr"
steps:
  - name: "{sys.step.NN}_run_tool"
    plugin: exec_script
    args:
      script: scripts/example.py
      script_args: "--ogr2ogr-bin \"{ogr2ogr_bin}\""
```

**Example Good Pattern**
```yaml
vars:
  ogr2ogr_bin: "/opt/software-current/.../ogr2ogr"
  gdal_module: "GDAL/3.11.1-foss-2025a"
steps:
  - name: "{sys.step.NN}_run_tool"
    plugin: exec_script
    args:
      script: scripts/example.py
      script_args: "--ogr2ogr-bin \"{ogr2ogr_bin}\" --gdal-module \"{gdal_module}\""
```

**Applies To**
- HPCC and any module-managed cluster environment

**Related Files**
- [`pipeline_prompt.md`](/C:/Joe%20Local%20Only/College/Research/etl/ai_prompts/pipeline_prompt.md)

**Notes**
- This is common for GDAL, R, and other cluster-managed toolchains.

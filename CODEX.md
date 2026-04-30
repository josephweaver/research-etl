# CODEX.md

Root AI routing file for this repo.

Use this file to decide which prompt-engineering assets to read before acting.
Keep this file short. Detailed guidance belongs in `ai_prompts/`.

## Primary Routing

For system-level orientation or cross-repo architectural questions:

- read [`ai_prompts/architecture_overview.md`](/C:/Joe%20Local%20Only/College/Research/etl/ai_prompts/architecture_overview.md)

For pipeline authoring work:

- read [`ai_prompts/pipeline_prompt.md`](/C:/Joe%20Local%20Only/College/Research/etl/ai_prompts/pipeline_prompt.md)
- read [`ai_prompts/plugin_selection.md`](/C:/Joe%20Local%20Only/College/Research/etl/ai_prompts/plugin_selection.md) when choosing plugins
- then read [`ai_prompts/pipeline_template.md`](/C:/Joe%20Local%20Only/College/Research/etl/ai_prompts/pipeline_template.md)
- use [`ai_prompts/pipeline_intake_checklist.md`](/C:/Joe%20Local%20Only/College/Research/etl/ai_prompts/pipeline_intake_checklist.md) to determine what information is still missing
- then inspect relevant example pipelines in the target repo
- if the task creates a new dataset, also read:
  - [`ai_prompts/data_dictionary_prompt.md`](/C:/Joe%20Local%20Only/College/Research/etl/ai_prompts/data_dictionary_prompt.md)
  - [`ai_prompts/data_dictionary_entry_template.md`](/C:/Joe%20Local%20Only/College/Research/etl/ai_prompts/data_dictionary_entry_template.md)
  - [`ai_prompts/provenance_guidelines.md`](/C:/Joe%20Local%20Only/College/Research/etl/ai_prompts/provenance_guidelines.md)
  - [`ai_prompts/data_dictionary_guidelines.md`](/C:/Joe%20Local%20Only/College/Research/etl/ai_prompts/data_dictionary_guidelines.md)
  - [`ai_prompts/data_dictionary_data_classes.md`](/C:/Joe%20Local%20Only/College/Research/etl/ai_prompts/data_dictionary_data_classes.md)
  - [`ai_prompts/data_dictionary_naming_guidelines.md`](/C:/Joe%20Local%20Only/College/Research/etl/ai_prompts/data_dictionary_naming_guidelines.md)
  - [`ai_prompts/data_dictionary_contributing_guidelines.md`](/C:/Joe%20Local%20Only/College/Research/etl/ai_prompts/data_dictionary_contributing_guidelines.md)
  - [`ai_prompts/data_dictionary_quality_guidelines.md`](/C:/Joe%20Local%20Only/College/Research/etl/ai_prompts/data_dictionary_quality_guidelines.md)

For pipeline debugging or failure analysis:

- read [`ai_prompts/pipeline_failure_patterns.md`](/C:/Joe%20Local%20Only/College/Research/etl/ai_prompts/pipeline_failure_patterns.md)
- use [`ai_prompts/pipeline_failure_triage_checklist.md`](/C:/Joe%20Local%20Only/College/Research/etl/ai_prompts/pipeline_failure_triage_checklist.md) to classify the failure first
- if the issue is environment-specific, also read the relevant environment note:
  - [`ai_prompts/environments/windows.md`](/C:/Joe%20Local%20Only/College/Research/etl/ai_prompts/environments/windows.md)
  - [`ai_prompts/environments/hpcc.md`](/C:/Joe%20Local%20Only/College/Research/etl/ai_prompts/environments/hpcc.md)

For onboarding a new user to run their own LandCore checkpointed risk-model fit:

- read [`docs/landcore_risk_model_onboarding.md`](/C:/Joe%20Local%20Only/College/Research/etl/docs/landcore_risk_model_onboarding.md)
- then inspect the target pipeline copy in `../landcore-etl-pipelines/pipelines/risk_model/`
- use the pipeline debugging guidance above if the first validation or run fails

For maintaining the prompt set itself:

- read [`ai_prompts/prompt_engineering_guidelines.md`](/C:/Joe%20Local%20Only/College/Research/etl/ai_prompts/prompt_engineering_guidelines.md)

For "what is next?" or workflow status questions:

- check [`PROJECT_STATUS.md`](/C:/Joe%20Local%20Only/College/Research/etl/PROJECT_STATUS.md) first for the active cross-repo execution checkpoint
- use [`ai_prompts/pipeline_progress_checklist.md`](/C:/Joe%20Local%20Only/College/Research/etl/ai_prompts/pipeline_progress_checklist.md)

## Current Repo Scope

Primary repo:

- `research-etl`

Related sibling repos commonly used from this workspace:

- `../landcore-etl-pipelines`
- `../landcore-data-catalog`
- `../landcore-duckdb`
- `../RiskModel-HPCC`

## Working Rule

When a session uncovers a recurring failure, environment quirk, or reliable workaround:

1. record it in the appropriate file under `ai_prompts/`
2. keep the note concrete and operational
3. if machine-checkable, prefer validator/framework enforcement in addition to prompt text

When a session uncovers reusable plugin understanding:

1. check whether the plugin is subtle, easy to misuse, or repeatedly used
2. if yes, add or update a short note under `ai_prompts/plugins/`
3. treat `ai_prompts/plugins/` as a cache of learned plugin usage guidance

When fixing a non-trivial bug, pipeline failure, or environment issue:

1. check whether the issue is likely to recur
2. check whether a future AI could make the same mistake again
3. if yes, update the relevant `ai_prompts/` file as part of the same task unless the user explicitly says not to
4. if the issue is machine-checkable, also consider whether validator/linter/framework enforcement is the better long-term fix

## Prompt Asset Layout

- `ai_prompts/pipeline_prompt.md`
  - canonical pipeline authoring rules
- `ai_prompts/plugin_selection.md`
  - how to choose among existing plugins
- `ai_prompts/architecture_overview.md`
  - high-level system map for repo roles, datasets, pipelines, and lifecycle
- `ai_prompts/pipeline_template.md`
  - starter pipeline shape
- `ai_prompts/pipeline_intake_checklist.md`
  - determine what information is still needed before drafting
- `ai_prompts/pipeline_failure_patterns.md`
  - recurring pipeline mistakes and fixes
- `ai_prompts/pipeline_failure_triage_checklist.md`
  - classify a failed run before proposing fixes
- `ai_prompts/pipeline_progress_checklist.md`
  - determine the next critical-path step from current workflow state
- `ai_prompts/data_dictionary_prompt.md`
  - canonical dataset dictionary / catalog authoring rules
- `ai_prompts/data_dictionary_entry_template.md`
  - starter structure for dataset entries
- `ai_prompts/provenance_guidelines.md`
  - provenance classification and attribution rules
- `ai_prompts/data_dictionary_guidelines.md`
  - what a dataset entry is and when it must exist
- `ai_prompts/data_dictionary_data_classes.md`
  - how to choose the correct data class
- `ai_prompts/data_dictionary_naming_guidelines.md`
  - dataset ID and file naming rules
- `ai_prompts/data_dictionary_contributing_guidelines.md`
  - practical entry-writing and update rules
- `ai_prompts/data_dictionary_quality_guidelines.md`
  - licensing, validation, missingness, known issues, and assumptions
- `ai_prompts/prompt_engineering_guidelines.md`
  - how to maintain and evolve the prompt set
- `ai_prompts/prompt_update_checklist.md`
  - post-fix checklist for deciding whether prompt assets should be updated
- `ai_prompts/environments/windows.md`
  - Windows-specific operational notes
- `ai_prompts/environments/hpcc.md`
  - HPCC / SLURM / module-environment notes
- `ai_prompts/plugins/`
  - cached plugin-specific usage guidance for subtle or high-value plugins

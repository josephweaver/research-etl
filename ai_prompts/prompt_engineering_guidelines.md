# Prompt Engineering Guidelines

Use this document to maintain the repo's AI prompt set intentionally.

The goal is not to write longer prompts. The goal is to make the AI more reliable
at producing correct work in this framework.

## Purpose

The prompt set should help the AI:

- draft pipelines that match the ETL framework
- choose existing patterns before inventing new ones
- avoid recurring environment-specific mistakes
- recover from known failure modes
- produce datasets that are easy to catalog and reuse

## Prompt Set Structure

Keep the prompt set split by function:

- [`pipeline_prompt.md`](/C:/Joe%20Local%20Only/College/Research/etl/ai_prompts/pipeline_prompt.md)
  - canonical pipeline authoring rules
- [`pipeline_template.md`](/C:/Joe%20Local%20Only/College/Research/etl/ai_prompts/pipeline_template.md)
  - starter shape and common layout
- [`pipeline_failure_patterns.md`](/C:/Joe%20Local%20Only/College/Research/etl/ai_prompts/pipeline_failure_patterns.md)
  - recurring mistakes and corrections
- [`environments/windows.md`](/C:/Joe%20Local%20Only/College/Research/etl/ai_prompts/environments/windows.md)
  - Windows-specific issues
- [`environments/hpcc.md`](/C:/Joe%20Local%20Only/College/Research/etl/ai_prompts/environments/hpcc.md)
  - HPCC-specific issues

Keep `CODEX.md` short and use it only as a router.

## Maintenance Workflow

When a pipeline session exposes a new issue:

1. Decide what kind of issue it is.
   - authoring mistake
   - environment quirk
   - framework limitation
   - plugin gap
   - example gap
2. Capture the issue in the narrowest appropriate file.
3. Only promote stable, recurring lessons into the main authoring prompt.
4. If the issue is machine-checkable, prefer adding validation/linting in addition to prompt text.
5. Environment-specific shell/runtime mistakes should usually go into `ai_prompts/environments/` and `pipeline_failure_patterns.md`, not only the main pipeline authoring prompt.

## What Belongs Where

Add to `pipeline_prompt.md` when:

- the rule should apply to most newly authored pipelines
- the instruction is short, stable, and broadly useful

Add to `pipeline_failure_patterns.md` when:

- the issue came from a real failure
- you want to preserve symptom, root cause, and fix
- the lesson may later become a prompt rule or validator

Add to an environment note when:

- the issue depends on the execution environment
- the same pipeline would behave differently on another machine

Add a validator or framework change when:

- the mistake can be detected automatically
- repeated prompt wording is less reliable than code enforcement

## Rules For Good Prompt Updates

- Prefer short operational rules over long prose.
- Prefer examples over abstract explanation.
- Prefer one rule per failure pattern.
- Do not mix environment quirks into the main pipeline authoring prompt unless they are nearly universal.
- Do not turn one-off incidents into permanent prompt clutter.
- Do not solve framework validation problems only with prompt text.

## Review Questions

Before updating a prompt file, ask:

1. Is this a recurring issue or a one-off?
2. Is this guidance stable enough to live in the canonical prompt?
3. Can the engine validate this automatically instead?
4. Would an example pipeline teach this better than a rule?
5. Is this environment-specific and better kept in `ai_prompts/environments/`?

## Preferred Style

- Keep instructions imperative.
- Keep wording concrete.
- Keep examples minimal.
- Keep files small enough that an AI can follow them reliably.

Good rule:

```text
If a step uses `foreach`, the referenced variable must be a YAML list.
```

Bad rule:

```text
When creating iterative ETL expansion constructs, carefully consider the possible
representations of collections and make sure the chosen representation is compatible
with the current execution engine semantics.
```

## Suggested Change Loop

After a pipeline task:

1. note the failure or friction
2. add a failure-pattern entry
3. decide whether it should also change:
   - `pipeline_prompt.md`
   - an environment note
   - a validator
   - an example pipeline
4. keep the change as small as possible

## Post-Fix Rule

After fixing a non-trivial bug, failure, or environment issue, ask:

1. Is this likely to recur?
2. Could a future AI make the same mistake again?
3. Is the lesson general enough to help future work?

If the answer is yes to any of the above:

- update the relevant `ai_prompts/` file in the same task
- unless the user explicitly says not to

If the issue is machine-checkable:

- also consider a validator, linter, framework guardrail, or helper abstraction
- do not rely on prompt text alone when code enforcement would be stronger

## Current Direction

The current prompt-engineering direction for this repo is:

- one reusable dataset per pipeline
- stable dataset IDs and output paths
- reuse existing plugins and repo patterns first
- prefer scripts over over-generalized abstractions for source-specific logic
- treat HPCC module/runtime assumptions as explicit environment guidance, not hidden assumptions

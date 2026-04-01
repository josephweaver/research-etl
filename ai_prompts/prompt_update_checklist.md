# Prompt Update Checklist

Use this checklist after fixing a non-trivial bug, pipeline failure, or
environment issue.

The goal is to decide whether the prompt set should be updated as part of the
same task.

## Decision Questions

### 1. Is this likely to recur?

- Could the same mistake happen again in a similar pipeline or script?
- Is it tied to a common pattern rather than a one-off typo?

### 2. Could a future AI make the same mistake?

- Was the mistake caused by:
  - missing authoring guidance
  - missing environment guidance
  - an unclear pattern
  - an easy-to-make shell or templating mistake

### 3. Is the lesson generalizable?

- Would capturing the lesson help future tasks beyond the immediate file?

## If Yes, Update One Or More Of

- [`pipeline_failure_patterns.md`](/C:/Joe%20Local%20Only/College/Research/etl/ai_prompts/pipeline_failure_patterns.md)
  - for recurring failure symptoms, causes, and fixes
- [`pipeline_prompt.md`](/C:/Joe%20Local%20Only/College/Research/etl/ai_prompts/pipeline_prompt.md)
  - for broad authoring rules
- [`environments/windows.md`](/C:/Joe%20Local%20Only/College/Research/etl/ai_prompts/environments/windows.md)
  - for Windows-specific issues
- [`environments/hpcc.md`](/C:/Joe%20Local%20Only/College/Research/etl/ai_prompts/environments/hpcc.md)
  - for HPCC-specific issues
- other task-specific prompt assets when appropriate

## Also Ask

Can the issue be prevented better by:

- validator/linter enforcement
- a helper abstraction
- a framework change
- a reusable wrapper

If yes, note that too. Prompt text should not be the only protection when a code
guardrail would be stronger.

## Working Rule

Unless the user explicitly says not to:

- fixing the code and updating the relevant prompt asset should be part of the
  same task when the lesson is recurring and reusable

# Pipeline Failure Triage Checklist

Use this checklist after a pipeline failure or when reviewing logs.

The goal is to classify the failure quickly and identify the most likely fix path.

## Working Rule

Do not jump to code changes before classifying the failure.

Instead:

1. identify the failing step
2. identify whether the failure happened:
   - before step expansion
   - during pipeline expansion
   - during execution
   - after outputs were created
3. classify the failure using the categories below

## Triage Categories

### 1. YAML / templating failure

Symptoms:

- parse errors
- bad variable expansion
- unsupported field usage
- missing required args

Examples:

- invalid YAML
- unresolved template variable
- unsupported plugin argument shape

### 2. `foreach` / expansion failure

Symptoms:

- failure before actual step execution
- expansion errors such as non-list `foreach` inputs

Examples:

- `foreach` expects a list

### 3. Missing input artifact

Symptoms:

- source path does not exist
- expected file glob is empty
- prerequisite dataset was never produced

### 4. Environment / runtime failure

Symptoms:

- module issues
- shared library issues
- interpreter mismatch
- secrets not propagated

Examples:

- missing `libgdal.so`
- command found but cannot load dependencies

### 5. Resource / hardware failure

Symptoms:

- OOM kill
- timeout
- disk exhaustion
- scheduler eviction

Working rule:

- first consider streaming or chunking
- only then raise explicit resource requests

### 6. Source schema mismatch

Symptoms:

- expected columns missing
- source layer names differ
- years/files differ from assumptions

### 7. Data-shape or contract mismatch

Symptoms:

- wrong key format
- wrong output columns
- duplicated rows
- wrong grain

### 8. Plugin or framework limitation

Symptoms:

- the toolchain cannot access a required source format
- the plugin contract cannot represent the needed operation
- the engine behavior is missing a needed validation

## Minimal Triage Output

After triage, the AI should be able to state:

- failing pipeline
- failing step
- failure category
- likely root cause
- next best fix

## Preferred Next-Step Pattern

Use:

```text
This is a <category> failure. The immediate cause is <cause>. The next fix should be <fix>.
```

## Relationship To Failure Patterns

After resolving a recurring issue:

- add or update an entry in [`pipeline_failure_patterns.md`](/C:/Joe%20Local%20Only/College/Research/etl/ai_prompts/pipeline_failure_patterns.md)
- decide whether the lesson belongs in:
  - the canonical prompt
  - an environment note
  - a validator/linter
  - an example pipeline

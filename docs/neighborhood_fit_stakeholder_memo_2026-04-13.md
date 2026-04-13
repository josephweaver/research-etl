# Stakeholder Memo: Interim Findings From The Neighborhood Corn Yield Model

Date: 2026-04-13

## Short Answer

At this stage, the model suggests that both `high tillage (0)` and `low tillage (1)` are usually associated with **lower corn yield than `NA tillage`**, when we compare fields under otherwise similar average conditions within the same local county-neighborhood dataset.

That answer comes with one important qualifier: the tillage effect is **not fixed**. It changes with July weather stress. In counties with hotter, drier July conditions, the negative tillage association becomes smaller and can sometimes move toward neutral because the model finds a strong interaction between tillage and July `vpdmax_7`.

So the clearest stakeholder answer is:

- compared with `NA tillage`, both `high tillage (0)` and `low tillage (1)` usually point to lower yield at average local conditions
- the size of that gap depends strongly on July weather stress
- neither `high tillage (0)` nor `low tillage (1)` shows a clear statewide advantage over the other

## What The Model Is Doing

The current run fits one model per focal county using data from that county plus nearby counties. The model predicts corn yield using:

- tillage composition
- July maximum vapor pressure deficit (`vpdmax_7`)
- `nccpi3corn`
- year
- a field-level random intercept

The tillage terms in the current model are:

- `tillage_0_prop`
- `tillage_1_prop`

The omitted category is `tillage_na_prop`, so the model is effectively reading higher `0` or `1` tillage share relative to more `NA tillage` share.

## Current Run Status

As of 2026-04-13 on HPCC:

- 70 counties are complete
- 31 counties are still pending
- this memo is based on the 70 completed county fits

## Main Findings In Plain Language

The strongest result in the completed models is July weather stress. Higher `vpdmax_7` is associated with lower yield in every completed county. This is the most stable and most consistent signal in the run so far.

Soil productivity, represented by `nccpi3corn`, is usually associated with higher yield, but that pattern is less universal than the July weather result.

The tillage findings are more nuanced. Looking only at the main effects, both `high tillage (0)` and `low tillage (1)` are usually associated with lower yield relative to `NA tillage`. But the model also shows that the tillage relationship changes as July weather stress changes. In other words, tillage does not appear to have one simple, fixed statewide effect.

## Direct Answer To The Tillage Question

### What effect does high (0) tillage and/or low (1) tillage have over NA tillage on crop yield?

The best interim answer is:

- `high tillage (0)` is usually associated with **lower yield than NA tillage**
- `low tillage (1)` is also usually associated with **lower yield than NA tillage**
- the difference between `0` and `1` is small enough that there is **no clear statewide winner** between them in the completed fits

Why we can say that:

- among 70 completed counties, the `tillage_0_prop` coefficient is negative in 67 counties
- among 70 completed counties, the `tillage_1_prop` coefficient is negative in 67 counties
- the 95% interval excludes zero in 65 counties for `tillage_0_prop`
- the 95% interval excludes zero in 63 counties for `tillage_1_prop`

This means that, at average local July weather and average local soil productivity, shifting the tillage mix away from `NA tillage` and toward either `0` or `1` usually corresponds to lower predicted yield.

At the same time, the model does **not** show a strong statewide difference between `0` and `1`:

- in 34 completed counties, `0` has the more negative main effect
- in 36 completed counties, `1` has the more negative main effect

That split is essentially even. So the safer interpretation is not “0 is worse” or “1 is worse” statewide. The safer interpretation is that both tend to underperform relative to `NA tillage`, and local conditions determine which one looks slightly less or more unfavorable.

## Why The Answer Depends On July Weather

The model finds strongly positive interaction terms for:

- `tillage_0_prop:vpdmax_7`
- `tillage_1_prop:vpdmax_7`

That means the negative tillage main effects weaken as July `vpdmax_7` rises. Put simply:

- under average July conditions, more `0` or `1` tillage share usually points to lower yield than `NA tillage`
- under more stressful July conditions, that negative gap becomes smaller
- in some counties, under sufficiently high July stress, the net effect could approach neutral

This does **not** mean tillage is suddenly beneficial everywhere under high VPD. It means the model sees tillage effects as conditional on weather stress rather than fixed across all environments.

## How Strong The Overall Signals Are

Across the 70 completed counties:

- `vpdmax_7` is negative in all 70 counties
- `nccpi3corn` is positive in 68 of 70 counties
- `tillage_0_prop:vpdmax_7` is positive in all 70 counties
- `tillage_1_prop:vpdmax_7` is positive in 69 of 70 counties

The practical meaning is straightforward: weather stress is driving a large share of the yield story, and the tillage terms matter most because they modify how yield responds under those weather conditions.

## What Stakeholders Should Take Away

The completed fits do **not** support a simple message that either `high tillage (0)` or `low tillage (1)` is generally better than the other statewide. They do support a simpler and more defensible message:

- July weather stress is the dominant factor
- soil productivity matters
- compared with `NA tillage`, both `0` and `1` tillage are usually associated with lower yield under average conditions
- the size of that disadvantage changes with July weather stress

## Important Cautions

- This is an interim readout based on 70 completed counties, not the full 101-county run.
- The model uses standardized predictors within each county-neighborhood dataset, so coefficient sizes should be read as relative local effects, not as a simple statewide bushels-per-acre conversion.
- These are observational model results, not causal treatment effects.
- The tillage comparison is against the omitted `NA tillage` share in the model, not against a separately randomized management treatment.

## Bottom Line

If asked for a plain-language answer now, the best summary is:

`High tillage (0)` and `low tillage (1)` both generally look worse than `NA tillage` for yield at average local conditions, but neither one clearly beats the other statewide, and both depend strongly on July weather stress.

# Neighborhood Fit Interim Report

Date: 2026-04-13

## Snapshot

This is an interim readout of the checkpointed neighborhood corn-yield model run on HPCC. As of 2026-04-13, the remote fit directory at `/mnt/scratch/weave151/fits` contains:

- 70 counties marked `process complete.`
- 31 counties still pending
- 0 counties currently marked `resume to process the next batch`

This is ahead of the earlier 60-of-101 count. The results below use the 70 completed county fits that were available on 2026-04-13.

## Model Summary

The active script is [Neighborhood_fit_chkptstanr.R](C:/Joe%20Local%20Only/College/Research/landcore-etl-pipelines/scripts/model/Neighborhood_fit_chkptstanr.R). The checkpointed model:

- fits one model per focal county using a neighborhood dataset that includes the focal county plus nearby counties
- predicts `unscaled_yield`
- standardizes `tillage_0_prop`, `tillage_1_prop`, `nccpi3corn`, and `vpdmax_7` within each county-neighborhood training dataset before fitting
- includes `year = year - 2010`
- uses a random intercept for `tile_field_ID`

The fitted formula is:

```r
unscaled_yield ~
  tillage_0_prop +
  tillage_1_prop +
  nccpi3corn +
  vpdmax_7 +
  tillage_0_prop*vpdmax_7 +
  tillage_1_prop*vpdmax_7 +
  tillage_0_prop*nccpi3corn +
  tillage_1_prop*nccpi3corn +
  year +
  (1 | tile_field_ID)
```

## Coverage Of The Completed Fits

Across the 70 completed counties:

- median row count per fit: 65,904.5
- mean row count per fit: 62,181.5
- row-count range: 1,799 to 123,772
- median unique field count: 7,270.5
- mean unique field count: 6,531.7
- field-count range: 288 to 11,950
- median neighborhood size: 7 counties
- neighborhood-size range: 4 to 9 counties

This means most completed models are being fit on fairly large pooled neighborhood datasets, but a few counties are much smaller and should be treated more cautiously.

## Interim Narrative

The completed fits already point to a clear story. July atmospheric stress, represented by `vpdmax_7`, is the most stable predictor in the model so far. Across all 70 completed county-neighborhood fits, the coefficient is negative, and in every completed county the 95% interval stays below zero. That makes July VPD the strongest consistent signal in the current run.

The tillage signals appear important as well, but not mainly as simple stand-alone effects. The main effects for `tillage_0_prop` and `tillage_1_prop` are usually negative across completed fits, which suggests that higher values of these tillage proportion measures are often associated with lower yield after controlling for the other terms in the model. At the same time, both tillage-by-`vpdmax_7` interaction terms are strongly and consistently positive. That means the relationship between tillage and yield is conditional on July weather stress. The practical reading is that tillage is not acting independently; its association with yield changes materially as July conditions become more stressful.

`nccpi3corn` is the next most interpretable predictor. It is positive in most completed counties and often credibly above zero, but it is not as universal as `vpdmax_7`. This suggests soil productivity is contributing useful explanatory power, though the size of that contribution is more local and county-neighborhood specific than the July weather effect. The interactions between tillage and `nccpi3corn` lean negative in many counties, which may indicate that the marginal yield relationship associated with tillage becomes less favorable in higher-productivity settings. That pattern is present often enough to be meaningful, but it is less uniform than the VPD result and should be described more cautiously.

The `year` term varies in sign across counties. That indicates the model is detecting real temporal structure, but not a single statewide directional trend. Some county-neighborhood fits show upward year effects, others show downward effects, which is consistent with the idea that the local production context differs across Illinois.

## What We Learned So Far

The strongest and most consistent signal in the completed fits is July `vpdmax_7`. It is negative in all 70 completed counties, and the 95% interval excludes zero in all 70. Across counties, the mean coefficient is about `-395.1`, with county-specific estimates ranging from about `-246.5` to `-480.2`.

Both tillage proportion main effects are also usually negative:

- `tillage_0_prop`: negative in 67 of 70 counties, with 63 counties showing a negative 95% interval that excludes zero
- `tillage_1_prop`: negative in 67 of 70 counties, with 62 counties showing a negative 95% interval that excludes zero

`nccpi3corn` is generally positive, but less universal than July VPD:

- positive in 68 of 70 counties
- positive with 95% interval excluding zero in 41 of 70 counties

The interaction pattern is consistent and important:

- `tillage_0_prop:vpdmax_7` is positive in all 70 completed counties and excludes zero in 69
- `tillage_1_prop:vpdmax_7` is positive in 69 of 70 counties and excludes zero in 65

That means the negative main effect of `vpdmax_7` is highly consistent, but its effect is moderated by tillage composition. In plain language: hotter/drier July conditions are associated with lower yield almost everywhere, and the size of that penalty changes with the tillage signal.

The `nccpi3corn` interactions are weaker and more mixed, but they lean negative:

- `tillage_0_prop:nccpi3corn` is negative in 56 of 70 counties and excludes zero in 44
- `tillage_1_prop:nccpi3corn` is negative in 61 of 70 counties and excludes zero in 49

This suggests a plausible pattern where better corn productivity potential is associated with higher yield overall, but the marginal tillage effects become less favorable as `nccpi3corn` rises. That pattern is present often enough to take seriously, but it is not as uniform as the July VPD signal.

The `year` term is not directionally uniform across counties:

- positive in 40 of 70 counties
- negative in 30 of 70 counties
- excludes zero in 63 of 70 counties, split across both signs

So there is a real year-related trend in many local fits, but its direction depends on county context rather than moving uniformly statewide.

## County Summary Table

The table below gives a compact count of how often each coefficient shows the same directional pattern across the 70 completed fits.

| Term | Mean estimate | Counties with positive estimate | Counties with negative estimate | 95% intervals excluding zero |
|---|---:|---:|---:|---:|
| `vpdmax_7` | -395.10 | 0 | 70 | 70 |
| `nccpi3corn` | 33.72 | 68 | 2 | 41 |
| `tillage_0_prop` | -35.14 | 3 | 67 | 65 |
| `tillage_1_prop` | -36.72 | 3 | 67 | 63 |
| `tillage_0_prop:vpdmax_7` | 16.91 | 70 | 0 | 69 |
| `tillage_1_prop:vpdmax_7` | 16.67 | 69 | 1 | 65 |
| `tillage_0_prop:nccpi3corn` | -4.91 | 14 | 56 | 44 |
| `tillage_1_prop:nccpi3corn` | -4.92 | 9 | 61 | 49 |
| `year` | 2.44 | 40 | 30 | 63 |

## Practical Interpretation

At this stage, the most defensible conclusions are:

1. July atmospheric dryness or heat stress, represented by `vpdmax_7`, is the clearest and most stable predictor in the completed runs.
2. The tillage proportion signals are usually associated with lower yield in their main effects, but they do not operate in isolation.
3. The tillage-by-`vpdmax_7` interactions are consistently positive, so the relationship between tillage and yield depends on July weather stress.
4. `nccpi3corn` is generally helpful as a secondary positive predictor, but its contribution is more county-specific than `vpdmax_7`.
5. The model is finding strong local heterogeneity, which is exactly what we would expect from fitting separate focal-county neighborhood models instead of one pooled statewide model.

## Examples From Completed Counties

County `17081` is representative of the broader pattern:

- `tillage_0_prop`: `-29.45` with 95% interval `[-31.90, -27.07]`
- `tillage_1_prop`: `-2.36` with 95% interval `[-4.80, 0.07]`
- `nccpi3corn`: `12.55` with 95% interval `[9.33, 15.73]`
- `vpdmax_7`: `-433.98` with 95% interval `[-436.44, -431.46]`
- `tillage_0_prop:vpdmax_7`: `16.63` with 95% interval `[14.39, 18.93]`
- `tillage_1_prop:vpdmax_7`: `27.53` with 95% interval `[24.88, 30.15]`

Some cross-county extremes help show the range of behavior:

- strongest `vpdmax_7` penalty among completed fits: county `17023`, estimate about `-480.24`
- weakest `vpdmax_7` penalty among completed fits: county `17031`, estimate about `-246.52`
- largest positive `nccpi3corn` effect among completed fits: county `17169`, estimate about `71.80`
- strongest negative `tillage_0_prop` effect among completed fits: county `17093`, estimate about `-75.21`
- strongest negative `tillage_1_prop` effect among completed fits: county `17175`, estimate about `-75.60`

## Caveats

- These coefficients are on the scale of standardized predictors, so they should be interpreted as within-dataset relative effects, not simple per-unit agronomic effects.
- The run is still incomplete: 31 counties are pending, so statewide summaries may still move.
- The model pools each focal county with neighboring counties, so these are local-neighborhood fits, not isolated single-county fits.
- This report is based on coefficient summaries from finished fits. It does not yet include posterior predictive checks, out-of-sample validation, or residual diagnostics.

## Pending Counties

As of 2026-04-13, the counties still pending are:

`17011`, `17019`, `17029`, `17037`, `17039`, `17041`, `17045`, `17053`, `17057`, `17063`, `17071`, `17073`, `17075`, `17091`, `17095`, `17099`, `17105`, `17107`, `17113`, `17115`, `17125`, `17131`, `17135`, `17147`, `17167`, `17173`, `17183`, `17191`, `17193`, `17195`, `17203`

## Bottom Line

Even before the full run is finished, the completed county fits already support a clear interim story: July `vpdmax_7` is the dominant and most consistent negative predictor of corn yield, `nccpi3corn` is usually positive but less universal, and the tillage signals matter most through their interactions with July weather stress rather than as simple stand-alone effects. The field-level random intercept lets repeated observations from the same field share information, while the county-neighborhood fitting strategy preserves local heterogeneity instead of forcing one statewide response pattern.

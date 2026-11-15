---
title: Analysis plan
description: The outcome, predictors, six models, evaluation and promotion rules used by every training run.
last_updated: 2026-11-08
---

# Analysis plan

Every training run follows this plan. The plan changes during development, so each run records the plan's SHA-256 and the Git commit in MLflow. A result therefore always names the plan version of its run.

## Unit, populations and timing

- The unit of analysis is the hospital-window: one CMS Certification Number (CCN) and one calendar-year healthcare-associated infection (HAI) window, keyed by `spine_key`. The spine keeps the calendar-year windows from the reviewed `first_window_year` (2018, `dbt/dbt_project.yml`) on. Earlier windows stay in the gold HAI fact and feed only the history features.
- The primary population is `is_primary_population`. Sensitivity analyses repeat every test on `is_sensitivity_population` and are marked exploratory.
- Every predictor takes the latest source period that ends before the window starts. The rule uses the measured period, not the publication date.
- The design is same-period: the model predicts the window's standardized infection ratio (SIR) from the information available at the window start. Forecasting later windows is a separate extension that adds the earlier-outcome candidates C275 to C280.

## Outcome

- The outcome is the published SIR of one HAI type, controls C269 to C274: HAI_1 CLABSI, HAI_2 CAUTI, HAI_3 SSI colon, HAI_4 SSI hysterectomy, HAI_5 MRSA and HAI_6 C. difficile. No SIR is computed from counts.
- A window without a published SIR for the type is left out of that type's analysis and counted in the run record. This covers an `alignment_status` other than aligned and tokens such as "Not Available".
- Each of the six types is a separate primary analysis with the same specification. Every research question and the six-model comparison are reported per type.

## Predictors

- The predictor families are Care Compare process and structure measures (RQ1), hospital characteristics and operations, HCRIS cost-report ratios and the Case Mix Index (CMI) (RQ4 additions) and county context (RQ4 additions). County context covers SVI, ACS, SAIPE, SAHIE, BLS, PLACES, Medicare geographic variation, CDC WONDER, the Mapping Medicare Disparities data, HPSA and MUA designations, RUCC and RUCA.
- CMI is a covariate in every model from Elastic Net on.
- C154 (influenza vaccination of healthcare personnel) is one measure under two published IDs, IMM_3_OP_27_FAC_ADHPCT up to the July 2019 release and IMM_3 after it. It takes the latest period of either ID that ends before the window starts.
- C141 (emergency department volume) and C167 (SM_7, the maternal morbidity structural measure) are categories. For C167, Yes, No and Not Applicable are categories and Not Available stays missing. It has values only for the 2022 and 2023 windows and the holdout, so the missing-share rule can drop it.
- These are never predictors: the parts of the outcome itself, the HAC Reduction Program and VBP scores, readmissions, complications and deaths. PSI-90 and PSI-13 enter only as the RQ3 mediator.
- A source field that two controls carry appears once in the feature lists. A column that is a fixed sum or midpoint of kept columns is left out: the BLS labor force, ACS occupied housing units and the SAIPE estimates between their 90 percent bounds. `config/training/redundant_features.json` lists each removed reference, the references kept in its place and the reason.
- The OLS, mixed effects, spatial and fixed-effects models drop any remaining column that is an exact linear combination of the others, found by QR decomposition on the fitting rows only. The full side of the RQ4 test does the same. Value columns are kept before missing-value indicators and category levels. CMI is never dropped. The RQ4 test keeps its CMS-only columns. The same columns are dropped from the holdout and prediction rows. Every fold and final fit records the dropped columns.

## Missing values

- A predictor that has no source period before the window stays missing. No value is filled with zero.
- Each run logs the share of missing values per predictor. A predictor missing in more than 50% of the training rows is dropped before fitting. Other thresholds run only as labeled exploratory reruns.
- In the linear and likelihood models, a missing value takes the median of the predictor, computed inside each training fold. Each such predictor also gets a column that marks the missing rows. Every model then scores the same holdout rows.
- Gradient boosting uses its own missing-value handling.

## Split and evaluation

- The newest window year with published SIRs is the holdout. All earlier years are training data.
- Tuning uses cross-validation inside the training years, grouped by hospital: all windows of one CCN fall in one fold. The panel model uses time-ordered folds, each with at least three fitting years.
- All learned preprocessing (imputation, scaling, encoding and selection) is fitted inside the training folds only.
- Holdout metrics are RMSE (primary), MAE and R². The likelihood models also report AIC and BIC.

## The six models

1. **OLS:** the original capstone specification on the CMS-only predictors, as the baseline.
2. **Elastic Net:** all predictor families with CMI forced in. The grouped cross-validation tunes alpha and the L1 ratio.
3. **Mixed effects (RQ2):** a random intercept per ownership group, taken from the ownership history as of the window start. The model reports the intraclass correlation (ICC) and a likelihood-ratio test against the fixed-only model (H2).
4. **Tree model with SHAP:** gradient boosting (XGBoost) is the primary tree model, with SHAP values on the holdout. Random Forest runs as a check and is never promoted.
5. **Spatial (RQ5):** Moran's I on the holdout-year residuals of the best non-spatial model. If it is significant at 0.05 (H5), a spatial lag or spatial error model follows, chosen by Lagrange multiplier tests.
6. **Fixed-effects panel (RQ6):** hospital fixed effects across window years with time-varying predictors. A holdout hospital without an estimated effect gets the mean effect. The run counts these hospitals.
   - The model fits complete cases on its own predictor set and adds no missing-value flags. This is its exception to the missing-value rule above. The run counts the rows left out. A holdout row with a missing predictor is still scored, with the fitting median.
   - Every fit needs at least three fitting years. The time-ordered folds therefore validate the fourth training year onward.

For Moran's I, two hospitals are neighbours when their counties are the same or adjacent in the county adjacency table. The weights are row-standardized. Hospitals with no neighbour are counted and left out of the test.

## Model runs

- A training run fits one model, m1 to m6 in the order of the list above. It reads the Feast feature service `model_<id>` of its feature set and runs only the tests that model owns. A run logs the tags `model` and `base_run`, the identity of the base publication that its feature set was built from.
- m1 runs the joint F test of RQ1 and fits the reduced model of RQ4 (the CMS-only list). m2 runs the Elastic Net comparison with the intercept and CMI model (RQ1), fits the full model of RQ4 on its own list and runs the mediation (RQ3). m3 reports the ownership test (RQ2), m4 the SHAP values, m5 the RQ5 test and m6 the RQ6 test. HAC validation refits each model's own specification.
- m5 has no other model's residuals, so its Moran's I test uses the residuals of an OLS fit on the m5 list.
- The research job runs the tests that need two models, once the models have finished runs with the same `base_run`: RQ4 compares the reduced and the full fit on the same rows and holdout windows.

## Research question tests

- **RQ1 (H1):** the joint significance of the process and structure measures in the OLS and Elastic Net models: an F test on OLS and the holdout RMSE against a model with only an intercept and CMI.
- **RQ2 (H2):** model 3.
- **RQ3 (H3):** mediation of the process measures on SIR through PSI-90, by the product of coefficients with a bootstrap confidence interval for the indirect effect (5,000 resamples). The result is associational, not causal.
- **RQ4 (H4):** a nested comparison of CMS-only against CMS plus HCRIS, CMI and county context on the same rows: likelihood-ratio and F tests in-sample and the holdout RMSE difference with a bootstrap interval.
- **RQ5 (H5):** model 5.
- **RQ6 (H6):** a Mundlak test on model 6. The outcome is regressed on the time-varying predictors, their hospital means and the window-year effects. H6 is the joint Wald test W that the coefficients of the hospital means are zero, with hospital-clustered standard errors (CR1). Its p-value comes from F = W / q on q and G - 1 degrees of freedom, with q the number of tested slopes and G the number of hospital clusters. A rejection means the hospital effects correlate with the predictors. The record holds the Wald statistic, the F statistic, both degrees of freedom, the p-value and the number of hospital clusters. The test needs at least 50 clusters and refuses a smaller design with its reason.
- **Validation:** hospitals are ranked by predicted SIR and compared with the HAC Reduction Program worst-performer flag of the matching program year. The newest window year with published SIRs is Y. The matching program year is the one whose two-year HAI performance period covers windows Y-1 and Y, as the reviewed year map `config/training/hac_year_mapping.json` records with its known weak spots. A hospital with a row in only some years of the period is dropped from the comparison and counted.
  1. A validation-only fit takes the selected model specification (the same predictors and tuning grid) and trains on the windows through Y-2, so no window of the flag's period is in its training data.
  2. It predicts windows Y-1 and Y. Each hospital's score is the average of its predictions over those windows.
  3. If the program year's period covers one year only, the fit predicts and compares that window alone. The result records this.
  4. The result validates the selected specification, not the final fitted model. The validation-only fit is never saved, promoted or counted in a research question test.
  5. The run reports the AUC and the share of flagged hospitals in the top predicted quartile. A hospital without a flag stays missing and is counted.
  6. The flag is never a training input.
- **Multiple testing:** each of the six hypotheses is reported at 0.05 with its exact p-value and no correction across hypotheses. Sensitivity analyses are marked exploratory.

## Tracking and promotion

- MLflow records, for every run, the plan's SHA-256, the Git commit, the DVC data version, the Iceberg snapshot IDs, the feature list, the target type, the parameters, the metrics and the SHAP and diagnostic artifacts.
- Only OLS, Elastic Net and gradient boosting can be promoted. The other models answer research questions and need inputs that the prediction API does not have.
- The champion and the challenger are scored on the same newest-period holdout. A challenger is promoted only when all three conditions hold:
  1. Its RMSE is at least 2% lower.
  2. The bootstrap 95% interval of the improvement excludes zero.
  3. Its RMSE is at most 5% worse in every hospital group of at least 100 holdout hospitals. The groups are ownership type, hospital subtype, census region and bed-size band (under 100, 100 to 299, 300 and over).
- While a type has no champion, its candidate is compared with the intercept and CMI model fitted on the training years instead. It can become the first champion when its provenance check passed and conditions 1 and 2 hold against that model.
- A promoted model then goes through the canary. The one exception is the first champion of a type: while the registry has no Production model there is nothing to run a canary beside, so a challenger whose verdict is `pass` or `pass_baseline` is promoted without one (one-time exception, owner decision of Nov 15 2026). It also skips the check for a held online load before the rollout, because a first deploy has none. Every later promotion keeps the canary and the check.

## Reproducibility

Each run fixes and logs its random seeds. The training image pins every package version. The training input is the DVC-versioned extract named in the run. A rerun on the same input gives the same metrics.

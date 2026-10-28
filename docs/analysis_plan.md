---
title: Analysis plan
description: The outcome, predictors, six models, evaluation and promotion rules used by every training run.
last_updated: 2026-10-23
---

# Analysis plan

Every training run follows this plan. The plan changes during development, so each run records the plan's SHA-256 and the Git commit in MLflow. A result therefore always names the plan version it used.

## Unit, populations and timing

- The unit of analysis is the hospital-window: one CMS Certification Number (CCN) and one calendar-year healthcare-associated infection (HAI) window, keyed by `spine_key`.
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
- These are never predictors: the parts of the outcome itself, the HAC Reduction Program and VBP scores, readmissions, complications and deaths. PSI-90 and PSI-13 enter only as the RQ3 mediator.

## Missing values

- A predictor that has no source period before the window stays missing. No value is filled with zero.
- Each run logs the share of missing values per predictor. A predictor missing in more than 50% of the training rows is dropped before fitting. Other thresholds run only as labelled exploratory reruns.
- In the linear and likelihood models, a missing value takes the median of the predictor, computed inside each training fold. Each such predictor also gets a column that marks the missing rows. Every model then scores the same holdout rows.
- Gradient boosting uses its own missing-value handling.

## Split and evaluation

- The newest window year with published SIRs is the holdout. All earlier years are training data.
- Tuning uses cross-validation inside the training years, grouped by hospital: all windows of one CCN fall in one fold. The panel model uses time-ordered folds.
- All learned preprocessing (imputation, scaling, encoding and selection) is fitted inside the training folds only.
- Holdout metrics are RMSE (primary), MAE and R². The likelihood models also report AIC and BIC.

## The six models

1. **OLS:** the original capstone specification on the CMS-only predictors, as the baseline.
2. **Elastic Net:** all predictor families with CMI forced in. The grouped cross-validation tunes alpha and the L1 ratio.
3. **Mixed effects (RQ2):** a random intercept per ownership group, taken from the ownership history as of the window start. The model reports the intraclass correlation (ICC) and a likelihood-ratio test against the fixed-only model (H2).
4. **Tree model with SHAP:** gradient boosting (XGBoost) is the primary tree model, with SHAP values on the holdout. Random Forest runs as a check and is never promoted.
5. **Spatial (RQ5):** Moran's I on the holdout-year residuals of the best non-spatial model. If it is significant at 0.05 (H5), a spatial lag or spatial error model follows, chosen by Lagrange multiplier tests.
6. **Fixed-effects panel (RQ6):** hospital fixed effects across window years with time-varying predictors. A Hausman test compares the model against random effects (H6). A holdout hospital without an estimated effect gets the mean effect. The run counts these hospitals.

For Moran's I, two hospitals are neighbours when their counties are the same or adjacent in the county adjacency table. The weights are row-standardized. Hospitals with no neighbour are counted and left out of the test.

## Research question tests

- **RQ1 (H1):** the joint significance of the process and structure measures in the OLS and Elastic Net models: an F test on OLS and the holdout RMSE against a model with only an intercept and CMI.
- **RQ2 (H2):** model 3.
- **RQ3 (H3):** mediation of the process measures on SIR through PSI-90, by the product of coefficients with a bootstrap confidence interval for the indirect effect (5,000 resamples). The result is associational, not causal.
- **RQ4 (H4):** a nested comparison of CMS-only against CMS plus HCRIS, CMI and county context on the same rows: likelihood-ratio and F tests in-sample and the holdout RMSE difference with a bootstrap interval.
- **RQ5 (H5) and RQ6 (H6):** models 5 and 6.
- **Validation:** hospitals are ranked by predicted SIR and compared with the HAC Reduction Program worst-performer flag of the matching program year. The run reports the AUC and the share of flagged hospitals in the top predicted quartile. The flag is never a training input.
- **Multiple testing:** each of the six hypotheses is reported at 0.05 with its exact p-value and no correction across hypotheses. Sensitivity analyses are marked exploratory.

## Tracking and promotion

- MLflow records, for every run, the plan's SHA-256, the Git commit, the DVC data version, the Iceberg snapshot IDs, the feature list, the target type, the parameters, the metrics and the SHAP and diagnostic artifacts.
- Only OLS, Elastic Net and gradient boosting can be promoted. The other models answer research questions and need inputs that the prediction API does not have.
- The champion and the challenger are scored on the same newest-period holdout. A challenger is promoted only when all three conditions hold:
  1. Its RMSE is at least 2% lower.
  2. The bootstrap 95% interval of the improvement excludes zero.
  3. Its RMSE is at most 5% worse in every hospital group of at least 100 holdout hospitals. The groups are ownership type, hospital subtype, census region and bed-size band (under 100, 100 to 299, 300 and over).
- A promoted model then goes through the canary.

## Reproducibility

Each run fixes and logs its random seeds. The training image pins every package version. The training input is the DVC-versioned extract named in the run. A rerun on the same input gives the same metrics.

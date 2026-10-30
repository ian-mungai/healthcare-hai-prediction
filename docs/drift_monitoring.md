---
title: Drift monitoring
description: What the drift check compares, its configuration values and the minimum of current rows that the owner reviews.
last_updated: 2026-10-30
audience: [developer, operator]
---

# Drift monitoring

The drift check asks one question for each infection type: have the features of the newest hospital windows moved away from the features the Production model was trained on? If at least half of the model's features have moved, the drift DAG asks for a new training run. The owner still approves every real training run and every promotion.

## What is compared

- The reference is the training matrix of the model that holds the `production` alias, loaded from DVC through its MLflow run.
- The current rows are the windows of the same population whose year is after the newest year in the reference. A window without a published infection ratio is kept, because those are the windows the model has not been scored on.
- Only the model's own features are compared. Only the primary population can start a training run. A sensitivity run is never mixed in.
- Evidently's dataset drift report makes the comparison. The tests are named in the call because Evidently chooses other tests above 1000 reference rows.

## The configuration

`config/monitoring/drift.json` holds the values. Its schema, `data_contracts/monitoring/drift.schema.json`, pins the first six rows so that a change needs a new owner decision and a schema change together.

| Setting | Value | Meaning |
| --- | --- | --- |
| `drift_share` | 0.5 | The share of drifted features that starts retraining. |
| `share_boundary_inclusive` | true | Exactly half counts as drift. |
| `numeric_test` | `ks` | Kolmogorov-Smirnov for numeric features. |
| `categorical_test` | `chisquare` | Chi-square for categorical features. |
| `test_threshold` | 0.05 | A feature drifts when its p-value is below this. |
| `trigger_populations` | `primary` | Only the primary population can start retraining. |
| `minimum_current_rows` | 100 | A report with fewer current rows is skipped and never starts retraining. |
| `deadline_seconds` | 600 | The check ends with a named failure after this time. |

The owner decided on a minimum of 100 current rows and keeps it. It was not derived from the data. The owner reviews it once the real number of current rows per infection type is known, because a type with few reported windows could be skipped every time while a type with many windows would still pass a much higher minimum.

A skipped report records its reason: `no_new_window`, `too_few_current_rows` or `population_not_monitored`. A feature with no value anywhere in the current window is refused by name and blocks that type's report. The owner decided this: the report gives no verdict and starts no retraining until someone looks at the feature.

## Where the parts are

- `monitoring/drift_check.py` compares the two matrices and writes one record per type, `drift_<type>.json`.
- `monitoring/baseline.py` and `monitoring/current_window.py` produce the two sides of the comparison.
- `monitoring/aws_run.py` runs the phases of the drift task: `baseline`, `current`, `check` and `publish`. A type without a Production model is recorded as not monitored. Publish writes the records to `drift/<run>/` in the data bucket and the marker `_complete.json` last.
- `dags/drift_monitoring_dag.py` starts the task through the broker template `drift_run`. It then reads the marker and the records and starts one `training_dag` run for all drifted types. The training run id and the trigger run id come from the drift run id, so a retry starts nothing new.
- `infra/jobs/drift.tf` registers the task behind `drift_enabled`. `infra/iam/drift.tf` prepares its role behind `drift_prepared`. The image is defined in `services/drift/`.

## Checks

```sh
.venv/bin/python -m monitoring.run_drift_e2e
```

The runner writes `data/e2e/drift/<UTC>/report.json`. Cases that run Evidently report `blocked` until the pinned Evidently environment exists in `.tools/evidently`. A blocked case is not a pass. The Terraform tests are `terraform test` in `infra/jobs` and `infra/iam`.

## Not yet proven

Nothing here has run against AWS, MLflow, Feast or Airflow. The checks use stand-ins for the MLflow run store, the feature reader, S3 and the Airflow SDK. Each check says so in its own description.

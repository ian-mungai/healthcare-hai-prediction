---
title: Model API
description: The private FastAPI model API, its canary rollout, the deploy DAG, the owner's steps and what is not yet proven.
last_updated: 2026-10-30
audience: [developer]
---

# Model API

The model API predicts the standardized infection ratio of one hospital-window from public CMS hospital data. It is a portfolio system on public data. It is not for clinical use. The service is private and off by default. Nothing here has run against AWS or in a container. The offline cases cover the service, the canary, the deploy action, the DAG contract, the Terraform plans and the metrics. The first real run proves the rest.

## What it is

| Part | Where | What it does |
| --- | --- | --- |
| Service core | `scripts/serving/service.py` | Checks the key, applies the rate limit, validates the request, routes it, reads features, predicts and records |
| Web adapter | `scripts/serving/api.py` | The FastAPI routes `/v1/predict`, `/v1/health` and `/metrics`. It holds no rule of its own |
| Deploy action | `scripts/serving/deploy.py` | Writes the active deployment of one HAI type. It never moves the production alias |
| Deploy DAG | `dags/serving_deploy_dag.py` | Runs the deploy action through the job broker |
| Replay | `scripts/serving/replay.py` | Sends a fixed list of hospital-windows and compares the two models |
| Service flag | `infra/services/model_api.tf` | `model_api_enabled`, default false |

The API binds the host loopback address. The only way in is a Session Manager port forward. A public load balancer is a later and separate step.

## Request and answers

`POST /v1/predict` with the header `X-API-Key` and a JSON body of exactly `hai_type`, `ccn` and `window_year`. The optional header `X-Request-ID` is kept when it is safe text and replaced otherwise.

| Status | Meaning |
| --- | --- |
| 200 | The prediction, the model version, whether the stable or the canary model answered, the date the features are as of and the feature schema hash |
| 401 | The key is missing, empty or unknown. All three answer the same |
| 404 | The hospital-window is not in the online store |
| 400 | The declared body length is not a number |
| 408 | The body did not arrive within 5 seconds |
| 413 | The body is over 1 KiB. The limit counts the bytes received, so a body with no declared length cannot pass it |
| 422 | The body is malformed |
| 429 | The key is over its rate. The header `Retry-After` gives the seconds to wait |
| 503 | The online store cannot serve the request: it is loading, holds another feature schema, did not answer, holds no row of a view the model needs or lacks a feature. Or the model failed or returned a number that is not finite. Or the prediction buffer is full, which sends `Retry-After: 10` |

The key check comes first, then the rate limit. Only then is the body read, with its size and time limits. Validation follows. A refused request reads no feature and writes no row. `GET /v1/health` answers 200 when every served HAI type has a loaded stable model and 503 otherwise. The OpenAPI document is at `/v1/openapi.json`.

## API keys

The service holds the SHA-256 hash of each key under a short identifier and never the key. The identifier names the caller in the rate limit and in each prediction row. No key reaches a log line, an answer, a row or a metric label.

Make a key with the project tool, not with a shell recipe. The tool writes the key to a file with no trailing newline and a mode only the owner can read. It prints the hash of exactly those bytes, which is what a caller presents. It never prints the key and never overwrites an existing key file:

```sh
umask 077
mkdir "$TMPDIR/api_key_folder"
.venv/bin/python -m scripts.serving.keys new --id <KEY_ID> --output "$TMPDIR/api_key_folder"
```

Merge the printed entry into the JSON object in the Parameter Store parameter `model_api_key_hashes` and give the key file to its caller through a channel the owner chooses. Callers send the file's text as the `X-API-Key` header. Remove the folder when done. To rotate a key, add the new entry, restart the service, then remove the old entry and restart again.

## Rate limit

Each identified key has a token bucket held in the process. The defaults are 60 requests a minute and a burst of 20, set by `model_api_rate_per_minute` and `model_api_burst`. One service process runs, so the bucket needs no shared store. A restart gives every key a full bucket. A caller who sends unknown keys creates no bucket.

## Feature reads

The service follows the serving read contract of the online feature store (`scripts/features/serving_contract_client.py`). It reads the Redis marker, then the features of the model, then the marker again. It answers 503 if the marker is not ready, holds another schema hash than the model was trained with or changed during the read. It answers 404 when the hospital-window was never written. Every Redis call has a one second timeout and none is retried. A feature that the store does not answer gives 503 and no partial prediction. A requested view whose row is absent (no event time) gives 503, because an absent row is not a value published as null. A value published as null reaches the model as NaN. The model's own preprocessing decides.

## Two versions in one process

A deployment record names the stable version, at most one canary version and the canary's share. The stable version is the production alias at the time of the deploy. The service keeps both models in memory for each served HAI type.

| Rule | Behavior |
| --- | --- |
| Split | The bucket is the first eight bytes of the SHA-256 of `deployment id, HAI type, ccn, window year`, taken modulo 10,000. A hospital-window goes to the canary when its bucket is below the share times 100 |
| Determinism | One hospital-window meets the same version under one deployment, in every process. A new deployment reshuffles the groups |
| Share | 1 to 50 percent |
| Canary fails a request | The request falls back to the stable model and the row records both versions. A failure is an exception or a prediction that is not a finite number. A feature error that belongs to the canary also counts: another schema hash, an absent view row or a missing feature. A store that is loading or did not answer fails every model alike and counts against none |
| Canary fails three in a row | The canary is ejected until the next deployment. The stable model keeps serving |
| Canary does not load | The stable model serves every request. Health reports the canary as `failed`. Each refresh tries the load again. A model that loads then serves |
| Stable fails | Only the requests routed to it answer 503. The canary still answers its own. A stable model that did not load is tried again at each refresh. Health turns 200 when it loads |
| Refresh | The service reads the active record at most every 30 seconds. The first request after that interval pays for the read and for any model load. An unreadable record keeps the last good deployment |
| Rollback | A deployment without a candidate. After the next refresh every request goes to the stable model and the canary model is released |

## Canary proof

There are no real callers. The proof of the canary is replayed requests, not live traffic. A replay shows that the split is deterministic and close to its share. It also shows how the two models' predictions differ on the same inputs. It does not show how the canary behaves under real use.

Run a replay through the port forward. The corpus is a text file with one `ccn,window_year` line for each hospital-window. The key comes from the environment and is never printed:

```sh
MODEL_API_KEY="$(cat "$TMPDIR/api_key_folder/api_key")" .venv/bin/python -m scripts.serving.replay \
  --url http://127.0.0.1:8000 --hai-type HAI_1 --corpus hospital_windows.csv --output replay_report.json
```

Each request has one deadline for the whole exchange. A result that arrives after the replay deadline is refused. The report holds the status counts and the split. It also holds a comparison of the two prediction sets: counts, mean, three quantiles, the Kolmogorov-Smirnov distance and the mean shift. A side with fewer than 30 rows gets no comparison. The comparison judges nothing. Whether the canary becomes the production model is the owner's decision, through the promotion DAG.

## Deploying

`serving_deploy_dag` has the steps `acquire_run`, `snapshot_serving`, `serve_deploy` and `run_record`, then the shared release steps. Its parameters are the run id and the mode. The DAG carries no model parameter. The owner stages a request file beside the run's code, as for a promotion:

```sh
printf '{"hai_type": "<HAI_TYPE>", "candidate_version": "<VERSION>", "percent": <PERCENT>, "deploy_mode": "canary"}' > /tmp/serving_request.json
aws s3 cp /tmp/serving_request.json s3://<DATA_BUCKET>/lakehouse/run_stage/<RUN_ID>/inputs/serving_request.json --profile <PROJECT_PROFILE>
```

For a rollback use `"candidate_version": null`, `"percent": 0` and `"deploy_mode": "rollback"`. The job checks the request, reads the production alias and the candidate's version record, then writes the deployment. It refuses a candidate that is the production version, does not exist or has no completed bundle. A refusal writes nothing and exits with code 2.

A retried deploy has no second effect. The run id is the deployment id. Each deployment carries a sequence number one above the deployment it replaced. A record from the first release has no sequence field. It reads as sequence 1, so the service starts from it and the next deploy is numbered 2. A retry with the same parameters returns `unchanged` while that deployment is active. A retry with other parameters is refused. The history record is written once before the active record moves, so a deploy that stopped between the two writes finishes on its retry, with the stable version it recorded the first time. A retry of a deployment that a later one replaced is refused, so an old retry cannot undo a rollback. The active record is replaced only if it is still the one the deploy read. The DAG allows one active run, which closes the short gap that remains on S3.

The deploy never moves an alias. Promotion stays the owner's, through `model_promotion_dag`. After a promotion, run a deploy without a candidate so the record names the new production version as stable.

## Predictions table

Each answered request adds one row to the buffer. A background thread appends the rows to the Iceberg table `serving.predictions` every 10 seconds, with a 20 second deadline. The columns are in `data_contracts/serving/predictions.json`. A row holds the key identifier and no feature value. It never holds the key. The buffer keeps up to 5,000 rows. A request reserves its room before it reads features or asks a model. When the buffer is full the request answers 503 with `Retry-After: 10`. The answer is counted in the request counter. The service never drops a row it acknowledged. A failed append keeps the rows for the next try. One append runs at a time in a daemon thread. After a timeout the thread may still commit, so the sink remembers the rows of a late commit and does not send them again. A new append fails at once while the old one runs. At shutdown the server flushes once and asks the sink to stop. It logs a warning if the worker is still running. Rows still in memory when the process is killed are lost, because there is no log on disk. An append whose answer was lost to a failure may be sent again, so a reader removes duplicates by `request_id`. The data is public hospital data and the table is classified Public.

## Metrics

`GET /metrics` on the loopback port gives the Prometheus text format with the two families the monitoring stack names. The endpoint needs no key and the service binds loopback only.

| Metric | Type | Labels |
| --- | --- | --- |
| `hai_api_requests_total` | counter | `route`, `method`, `status` |
| `hai_api_request_duration_seconds` | histogram | `route` |

Label values come from closed sets, so the series stay bounded. Routes outside the known three count as `other`. The scrape itself is not counted. The statuses that appear are 200, 400, 401, 404, 405, 408, 413, 422, 429, 500 and 503. Any other status counts as `other`. The duration of answers for unknown paths is recorded as zero. A full prediction buffer shows as a 503 answer and cannot be told apart from other 503 answers by label, because the label set is a contract with the monitoring stack.

## Run it

The owner does every step. Show each plan first and apply exactly that plan.

Set each switch through the wrapper as `docs/infrastructure_controls.md` describes under activation inputs. The IAM stack takes its file. The platform, services and jobs stacks take a reviewed file and its digest.

1. Compile the image lock after the training image lock exists: `docker run` of `services/model_api/compile_lock.sh` as its header shows. The compile fails if a model library differs from the training lock.
2. Set `serving_enabled = true` in the platform variable file, then plan and apply the platform stack. It adds the repositories `model_api` and `streamlit`. `infra/services/images.json` lists both images. Build the image with the command in `services/model_api/Dockerfile` (the repository root is the build context) and push it with `push_images`.
3. In `infra/iam`, set `model_api_prepared = true` with `model_api_registry_key` and `model_api_manifest_prefix`. The role reads the Feast registry object and the deployment records and lists the deployment prefix, so an absent record reads as absent and not as denied. It reads the model artifacts through the MLflow server, so it holds no grant for them. It cannot append predictions yet. Set `model_api_prediction_writes = true` to let it write the data and manifest objects of the prediction table under `lakehouse/serving/` and nowhere else. Both switches default to false. The same flag adds a separate policy to the execution role that injects exactly six parameters: the key hashes, the serving writer pair, the MLflow user and credential and the page key. Set `serve_prepared = true` as well. It creates the role of the serve job, which reads and creates the deployment records and deletes none.
4. In Polaris, run `scripts.lakehouse.aws_catalog_setup principal --identity serving`. It creates the principal and the catalog role with the same privileges the silver and gold writers hold on their own namespace. It stores the pair under the names `aws_polaris_serving_client_id` and `aws_polaris_serving_client_secret`. Approve the namespace `serving` for it with `--namespace-approval`. Then run `create_table` from `scripts/serving/iceberg_sink.py` under the catalog administrator and check that the created table's location is under `lakehouse/serving/` before you enable the writes.
5. Put the four other parameters: the key hashes, the MLflow user, the MLflow credential and, for the page, its key. Their names are the defaults of the variables in `infra/iam/variables.tf`. The services stack uses the same defaults. The project user may read and put them, because the SSM policy lists them with the other managed names.
6. In `infra/jobs` set `serve_enabled = true` with the serve role and the deployment prefix. Add `serve` to `broker_jobs.families` in `infra/services` and to `broker_job_families` in `infra/iam`. In `infra/services` set `model_api_enabled = true` with the role ARN, the three S3 and MLflow addresses and the image digests. Apply each shown plan.
7. Open the port forward:

   ```sh
   aws ssm start-session --profile <PROJECT_PROFILE> --region <REGION> --target <INSTANCE_ID> \
     --document-name AWS-StartPortForwardingSession --parameters portNumber=8000,localPortNumber=8000
   ```

8. Check `http://127.0.0.1:8000/v1/health`, then stage a request file and run `serving_deploy_dag`, then replay.

To stop, set `model_api_enabled = false` and apply the shown plan. The deployment records and the predictions table stay.

## What an append calls

The catalog has `stsUnavailable` set, so it hands out no storage credentials. The sink therefore needs more than a Polaris grant. Reasoned from the sink, PyIceberg's append and the project's catalog setup, one append makes these calls:

| Call | Needs |
| --- | --- |
| Token request to Polaris | The writer's client id and credential from the parameters |
| Catalog configuration read | The catalog read privilege of the principal |
| Table load | The table read privilege of the principal |
| Read of the table's metadata and manifest list in S3 | `s3:GetObject` under `lakehouse/serving/` for the task role |
| Write of the data, manifest and manifest list files in S3 | `s3:PutObject` and the multipart actions under `lakehouse/serving/` plus `s3:ListBucket` on that prefix for the task role |
| Commit to the catalog | The table data write privilege of the principal. The catalog service then writes the new table metadata with its own role, which already covers `lakehouse/` |

The role grants no delete, so a failed commit leaves its files behind. None of this has run.

## Memory limits assumed

The host is short of memory. The service reserves 640 MiB and may use 1,280 MiB, with a 256 MiB temporary folder inside that limit. These are assumptions, not measurements. Two models for each served HAI type stay in memory, so the default serves one HAI type. With Marquez on and the API on, the hard limits of the always-on containers exceed the host minus 2 GiB on the `r7g.large` host. The plan test shows the refusal and shows that the resized host accepts both. Measure the real resident memory with the models loaded, then adjust the limits.

## Not proven

- FastAPI, uvicorn, the MLflow loader, the PyIceberg sink and the S3 deployment store against real S3 have not run. `scripts/serving/api_image_cases.py` runs the adapter over HTTP inside the image.
- That `mlflow-skinny` can load the saved models.
- That a null feature value passes through the model's preprocessing.
- That the lock resolves with the training libraries.
- Conditional S3 writes with `IfMatch` and `IfNoneMatch` against the real bucket. A stand-in client ran them.
- That the AWS broker runs the `serve_deploy` job against real AWS. The broker table, the task definition, the roles and the entry point `scripts/serving/deploy_job.py` exist and have run over stand-ins, including the S3 deployment store over a stand-in client with conditional writes. The local launcher has no template for the DAG, so a run there is refused.
- That Polaris grants the writer principal an append to a new namespace.
- Any timeout of PyIceberg's REST calls. The sink sets the S3 timeouts and the token timeout. The one-worker rule is what bounds the rest.
- That an append under the task role and the writer principal commits to `serving.predictions`. The table location, the S3 actions and the catalog privileges above come from the code and the project's catalog setup, not from a run.
- Throughput, latency and memory under load.

## Not built

- The public load balancer.
- A Docker Compose service for the local lane.
- A signed or per-key quota beyond the rate limit.

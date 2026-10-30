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
| 413 | The body is over 1 KiB |
| 422 | The body is malformed |
| 429 | The key is over its rate. The header `Retry-After` gives the seconds to wait |
| 503 | The online store is loading, holds another feature schema, did not answer, lacks a feature the model needs or the model failed |

The key check comes first, then the rate limit, then validation. A refused request reads no feature and writes no row. `GET /v1/health` answers 200 when every served HAI type has a loaded stable model and 503 otherwise. The OpenAPI document is at `/v1/openapi.json`.

## API keys

The service holds the SHA-256 hash of each key under a short identifier and never the key. The identifier names the caller in the rate limit and in each prediction row. No key reaches a log line, an answer, a row or a metric label. Create a key and its hash on the Mac without printing the key:

```sh
umask 077
openssl rand -hex 24 > "$TMPDIR/api_key"
printf '{"<KEY_ID>": "%s"}' "$(shasum -a 256 < "$TMPDIR/api_key" | cut -d ' ' -f 1)" > "$TMPDIR/key_hashes.json"
```

Merge the entry into the JSON object in the Parameter Store parameter `model_api_key_hashes` and give the key file to its caller through a channel the owner chooses. Remove both files when done. To rotate a key, add the new entry, restart the service, then remove the old entry and restart again.

## Rate limit

Each identified key has a token bucket held in the process. The defaults are 60 requests a minute and a burst of 20, set by `model_api_rate_per_minute` and `model_api_burst`. One service process runs, so the bucket needs no shared store. A restart gives every key a full bucket. A caller who sends unknown keys creates no bucket.

## Feature reads

The service follows the serving read contract of the online feature store (`scripts/features/serving_contract_client.py`). It reads the Redis marker, then the features of the model, then the marker again. It answers 503 if the marker is not ready, holds another schema hash than the model was trained with or changed during the read. It answers 404 when the hospital-window was never written. Every Redis call has a one second timeout and none is retried. A feature that the store does not answer gives 503 and no partial prediction. A feature that the store answers as null reaches the model as NaN. The model's own preprocessing decides.

## Two versions in one process

A deployment record names the stable version, at most one canary version and the canary's share. The stable version is the production alias at the time of the deploy. The service keeps both models in memory for each served HAI type.

| Rule | Behavior |
| --- | --- |
| Split | The bucket is the first eight bytes of the SHA-256 of `deployment id, HAI type, ccn, window year`, taken modulo 10,000. A hospital-window goes to the canary when its bucket is below the share times 100 |
| Determinism | One hospital-window meets the same version under one deployment, in every process. A new deployment reshuffles the groups |
| Share | 1 to 50 percent |
| Canary fails a request | The request falls back to the stable model and the row records both versions |
| Canary fails three in a row | The canary is ejected until the next deployment. The stable model keeps serving |
| Canary does not load | The stable model serves every request. Health reports the canary as `failed` |
| Stable fails | Only the requests routed to it answer 503. The canary still answers its own |
| Refresh | The service reads the active record at most every 30 seconds. The first request after that interval pays for the read and for any model load. An unreadable record keeps the last good deployment |
| Rollback | A deployment without a candidate. After the next refresh every request goes to the stable model and the canary model is released |

## Canary proof

There are no real callers. The proof of the canary is replayed requests, not live traffic. A replay shows that the split is deterministic and close to its share. It also shows how the two models' predictions differ on the same inputs. It does not show how the canary behaves under real use.

Run a replay through the port forward. The corpus is a text file with one `ccn,window_year` line for each hospital-window. The key comes from the environment and is never printed:

```sh
MODEL_API_KEY="$(cat "$TMPDIR/api_key")" .venv/bin/python -m scripts.serving.replay \
  --url http://127.0.0.1:8000 --hai-type HAI_1 --corpus hospital_windows.csv --output replay_report.json
```

The report holds the status counts and the split. It also holds a comparison of the two prediction sets: counts, mean, three quantiles, the Kolmogorov-Smirnov distance and the mean shift. A side with fewer than 30 rows gets no comparison. The comparison judges nothing. Whether the canary becomes the production model is the owner's decision, through the promotion DAG.

## Deploying

`serving_deploy_dag` has the steps `acquire_run`, `snapshot_serving`, `serve_deploy` and `run_record`, then the shared release steps. Its parameters are the run id and the mode. The DAG carries no model parameter. The owner stages a request file beside the run's code, as for a promotion:

```sh
printf '{"hai_type": "<HAI_TYPE>", "candidate_version": "<VERSION>", "percent": <PERCENT>, "deploy_mode": "canary"}' > /tmp/serving_request.json
aws s3 cp /tmp/serving_request.json s3://<DATA_BUCKET>/lakehouse/run_stage/<RUN_ID>/inputs/serving_request.json --profile <PROJECT_PROFILE>
```

For a rollback use `"candidate_version": null`, `"percent": 0` and `"deploy_mode": "rollback"`. The job checks the request, reads the production alias and the candidate's version record, then writes the deployment. It refuses a candidate that is the production version, does not exist or has no completed bundle. A refusal writes nothing and exits with code 2.

A retried deploy has no second effect. The run id is the deployment id. A retry with the same parameters returns `unchanged`. A retry with other parameters is refused. The history record is written once before the active record moves, so a deploy that stopped between the two writes finishes on its retry. The active record is replaced only if it is still the one the deploy read. The DAG allows one active run, which closes the short gap that remains on S3.

The deploy never moves an alias. Promotion stays the owner's, through `model_promotion_dag`. After a promotion, run a deploy without a candidate so the record names the new production version as stable.

## Predictions table

Each answered request adds one row to the buffer. A background thread appends the rows to the Iceberg table `serving.predictions` every 10 seconds, with a 20 second deadline. The columns are in `data_contracts/serving/predictions.json`. A row holds the key identifier and no feature value. It never holds the key. The buffer keeps up to 5,000 rows. A failed append keeps the rows for the next try. A full buffer drops the oldest rows and counts them. An append whose answer was lost may be sent again, so a reader removes duplicates by `request_id`. The data is public hospital data and the table is classified Public.

## Metrics

`GET /metrics` on the loopback port gives the Prometheus text format with the two families the monitoring stack names. The endpoint needs no key and the service binds loopback only.

| Metric | Type | Labels |
| --- | --- | --- |
| `hai_api_requests_total` | counter | `route`, `method`, `status` |
| `hai_api_request_duration_seconds` | histogram | `route` |

Label values come from closed sets, so the series stay bounded. Routes outside the known three count as `other`. The scrape itself is not counted. The duration of answers that the adapter refuses before the service sees them (unknown paths and oversized bodies) is recorded as zero.

## Run it

The owner does every step. Show each plan first and apply exactly that plan.

1. Compile the image lock after the training image lock exists: `docker run` of `services/model_api/compile_lock.sh` as its header shows. The compile fails if a model library differs from the training lock.
2. Add `model_api` to the ECR repository list of `infra/platform` and to `infra/services/images.json`. Build the image with the command in `services/model_api/Dockerfile` and push it with the push script.
3. In `infra/iam`, add a task role for the API with read access to the model artifacts, the Feast registry and the deployment prefix. Grant nothing else. Add the five new parameter names to the execution role (the `parameter_names_used` output lists them). Let the training task role write the deployment prefix.
4. In Polaris, approve the namespace `serving`, create a principal that can append to it and run `create_table` from `scripts/serving/iceberg_sink.py` under the catalog administrator.
5. Put the five parameters: the key hashes, the writer's client id, the writer's credential, the MLflow user and the MLflow credential.
6. In `infra/services` set `model_api_enabled = true` with the role ARN, the three S3 and MLflow addresses and the image digests. Apply the shown plan.
7. Open the port forward:

   ```sh
   aws ssm start-session --profile <PROJECT_PROFILE> --region <REGION> --target <INSTANCE_ID> \
     --document-name AWS-StartPortForwardingSession --parameters portNumber=8000,localPortNumber=8000
   ```

8. Check `http://127.0.0.1:8000/v1/health`, then stage a request file and run `serving_deploy_dag`, then replay.

To stop, set `model_api_enabled = false` and apply the shown plan. The deployment records and the predictions table stay.

## Memory limits assumed

The host is short of memory. The service reserves 640 MiB and may use 1,280 MiB, with a 256 MiB temporary folder inside that limit. These are assumptions, not measurements. Two models for each served HAI type stay in memory, so the default serves one HAI type. With Marquez on and the API on, the hard limits of the always-on containers exceed the host minus 2 GiB on the `r7g.large` host. The plan test shows the refusal and shows that the resized host accepts both. Measure the real resident memory with the models loaded, then adjust the limits.

## Not proven

- FastAPI, uvicorn, the MLflow loader, the PyIceberg sink and the S3 deployment store have not run. `scripts/serving/api_image_cases.py` runs the adapter over HTTP inside the image.
- That `mlflow-skinny` can load the saved models.
- That a null feature value passes through the model's preprocessing.
- That the lock resolves with the training libraries.
- Conditional S3 writes with `IfMatch` against the real bucket.
- That the AWS broker runs the `serve_deploy` job. The job file is `scripts/serving/deploy_job.py`. The broker job table and the task definition belong with the AWS broker work and are not part of this change. The local launcher has no template for the DAG, so a run there is refused.
- That Polaris grants the writer principal an append to a new namespace.
- Throughput, latency and memory under load.

## Not built

- The public load balancer.
- A Docker Compose service for the local lane.
- A signed or per-key quota beyond the rate limit.

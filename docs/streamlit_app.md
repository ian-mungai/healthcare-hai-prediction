---
title: Streamlit page
description: The Streamlit page that asks the model API for a prediction, its failures, the owner's steps and what is unproven.
last_updated: 2026-10-30
audience: [developer]
---

# Streamlit page

The Streamlit page lets a visitor enter a hospital and a year and shows the prediction that the model API returns. It is a portfolio demonstration on public data. It is not clinical guidance and not a clinical validation. The service is private and off by default. Nothing here has run in a container or against AWS. The offline cases cover the client layer against a stand-in API over real HTTP. The page cases need Streamlit's test harness, which the host environment does not install, so they have not run.

## What it is

| Part | Where | What it does |
| --- | --- | --- |
| Page | `scripts/streamlit_app/app.py` | Draws the limits, the inputs and the answer. It holds no rule of its own |
| Client | `scripts/streamlit_app/client.py` | Reads the settings, calls the API once with one deadline and returns a prediction or a named failure. It uses the standard library only |
| Words | `scripts/streamlit_app/view.py` | The limits, the introduction, the notice for each failure and the lines under a prediction |
| Stand-in API | `scripts/streamlit_app/stand_in_api.py` | A real HTTP server on the loopback address that plays each answer of the API |
| Cases | `client_cases.py`, `app_cases.py`, `run_streamlit_checks.py` in the same folder | The client and words cases, the page cases and the runner |
| Image | `services/streamlit/` | The Dockerfile, the source of the lock, the script that compiles it and the Streamlit settings file |
| Service flag | `infra/services/streamlit.tf` | `streamlit_enabled`, default false |

The page is a thin client. It gets predictions only through the API, with its key from the project's secret store. It reads no model, no feature store and no lake table. The image holds none of that code.

## What the page shows

The page shows the limits first: a portfolio demonstration on public data, not clinical guidance, not a clinical validation and no patient data. It explains the standardized infection ratio in plain words and says that it does not show how well the model performs. The page makes no claim about performance because it holds no evidence of it.

An answer shows the predicted ratio and the model version that the API returned. It also shows whether the stable or the canary model answered, whether the API fell back from the canary, the date the features are as of, the first 12 characters of the feature schema hash and the request id. The page refuses an answer that is not in the documented form: a non-finite number, text where a number belongs, a model version with unsafe characters or an answer for another hospital-window than the one asked.

## Failures

Every call has one deadline, 30 seconds by default and 60 at most, set by `MODEL_API_DEADLINE_SECONDS`. A watchdog closes the socket when the deadline passes, so a slow or dripped answer cannot hold the page. The client follows no redirect and retries nothing. Each failure shows a notice with its own title.

| Cause | Notice |
| --- | --- |
| 401 | The API refused the app's key. The owner checks the key |
| 413 | The request was too large |
| 429 | Too many requests. It names the wait from `Retry-After` or says to try later when the header is absent or not a number of seconds |
| 503 | The API cannot serve the request now. It names the wait in the same way |
| Deadline passed | The API did not answer within the deadline |
| Connection refused or broken | The API could not be reached |
| 404 | No data for this hospital and year |
| 400 or 422 | The API refused the request |
| 200 with an unusable body | The answer could not be used and nothing from it is shown |
| Any other status | The status is named |

The wait that the page shows is at most 3,600 seconds, whatever the header says.

## API key

The page presents its own key, so the API rate limit and the prediction rows name it apart from other callers. Make the key with the project tool under its own identifier, as in [the model API](model_api.md#api-keys). The container receives the key as the environment variable `MODEL_API_KEY` from a Parameter Store SecureString parameter. The key lives in a `Credential` object that prints as hidden and cannot be pickled, so it cannot enter the session. It is on no page element and in no log line. The settings refuse a key with spaces or control characters and refuse a plain http address that is not a loopback address, so the key never crosses a network unencrypted. The Streamlit settings file sets `showErrorDetails` to `none`, so an error shows no traceback.

Anyone who can open the page through the tunnel uses the page's key and its share of the rate limit. The environment variable is readable by anyone who can inspect the container on the host.

## Run it

The owner does every step. Show each plan first and apply exactly that plan.

1. Compile the image lock: `docker run` of `services/streamlit/compile_lock.sh` as its header shows. Review the diff and pin the Streamlit release in `services/streamlit/requirements.in`.
2. Add `streamlit` to the ECR repository list of `infra/platform` and to `infra/services/images.json`. Build the image with the command in `services/streamlit/Dockerfile` and push it with the push script.
3. In `infra/iam`, add a task role for the page that grants nothing. Add the parameter name from the `parameter_names_used` output to the execution role.
4. Make a key and keep it out of chat and logs:

   ```sh
   umask 077
   mkdir "$TMPDIR/streamlit_key_folder"
   .venv/bin/python -m scripts.serving.keys new --id streamlit --output "$TMPDIR/streamlit_key_folder"
   aws ssm put-parameter --profile <PROJECT_PROFILE> --region <REGION> --type SecureString \
     --name /healthcare_hai_prediction/streamlit_model_api_key --value "file://$TMPDIR/streamlit_key_folder/api_key"
   ```

   Merge the printed hash entry into the JSON object in the `model_api_key_hashes` parameter, restart the model API and remove the folder.
5. In `infra/services` set `streamlit_enabled = true` with the role ARN and the image digest. The model API must be enabled too. Apply the shown plan.
6. Open the port forward and then the page at `http://127.0.0.1:8501`:

   ```sh
   aws ssm start-session --profile <PROJECT_PROFILE> --region <REGION> --target <INSTANCE_ID> \
     --document-name AWS-StartPortForwardingSession --parameters portNumber=8501,localPortNumber=8501
   ```

To stop, set `streamlit_enabled = false` and apply the shown plan. A public address is a later and separate step. The page has no sign-in, so it must not be exposed before that step adds one.

## Checks

The client cases run on the host and in CI. They start the stand-in API on a free loopback port, so they need no AWS call, no Docker and no key:

```sh
.venv/bin/python -B -m scripts.streamlit_app.run_streamlit_checks --output "$TMPDIR/streamlit_checks"
```

The report says `blocked` for the page cases when Streamlit is not installed. In the page image, mount the repository read-only at `/app` and run the same module with `--require-app`, which makes a missing harness a failure.

## Memory limits assumed

The host is short of memory. The page reserves 256 MiB and may use 512 MiB, with a 64 MiB temporary folder inside that limit. These are assumptions, not measurements. Measure the resident memory with a page open, then adjust. The plan test checks that the limits fit the resized host with the model API on.

## Not proven

- That the Streamlit server, its settings file and the `showErrorDetails` value work in the image and that the page cases pass in the harness. They have not run.
- That the lock resolves and that the image builds and runs as a non-root user on a read-only root file system.
- That a browser reaches the page through the port forward, including the origin check of the Streamlit websocket.
- That the stored key equals the key file's bytes. A 401 on the first call shows a mismatch.
- That the page sends a real prediction request through the real API. The stand-in plays only the answers the API documents.
- The memory the page needs and its behavior when many visitors use it at once.

## Not built

- A sign-in for the page and the public address.
- Evidence of model performance on the page. The page shows none because none has been produced for it.

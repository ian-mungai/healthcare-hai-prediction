# healthcare-hai-prediction

Predicts hospital healthcare-associated infection rates from public CMS, Census, BLS and state data on AWS.

**Status:** In development.

## Install

Prerequisites: macOS on Apple silicon, Python 3.12, Git 2.54 or later, Terraform 1.16.1 or a later 1.16 release, AWS CLI v2, the AWS Session Manager plugin (it opens the web interfaces), Node.js 22 or later and Docker Desktop.

1. Create the environment and install the pinned tools, the hash-locked host dependencies, the hooks and the push gate. The script also copies `.env.example` to `.env`. It checks the prerequisites first and names any that is missing. A rerun on an installed checkout changes nothing and never overwrites `.env`. The `check-tools` hook runs before every commit and push. It compares the copy of its lock group that the installer of SQLFluff, DVC, import-linter or check-jsonschema saved in `.tools/<tool>` (DVC saves two) with the export of that group of `uv.lock` now. It does not inspect the installed packages. It fails when a saved copy differs, is missing or cannot be read, when the lock cannot be read or when an export does not finish within its own timeout (120 s). Its message names the command to rerun, for example `.venv/bin/python -m scripts.quality.install_dvc`. It also asks the yamllint that the lint command runs (the `.tools/yamllint` environment, else `.venv`) for its version and fails when that is not the pinned one, with the fix `bash scripts/install.sh`. To install everything:

   ```sh
   bash scripts/install.sh
   ```

2. Fill every value in `.env`. Git ignores the file. `.env.example` lists the settings:

   | Setting | Value |
   | --- | --- |
   | `AWS_REGION` | The AWS Region of the project |
   | `AWS_PROFILE` | The project profile, named `<PROJECT_NAME>_<ENVIRONMENT>` after its same-named IAM user |
   | `AWS_ACCOUNT_ID` | The AWS account ID |
   | `PROJECT_NAME` | The project name |
   | `ENVIRONMENT` | The environment name (`dev`) |
   | `S3_BUCKET` | The project data bucket |
   | `ALERT_EMAIL` | Optional. One address for every project alert topic (run failures and cost alerts). Empty creates no subscription |

3. Store no secret in `.env` or in any other file on the host. Project secrets live in AWS Systems Manager Parameter Store and the shared API keys in AWS Secrets Manager.

## Usage

Hosted CI runs every check group in full on each push to a branch other than `main`. `main` moves to a commit only after that run is green. The push gate (`scripts/quality/pre_push.py`) runs a short set before every push: the changed-file checks, the commit messages, the removed names and the OSV scan, plus the secret scan and the privacy scan of the files, every commit the push sends and the messages of pushed tags. The commit scans do not read the payload of a Git LFS file of an earlier commit or a file that cannot be read as text (it is listed as unreviewed and does not fail), so GitHub push protection is the backstop for secrets there. Run the full local checks on demand, without AWS credentials. The script sets its own environment. Each run writes its report under `data/e2e/`.

```sh
bash scripts/run_ci.sh
```

Run one end-to-end (E2E) check on its own. A runner that takes an output directory needs a new one:

```sh
bash scripts/run_checks_e2e.sh
```

## Web interfaces

Only the listed web interfaces can be opened from your Mac, each through its own Session Manager document. Every other port of the host is internal.

1. List the web interfaces with the port and health path of each:

   ```sh
   bash scripts/open_ui.sh --list
   ```

2. Start the host:

   ```sh
   bash scripts/host.sh start
   ```

   In development, one command can start the host, keep it on for a bounded time, try to stop it and write a record of its status under `data/host_session/sessions/`. At the end of the hold it waits up to 30 minutes for running job tasks. When a job task still runs, the host stays on and needs a later `bash scripts/host.sh stop`. Run it in place of `start` and `stop`, then open the web interface from a second window. In production the host is always on and this command refuses:

   ```sh
   bash scripts/host.sh session --hold-seconds 3600
   ```

3. Open one web interface by the name in the list and keep the command running. The command reads `.env`, finds the one running host and prints the browser address:

   ```sh
   bash scripts/open_ui.sh airflow
   ```

4. Press Ctrl+C in that window to close the session. AWS Session Manager's own idle timeout and maximum session duration also end it. This repository sets neither, so the account defaults apply. When a local service already holds the port, add `--local-port` and another port number to the command and use that port in the browser.

## Worktrees

Create each worktree under `.worktrees/` of the main checkout, then run this from the main checkout (or any worktree that has a `.venv`) with the folder of the new worktree to make or check every Git-ignored input of the new one: the `.venv` and `.tools` links, the dbt package folders, the `.terraform` folder of each stack, `.env`, `infra/deployment.auto.tfvars.json` and the generated requirement and pin files, such as `services/orchestration/constraints-3.12.txt`. It copies from the main checkout, never overwrites a file that differs, copies no Terraform state file and changes nothing on a second run. It refuses a destination that a tracked path of the Git index touches (also under another case or Unicode spelling on a case-insensitive volume) and any link or dangling link in a destination path. It writes each file to a staging name, verifies its bytes and publishes it without replacing anything, so a stopped run leaves no partial file. An empty `.env` left by an older run is reported and not overwritten. It calls Terraform for `infra/jobs` only after it has checked that the stack's cache and lock file hold no link (but the one that init makes to the copied providers) and no file with another name. It takes an existing cache as complete only when `terraform modules -json` succeeds, which Terraform does only when every module that the configuration declares is installed from its declared source. Otherwise it runs init again. It runs once at a time. It prints what it did and exits 1 with the missing item named.

```sh
bash scripts/worktree_setup.sh .worktrees/example
```

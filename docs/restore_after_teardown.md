---
title: Restore after a teardown
description: What the host backs up, how a start restores an empty volume, the owner steps to turn it on and the drill.
last_updated: 2026-11-18
---

# Restore after a teardown

After a teardown or the loss of the host data volume, `host_session start` brings back the state that matters without hand steps. Creating the data volume from a snapshot stays a documented manual step. Airflow logs and Grafana edits are not restored.

## What is kept

- **Postgres:** each database (`polaris`, `airflow`, `marquez`, `mlflow`, `mlflow_auth` and `openmetadata_db` where they exist) is dumped with `pg_dump -Fc` and uploaded to `pg_backups/<database>/<UTC>.dump` in the data bucket.
- **Completion marker:** after every dump of a set is uploaded, `pg_backups/complete/<UTC>.json` lists each dump with its size, SHA-256 and table count. A dump without a marker is never restored.
- **Write once:** every key is created with `If-None-Match: *` under a role that can only create objects under `pg_backups/`. Nothing is replaced and nothing is deleted by the project. A bucket rule expires the prefix after 30 days.
- **Catalog backups:** each publish already writes `catalog_backup_<run>.json` under `lakehouse/catalog_backups/`, `lakehouse/silver/catalog_backups/` and `lakehouse/features/catalog_backups/`.
- **Code run record:** `host_session` writes the run ID and digest of the code it placed to the object `lakehouse/host_state/code_run_record.json` in the data bucket, which the project user can already write. The record is a plain text line.

## How the backup runs

The `postgres_backup` service runs two containers on the host. The dump container waits 15 minutes after it starts and then dumps every six hours. A run is refused and nothing is uploaded when a dump does not read back completely, lists no table or passes its 15 minute deadline. The upload container checks every dump again, uploads the set and writes the marker last. The last result of each step is in `last_result.json` and `upload_result.json` in the backup folder of the host for the monitoring push that merges separately.

## How start restores

1. `host_prepare`, then the code checkout. Without `--code-run`, a host that has no checkout or no record reads the code run record from the data bucket and installs that archive.
2. The postgres task starts. `postgres_restore_fetch` reads the newest marker whose every dump matches its size and SHA-256. It falls back to an older set when the newest is damaged.
3. `postgres_databases` creates the roles. A database that has data is never touched. A missing database that has a dump is restored in one transaction into `<database>__restore`, checked against the table list of the dump and renamed only after every database passed. `polaris` exists empty from the image, so it is restored in place in one transaction. A missing database with no dump is created empty, as before.
4. A failed fetch or restore makes `host_session start` stop with the named reason and turn the host off. No database was made visible, so no service starts on half-restored data.
5. After the services are stable, the `mlflow_user` task creates or checks the model API user and its READ grants. The OpenMetadata bootstrap starts one search reindex when the catalog has tables and the index is empty.
6. The catalog restore runs as the catalog setup one-shot, the `restore` step of `aws_catalog_setup`, under that task's own role. It reads the newest catalog backup of each folder from the data bucket. It registers tables published after the dump and advances tables whose catalog file is older than the newest backup. To see what it would change first, run `python -m scripts.infrastructure.host_session catalog-setup restore --dry-run` once the host is on and the services are stable, read the result line, then run it without `--dry-run`.

## Turn it on (owner order)

1. Infra root: apply the lifecycle rule `expire_postgres_backups`.
2. IAM: set `restore_after_teardown_prepared` and `host_catalog_setup_prepared` and apply. The catalog setup role then also reads the three catalog backup folders. Apply the project user policy that adds the three roles to its pass list.
3. Lower `host_job_cap_mib` to 22528 or less in the same services apply: the new containers reserve 192 MiB more.
4. Services: set `postgres_backup_enabled`, `postgres_restore_enabled`, the three role ARNs from the IAM output `restore_after_teardown_roles`, `postgres_backup_bucket_name` and `catalog_setup_task_role_arn` from the IAM output `host_catalog_setup_role`, then apply. Without the catalog setup one-shot a start that restored a database cannot restore the catalog and says so.
5. Start the host and wait for the first backup (15 minutes plus the dump time). Check `pg_backups/complete/` for a marker.

## Drill (first live proof)

1. Wait for one marker. 2. Stop the host. 3. Move the postgres data folder aside on a scratch host or delete the data volume in a test project, never the live one without approval. 4. `host_session start` and read `restored database` lines in the postgres log. 5. Read the `catalog_restored` line of the start or run `catalog-setup restore --dry-run`, then compare the counts of tables in Polaris with the last catalog backup.

## Limits

- Backups expire after 30 days. A project idle for more than 30 days has none left.
- A dump is written every six hours while the host is on, so a restore returns the state of the last completed set.
- The MLflow and OpenMetadata routes in the new steps are unproven against the live servers.

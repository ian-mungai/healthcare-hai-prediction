mock_provider "aws" {
  override_during = plan
}

variables {
  # TEST FIXTURE ONLY, not a measured value: the host job cap (host_job_cap_mib) these plans use. The default is the room the live stack leaves, which the
  # larger fixtures here do not have.
  host_job_cap_mib                = 4096
  aws_profile                     = "example_project_dev"
  environment                     = "dev"
  project_name                    = "example_project"
  project_tag                     = "example_project"
  aws_region                      = "us-west-2"
  expected_account_id             = "111111111111"
  cluster_name                    = "example-project-dev"
  ec2_capacity_provider_name      = "example-project-dev-host"
  execution_role_arn              = "arn:aws:iam::111111111111:role/example-execution"
  polaris_task_role_arn           = "arn:aws:iam::111111111111:role/example-polaris"
  control_plane_task_role_arn     = "arn:aws:iam::111111111111:role/example-control"
  openmetadata_task_role_arn      = "arn:aws:iam::111111111111:role/example-openmetadata"
  data_mount_path                 = "/mnt/example-data"
  openmetadata_persistent_enabled = true
  monitoring_enabled              = true
  mlflow_enabled                  = true
  mlflow_task_role_arn            = "arn:aws:iam::111111111111:role/example-mlflow"
  host_private_ip                 = "10.0.1.20"
  mlflow_artifact_bucket          = "example-project-data-dev"

  log_group_names = {
    postgres     = "/example/postgres"
    redis        = "/example/redis"
    polaris      = "/example/polaris"
    airflow      = "/example/airflow"
    mlflow       = "/example/mlflow"
    monitoring   = "/example/monitoring"
    openmetadata = "/example/openmetadata"
  }

  ecr_repository_urls = {
    postgres               = "111111111111.dkr.ecr.us-west-2.amazonaws.com/example_project/postgres"
    redis                  = "111111111111.dkr.ecr.us-west-2.amazonaws.com/example_project/redis"
    polaris                = "111111111111.dkr.ecr.us-west-2.amazonaws.com/example_project/polaris"
    polaris_admin          = "111111111111.dkr.ecr.us-west-2.amazonaws.com/example_project/polaris_admin"
    airflow                = "111111111111.dkr.ecr.us-west-2.amazonaws.com/example_project/airflow"
    openmetadata_server    = "111111111111.dkr.ecr.us-west-2.amazonaws.com/example_project/openmetadata_server"
    openmetadata_ingestion = "111111111111.dkr.ecr.us-west-2.amazonaws.com/example_project/openmetadata_ingestion"
    openmetadata_search    = "111111111111.dkr.ecr.us-west-2.amazonaws.com/example_project/openmetadata_search"
    mlflow                 = "111111111111.dkr.ecr.us-west-2.amazonaws.com/example_project/mlflow"
    prometheus             = "111111111111.dkr.ecr.us-west-2.amazonaws.com/example_project/prometheus"
    grafana                = "111111111111.dkr.ecr.us-west-2.amazonaws.com/example_project/grafana"
    statsd_exporter        = "111111111111.dkr.ecr.us-west-2.amazonaws.com/example_project/statsd_exporter"
    training               = "111111111111.dkr.ecr.us-west-2.amazonaws.com/example_project/training"
    spark                  = "111111111111.dkr.ecr.us-west-2.amazonaws.com/example_project/spark"
    analytics              = "111111111111.dkr.ecr.us-west-2.amazonaws.com/example_project/analytics"
    quality                = "111111111111.dkr.ecr.us-west-2.amazonaws.com/example_project/quality"
    egress_proxy           = "111111111111.dkr.ecr.us-west-2.amazonaws.com/example_project/egress_proxy"
    feature_store          = "111111111111.dkr.ecr.us-west-2.amazonaws.com/example_project/feature_store"
    drift                  = "111111111111.dkr.ecr.us-west-2.amazonaws.com/example_project/drift"
    pushgateway            = "111111111111.dkr.ecr.us-west-2.amazonaws.com/example_project/pushgateway"
    redis_exporter         = "111111111111.dkr.ecr.us-west-2.amazonaws.com/example_project/redis_exporter"
    postgres_exporter      = "111111111111.dkr.ecr.us-west-2.amazonaws.com/example_project/postgres_exporter"
    node_exporter          = "111111111111.dkr.ecr.us-west-2.amazonaws.com/example_project/node_exporter"
    model_api              = "111111111111.dkr.ecr.us-west-2.amazonaws.com/example_project/model_api"
    streamlit              = "111111111111.dkr.ecr.us-west-2.amazonaws.com/example_project/streamlit"
  }

  image_digests = {
    postgres               = "sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
    redis                  = "sha256:bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"
    polaris                = "sha256:cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc"
    polaris_admin          = "sha256:1111111111111111111111111111111111111111111111111111111111111111"
    airflow                = "sha256:dddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddd"
    openmetadata_server    = "sha256:5555555555555555555555555555555555555555555555555555555555555555"
    openmetadata_ingestion = "sha256:6666666666666666666666666666666666666666666666666666666666666666"
    openmetadata_search    = "sha256:7777777777777777777777777777777777777777777777777777777777777777"
    mlflow                 = "sha256:8888888888888888888888888888888888888888888888888888888888888888"
    prometheus             = "sha256:2222222222222222222222222222222222222222222222222222222222222222"
    grafana                = "sha256:3333333333333333333333333333333333333333333333333333333333333333"
    statsd_exporter        = "sha256:4444444444444444444444444444444444444444444444444444444444444444"
    training               = "sha256:9999999999999999999999999999999999999999999999999999999999999999"
    spark                  = "sha256:92f473809e5979a7da2b7f52771b3a9e3d7105dcb0f24ae333c5bf279b863d73"
    analytics              = "sha256:65f98121a162a56ad8ee919ed9ea394b9eabd8714616acf30636c6092ee350b9"
    quality                = "sha256:acef2c15bcd349db900dffece73e1256e881c4416fc1f2d3a494640183490d9a"
    egress_proxy           = "sha256:e545c79a1e4fc0be84ef5760d207dbe164a553fa204b4d3b82739fd3ea02089e"
    feature_store          = "sha256:bf5e4012ff672047c4fad65033f39130413faf09792f6e0d9519d4e9d7f9bba7"
    drift                  = "sha256:0b7a461fefbb68e518e51884369a4b88baffdb40b7e578921f3f88649ebc6494"
    pushgateway            = "sha256:12b207391a415f7d41934a6d80bdf3587db3b550b281b29798aa604051fefdab"
    redis_exporter         = "sha256:6f03db2c8e83e7eef5f97fc80513a6144b9bac96783f2e248d62ca34310520d2"
    postgres_exporter      = "sha256:f721ed8f824cad593a65ba317743df32afc96ce110805b8e978c993ff40c09f8"
    node_exporter          = "sha256:a8b8139c6a4a2713aef45c7e88f08e58ca6c2515984c3033f39e25dd9eee1a9f"
    model_api              = "sha256:bbb1d60d27d44627ec1b5554be177e12bd1ca90a442d01beb8563f9f3a569cc3"
    streamlit              = "sha256:c4e96ef720ad19a25112b228d0ed40252968bd8b5f7c0fc7158424f4b1a4fe78"
  }
  model_api_enabled            = false
  streamlit_enabled            = false
  airflow_lineage_http_enabled = false
  alerts_topic_arn             = "arn:aws:sns:us-west-2:111111111111:example_project_alerts_dev"
}

# Failure modes pinned before the change:
# - the default postgres task changes while backup and restore are off, so an apply that adds nothing replaces the database task;
# - the restore reads a database of the wrong set, or creates a database where the consumers connect before the restore is whole;
# - the backup service holds a read of the dumps, or the postgres task holds a write;
# - the backup or restore flag is on without its role or bucket and the plan still passes.

run "default_postgres_task_is_unchanged" {
  command = plan

  assert {
    condition = (
      [for container in jsondecode(aws_ecs_task_definition.service["postgres"].container_definitions) : container.name] == ["postgres", "postgres_databases"]
      && aws_ecs_task_definition.service["postgres"].task_role_arn == "arn:aws:iam::111111111111:role/example-control"
      && strcontains(jsondecode(aws_ecs_task_definition.service["postgres"].container_definitions)[1].command[0], "exec psql -X")
      && !strcontains(jsondecode(aws_ecs_task_definition.service["postgres"].container_definitions)[1].command[0], "pg_restore")
      && !contains(keys(aws_ecs_task_definition.service), "postgres_backup")
      && !contains(keys(jsondecode(aws_ecs_task_definition.service["postgres"].container_definitions)[1]), "mountPoints")
    )
    error_message = "With both flags off the postgres task keeps its two containers, the control plane role and the original setup script, and no backup service exists."
  }

  assert {
    condition     = !strcontains(jsondecode(aws_ecs_task_definition.one_shot["host_prepare"].container_definitions)[0].command[0], "postgres_backup") && !strcontains(jsondecode(aws_ecs_task_definition.one_shot["host_prepare"].container_definitions)[0].command[0], "postgres_restore")
    error_message = "Host preparation makes the backup and restore folders only while a flag is on."
  }
}

run "restore_adds_the_fetch_container_and_the_restore_script" {
  command = plan

  variables {
    postgres_restore_enabled       = true
    postgres_restore_task_role_arn = "arn:aws:iam::111111111111:role/example_project_postgres_restore_task_dev"
    postgres_backup_bucket_name    = "example-project-data-dev"
  }

  assert {
    condition = (
      aws_ecs_task_definition.service["postgres"].task_role_arn == "arn:aws:iam::111111111111:role/example_project_postgres_restore_task_dev"
      && [for container in jsondecode(aws_ecs_task_definition.service["postgres"].container_definitions) : container.name] == ["postgres", "postgres_databases", "postgres_restore_fetch"]
      && jsondecode(aws_ecs_task_definition.service["postgres"].container_definitions)[2].entryPoint == ["python", "-I", "-c"]
      && strcontains(jsondecode(aws_ecs_task_definition.service["postgres"].container_definitions)[2].image, "/example_project/training@sha256:9999")
      && jsondecode(aws_ecs_task_definition.service["postgres"].container_definitions)[2].essential == false
    )
    error_message = "The postgres task gets its own restore role and a non-essential fetch container from the training image."
  }

  assert {
    condition = (
      strcontains(jsondecode(aws_ecs_task_definition.service["postgres"].container_definitions)[1].command[0], "pg_restore")
      && jsondecode(aws_ecs_task_definition.service["postgres"].container_definitions)[1].dependsOn == [{ containerName = "postgres", condition = "HEALTHY" }, { containerName = "postgres_restore_fetch", condition = "SUCCESS" }]
      && jsondecode(aws_ecs_task_definition.service["postgres"].container_definitions)[1].mountPoints == [{ sourceVolume = "postgres_restore", containerPath = "/restore", readOnly = false }]
      && jsondecode(aws_ecs_task_definition.service["postgres"].container_definitions)[2].mountPoints == [{ sourceVolume = "postgres_restore", containerPath = "/restore", readOnly = false }]
    )
    error_message = "The setup waits for the fetch to succeed and both containers share the restore folder."
  }

  assert {
    condition = (
      contains(jsondecode(aws_ecs_task_definition.service["postgres"].container_definitions)[1].environment, { name = "PG_DATABASES", value = "polaris:polaris airflow:airflow mlflow:mlflow mlflow_auth:mlflow openmetadata_db:openmetadata_user" })
      && contains(jsondecode(aws_ecs_task_definition.service["postgres"].container_definitions)[1].environment, { name = "PG_ROLES", value = "airflow:AIRFLOW_DB_PASSWORD mlflow:MLFLOW_DB_PASSWORD openmetadata_user:OPENMETADATA_DB_PASSWORD" })
      && contains(jsondecode(aws_ecs_task_definition.service["postgres"].container_definitions)[2].environment, { name = "DATA_BUCKET_NAME", value = "example-project-data-dev" })
    )
    error_message = "The restore covers the same databases and roles that the task creates, and the fetch reads the named bucket."
  }

  assert {
    condition     = strcontains(jsondecode(aws_ecs_task_definition.one_shot["host_prepare"].container_definitions)[0].command[0], "/host/postgres_restore") && !strcontains(jsondecode(aws_ecs_task_definition.one_shot["host_prepare"].container_definitions)[0].command[0], "/host/postgres_backup")
    error_message = "Host preparation makes and owns the restore folder only."
  }
}

run "restore_without_mlflow_and_the_catalog_lists_only_the_core_databases" {
  command = plan

  variables {
    postgres_restore_enabled        = true
    postgres_restore_task_role_arn  = "arn:aws:iam::111111111111:role/example_project_postgres_restore_task_dev"
    postgres_backup_bucket_name     = "example-project-data-dev"
    mlflow_enabled                  = false
    openmetadata_persistent_enabled = false
  }

  assert {
    condition     = contains(jsondecode(aws_ecs_task_definition.service["postgres"].container_definitions)[1].environment, { name = "PG_DATABASES", value = "polaris:polaris airflow:airflow" })
    error_message = "Without MLflow and the catalog the restore names only polaris and airflow."
  }
}

run "backup_adds_its_own_service_with_a_write_only_role" {
  command = plan

  variables {
    postgres_backup_enabled          = true
    postgres_backup_task_role_arn    = "arn:aws:iam::111111111111:role/example_project_postgres_backup_task_dev"
    postgres_backup_bucket_name      = "example-project-data-dev"
    postgres_backup_interval_seconds = 3600
  }

  assert {
    condition = (
      aws_ecs_task_definition.service["postgres_backup"].task_role_arn == "arn:aws:iam::111111111111:role/example_project_postgres_backup_task_dev"
      && aws_ecs_task_definition.service["postgres_backup"].network_mode == "host"
      && [for container in jsondecode(aws_ecs_task_definition.service["postgres_backup"].container_definitions) : container.name] == ["postgres_backup_dump", "postgres_backup_upload"]
      && aws_ecs_task_definition.service["postgres"].task_role_arn == "arn:aws:iam::111111111111:role/example-control"
      && length(aws_ecs_service.service["postgres_backup"].name) > 0
    )
    error_message = "The backup is its own service under its own role, and the postgres task keeps the control plane role."
  }

  assert {
    condition = (
      jsondecode(aws_ecs_task_definition.service["postgres_backup"].container_definitions)[0].entryPoint == ["sh", "-ec"]
      && strcontains(jsondecode(aws_ecs_task_definition.service["postgres_backup"].container_definitions)[0].command[0], "pg_dump --format=custom")
      && contains(jsondecode(aws_ecs_task_definition.service["postgres_backup"].container_definitions)[0].environment, { name = "BACKUP_INTERVAL_SECONDS", value = "3600" })
      && contains(jsondecode(aws_ecs_task_definition.service["postgres_backup"].container_definitions)[0].environment, { name = "PG_DATABASES", value = "polaris airflow mlflow mlflow_auth openmetadata_db" })
      && jsondecode(aws_ecs_task_definition.service["postgres_backup"].container_definitions)[0].secrets[0].name == "PGPASSWORD"
      && jsondecode(aws_ecs_task_definition.service["postgres_backup"].container_definitions)[1].entryPoint == ["python", "-I", "-c"]
      && jsondecode(aws_ecs_task_definition.service["postgres_backup"].container_definitions)[1].readonlyRootFilesystem == true
      && !contains(keys(jsondecode(aws_ecs_task_definition.service["postgres_backup"].container_definitions)[1]), "secrets")
    )
    error_message = "The dump container runs the reviewed script as the postgres image with only the database password, and the upload container runs the reviewed script with no secret and a read-only root."
  }

  assert {
    condition     = strcontains(jsondecode(aws_ecs_task_definition.one_shot["host_prepare"].container_definitions)[0].command[0], "/host/postgres_backup") && !strcontains(jsondecode(aws_ecs_task_definition.one_shot["host_prepare"].container_definitions)[0].command[0], "/host/postgres_restore")
    error_message = "Host preparation makes and owns the backup folder only."
  }
}

run "backup_needs_its_role_and_bucket" {
  command = plan

  variables {
    postgres_backup_enabled = true
  }

  expect_failures = [var.postgres_backup_enabled]
}

run "restore_needs_its_role_and_bucket" {
  command = plan

  variables {
    postgres_restore_enabled = true
  }

  expect_failures = [var.postgres_restore_enabled]
}

run "only_the_named_roles_are_accepted" {
  command = plan

  variables {
    postgres_backup_task_role_arn  = "arn:aws:iam::111111111111:role/example_project_mlflow_task_dev"
    postgres_restore_task_role_arn = "arn:aws:iam::111111111111:role/example_project_host_code_fetch_task_dev"
  }

  expect_failures = [var.postgres_backup_task_role_arn, var.postgres_restore_task_role_arn]
}

run "no_mlflow_user_task_without_its_role" {
  command = plan

  assert {
    condition     = !contains(keys(aws_ecs_task_definition.one_shot), "mlflow_user") && !contains(keys(output.one_shot_run_task), "mlflow_user")
    error_message = "Without mlflow_user_task_role_arn there is no MLflow user one-shot."
  }
}

run "the_mlflow_user_task_runs_the_reviewed_script_under_its_role" {
  command = plan

  variables {
    mlflow_user_task_role_arn = "arn:aws:iam::111111111111:role/example_project_mlflow_user_task_dev"
  }

  assert {
    condition = (
      aws_ecs_task_definition.one_shot["mlflow_user"].task_role_arn == "arn:aws:iam::111111111111:role/example_project_mlflow_user_task_dev"
      && aws_ecs_task_definition.one_shot["mlflow_user"].network_mode == "host"
      && contains(keys(output.one_shot_run_task), "mlflow_user")
      && jsondecode(aws_ecs_task_definition.one_shot["mlflow_user"].container_definitions)[0].entryPoint == ["python", "-I", "-c"]
      && strcontains(jsondecode(aws_ecs_task_definition.one_shot["mlflow_user"].container_definitions)[0].image, "/example_project/mlflow@sha256:8888")
      && strcontains(jsondecode(aws_ecs_task_definition.one_shot["mlflow_user"].container_definitions)[0].command[0], "hai_model_")
      && !contains(keys(jsondecode(aws_ecs_task_definition.one_shot["mlflow_user"].container_definitions)[0]), "secrets")
      && jsondecode(aws_ecs_task_definition.one_shot["mlflow_user"].container_definitions)[0].readonlyRootFilesystem == true
    )
    error_message = "The one-shot runs the reviewed script in the MLflow image under its own role, with no injected secret and a read-only root."
  }

  assert {
    condition = (
      contains(jsondecode(aws_ecs_task_definition.one_shot["mlflow_user"].container_definitions)[0].environment, { name = "MLFLOW_URL", value = "http://127.0.0.1:5100" })
      && contains(jsondecode(aws_ecs_task_definition.one_shot["mlflow_user"].container_definitions)[0].environment, { name = "USER_PARAMETER", value = "/healthcare_hai_prediction/model_api_mlflow_user" })
      && contains(jsondecode(aws_ecs_task_definition.one_shot["mlflow_user"].container_definitions)[0].environment, { name = "ADMIN_PASSWORD_PARAMETER", value = "/healthcare_hai_prediction/mlflow_admin_password" })
    )
    error_message = "The one-shot reads the loopback MLflow address and the three named parameters."
  }
}

run "the_mlflow_user_task_needs_mlflow_and_its_own_role" {
  command = plan

  variables {
    mlflow_user_task_role_arn = "arn:aws:iam::111111111111:role/example_project_postgres_backup_task_dev"
  }

  expect_failures = [var.mlflow_user_task_role_arn]
}

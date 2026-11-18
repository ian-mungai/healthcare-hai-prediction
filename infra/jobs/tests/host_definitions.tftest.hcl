mock_provider "aws" {}

variables {
  # The committed defaults of the jobs stack are the live ones (every job family on, host_repo_path and serve_manifest_prefix set). Every case here that does
  # not set a switch tests it off, so the fixture sets each one explicitly.
  host_jobs_enabled      = false
  publisher_job_enabled  = false
  publisher_keys_enabled = false
  host_jobs              = ["bronze", "bronze_stage", "drift", "features", "features_m1", "features_m2", "features_m3", "features_m4", "features_m5", "features_m6", "gold", "metadata", "promote", "serve", "silver", "train"]
  aws_profile            = "example_project_dev"
  aws_region             = "us-west-2"
  expected_account_id    = "111111111111"
  project_name           = "example_project"
  environment            = "dev"
  data_bucket_name       = "example-project-data-dev"
  host_job_memory_mib    = 23552
  # TEST FIXTURE ONLY, not measured values: the services output host_capacity_binding for a 30720 MiB usable host.
  host_capacity_binding = "job_memory_mib=23552 usable_memory_mib=30720 system_reserve_mib=2048 service_reservations_mib=5120"
  log_group_name        = "example_project_jobs_dev"
  images = {
    analytics = "111111111111.dkr.ecr.us-west-2.amazonaws.com/example_project_analytics_dev@sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
    quality   = "111111111111.dkr.ecr.us-west-2.amazonaws.com/example_project_quality_dev@sha256:bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"
    spark     = "111111111111.dkr.ecr.us-west-2.amazonaws.com/example_project_spark_dev@sha256:cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc"
  }
  train_images = {
    training      = "111111111111.dkr.ecr.us-west-2.amazonaws.com/example_project_training_dev@sha256:dddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddd"
    feature_store = "111111111111.dkr.ecr.us-west-2.amazonaws.com/example_project_feature_store_dev@sha256:eeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee"
  }
  drift_images = {
    training      = "111111111111.dkr.ecr.us-west-2.amazonaws.com/example_project_training_dev@sha256:dddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddd"
    feature_store = "111111111111.dkr.ecr.us-west-2.amazonaws.com/example_project_feature_store_dev@sha256:eeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee"
    drift         = "111111111111.dkr.ecr.us-west-2.amazonaws.com/example_project_drift_dev@sha256:ffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff"
  }
  openmetadata_images = {
    ingestion = "111111111111.dkr.ecr.us-west-2.amazonaws.com/example_project_openmetadata_ingestion_dev@sha256:2222222222222222222222222222222222222222222222222222222222222222"
  }
  job_role_arns = {
    execution           = "arn:aws:iam::111111111111:role/example_project_job_execution_dev"
    write_silver_task   = "arn:aws:iam::111111111111:role/example_project_job_write_silver_task_dev"
    write_gold_task     = "arn:aws:iam::111111111111:role/example_project_job_write_gold_task_dev"
    write_features_task = "arn:aws:iam::111111111111:role/example_project_job_write_features_task_dev"
    write_bronze_task   = "arn:aws:iam::111111111111:role/example_project_job_write_bronze_task_dev"
  }
  train_roles = {
    execution  = "arn:aws:iam::111111111111:role/example_project_job_execution_dev"
    train_task = "arn:aws:iam::111111111111:role/example_project_job_train_task_dev"
  }
  drift_roles = {
    execution  = "arn:aws:iam::111111111111:role/example_project_job_execution_dev"
    drift_task = "arn:aws:iam::111111111111:role/example_project_job_drift_task_dev"
  }
  serve_roles = {
    serve_task = "arn:aws:iam::111111111111:role/example_project_job_serve_task_dev"
  }
  openmetadata_roles = {
    execution         = "arn:aws:iam::111111111111:role/example_project_job_execution_dev"
    openmetadata_task = "arn:aws:iam::111111111111:role/example_project_job_openmetadata_task_dev"
  }
  writer_parameter_names = {
    write_silver_task   = ["/example_project/aws_polaris_silver_client_id", "/example_project/aws_polaris_silver_client_secret"]
    write_gold_task     = ["/example_project/aws_polaris_gold_client_id", "/example_project/aws_polaris_gold_client_secret"]
    write_features_task = ["/example_project/aws_polaris_features_client_id", "/example_project/aws_polaris_features_client_secret"]
    write_bronze_task   = ["/example_project/aws_polaris_bronze_client_id", "/example_project/aws_polaris_bronze_client_secret"]
  }
  aws_polaris_reader_client_id_name     = "/example_project/aws_polaris_reader_client_id"
  aws_polaris_reader_client_secret_name = "/example_project/aws_polaris_reader_client_secret"
  mlflow_admin_password_name            = "/example_project/mlflow_admin_password"
  drift_mlflow_password_name            = "/example_project/mlflow_admin_password"
  serve_manifest_prefix                 = "serving/deployments"
  openmetadata_ingestion_token_name     = "/example_project/openmetadata_bot_ingestion"
  mlflow_metadata_reader_password_name  = "/example_project/mlflow_metadata_reader_password"
  host_private_ip                       = "10.0.1.20"
  host_repo_path                        = "/data/repo"
  layer_jobs_enabled                    = true
  bronze_job_enabled                    = true
  train_enabled                         = true
  drift_enabled                         = true
  promote_enabled                       = true
  serve_enabled                         = true
}

run "host_definitions_equal_the_fixture_the_airflow_mode_rig_reads" {
  command = plan

  variables {
    host_jobs_enabled = true
  }

  assert {
    condition = {
      for family, task in aws_ecs_task_definition.host : family => {
        containers              = [for container in jsondecode(task.container_definitions) : container.name]
        start_with_scratch_wipe = [for container in jsondecode(task.container_definitions) : container.name if startswith(container.command[0], "find /workspace /tmp -mindepth 1 -delete && ")]
      }
    } == jsondecode(file("${path.module}/tests/host_definitions.json"))
    error_message = "The containers of the ten host definitions differ from tests/host_definitions.json, which the Airflow mode rig reads. If the change is intended, regenerate it with: .venv/bin/python -m scripts.orchestration.host_definitions_fixture"
  }
}

# ECS refuses RegisterTaskDefinition with "Too many containers" above 10 containers per task definition. The features host definitions went to 11 with a release
# container, and the apply failed after a plan that passed, because a plan does not check the limit.
run "no_host_definition_has_more_containers_than_ecs_allows" {
  command = plan

  variables {
    host_jobs_enabled = true
  }

  assert {
    condition = (
      length(aws_ecs_task_definition.host) == 16 &&
      alltrue([for family, task in aws_ecs_task_definition.host : length(jsondecode(task.container_definitions)) <= 10])
    )
    error_message = "A host task definition holds more than 10 containers, which ECS refuses at registration (ClientException: Too many containers): ${jsonencode({ for family, task in aws_ecs_task_definition.host : family => length(jsondecode(task.container_definitions)) })}"
  }
}

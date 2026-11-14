mock_provider "aws" {}

variables {
  aws_profile         = "example_project_dev"
  aws_region          = "us-west-2"
  expected_account_id = join("", ["1111", "1111", "1111"])
  project_name        = "example_project"
  environment         = "dev"
  data_bucket_name    = "example-project-data-dev"
  host_job_memory_mib = 23552
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
    execution           = "arn:aws:iam::${var.expected_account_id}:role/example_project_job_execution_dev"
    publisher_task      = "arn:aws:iam::${var.expected_account_id}:role/example_project_job_publisher_task_dev"
    write_silver_task   = "arn:aws:iam::${var.expected_account_id}:role/example_project_job_write_silver_task_dev"
    write_gold_task     = "arn:aws:iam::${var.expected_account_id}:role/example_project_job_write_gold_task_dev"
    write_features_task = "arn:aws:iam::${var.expected_account_id}:role/example_project_job_write_features_task_dev"
    write_bronze_task   = "arn:aws:iam::${var.expected_account_id}:role/example_project_job_write_bronze_task_dev"
  }
  train_roles = {
    execution  = "arn:aws:iam::${var.expected_account_id}:role/example_project_job_execution_dev"
    train_task = "arn:aws:iam::${var.expected_account_id}:role/example_project_job_train_task_dev"
  }
  drift_roles = {
    execution  = "arn:aws:iam::${var.expected_account_id}:role/example_project_job_execution_dev"
    drift_task = "arn:aws:iam::${var.expected_account_id}:role/example_project_job_drift_task_dev"
  }
  serve_roles = {
    serve_task = "arn:aws:iam::${var.expected_account_id}:role/example_project_job_serve_task_dev"
  }
  publisher_job_enabled = true
  openmetadata_roles = {
    execution         = "arn:aws:iam::${var.expected_account_id}:role/example_project_job_execution_dev"
    openmetadata_task = "arn:aws:iam::${var.expected_account_id}:role/example_project_job_openmetadata_task_dev"
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
  host_openmetadata_api                 = "http://10.0.1.20:8585/api"
  host_repo_path                        = "/data/repo"
  layer_jobs_enabled                    = true
  bronze_job_enabled                    = true
  train_enabled                         = true
  drift_enabled                         = true
  promote_enabled                       = true
  serve_enabled                         = true
}

# The removal transition. scripts/infrastructure/run_retired_definitions_e2e.py copies retired_history/fargate_history.tf.txt next to the stack, so the first run
# builds the state the stack had before it dropped the Fargate job definitions: the 19 retired definitions with skip_destroy on, beside the host definitions
# and the publisher. The next runs apply the final configuration, which declares none of them, to that state with the same host inputs, and then plan again. The
# E2E reads the plan of remove_retired_only and refuses anything but the forgetting of exactly the 19 retired addresses, and it requires the plan after the
# removal to be empty.
run "register_history_and_host" {
  command = apply

  variables {
    host_jobs_enabled       = true
    retired_history_present = true
  }

  assert {
    condition = alltrue([
      for task in concat(
        values(aws_ecs_task_definition.read), values(aws_ecs_task_definition.writer), values(aws_ecs_task_definition.layer), aws_ecs_task_definition.bronze,
        aws_ecs_task_definition.drift, aws_ecs_task_definition.train, aws_ecs_task_definition.promote, aws_ecs_task_definition.serve, aws_ecs_task_definition.openmetadata
      ) : toset(task.requires_compatibilities) == toset(["FARGATE"]) && task.skip_destroy
    ]) && length(aws_ecs_task_definition.read) == 5 && length(aws_ecs_task_definition.writer) == 5 && length(aws_ecs_task_definition.layer) == 3
    error_message = "The historical state holds all 19 Fargate definitions with skip_destroy on."
  }

  assert {
    condition     = length(aws_ecs_task_definition.host) == 15 && length(aws_ecs_task_definition.publisher_host) == 1
    error_message = "The historical state holds the fifteen host definitions and the publisher."
  }
}

run "remove_retired_only" {
  command = plan

  variables {
    host_jobs_enabled       = true
    retired_history_present = false
  }

  assert {
    condition     = length(aws_ecs_task_definition.read) + length(aws_ecs_task_definition.writer) + length(aws_ecs_task_definition.layer) == 0
    error_message = "The final configuration declares no Fargate definition."
  }
}

run "apply_removal" {
  command = apply

  variables {
    host_jobs_enabled       = true
    retired_history_present = false
  }
}

run "plan_after_removal" {
  command = plan

  variables {
    host_jobs_enabled       = true
    retired_history_present = false
  }

  assert {
    condition     = length(aws_ecs_task_definition.host) == 15 && length(aws_ecs_task_definition.publisher_host) == 1
    error_message = "The host definitions and the publisher stay after the removal."
  }
}

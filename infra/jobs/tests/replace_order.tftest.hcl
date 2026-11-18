mock_provider "aws" {}

variables {
  aws_profile         = "example_project_dev"
  aws_region          = "us-west-2"
  expected_account_id = "111111111111"
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
    execution           = "arn:aws:iam::111111111111:role/example_project_job_execution_dev"
    publisher_task      = "arn:aws:iam::111111111111:role/example_project_job_publisher_task_dev"
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
  publisher_job_enabled = true
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
  host_private_ip                       = "10.0.1.20"
  host_repo_path                        = "/data/repo"
  layer_jobs_enabled                    = true
  bronze_job_enabled                    = true
  train_enabled                         = true
  drift_enabled                         = true
  promote_enabled                       = true
  serve_enabled                         = true
  mlflow_metadata_reader_password_name  = "/example_project/mlflow_metadata_reader_password"
  host_jobs                             = ["bronze", "drift", "features", "features_m1", "features_m2", "features_m3", "features_m4", "features_m5", "features_m6", "gold", "metadata", "promote", "serve", "silver", "train"]
}

# A jobs apply that replaces a task definition must register the new revision before it retires the old one, so a failed registration leaves the old revision in
# the state and the family keeps its definition. On Nov 16 2026 ECS refused the seven new features definitions ("Too many containers") after Terraform had already
# destroyed the seven old ones in the state, and the apply left the family with no definition in the state. A plan does not run the ECS limits, so the proof is the
# order of the actions: a replacement that creates first is planned as ["create", "delete"], one that destroys first as ["delete", "create"].
# scripts/infrastructure/run_jobs_replace_order_e2e.py reads the plan of replace_every_definition and refuses any task definition that is replaced in the old order.
run "register_every_definition" {
  command = apply

  variables {
    host_jobs_enabled = true
  }

  assert {
    condition     = length(aws_ecs_task_definition.host) == 15 && length(aws_ecs_task_definition.publisher_host) == 1
    error_message = "The state holds the fifteen host definitions and the publisher before the replacement."
  }
}

# The mock provider cannot decide which change forces a new registration (the real provider does), so the run replaces the sixteen definitions by address. A task
# definition that this list does not name stays unchanged in the plan, and the E2E refuses a plan that holds any task definition outside the replaced ones.
run "replace_every_definition" {
  command = plan

  variables {
    host_jobs_enabled = true
  }

  plan_options {
    replace = [
      aws_ecs_task_definition.host["bronze"],
      aws_ecs_task_definition.host["drift"],
      aws_ecs_task_definition.host["features"],
      aws_ecs_task_definition.host["features_m1"],
      aws_ecs_task_definition.host["features_m2"],
      aws_ecs_task_definition.host["features_m3"],
      aws_ecs_task_definition.host["features_m4"],
      aws_ecs_task_definition.host["features_m5"],
      aws_ecs_task_definition.host["features_m6"],
      aws_ecs_task_definition.host["gold"],
      aws_ecs_task_definition.host["metadata"],
      aws_ecs_task_definition.host["promote"],
      aws_ecs_task_definition.host["serve"],
      aws_ecs_task_definition.host["silver"],
      aws_ecs_task_definition.host["train"],
      aws_ecs_task_definition.publisher_host[0]
    ]
  }

  assert {
    condition     = length(aws_ecs_task_definition.host) == 15 && length(aws_ecs_task_definition.publisher_host) == 1
    error_message = "The replacement plan still declares the fifteen host definitions and the publisher."
  }
}

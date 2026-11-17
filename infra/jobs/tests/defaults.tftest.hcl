# The committed defaults of the jobs stack are the live settings: given only the inputs the renderer builds from .env, the stack outputs and the recorded
# image digests (the roles, the log group, the writer parameter names, the image URIs, the job room, the host address and the alerts topic), a plan registers
# the host definition of every live family and the publisher, with no other input.

mock_provider "aws" {}

variables {
  aws_profile           = "healthcare_hai_prediction_dev"
  aws_region            = "us-west-2"
  expected_account_id   = "111111111111"
  project_name          = "healthcare_hai_prediction"
  environment           = "dev"
  data_bucket_name      = "example-project-data-dev"
  log_group_name        = "healthcare_hai_prediction_jobs_dev"
  host_job_memory_mib   = 22800
  host_capacity_binding = "job_memory_mib=22800 usable_memory_mib=32768 system_reserve_mib=2048 service_reservations_mib=7920"
  host_private_ip       = "10.0.1.20"
  alerts_topic_arn      = "arn:aws:sns:us-west-2:111111111111:healthcare_hai_prediction_alerts_dev"
  images = {
    analytics = "111111111111.dkr.ecr.us-west-2.amazonaws.com/healthcare_hai_prediction_analytics_dev@sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
    quality   = "111111111111.dkr.ecr.us-west-2.amazonaws.com/healthcare_hai_prediction_quality_dev@sha256:bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"
    spark     = "111111111111.dkr.ecr.us-west-2.amazonaws.com/healthcare_hai_prediction_spark_dev@sha256:cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc"
  }
  train_images = {
    training      = "111111111111.dkr.ecr.us-west-2.amazonaws.com/healthcare_hai_prediction_training_dev@sha256:dddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddd"
    feature_store = "111111111111.dkr.ecr.us-west-2.amazonaws.com/healthcare_hai_prediction_feature_store_dev@sha256:eeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee"
  }
  drift_images = {
    training      = "111111111111.dkr.ecr.us-west-2.amazonaws.com/healthcare_hai_prediction_training_dev@sha256:dddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddd"
    feature_store = "111111111111.dkr.ecr.us-west-2.amazonaws.com/healthcare_hai_prediction_feature_store_dev@sha256:eeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee"
    drift         = "111111111111.dkr.ecr.us-west-2.amazonaws.com/healthcare_hai_prediction_drift_dev@sha256:ffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff"
  }
  openmetadata_images = {
    ingestion = "111111111111.dkr.ecr.us-west-2.amazonaws.com/healthcare_hai_prediction_openmetadata_ingestion_dev@sha256:2222222222222222222222222222222222222222222222222222222222222222"
  }
  job_role_arns = {
    execution           = "arn:aws:iam::111111111111:role/healthcare_hai_prediction_job_execution_dev"
    publisher_task      = "arn:aws:iam::111111111111:role/healthcare_hai_prediction_job_publisher_task_dev"
    write_silver_task   = "arn:aws:iam::111111111111:role/healthcare_hai_prediction_job_write_silver_task_dev"
    write_gold_task     = "arn:aws:iam::111111111111:role/healthcare_hai_prediction_job_write_gold_task_dev"
    write_features_task = "arn:aws:iam::111111111111:role/healthcare_hai_prediction_job_write_features_task_dev"
  }
  train_roles = {
    execution  = "arn:aws:iam::111111111111:role/healthcare_hai_prediction_job_execution_dev"
    train_task = "arn:aws:iam::111111111111:role/healthcare_hai_prediction_job_train_task_dev"
  }
  drift_roles = {
    execution  = "arn:aws:iam::111111111111:role/healthcare_hai_prediction_job_execution_dev"
    drift_task = "arn:aws:iam::111111111111:role/healthcare_hai_prediction_job_drift_task_dev"
  }
  serve_roles = {
    serve_task = "arn:aws:iam::111111111111:role/healthcare_hai_prediction_job_serve_task_dev"
  }
  openmetadata_roles = {
    execution         = "arn:aws:iam::111111111111:role/healthcare_hai_prediction_job_execution_dev"
    openmetadata_task = "arn:aws:iam::111111111111:role/healthcare_hai_prediction_job_openmetadata_task_dev"
  }
  writer_parameter_names = {
    write_silver_task   = ["/healthcare_hai_prediction/aws_polaris_silver_client_id", "/healthcare_hai_prediction/aws_polaris_silver_client_secret"]
    write_gold_task     = ["/healthcare_hai_prediction/aws_polaris_gold_client_id", "/healthcare_hai_prediction/aws_polaris_gold_client_secret"]
    write_features_task = ["/healthcare_hai_prediction/aws_polaris_features_client_id", "/healthcare_hai_prediction/aws_polaris_features_client_secret"]
  }
}

run "the_defaults_are_the_live_settings" {
  command = plan

  assert {
    condition = (
      var.host_jobs_enabled && var.layer_jobs_enabled && var.train_enabled && var.drift_enabled && var.promote_enabled && var.serve_enabled &&
      var.publisher_job_enabled && var.publisher_keys_enabled && !var.bronze_job_enabled
    )
    error_message = "Every job family of the live stack and the publisher are on by default, and the bronze job is off."
  }

  assert {
    condition     = var.host_repo_path == "/data/repo" && var.serve_manifest_prefix == "serving/deployments" && toset(var.layer_jobs) == toset(["silver", "gold", "features"])
    error_message = "The metadata task mounts /data/repo, the serve records go under serving/deployments and the layer jobs are silver, gold and features."
  }

  assert {
    condition = toset(keys(aws_ecs_task_definition.host)) == toset([
      "silver", "gold", "features", "features_m1", "features_m2", "features_m3", "features_m4", "features_m5", "features_m6", "train", "drift", "promote", "serve", "metadata"
    ])
    error_message = "The default host families are the live ones, with no bronze definition."
  }

  assert {
    condition     = length(aws_ecs_task_definition.publisher_host) == 1
    error_message = "The publisher definition is registered by default."
  }
}

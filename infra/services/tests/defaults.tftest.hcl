# The committed defaults of the services stack are the live settings: given only the inputs the renderer builds from .env and the platform outputs (the
# profile, the account, the roles, the buckets and addresses, the repositories and the image digests), a plan turns on MLflow, the model API, the page,
# monitoring, persistent OpenMetadata and HTTP lineage on and gives a host job 22800 MiB.

mock_provider "aws" {
  override_during = plan
}

variables {
  aws_profile                  = "example_project_dev"
  environment                  = "dev"
  project_name                 = "example_project"
  project_tag                  = "example_project"
  aws_region                   = "us-west-2"
  expected_account_id          = "111111111111"
  cluster_name                 = "example-project-dev"
  ec2_capacity_provider_name   = "example-project-dev-host"
  execution_role_arn           = "arn:aws:iam::111111111111:role/example-execution"
  polaris_task_role_arn        = "arn:aws:iam::111111111111:role/example-polaris"
  control_plane_task_role_arn  = "arn:aws:iam::111111111111:role/example-control"
  serving_execution_role_arn   = "arn:aws:iam::111111111111:role/example-serving-execution"
  mlflow_task_role_arn         = "arn:aws:iam::111111111111:role/example-mlflow"
  model_api_task_role_arn      = "arn:aws:iam::111111111111:role/example-model-api"
  streamlit_task_role_arn      = "arn:aws:iam::111111111111:role/example-streamlit"
  openmetadata_task_role_arn   = "arn:aws:iam::111111111111:role/example-openmetadata"
  airflow_launch_task_role_arn = "arn:aws:iam::111111111111:role/example-airflow-launch"
  code_fetch_task_role_arn     = "arn:aws:iam::111111111111:role/example-code-fetch"
  data_mount_path              = "/mnt/example-data"
  host_private_ip              = "10.0.1.20"
  mlflow_artifact_bucket       = "example-project-data-dev"
  airflow_launch_bucket_name   = "example-project-data-dev"
  code_bucket_name             = "example-project-data-dev"
  model_api_manifest_uri       = "s3://example-project-data-dev/serving/deployments"
  model_api_registry_uri       = "s3://example-project-data-dev/lakehouse/feast/registry.pb"

  log_group_names = {
    postgres     = "/example/postgres"
    redis        = "/example/redis"
    polaris      = "/example/polaris"
    airflow      = "/example/airflow"
    mlflow       = "/example/mlflow"
    monitoring   = "/example/monitoring"
    openmetadata = "/example/openmetadata"
    jobs         = "/example/jobs"
  }

  ecr_repository_urls = {
    postgres               = "111111111111.dkr.ecr.us-west-2.amazonaws.com/example_project/postgres"
    redis                  = "111111111111.dkr.ecr.us-west-2.amazonaws.com/example_project/redis"
    polaris                = "111111111111.dkr.ecr.us-west-2.amazonaws.com/example_project/polaris"
    polaris_admin          = "111111111111.dkr.ecr.us-west-2.amazonaws.com/example_project/polaris_admin"
    airflow                = "111111111111.dkr.ecr.us-west-2.amazonaws.com/example_project/airflow"
    mlflow                 = "111111111111.dkr.ecr.us-west-2.amazonaws.com/example_project/mlflow"
    model_api              = "111111111111.dkr.ecr.us-west-2.amazonaws.com/example_project/model_api"
    streamlit              = "111111111111.dkr.ecr.us-west-2.amazonaws.com/example_project/streamlit"
    prometheus             = "111111111111.dkr.ecr.us-west-2.amazonaws.com/example_project/prometheus"
    grafana                = "111111111111.dkr.ecr.us-west-2.amazonaws.com/example_project/grafana"
    statsd_exporter        = "111111111111.dkr.ecr.us-west-2.amazonaws.com/example_project/statsd_exporter"
    openmetadata_server    = "111111111111.dkr.ecr.us-west-2.amazonaws.com/example_project/openmetadata_server"
    openmetadata_ingestion = "111111111111.dkr.ecr.us-west-2.amazonaws.com/example_project/openmetadata_ingestion"
    openmetadata_search    = "111111111111.dkr.ecr.us-west-2.amazonaws.com/example_project/openmetadata_search"
    training               = "111111111111.dkr.ecr.us-west-2.amazonaws.com/example_project/training"
  }

  image_digests = {
    postgres               = "sha256:1111111111111111111111111111111111111111111111111111111111111111"
    redis                  = "sha256:2222222222222222222222222222222222222222222222222222222222222222"
    polaris                = "sha256:3333333333333333333333333333333333333333333333333333333333333333"
    polaris_admin          = "sha256:4444444444444444444444444444444444444444444444444444444444444444"
    airflow                = "sha256:5555555555555555555555555555555555555555555555555555555555555555"
    mlflow                 = "sha256:6666666666666666666666666666666666666666666666666666666666666666"
    model_api              = "sha256:7777777777777777777777777777777777777777777777777777777777777777"
    streamlit              = "sha256:8888888888888888888888888888888888888888888888888888888888888888"
    prometheus             = "sha256:9999999999999999999999999999999999999999999999999999999999999999"
    grafana                = "sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
    statsd_exporter        = "sha256:bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"
    openmetadata_server    = "sha256:cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc"
    openmetadata_ingestion = "sha256:dddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddd"
    openmetadata_search    = "sha256:eeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee"
    training               = "sha256:ffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff"
  }
}

run "the_defaults_are_the_live_settings" {
  command = plan

  assert {
    condition = (
      var.mlflow_enabled && var.model_api_enabled && var.streamlit_enabled && var.monitoring_enabled && var.openmetadata_persistent_enabled &&
      var.airflow_lineage_http_enabled
    )
    error_message = "MLflow, the model API, the page, monitoring, persistent OpenMetadata and HTTP lineage are on by default."
  }

  assert {
    condition     = var.model_api_mlflow_uri == "http://127.0.0.1:5100" && var.model_api_hai_types == "HAI_1" && var.streamlit_hai_types == "HAI_1"
    error_message = "The model API reaches MLflow on the host loopback and serves, with the page, the HAI_1 type."
  }

  assert {
    condition     = var.host_job_cap_mib == 22800 && output.reserved_memory_mib == 7920 && output.host_job_memory_mib == 22800
    error_message = "A host job gets 22800 MiB beside the services of the defaults, which reserve 7920 MiB."
  }
}

mock_provider "aws" {
  override_during = plan
}

# The memory rule with every service on (owner decision B, Nov 17 2026): the host job cap plus the service reservations plus the 2048 MiB system reserve must fit the
# 32768 MiB host. The monitoring task reserves 464 MiB (the preparation container 16, Prometheus 128, Grafana 128, the StatsD exporter 32, the Pushgateway 32, the host
# state exporter 56 and the Redis, Postgres and node exporters 24 each), so the reservations of the default services are 8080 MiB. The optional services add 512 MiB (the probe of Nov 17 2026 on the merged tree: 8592 MiB with all of them on), so the cap is at most 22128 MiB. The live cap of 22800 MiB fits only the stack without the Pushgateway,
# the host state exporter and the three exporters. These cases read the plan only: no image runs and nothing reaches AWS. The cap value itself is set where the settings live, not here.
variables {
  aws_profile                      = "example_project_dev"
  environment                      = "dev"
  project_name                     = "example_project"
  project_tag                      = "example_project"
  aws_region                       = "us-west-2"
  expected_account_id              = "111111111111"
  cluster_name                     = "example-project-dev"
  ec2_capacity_provider_name       = "example-project-dev-host"
  execution_role_arn               = "arn:aws:iam::111111111111:role/example-execution"
  serving_execution_role_arn       = "arn:aws:iam::111111111111:role/example-serving-execution"
  polaris_task_role_arn            = "arn:aws:iam::111111111111:role/example-polaris"
  control_plane_task_role_arn      = "arn:aws:iam::111111111111:role/example-control"
  openmetadata_task_role_arn       = "arn:aws:iam::111111111111:role/example-openmetadata"
  mlflow_task_role_arn             = "arn:aws:iam::111111111111:role/example-mlflow"
  model_api_task_role_arn          = "arn:aws:iam::111111111111:role/example-model-api"
  streamlit_task_role_arn          = "arn:aws:iam::111111111111:role/example-streamlit"
  data_mount_path                  = "/mnt/example-data"
  openmetadata_persistent_enabled  = true
  monitoring_enabled               = true
  alerts_topic_arn                 = "arn:aws:sns:us-west-2:111111111111:example_project_alerts_dev"
  mlflow_enabled                   = true
  model_api_enabled                = true
  streamlit_enabled                = true
  model_api_manifest_uri           = "s3://example-data/serving/deployments"
  model_api_registry_uri           = "s3://example-data/lakehouse/feast/registry.pb"
  model_api_mlflow_uri             = "http://127.0.0.1:5100"
  host_private_ip                  = "10.0.1.20"
  mlflow_artifact_bucket           = "example-project-data-dev"
  host_job_cap_mib                 = 22128
  evidently_ui_enabled             = true
  evidently_ui_task_role_arn       = "arn:aws:iam::111111111111:role/example-evidently-ui"
  evidently_ui_bucket_name         = "example-project-data-dev"
  prometheus_history_task_role_arn = "arn:aws:iam::111111111111:role/example-prometheus-history"
  prometheus_restore_task_role_arn = "arn:aws:iam::111111111111:role/example-prometheus-restore"
  prometheus_history_bucket_name   = "example-project-data-dev"
  postgres_backup_enabled          = true
  postgres_restore_enabled         = true
  postgres_backup_task_role_arn    = "arn:aws:iam::111111111111:role/example_project_postgres_backup_task_dev"
  postgres_restore_task_role_arn   = "arn:aws:iam::111111111111:role/example_project_postgres_restore_task_dev"
  mlflow_user_task_role_arn        = "arn:aws:iam::111111111111:role/example_project_mlflow_user_task_dev"
  postgres_backup_bucket_name      = "example-project-data-dev"

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
    model_api              = "111111111111.dkr.ecr.us-west-2.amazonaws.com/example_project/model_api"
    streamlit              = "111111111111.dkr.ecr.us-west-2.amazonaws.com/example_project/streamlit"
    prometheus             = "111111111111.dkr.ecr.us-west-2.amazonaws.com/example_project/prometheus"
    grafana                = "111111111111.dkr.ecr.us-west-2.amazonaws.com/example_project/grafana"
    statsd_exporter        = "111111111111.dkr.ecr.us-west-2.amazonaws.com/example_project/statsd_exporter"
    pushgateway            = "111111111111.dkr.ecr.us-west-2.amazonaws.com/example_project/pushgateway"
    redis_exporter         = "111111111111.dkr.ecr.us-west-2.amazonaws.com/example_project/redis_exporter"
    postgres_exporter      = "111111111111.dkr.ecr.us-west-2.amazonaws.com/example_project/postgres_exporter"
    node_exporter          = "111111111111.dkr.ecr.us-west-2.amazonaws.com/example_project/node_exporter"
    spark                  = "111111111111.dkr.ecr.us-west-2.amazonaws.com/example_project/spark"
    analytics              = "111111111111.dkr.ecr.us-west-2.amazonaws.com/example_project/analytics"
    quality                = "111111111111.dkr.ecr.us-west-2.amazonaws.com/example_project/quality"
    egress_proxy           = "111111111111.dkr.ecr.us-west-2.amazonaws.com/example_project/egress_proxy"
    training               = "111111111111.dkr.ecr.us-west-2.amazonaws.com/example_project/training"
    feature_store          = "111111111111.dkr.ecr.us-west-2.amazonaws.com/example_project/feature_store"
    drift                  = "111111111111.dkr.ecr.us-west-2.amazonaws.com/example_project/drift"
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
    model_api              = "sha256:2222222222222222222222222222222222222222222222222222222222222222"
    streamlit              = "sha256:3333333333333333333333333333333333333333333333333333333333333333"
    prometheus             = "sha256:2222222222222222222222222222222222222222222222222222222222222222"
    grafana                = "sha256:3333333333333333333333333333333333333333333333333333333333333333"
    statsd_exporter        = "sha256:4444444444444444444444444444444444444444444444444444444444444444"
    pushgateway            = "sha256:9999999999999999999999999999999999999999999999999999999999999999"
    redis_exporter         = "sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
    postgres_exporter      = "sha256:bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"
    node_exporter          = "sha256:cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc"
    spark                  = "sha256:92f473809e5979a7da2b7f52771b3a9e3d7105dcb0f24ae333c5bf279b863d73"
    analytics              = "sha256:65f98121a162a56ad8ee919ed9ea394b9eabd8714616acf30636c6092ee350b9"
    quality                = "sha256:acef2c15bcd349db900dffece73e1256e881c4416fc1f2d3a494640183490d9a"
    egress_proxy           = "sha256:e545c79a1e4fc0be84ef5760d207dbe164a553fa204b4d3b82739fd3ea02089e"
    training               = "sha256:c2fb788c7deedbeaa296e424d4c2921b871a4f6cb4cf393c1c1105653ab399b4"
    feature_store          = "sha256:bf5e4012ff672047c4fad65033f39130413faf09792f6e0d9519d4e9d7f9bba7"
    drift                  = "sha256:0b7a461fefbb68e518e51884369a4b88baffdb40b7e578921f3f88649ebc6494"
  }
}

run "the_full_stack_reserves_8592_mib_and_a_cap_of_22128_fits" {
  command = plan

  assert {
    condition     = output.reserved_memory_mib == 8592
    error_message = "With every service on, the optional ones included (the Evidently UI, the Prometheus history shipper and restore, the Postgres backup and restore and the MLflow user task), the reservations are 8592 MiB: 8080 for the default services and 512 for the optional ones."
  }

  assert {
    condition     = output.host_job_memory_mib == 22128 && output.host_job_memory_mib + output.reserved_memory_mib + var.host_system_reserve_mib == 32768
    error_message = "The cap of 22128 MiB plus the reservations plus the 2048 MiB system reserve fills the 32768 MiB host exactly."
  }
}

run "the_cap_of_22800_does_not_fit_the_full_stack" {
  command = plan

  variables {
    host_job_cap_mib = 22800
  }

  expect_failures = [output.host_job_memory_mib]
}

run "a_cap_one_mib_over_the_room_is_refused" {
  command = plan

  variables {
    host_job_cap_mib = 22129
  }

  expect_failures = [output.host_job_memory_mib]
}

run "mlflow_exposes_its_request_metrics_to_a_temporary_folder_while_monitoring_is_on" {
  command = plan

  assert {
    condition = (
      contains(jsondecode(aws_ecs_task_definition.service["mlflow"].container_definitions)[0].command, "--expose-prometheus")
      && contains(jsondecode(aws_ecs_task_definition.service["mlflow"].container_definitions)[0].command, "/tmp/mlflow_metrics")
      && contains(jsondecode(aws_ecs_task_definition.service["mlflow"].container_definitions)[0].command, "--workers")
      && contains(jsondecode(aws_ecs_task_definition.service["mlflow"].container_definitions)[0].command, "1")
    )
    error_message = "With monitoring on, MLflow runs one worker and writes its Prometheus request counters to /tmp, which is a 256 MiB temporary file system."
  }
}

run "without_monitoring_mlflow_keeps_its_command" {
  command = plan

  variables {
    monitoring_enabled               = false
    alerts_topic_arn                 = null
    prometheus_history_task_role_arn = null
    prometheus_restore_task_role_arn = null
    prometheus_history_bucket_name   = null
  }

  assert {
    condition     = !contains(jsondecode(aws_ecs_task_definition.service["mlflow"].container_definitions)[0].command, "--expose-prometheus")
    error_message = "With monitoring off MLflow's definition carries no metrics flag, so a stack without the rebuilt image still starts."
  }
}

run "only_the_host_state_exporter_gets_the_mlflow_admin_credential_for_the_relay" {
  command = plan

  assert {
    condition = (
      contains(jsondecode(aws_ecs_task_definition.service["monitoring"].container_definitions)[5].secrets, { name = "MLFLOW_ADMIN_PASSWORD", valueFrom = "arn:aws:ssm:us-west-2:111111111111:parameter/healthcare_hai_prediction/mlflow_admin_password" })
      && contains(jsondecode(aws_ecs_task_definition.service["monitoring"].container_definitions)[5].environment, { name = "MLFLOW_METRICS_URL", value = "http://127.0.0.1:5100/metrics" })
    )
    error_message = "The host state exporter reads the MLflow admin password from Parameter Store at start, and reaches MLflow's metrics path on loopback."
  }

  assert {
    condition = alltrue([
      for container in jsondecode(aws_ecs_task_definition.service["monitoring"].container_definitions) :
      contains([for secret in lookup(container, "secrets", []) : secret.name], "MLFLOW_ADMIN_PASSWORD") == (container.name == "host_state_exporter")
    ])
    error_message = "No other monitoring container holds the MLflow admin credential."
  }

  assert {
    condition     = !can(regex("mlflow_admin_password", jsonencode([for container in jsondecode(aws_ecs_task_definition.service["monitoring"].container_definitions) : [container.command, container.environment]])))
    error_message = "The credential is injected as a secret and named nowhere in a command or an environment value."
  }
}

run "with_openmetadata_the_host_state_exporter_reads_opensearch_and_the_openmetadata_bootstrap_and_reserves_no_memory" {
  command = plan

  assert {
    condition = (
      contains(jsondecode(aws_ecs_task_definition.service["monitoring"].container_definitions)[5].environment, { name = "OPENSEARCH_HEALTH_URL", value = "http://127.0.0.1:9200/_cluster/health" })
      && contains(jsondecode(aws_ecs_task_definition.service["monitoring"].container_definitions)[5].environment, { name = "BOOTSTRAP_TASKS", value = "polaris_bootstrap,openmetadata_bootstrap" })
    )
    error_message = "With OpenMetadata on, the exporter gets the OpenSearch health address and both bootstrap one-shot names."
  }

  assert {
    condition     = length(jsondecode(aws_ecs_task_definition.service["monitoring"].container_definitions)) == 9
    error_message = "The gap reads add no container, so the memory rule (job cap plus service reservations plus 2048 within 32768) is unchanged."
  }
}

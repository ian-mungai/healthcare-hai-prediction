mock_provider "aws" {
  override_during = plan
}

# Prometheus history in S3: a shipper service and a restore one-shot, each with its own task role, and the two Prometheus block-duration flags. Off while the role ARNs
# are null. With every service on and the shipper reserving 48 MiB, the reservations are 8128 MiB and the host job cap is at most 22592 MiB. Every case reads the plan only.
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
  prometheus_history_task_role_arn = "arn:aws:iam::111111111111:role/example-prometheus-history"
  prometheus_restore_task_role_arn = "arn:aws:iam::111111111111:role/example-prometheus-restore"
  prometheus_history_bucket_name   = "example-project-data-dev"
  host_job_cap_mib                 = 22592

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
    training               = "111111111111.dkr.ecr.us-west-2.amazonaws.com/example_project/training"
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
  }

  image_digests = {
    postgres               = "sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
    redis                  = "sha256:bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"
    polaris                = "sha256:cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc"
    polaris_admin          = "sha256:1111111111111111111111111111111111111111111111111111111111111111"
    airflow                = "sha256:dddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddd"
    training               = "sha256:eeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee"
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
  }
}

run "the_history_service_is_the_shipper_with_its_own_role_and_a_read_only_data_mount" {
  command = plan

  assert {
    condition = (
      aws_ecs_task_definition.service["prometheus_history"].task_role_arn == "arn:aws:iam::111111111111:role/example-prometheus-history"
      && aws_ecs_task_definition.service["prometheus_history"].family == "example_project_dev_prometheus_history"
      && length(jsondecode(aws_ecs_task_definition.service["prometheus_history"].container_definitions)) == 1
    )
    error_message = "The shipper is its own service with its own task role, so no container of another task holds its write."
  }

  assert {
    condition = (
      jsondecode(aws_ecs_task_definition.service["prometheus_history"].container_definitions)[0].name == "prometheus_history"
      && jsondecode(aws_ecs_task_definition.service["prometheus_history"].container_definitions)[0].user == "65534:65534"
      && jsondecode(aws_ecs_task_definition.service["prometheus_history"].container_definitions)[0].readonlyRootFilesystem == true
      && jsondecode(aws_ecs_task_definition.service["prometheus_history"].container_definitions)[0].entryPoint == ["python"]
      && jsondecode(aws_ecs_task_definition.service["prometheus_history"].container_definitions)[0].command == ["-m", "monitoring.prometheus_history", "ship", "--data-dir", "/prometheus", "--interval", "300"]
      && jsondecode(aws_ecs_task_definition.service["prometheus_history"].container_definitions)[0].memory == 128
      && jsondecode(aws_ecs_task_definition.service["prometheus_history"].container_definitions)[0].memoryReservation == 48
      && jsondecode(aws_ecs_task_definition.service["prometheus_history"].container_definitions)[0].linuxParameters.capabilities.drop == ["ALL"]
    )
    error_message = "The shipper runs the reviewed module as the Prometheus user on a read-only root file system with no capability, every 300 seconds, 48 MiB reserved and 128 MiB at most."
  }

  assert {
    condition = (
      toset([for mount in jsondecode(aws_ecs_task_definition.service["prometheus_history"].container_definitions)[0].mountPoints : "${mount.containerPath}:${mount.readOnly}"])
      == toset(["/prometheus:true", "/opt/hai/monitoring:true"])
    )
    error_message = "The shipper reads the data folder and the code read only: it can never change a block."
  }

  assert {
    condition = (
      length(setsubtract(toset(["HISTORY_BUCKET_NAME=example-project-data-dev", "AWS_REGION=us-west-2"]), toset([for variable in jsondecode(aws_ecs_task_definition.service["prometheus_history"].container_definitions)[0].environment : "${variable.name}=${variable.value}"]))) == 0
      && length(lookup(jsondecode(aws_ecs_task_definition.service["prometheus_history"].container_definitions)[0], "secrets", [])) == 0
    )
    error_message = "The shipper gets the bucket and the region and no secret."
  }
}

run "the_restore_one_shot_is_the_read_only_role_and_runs_before_prometheus" {
  command = plan

  assert {
    condition = (
      aws_ecs_task_definition.one_shot["prometheus_restore"].task_role_arn == "arn:aws:iam::111111111111:role/example-prometheus-restore"
      && aws_ecs_task_definition.one_shot["prometheus_restore"].network_mode == "host"
      && length(jsondecode(aws_ecs_task_definition.one_shot["prometheus_restore"].container_definitions)) == 1
    )
    error_message = "The restore is a one-shot task with its own read-only task role."
  }

  assert {
    condition = (
      jsondecode(aws_ecs_task_definition.one_shot["prometheus_restore"].container_definitions)[0].command == ["-m", "monitoring.prometheus_history", "restore", "--data-dir", "/host/prometheus", "--scratch-dir", "/host/prometheus_restore", "--owner", "65534:65534"]
      && jsondecode(aws_ecs_task_definition.one_shot["prometheus_restore"].container_definitions)[0].user == "0"
      && jsondecode(aws_ecs_task_definition.one_shot["prometheus_restore"].container_definitions)[0].readonlyRootFilesystem == true
      && toset(jsondecode(aws_ecs_task_definition.one_shot["prometheus_restore"].container_definitions)[0].linuxParameters.capabilities.add) == toset(["CHOWN", "DAC_OVERRIDE", "FOWNER"])
      && jsondecode(aws_ecs_task_definition.one_shot["prometheus_restore"].container_definitions)[0].linuxParameters.capabilities.drop == ["ALL"]
      && jsondecode(aws_ecs_task_definition.one_shot["prometheus_restore"].container_definitions)[0].restartPolicy.enabled == false
    )
    error_message = "The restore runs the reviewed module as root with only the three capabilities that hand the blocks to the Prometheus user, and never restarts."
  }

  assert {
    condition = (
      toset([for mount in jsondecode(aws_ecs_task_definition.one_shot["prometheus_restore"].container_definitions)[0].mountPoints : "${mount.containerPath}:${mount.readOnly}"])
      == toset(["/host:false", "/opt/hai/monitoring:true"])
    )
    error_message = "The restore writes the data volume (scratch and the Prometheus folder, one volume) and reads the code only."
  }
}

run "both_history_tasks_run_in_the_training_image_that_holds_boto3" {
  command = plan

  assert {
    condition = (
      jsondecode(aws_ecs_task_definition.service["prometheus_history"].container_definitions)[0].image == "111111111111.dkr.ecr.us-west-2.amazonaws.com/example_project/training@sha256:eeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee"
      && jsondecode(aws_ecs_task_definition.one_shot["prometheus_restore"].container_definitions)[0].image == "111111111111.dkr.ecr.us-west-2.amazonaws.com/example_project/training@sha256:eeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee"
    )
    error_message = "The shipper and the restore run in the training image, whose own virtual environment holds boto3 for the module; the Airflow image failed at start with no module named boto3."
  }
}

run "prometheus_keeps_only_two_hour_blocks_so_a_finished_block_is_never_rewritten" {
  command = plan

  assert {
    condition = (
      contains(jsondecode(aws_ecs_task_definition.service["monitoring"].container_definitions)[1].command, "--storage.tsdb.min-block-duration=2h")
      && contains(jsondecode(aws_ecs_task_definition.service["monitoring"].container_definitions)[1].command, "--storage.tsdb.max-block-duration=2h")
    )
    error_message = "With history on, Prometheus is started with min and max block duration both 2h, so no block is compacted into another."
  }
}

run "the_shipper_reserves_48_mib_so_the_cap_is_at_most_22592" {
  command = plan

  assert {
    condition     = output.reserved_memory_mib == 8128 && output.host_job_memory_mib + output.reserved_memory_mib + var.host_system_reserve_mib == 32768
    error_message = "The reservations are 8128 MiB with the shipper, and the cap of 22592 MiB plus them plus the 2048 MiB reserve fills the 32768 MiB host exactly."
  }
}

run "a_cap_of_22593_is_refused_with_the_shipper" {
  command = plan

  variables {
    host_job_cap_mib = 22593
  }

  expect_failures = [output.host_job_memory_mib]
}

run "the_cap_of_the_stack_without_history_is_refused_with_it" {
  command = plan

  variables {
    host_job_cap_mib = 22640
  }

  expect_failures = [output.host_job_memory_mib]
}

run "without_the_roles_there_is_no_shipper_no_restore_and_prometheus_keeps_its_flags" {
  command = plan

  variables {
    prometheus_history_task_role_arn = null
    prometheus_restore_task_role_arn = null
    prometheus_history_bucket_name   = null
    host_job_cap_mib                 = 22640
  }

  assert {
    condition = (
      !contains(keys(aws_ecs_task_definition.service), "prometheus_history")
      && !contains(keys(aws_ecs_task_definition.one_shot), "prometheus_restore")
      && output.reserved_memory_mib == 8080
      && !contains(jsondecode(aws_ecs_task_definition.service["monitoring"].container_definitions)[1].command, "--storage.tsdb.min-block-duration=2h")
    )
    error_message = "With no role the stack is the stack of before: no service, no one-shot, 8080 MiB reserved and the default block durations."
  }
}

run "one_role_without_the_other_is_refused" {
  command = plan

  variables {
    prometheus_restore_task_role_arn = null
  }

  expect_failures = [var.prometheus_restore_task_role_arn]
}

run "the_two_roles_must_differ" {
  command = plan

  variables {
    prometheus_restore_task_role_arn = "arn:aws:iam::111111111111:role/example-prometheus-history"
  }

  expect_failures = [var.prometheus_restore_task_role_arn]
}

run "the_shipper_may_not_run_as_the_control_plane_role" {
  command = plan

  variables {
    prometheus_history_task_role_arn = "arn:aws:iam::111111111111:role/example-control"
  }

  expect_failures = [var.prometheus_history_task_role_arn]
}

run "history_needs_a_bucket" {
  command = plan

  variables {
    prometheus_history_bucket_name = null
  }

  expect_failures = [var.prometheus_history_task_role_arn]
}

run "history_needs_monitoring" {
  command = plan

  variables {
    monitoring_enabled = false
    alerts_topic_arn   = null
    host_job_cap_mib   = 22128
  }

  expect_failures = [var.prometheus_history_task_role_arn]
}

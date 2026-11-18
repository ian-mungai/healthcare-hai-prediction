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

# The catalog setup one-shot: the bootstrap catalog setup (aws_catalog_setup) runs on the host under its own task role and reaches Polaris on the host loopback.
# Failure modes pinned before the change:
# - the one-shot shares a service's task role, so a service holds the pair grants or the setup holds a service's grants;
# - the registered command is a step that writes (a principal step), so a plain launch changes the catalog;
# - the Polaris root secret reaches the container as a command-line argument or a plain environment value instead of the execution role's injection;
# - the container has a writable repository mount, a capability, a port or a restart policy it does not need;
# - the S3 records go to a bucket the setup is not told, or the apply lock parameter it honours is another one.
run "no_catalog_setup_task_without_its_role" {
  command = plan

  assert {
    condition     = !contains(keys(aws_ecs_task_definition.one_shot), "catalog_setup") && !contains(keys(output.one_shot_run_task), "catalog_setup")
    error_message = "Without catalog_setup_task_role_arn there is no catalog setup one-shot."
  }
}

run "the_catalog_setup_task_runs_the_setup_module_from_the_checkout" {
  command = plan

  variables {
    catalog_setup_task_role_arn = "arn:aws:iam::111111111111:role/example_project_host_catalog_setup_task_dev"
    code_bucket_name            = "example-project-data-dev"
  }

  assert {
    condition = (
      aws_ecs_task_definition.one_shot["catalog_setup"].task_role_arn == "arn:aws:iam::111111111111:role/example_project_host_catalog_setup_task_dev"
      && aws_ecs_task_definition.one_shot["catalog_setup"].network_mode == "host"
      && aws_ecs_task_definition.one_shot["catalog_setup"].execution_role_arn == var.execution_role_arn
      && contains(keys(output.one_shot_run_task), "catalog_setup")
    )
    error_message = "The one-shot uses its own task role, the shared execution role and the host network, and host_session finds it in the one_shot_run_task output."
  }

  assert {
    condition = (
      jsondecode(aws_ecs_task_definition.one_shot["catalog_setup"].container_definitions)[0].name == "catalog_setup"
      && jsondecode(aws_ecs_task_definition.one_shot["catalog_setup"].container_definitions)[0].image == "111111111111.dkr.ecr.us-west-2.amazonaws.com/example_project/airflow@sha256:dddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddd"
      && jsondecode(aws_ecs_task_definition.one_shot["catalog_setup"].container_definitions)[0].entryPoint == ["python", "-m", "scripts.lakehouse.aws_catalog_setup"]
      && jsondecode(aws_ecs_task_definition.one_shot["catalog_setup"].container_definitions)[0].command == ["check"]
      && jsondecode(aws_ecs_task_definition.one_shot["catalog_setup"].container_definitions)[0].user == "50000:0"
      && jsondecode(aws_ecs_task_definition.one_shot["catalog_setup"].container_definitions)[0].workingDirectory == "/opt/hai/repo"
    )
    error_message = "The container runs the setup module in the Airflow image by digest as the Airflow user, and its registered command is the read-only check."
  }

  assert {
    condition = (
      jsondecode(aws_ecs_task_definition.one_shot["catalog_setup"].container_definitions)[0].readonlyRootFilesystem
      && jsondecode(aws_ecs_task_definition.one_shot["catalog_setup"].container_definitions)[0].linuxParameters.capabilities.drop == ["ALL"]
      && length(jsondecode(aws_ecs_task_definition.one_shot["catalog_setup"].container_definitions)[0].linuxParameters.capabilities.add) == 0
      && jsondecode(aws_ecs_task_definition.one_shot["catalog_setup"].container_definitions)[0].mountPoints == [{ sourceVolume = "code_repo", containerPath = "/opt/hai/repo", readOnly = true }]
      && one([for volume in aws_ecs_task_definition.one_shot["catalog_setup"].volume : volume.host_path if volume.name == "code_repo"]) == "/mnt/example-data/repo"
      && !contains(keys(jsondecode(aws_ecs_task_definition.one_shot["catalog_setup"].container_definitions)[0]), "portMappings")
      && !jsondecode(aws_ecs_task_definition.one_shot["catalog_setup"].container_definitions)[0].restartPolicy.enabled
      && !jsondecode(aws_ecs_task_definition.one_shot["catalog_setup"].container_definitions)[0].privileged
    )
    error_message = "The container has a read-only root, no capability, a read-only checkout mount, no port and no restart."
  }

  assert {
    condition = (
      jsondecode(aws_ecs_task_definition.one_shot["catalog_setup"].container_definitions)[0].secrets == [
        { name = "POLARIS_ROOT_SECRET", valueFrom = "arn:aws:ssm:us-west-2:111111111111:parameter${var.polaris_root_secret_name}" }
      ]
      && !strcontains(jsondecode(aws_ecs_task_definition.one_shot["catalog_setup"].container_definitions)[0].command[0], "SECRET")
      && !contains([for item in jsondecode(aws_ecs_task_definition.one_shot["catalog_setup"].container_definitions)[0].environment : item.name], "POLARIS_ROOT_SECRET")
    )
    error_message = "The root secret reaches the container only as the execution role's injection of the root parameter, never as an argument or a plain value."
  }

  assert {
    condition = (
      contains(jsondecode(aws_ecs_task_definition.one_shot["catalog_setup"].container_definitions)[0].environment, { name = "DATA_BUCKET_NAME", value = "example-project-data-dev" })
      && contains(jsondecode(aws_ecs_task_definition.one_shot["catalog_setup"].container_definitions)[0].environment, { name = "CATALOG_SETUP_ROLE_ARN", value = "arn:aws:iam::111111111111:role/example_project_host_catalog_setup_task_dev" })
      && contains(jsondecode(aws_ecs_task_definition.one_shot["catalog_setup"].container_definitions)[0].environment, { name = "CATALOG_SETUP_LOCK_PARAMETER", value = var.services_apply_lock_parameter_name })
      && contains(jsondecode(aws_ecs_task_definition.one_shot["catalog_setup"].container_definitions)[0].environment, { name = "CATALOG_SETUP_CLUSTER", value = "example-project-dev" })
      && contains(jsondecode(aws_ecs_task_definition.one_shot["catalog_setup"].container_definitions)[0].environment, { name = "CATALOG_SETUP_FAMILY_PREFIX", value = "example_project_dev_job_" })
      && !contains([for item in jsondecode(aws_ecs_task_definition.one_shot["catalog_setup"].container_definitions)[0].environment : item.name], "AWS_PROFILE")
      && contains(jsondecode(aws_ecs_task_definition.one_shot["catalog_setup"].container_definitions)[0].environment, { name = "HAI_JOB", value = "catalog_setup" })
      && contains(jsondecode(aws_ecs_task_definition.one_shot["catalog_setup"].container_definitions)[0].environment, { name = "PUSHGATEWAY_URL", value = "http://10.0.1.20:9091" })
    )
    error_message = "The task is told its bucket, its own role, the apply lock it honours, the cluster and the job family prefix, and no profile."
  }

  assert {
    condition     = jsondecode(aws_ecs_task_definition.one_shot["catalog_setup"].container_definitions)[0].logConfiguration.options["awslogs-stream-prefix"] == "catalog_setup"
    error_message = "The task log stream starts with the container name, where host_session reads the result line."
  }

  assert {
    condition     = contains(output.parameter_arns_used, "arn:aws:ssm:us-west-2:111111111111:parameter${var.polaris_root_secret_name}")
    error_message = "The execution role's secret grant covers the root parameter, which the one-shot injects."
  }
}

run "a_catalog_setup_role_without_the_bucket_is_refused" {
  command = plan

  variables {
    catalog_setup_task_role_arn = "arn:aws:iam::111111111111:role/example_project_host_catalog_setup_task_dev"
  }

  expect_failures = [var.catalog_setup_task_role_arn]
}

run "a_catalog_setup_role_shared_with_the_code_fetch_is_refused" {
  command = plan

  variables {
    catalog_setup_task_role_arn = "arn:aws:iam::111111111111:role/example-code-fetch"
    code_fetch_task_role_arn    = "arn:aws:iam::111111111111:role/example-code-fetch"
    code_bucket_name            = "example-project-data-dev"
  }

  expect_failures = [var.catalog_setup_task_role_arn]
}

run "a_catalog_setup_role_shared_with_a_service_is_refused" {
  command = plan

  variables {
    catalog_setup_task_role_arn = "arn:aws:iam::111111111111:role/example-control"
    code_bucket_name            = "example-project-data-dev"
  }

  expect_failures = [var.catalog_setup_task_role_arn]
}

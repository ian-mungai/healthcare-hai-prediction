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
    postgres   = "/example/postgres"
    redis      = "/example/redis"
    polaris    = "/example/polaris"
    airflow    = "/example/airflow"
    mlflow     = "/example/mlflow"
    monitoring = "/example/monitoring"
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
  }
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

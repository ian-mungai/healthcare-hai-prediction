mock_provider "aws" {
  override_during = plan
}

variables {
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
  marquez_enabled                 = true
  openmetadata_persistent_enabled = true
  monitoring_enabled              = true
  mlflow_enabled                  = true
  mlflow_task_role_arn            = "arn:aws:iam::111111111111:role/example-mlflow"
  mlflow_host_ip                  = "10.0.1.20"
  mlflow_artifact_bucket          = "example-project-data-dev"

  log_group_names = {
    postgres   = "/example/postgres"
    redis      = "/example/redis"
    polaris    = "/example/polaris"
    airflow    = "/example/airflow"
    marquez    = "/example/marquez"
    mlflow     = "/example/mlflow"
    monitoring = "/example/monitoring"
  }

  ecr_repository_urls = {
    postgres               = "111111111111.dkr.ecr.us-west-2.amazonaws.com/example_project/postgres"
    redis                  = "111111111111.dkr.ecr.us-west-2.amazonaws.com/example_project/redis"
    polaris                = "111111111111.dkr.ecr.us-west-2.amazonaws.com/example_project/polaris"
    polaris_admin          = "111111111111.dkr.ecr.us-west-2.amazonaws.com/example_project/polaris_admin"
    airflow                = "111111111111.dkr.ecr.us-west-2.amazonaws.com/example_project/airflow"
    marquez                = "111111111111.dkr.ecr.us-west-2.amazonaws.com/example_project/marquez"
    marquez_web            = "111111111111.dkr.ecr.us-west-2.amazonaws.com/example_project/marquez_web"
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
    marquez                = "sha256:eeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee"
    marquez_web            = "sha256:ffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff"
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

# Failure modes pinned before the change:
# - the services reserve more CPU than the host keeps beside a 3072-unit job, so the job cannot be placed or the services starve it;
# - a service reserves nothing and, under a full job, gets the smallest CPU weight, so the scheduler, Polaris or Postgres stall;
# - an optional service (monitoring, MLflow, OpenMetadata) added later pushes the total over the limit without any plan failing;
# - a job scratch folder is missing, so Docker creates it owned by root and the job user cannot write to it;
# - the preparation task changes the owner of, or opens, a folder it should not touch.


# The code one-shot: it puts the reviewed code archive of one run on the host as the checkout that Airflow and the catalog bootstrap mount.
run "no_code_fetch_task_without_its_role" {
  command = plan

  assert {
    condition     = !contains(keys(aws_ecs_task_definition.one_shot), "code_fetch") && !contains(keys(output.one_shot_run_task), "code_fetch")
    error_message = "Without code_fetch_task_role_arn there is no code one-shot."
  }
}

run "the_code_fetch_task_runs_the_image_fetch_script" {
  command = plan

  variables {
    code_fetch_task_role_arn = "arn:aws:iam::111111111111:role/example-code-fetch"
    code_bucket_name         = "example-project-data-dev"
  }

  assert {
    condition = (
      aws_ecs_task_definition.one_shot["code_fetch"].task_role_arn == "arn:aws:iam::111111111111:role/example-code-fetch"
      && aws_ecs_task_definition.one_shot["code_fetch"].network_mode == "host"
      && contains(keys(output.one_shot_run_task), "code_fetch")
    )
    error_message = "The code one-shot uses its own task role and the host network, and start finds it in the one_shot_run_task output."
  }

  assert {
    condition = (
      jsondecode(aws_ecs_task_definition.one_shot["code_fetch"].container_definitions)[0].name == "code_fetch"
      && jsondecode(aws_ecs_task_definition.one_shot["code_fetch"].container_definitions)[0].image == "111111111111.dkr.ecr.us-west-2.amazonaws.com/example_project/training@sha256:9999999999999999999999999999999999999999999999999999999999999999"
      && jsondecode(aws_ecs_task_definition.one_shot["code_fetch"].container_definitions)[0].entryPoint == ["python", "-I", "-c"]
      && strcontains(jsondecode(aws_ecs_task_definition.one_shot["code_fetch"].container_definitions)[0].command[0], "/opt/hai")
      && strcontains(jsondecode(aws_ecs_task_definition.one_shot["code_fetch"].container_definitions)[0].command[0], "import_module(\"fetch_code\")")
    )
    error_message = "The container runs the training image by digest and calls the image's own fetch_code.py."
  }

  assert {
    condition = (
      jsondecode(aws_ecs_task_definition.one_shot["code_fetch"].container_definitions)[0].readonlyRootFilesystem
      && jsondecode(aws_ecs_task_definition.one_shot["code_fetch"].container_definitions)[0].linuxParameters.capabilities.drop == ["ALL"]
      && length(jsondecode(aws_ecs_task_definition.one_shot["code_fetch"].container_definitions)[0].linuxParameters.capabilities.add) == 0
      && jsondecode(aws_ecs_task_definition.one_shot["code_fetch"].container_definitions)[0].mountPoints == [{ sourceVolume = "data", containerPath = "/host", readOnly = false }]
      && one([for volume in aws_ecs_task_definition.one_shot["code_fetch"].volume : volume.host_path if volume.name == "data"]) == "/mnt/example-data"
    )
    error_message = "The container has a read-only root, no capability and only the data volume, where it writes the releases and the repo link."
  }

  assert {
    condition = (
      contains(jsondecode(aws_ecs_task_definition.one_shot["code_fetch"].container_definitions)[0].environment, { name = "DATA_BUCKET_NAME", value = "example-project-data-dev" })
      && !contains([for item in jsondecode(aws_ecs_task_definition.one_shot["code_fetch"].container_definitions)[0].environment : item.name], "CODE_RUN_ID")
      && !contains([for item in jsondecode(aws_ecs_task_definition.one_shot["code_fetch"].container_definitions)[0].environment : item.name], "CODE_SHA256")
    )
    error_message = "The bucket is fixed. The run ID and the code digest come only from the RunTask overrides of host_session start."
  }
}

run "a_code_fetch_role_without_the_bucket_is_refused" {
  command = plan

  variables {
    code_fetch_task_role_arn = "arn:aws:iam::111111111111:role/example-code-fetch"
  }

  expect_failures = [var.code_fetch_task_role_arn]
}

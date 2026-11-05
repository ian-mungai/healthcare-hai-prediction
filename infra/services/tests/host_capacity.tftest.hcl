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
  }
}

# Failure modes pinned before the change:
# - the services reserve more CPU than the host keeps beside a 3072-unit job, so the job cannot be placed or the services starve it;
# - a service reserves nothing and, under a full job, gets the smallest CPU weight, so the scheduler, Polaris or Postgres stall;
# - an optional service (monitoring, MLflow, OpenMetadata) added later pushes the total over the limit without any plan failing;
# - a job scratch folder is missing, so Docker creates it owned by root and the job user cannot write to it;
# - the preparation task changes the owner of, or opens, a folder it should not touch.

run "service_cpu_reservations_fit_beside_a_full_job" {
  command = plan

  assert {
    condition     = output.reserved_cpu_units <= var.host_cpu_units - var.job_cpu_units && var.host_cpu_units == 4096 && var.job_cpu_units == 3072
    error_message = "The services reserve at most 1024 of the host's 4096 CPU units, which leaves the 3072 a job reserves."
  }

  assert {
    condition = (
      output.service_cpu_units["airflow_scheduler"] == 256 && output.service_cpu_units["polaris"] == 256 && output.service_cpu_units["postgres"] == 256 &&
      output.service_cpu_units["airflow_apiserver"] == 128
    )
    error_message = "The scheduler, Polaris and Postgres reserve 256 units each and the API server 128, the owner's split of about 1024."
  }

  assert {
    condition     = sum(values(output.service_cpu_units)) <= 1024
    error_message = "Every container that the reservation table names, planned or not, fits in 1024 units together."
  }

  assert {
    condition = alltrue([
      for definition in aws_ecs_task_definition.service : alltrue([
        for container in jsondecode(definition.container_definitions) : lookup(container, "cpu", 0) == lookup(output.service_cpu_units, container.name, 0)
      ])
    ])
    error_message = "Each planned service container carries exactly the CPU units of the reservation table, and a container the table does not name carries none."
  }

  assert {
    condition = sum(flatten([
      for definition in aws_ecs_task_definition.service : [for container in jsondecode(definition.container_definitions) : lookup(container, "cpu", 0)]
    ])) == output.reserved_cpu_units && output.reserved_cpu_units > 0
    error_message = "The reserved CPU output is the sum of the planned containers."
  }

  assert {
    condition = alltrue([
      for definition in aws_ecs_task_definition.one_shot : alltrue([for container in jsondecode(definition.container_definitions) : !contains(keys(container), "cpu")])
    ])
    error_message = "One-shot tasks reserve no CPU: they run briefly, before any job."
  }
}

run "a_job_that_leaves_less_room_than_the_services_reserve_is_refused" {
  command = plan

  variables {
    job_cpu_units = 3584
  }

  expect_failures = [aws_ecs_task_definition.service]
}

run "a_smaller_host_is_refused" {
  command = plan

  variables {
    host_cpu_units = 2048
    job_cpu_units  = 1536
  }

  expect_failures = [aws_ecs_task_definition.service]
}

run "host_prepare_makes_one_owned_folder_pair_per_job_family" {
  command = plan

  assert {
    condition = alltrue([
      for family in ["bronze", "drift", "features", "gold", "metadata", "promote", "serve", "silver", "train"] : (
        strcontains(jsondecode(aws_ecs_task_definition.one_shot["host_prepare"].container_definitions)[0].command[0], "/host/job_scratch/${family}/work /host/job_scratch/${family}/tmp")
      )
    ])
    error_message = "Preparation makes a work and a tmp folder for each of the nine job families under the scratch folder."
  }

  assert {
    condition = (
      strcontains(jsondecode(aws_ecs_task_definition.one_shot["host_prepare"].container_definitions)[0].command[0], "chown 10001:10001 ")
      && strcontains(jsondecode(aws_ecs_task_definition.one_shot["host_prepare"].container_definitions)[0].command[0], "chmod 0700 /host/job_scratch")
      && !strcontains(jsondecode(aws_ecs_task_definition.one_shot["host_prepare"].container_definitions)[0].command[0], "-R")
    )
    error_message = "The job folders belong to the job user, the scratch folder itself is private and no command walks a tree."
  }

  assert {
    condition = (
      toset(jsondecode(aws_ecs_task_definition.one_shot["host_prepare"].container_definitions)[0].linuxParameters.capabilities.add) == toset(["CHOWN", "DAC_OVERRIDE", "FOWNER"]) &&
      strcontains(jsondecode(aws_ecs_task_definition.one_shot["host_prepare"].container_definitions)[0].healthCheck.command[1], "test -d /host/job_scratch")
    )
    error_message = "Preparation keeps its three capabilities and its health check also requires the scratch folder."
  }
}

run "an_unsafe_scratch_folder_name_is_refused" {
  command = plan

  variables {
    host_scratch_folder = "../etc"
  }

  expect_failures = [var.host_scratch_folder]
}

run "an_unknown_job_family_name_is_refused" {
  command = plan

  variables {
    host_job_families = ["silver", "gold space"]
  }

  expect_failures = [var.host_job_families]
}

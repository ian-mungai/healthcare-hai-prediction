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
  host_private_ip                 = "10.0.1.20"
  serving_execution_role_arn      = "arn:aws:iam::111111111111:role/example-serving-execution"
  model_api_task_role_arn         = "arn:aws:iam::111111111111:role/example-model-api"
  streamlit_task_role_arn         = "arn:aws:iam::111111111111:role/example-streamlit"
  model_api_manifest_uri          = "s3://example-project-data-dev/serving/deployments"
  model_api_registry_uri          = "s3://example-project-data-dev/lakehouse/feast/registry.pb"
  mlflow_artifact_bucket          = "example-project-data-dev"
  # TEST FIXTURE ONLY, not a measured value: an explicit override of the 32768 MiB default, as the owner sets it from the ECS registered memory of the host.
  host_usable_memory_mib = 30720
  # TEST FIXTURE ONLY, not a measured value: the cap the owner sets (host_job_cap_mib). Here it equals the job room of this fixture (30720 usable MiB less the
  # 2048 MiB system reserve and the 7632 MiB the four optional services and Airflow reserve).
  host_job_cap_mib = 21040

  # The committed defaults of the services stack are the live ones (MLflow, model API, page, monitoring, OpenMetadata and HTTP lineage on). Every case here
  # that does not set a switch tests it off, so the fixture sets each one explicitly.
  model_api_enabled            = false
  streamlit_enabled            = false
  airflow_lineage_http_enabled = false
  model_api_mlflow_uri         = ""

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
    model_api              = "111111111111.dkr.ecr.us-west-2.amazonaws.com/example_project/model_api"
    streamlit              = "111111111111.dkr.ecr.us-west-2.amazonaws.com/example_project/streamlit"
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
    model_api              = "sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaab"
    streamlit              = "sha256:bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbc"
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

run "the_job_memory_is_the_cap_and_fits_the_room" {
  command = plan

  assert {
    condition     = output.host_job_memory_mib == var.host_job_cap_mib && var.host_system_reserve_mib == 2048
    error_message = "One job gets exactly the memory of the one cap value host_job_cap_mib, and the system reserve stays 2048 MiB."
  }

  assert {
    condition     = output.host_job_memory_mib + output.reserved_memory_mib + var.host_system_reserve_mib <= var.host_usable_memory_mib
    error_message = "The job, the service reservations and the safety margin together fit the memory ECS can place."
  }

  assert {
    condition     = output.host_job_memory_mib >= 4096 && output.host_job_memory_mib < var.host_usable_memory_mib
    error_message = "The cap is a usable job size and smaller than the usable memory."
  }

  assert {
    condition     = output.host_capacity_binding == "job_memory_mib=${var.host_job_cap_mib} usable_memory_mib=30720 system_reserve_mib=2048 service_reservations_mib=${output.reserved_memory_mib}"
    error_message = "The capacity binding names the cap and the usable memory, the safety margin and the service reservations it fits (owner decision A, Nov 10 2026: host launch capacity binding)."
  }

  assert {
    condition = alltrue([
      for key in ["airflow_apiserver", "airflow_scheduler", "airflow_dag_processor", "airflow_triggerer"] : alltrue([
        for container in jsondecode(aws_ecs_task_definition.service[key].container_definitions) :
        contains([for item in container.environment : "${item.name}=${item.value}"], "HAI_LAUNCH_HOST_CAPACITY=usable_memory_mib=30720 system_reserve_mib=2048") if startswith(container.name, "airflow")
      ])
    ])
    error_message = "Every Airflow container carries the current usable-memory inputs in HAI_LAUNCH_HOST_CAPACITY, the part of the binding the launch preflight checks a host revision against."
  }
}

run "a_smaller_reserve_leaves_the_cap_unchanged" {
  command = plan

  variables {
    host_system_reserve_mib = 1024
  }

  # The rejected design started from the nominal 32768 MiB: with a 1024 MiB reserve the job and the reservations then took 31744 MiB, more than ECS can place.
  assert {
    condition     = output.host_job_memory_mib + output.reserved_memory_mib + 1024 <= var.host_usable_memory_mib
    error_message = "With a 1024 MiB reserve the job, the service reservations and the margin still fit the memory ECS can place."
  }

  assert {
    condition     = output.host_job_memory_mib == var.host_job_cap_mib
    error_message = "More room does not change the cap: the owner raises host_job_cap_mib to use it."
  }
}

run "a_larger_system_reserve_that_removes_the_room_for_the_cap_is_refused" {
  command = plan

  variables {
    host_system_reserve_mib = 4096
  }

  # The cap (21040) no longer fits: 30720 less 4096 less 7632 leaves 18992 MiB. The owner lowers host_job_cap_mib in the same change.
  expect_failures = [output.host_job_memory_mib]
}

run "a_larger_system_reserve_with_a_lowered_cap_is_accepted" {
  command = plan

  variables {
    host_system_reserve_mib = 4096
    host_job_cap_mib        = 18992
  }

  assert {
    condition     = output.host_job_memory_mib == var.host_usable_memory_mib - 4096 - output.reserved_memory_mib
    error_message = "A cap equal to the room left by the larger reserve is accepted."
  }
}

run "the_default_usable_memory_is_the_current_host_size" {
  command = plan

  # Recommended capacity from the start (owner decision, Nov 10 2026): an unset input takes the default, the current r7g.xlarge host size of 32768 MiB,
  # so the stack always gives a job room. A null input is the same as no input for a variable that is not nullable.
  variables {
    host_usable_memory_mib = null
  }

  assert {
    condition     = var.host_usable_memory_mib == 32768 && output.host_job_memory_mib == var.host_job_cap_mib && var.host_job_cap_mib <= 32768 - 2048 - output.reserved_memory_mib
    error_message = "Without an owner value the room comes from the current host size (32768 MiB) less the 2048 MiB system reserve and the service reservations, and the cap fits it."
  }

  assert {
    condition = output.host_capacity_binding == "job_memory_mib=${var.host_job_cap_mib} usable_memory_mib=32768 system_reserve_mib=2048 service_reservations_mib=${output.reserved_memory_mib}" && alltrue([
      for key in ["airflow_apiserver", "airflow_scheduler", "airflow_dag_processor", "airflow_triggerer"] : alltrue([
        for container in jsondecode(aws_ecs_task_definition.service[key].container_definitions) :
        contains([for item in container.environment : "${item.name}=${item.value}"], "HAI_LAUNCH_HOST_CAPACITY=usable_memory_mib=32768 system_reserve_mib=2048") if startswith(container.name, "airflow")
      ])
    ])
    error_message = "The default gives a capacity binding and every Airflow container carries its usable-memory inputs, so host jobs are not refused for an empty capacity."
  }
}

run "usable_memory_above_the_nominal_host_is_refused" {
  command = plan

  variables {
    host_usable_memory_mib = 32769
  }

  expect_failures = [var.host_usable_memory_mib]
}

run "usable_memory_of_zero_is_refused" {
  command = plan

  variables {
    host_usable_memory_mib = 0
  }

  expect_failures = [var.host_usable_memory_mib]
}

run "usable_memory_that_is_not_a_whole_number_is_refused" {
  command = plan

  variables {
    host_usable_memory_mib = 30720.5
  }

  expect_failures = [var.host_usable_memory_mib]
}

run "services_that_leave_no_room_for_a_job_are_refused" {
  command = plan

  variables {
    host_job_memory_floor_mib = 30720
  }

  # Only the job-room guard can refuse this: the reserve stays at its default, so the hard-limit guard on the service definitions passes.
  expect_failures = [output.host_job_memory_mib]
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
      && strcontains(jsondecode(aws_ecs_task_definition.one_shot["host_prepare"].container_definitions)[0].command[0], "chmod 0711 /host/job_scratch\n")
      && !strcontains(jsondecode(aws_ecs_task_definition.one_shot["host_prepare"].container_definitions)[0].command[0], "chmod 0700 /host/job_scratch")
      && !strcontains(jsondecode(aws_ecs_task_definition.one_shot["host_prepare"].container_definitions)[0].command[0], "chmod 0755 /host/job_scratch")
      && !strcontains(jsondecode(aws_ecs_task_definition.one_shot["host_prepare"].container_definitions)[0].command[0], "-R")
    )
    error_message = "The job folders belong to the job user, the scratch folder itself can be traversed but not listed (0711, so the Airflow fit probe reaches each family folder through its read-only mount) and no command walks a tree."
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

run "the_host_job_cap_is_rendered_into_every_airflow_container" {
  command = plan

  # Host job cap setting (Nov 11 2026): the one variable host_job_cap_mib is the cap of the Airflow environment, the job memory the jobs stack gets and the
  # capacity binding. The hold and the apply lock stay Parameter Store parameters that the services apply command manages.
  assert {
    condition = alltrue([
      for key in ["airflow_apiserver", "airflow_scheduler", "airflow_dag_processor", "airflow_triggerer"] : alltrue([
        for container in jsondecode(aws_ecs_task_definition.service[key].container_definitions) :
        contains([for item in container.environment : "${item.name}=${item.value}"], "HAI_LAUNCH_HOST_JOB_CAP_MIB=21040") &&
        contains([for item in container.environment : "${item.name}=${item.value}"], "HAI_LAUNCH_HOST_HOLD_PARAMETER=/healthcare_hai_prediction/host_launch_hold") &&
        contains([for item in container.environment : "${item.name}=${item.value}"], "HAI_LAUNCH_APPLY_LOCK_PARAMETER=/healthcare_hai_prediction/services_apply_lock") if startswith(container.name, "airflow")
      ])
    ])
    error_message = "Every Airflow container carries the cap as HAI_LAUNCH_HOST_JOB_CAP_MIB and still names the launch hold and the apply lock parameters."
  }
}

run "the_default_cap_is_the_job_room_the_committed_default_services_leave" {
  command = plan

  # The committed defaults run MLflow, the model API, the page, monitoring and persistent OpenMetadata, and not Marquez, on the 32768 MiB host: the services
  # reserve 7920 MiB, so one job has 32768 - 2048 - 7920 = 22800 MiB. The default of host_job_cap_mib is that figure (one host job at a time, set from the
  # Airflow pool of one slot).
  variables {
    marquez_enabled                 = false
    mlflow_enabled                  = true
    model_api_enabled               = true
    model_api_mlflow_uri            = "http://127.0.0.1:5100"
    streamlit_enabled               = true
    monitoring_enabled              = true
    openmetadata_persistent_enabled = true
    airflow_lineage_http_enabled    = true
    host_usable_memory_mib          = null
    host_job_cap_mib                = null
  }

  assert {
    condition     = var.host_job_cap_mib == 22800 && output.reserved_memory_mib == 7920 && output.host_job_memory_mib == 22800 && output.host_job_memory_mib == 32768 - 2048 - output.reserved_memory_mib
    error_message = "The default cap is 22800 MiB, the job room the services leave with every committed default on."
  }

  assert {
    condition = alltrue([
      for key in ["airflow_apiserver", "airflow_scheduler", "airflow_dag_processor", "airflow_triggerer"] : alltrue([
        for container in jsondecode(aws_ecs_task_definition.service[key].container_definitions) :
        contains([for item in container.environment : "${item.name}=${item.value}"], "HAI_LAUNCH_HOST_JOB_CAP_MIB=22800") if startswith(container.name, "airflow")
      ])
    ])
    error_message = "Every Airflow container carries the default cap of 22800 MiB."
  }
}

run "a_cap_above_the_job_room_is_refused" {
  command = plan

  # Marquez on beside the committed default services leaves less than the default cap of 22800 MiB on the 32768 MiB host.
  variables {
    marquez_enabled                 = true
    mlflow_enabled                  = true
    model_api_enabled               = true
    model_api_mlflow_uri            = "http://127.0.0.1:5100"
    streamlit_enabled               = true
    monitoring_enabled              = true
    openmetadata_persistent_enabled = true
    airflow_lineage_http_enabled    = true
    host_usable_memory_mib          = null
    host_job_cap_mib                = null
  }

  expect_failures = [output.host_job_memory_mib]
}

run "a_cap_of_zero_is_refused" {
  command = plan

  variables {
    host_job_cap_mib = 0
  }

  expect_failures = [var.host_job_cap_mib]
}

run "a_cap_below_1024_is_refused" {
  command = plan

  variables {
    host_job_cap_mib = 1023
  }

  expect_failures = [var.host_job_cap_mib]
}

run "a_cap_that_is_not_a_whole_number_is_refused" {
  command = plan

  variables {
    host_job_cap_mib = 21040.5
  }

  expect_failures = [var.host_job_cap_mib]
}

run "a_cap_at_or_above_the_host_memory_is_refused" {
  command = plan

  variables {
    host_job_cap_mib = 32768
  }

  expect_failures = [var.host_job_cap_mib]
}

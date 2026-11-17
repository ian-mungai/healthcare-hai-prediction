mock_provider "aws" {}

# The bronze stage job: a small host family under the bronze writer role that stages the request and the source inventory of a pipeline run before the bronze
# job starts. It reserves its own size, holds no catalog pair and shares the bronze task's raw read environment.
variables {
  aws_profile         = "example_project_dev"
  aws_region          = "us-west-2"
  expected_account_id = "111111111111"
  project_name        = "example_project"
  environment         = "dev"
  data_bucket_name    = "example-project-data-dev"
  images = {
    spark = "111111111111.dkr.ecr.us-west-2.amazonaws.com/example_project_spark_dev@sha256:cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc"
  }
  job_role_arns = {
    execution         = "arn:aws:iam::111111111111:role/example_project_job_execution_dev"
    write_bronze_task = "arn:aws:iam::111111111111:role/example_project_job_write_bronze_task_dev"
  }
  writer_parameter_names = {
    write_bronze_task = ["/example_project/aws_polaris_bronze_client_id", "/example_project/aws_polaris_bronze_client_secret"]
  }
  log_group_name      = "example_project_jobs_dev"
  host_job_memory_mib = 23552
  # TEST FIXTURE ONLY, not measured values: the services output host_capacity_binding for a 30720 MiB usable host.
  host_capacity_binding = "job_memory_mib=23552 usable_memory_mib=30720 system_reserve_mib=2048 service_reservations_mib=5120"
  host_jobs_enabled     = true
  host_jobs             = ["bronze", "bronze_stage"]
  bronze_job_enabled    = true
}

run "the_stage_job_is_one_small_host_task_under_the_bronze_writer_role" {
  command = plan

  assert {
    condition = (
      aws_ecs_task_definition.host["bronze_stage"].family == "example_project_dev_job_bronze_stage_host" &&
      aws_ecs_task_definition.host["bronze_stage"].task_role_arn == "arn:aws:iam::111111111111:role/example_project_job_write_bronze_task_dev" &&
      aws_ecs_task_definition.host["bronze_stage"].execution_role_arn == "arn:aws:iam::111111111111:role/example_project_job_execution_dev" &&
      aws_ecs_task_definition.host["bronze_stage"].network_mode == "bridge" && aws_ecs_task_definition.host["bronze_stage"].skip_destroy &&
      aws_ecs_task_definition.host["bronze_stage"].runtime_platform[0].cpu_architecture == "ARM64" && !aws_ecs_task_definition.host["bronze_stage"].enable_fault_injection
    )
    error_message = "The stage job is one ARM64 host definition with the execution role and only the bronze writer role."
  }

  assert {
    condition = (
      aws_ecs_task_definition.host["bronze_stage"].cpu == "512" && aws_ecs_task_definition.host["bronze_stage"].memory == "2048" &&
      aws_ecs_task_definition.host["bronze"].cpu == "3072" && aws_ecs_task_definition.host["bronze"].memory == "23552" &&
      output.host_dispatch_contract.family_memory_mib["bronze_stage"] == 2048 && output.host_dispatch_contract.family_cpu_units["bronze_stage"] == 512 &&
      output.host_dispatch_contract.family_networks["bronze_stage"] == "lakehouse"
    )
    error_message = "The stage job reserves the 512 units and 2048 MiB of its row in host_jobs.json, and the bronze job keeps the whole job room."
  }

  assert {
    condition = (
      strcontains([for container in jsondecode(output.host_container_definitions["bronze_stage"]) : container if container.name == "init"][0].dockerLabels["hai.host_capacity"], "job_memory_mib=2048 ") &&
      output.host_run_tasks["bronze_stage"].task_role_key == "write_bronze_task" && output.host_run_tasks["bronze_stage"].dry_run_variable == null
    )
    error_message = "The init container carries the capacity binding sized to the stage job's own memory, the run task names the bronze writer role and the job has no dry run override."
  }
}

run "the_stage_containers_follow_the_direct_job_table" {
  command = plan

  assert {
    condition = (
      [for container in jsondecode(output.host_container_definitions["bronze_stage"]) : container.name] == ["init", "fetch", "stage"] &&
      toset([for container in jsondecode(output.host_container_definitions["bronze_stage"]) : container.name if container.name != "init"]) == toset(keys(jsondecode(file("${path.module}/../../config/orchestration/direct_jobs.json")).jobs.bronze_stage.containers))
    )
    error_message = "The stage containers run in phase order after the volume ownership step and are the ones the direct job table names."
  }

  assert {
    condition = alltrue([for container in jsondecode(output.host_container_definitions["bronze_stage"]) : (
      container.name == "init" || toset([for variable in container.environment : variable.name if variable.value == ""]) == toset(jsondecode(file("${path.module}/../../config/orchestration/direct_jobs.json")).jobs.bronze_stage.containers[container.name])
    )])
    error_message = "The empty variables of every stage container equal the direct job table."
  }

  assert {
    condition     = [for container in jsondecode(output.host_container_definitions["bronze_stage"]) : container.essential] == [false, false, true]
    error_message = "Only the stage container is essential."
  }

  assert {
    condition = alltrue([for index, container in jsondecode(output.host_container_definitions["bronze_stage"]) : (
      index == 0 ? length(container.dependsOn) == 0 : (
        length(container.dependsOn) == 1 && container.dependsOn[0].containerName == jsondecode(output.host_container_definitions["bronze_stage"])[index - 1].name && container.dependsOn[0].condition == "SUCCESS"
      )
    )])
    error_message = "Every stage container starts after the success of the one before."
  }
}

run "the_stage_phase_runs_the_bronze_driver_in_the_spark_image_with_no_secret" {
  command = plan

  assert {
    condition = (
      jsondecode(output.host_container_definitions["bronze_stage"])[2].command == ["(cd /workspace/code && python -m scripts.lakehouse.bronze_run stage --run \"$RUN_ID\") && find /workspace /tmp -mindepth 1 -delete"] &&
      strcontains(jsondecode(output.host_container_definitions["bronze_stage"])[1].command[0], "CODE_SHA256") &&
      alltrue([for container in jsondecode(output.host_container_definitions["bronze_stage"]) : container.name == "init" || container.image == var.images.spark])
    )
    error_message = "The fetch step downloads the staged archive and checks it against CODE_SHA256, the stage phase runs the driver with the run ID only and then the scratch cleanup, and every phase runs in the Spark image."
  }

  assert {
    condition     = alltrue([for container in jsondecode(output.host_container_definitions["bronze_stage"]) : length(container.secrets) == 0])
    error_message = "The stage job reads the sources and writes two objects: no container holds a catalog pair."
  }

  assert {
    condition     = length(output.host_secret_flow.violations) == 0 && !contains(output.host_secret_flow.containers_checked, "host_bronze_stage/stage")
    error_message = "A job with no secret has nothing for the secret flow to check."
  }

  assert {
    condition     = alltrue([for container in jsondecode(output.host_container_definitions["bronze_stage"]) : !strcontains(jsonencode(container), "POLARIS") && !strcontains(jsonencode(container), "ALERTS_TOPIC_ARN")])
    error_message = "The stage job uses no catalog and sends no notice."
  }
}

run "the_raw_read_environment_reaches_the_stage_phases" {
  command = plan

  variables {
    bronze_read_environment = { HAI_READ_RAW_ROOT = "true" }
  }

  assert {
    condition = alltrue([
      for container in slice(jsondecode(output.host_container_definitions["bronze_stage"]), 1, 3) : contains(container.environment, { name = "HAI_READ_RAW_ROOT", value = "true" })
    ])
    error_message = "The fetch and stage phases receive the raw read environment of the bronze writer contract, as the bronze phases do."
  }

  assert {
    condition     = !contains(jsondecode(output.host_container_definitions["bronze_stage"])[0].environment, { name = "HAI_READ_RAW_ROOT", value = "true" })
    error_message = "The init step receives no read environment."
  }
}

run "the_stage_job_needs_the_bronze_inputs" {
  command = plan

  variables {
    bronze_job_enabled = false
  }

  expect_failures = [var.host_jobs]
}

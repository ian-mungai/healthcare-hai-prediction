mock_provider "aws" {}

variables {
  # The committed defaults of the jobs stack are the live ones (every job family on, host_repo_path and serve_manifest_prefix set). Every case here that does
  # not set a switch tests it off, so the fixture sets each one explicitly.
  drift_enabled          = false
  promote_enabled        = false
  layer_jobs_enabled     = false
  serve_enabled          = false
  train_enabled          = false
  publisher_job_enabled  = false
  publisher_keys_enabled = false
  host_repo_path         = null
  serve_manifest_prefix  = ""
  aws_profile            = "example_project_dev"
  aws_region             = "us-west-2"
  expected_account_id    = "111111111111"
  project_name           = "example_project"
  environment            = "dev"
  data_bucket_name       = "example-project-data-dev"
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
  host_jobs             = ["bronze"]
}

run "the_bronze_job_is_one_host_task_with_the_bronze_writer_role" {
  command = plan

  variables {
    bronze_job_enabled = true
  }

  assert {
    condition = (
      aws_ecs_task_definition.host["bronze"].family == "example_project_dev_job_bronze_host" &&
      aws_ecs_task_definition.host["bronze"].task_role_arn == "arn:aws:iam::111111111111:role/example_project_job_write_bronze_task_dev" &&
      aws_ecs_task_definition.host["bronze"].execution_role_arn == "arn:aws:iam::111111111111:role/example_project_job_execution_dev" &&
      toset(aws_ecs_task_definition.host["bronze"].requires_compatibilities) == toset(["EC2"]) && aws_ecs_task_definition.host["bronze"].network_mode == "bridge" &&
      aws_ecs_task_definition.host["bronze"].runtime_platform[0].cpu_architecture == "ARM64" && aws_ecs_task_definition.host["bronze"].skip_destroy &&
      !aws_ecs_task_definition.host["bronze"].enable_fault_injection &&
      aws_ecs_task_definition.host["bronze"].cpu == "3072" && aws_ecs_task_definition.host["bronze"].memory == "23552"
    )
    error_message = "The bronze job is one ARM64 host definition with the execution role, only the bronze writer role and the host job cap."
  }

  assert {
    condition = (
      output.host_run_tasks["bronze"].dry_run_variable == "BRONZE_DRY_RUN" && output.host_run_tasks["bronze"].dry_run_value == "1" &&
      output.host_run_tasks["bronze"].container_environment.load == jsondecode(file("${path.module}/../../config/orchestration/ecs_broker_jobs.json")).jobs.bronze.containers.load
    )
    error_message = "The run task output lists what the launch passes to each container and the one override that makes a dry run."
  }
}

run "containers_follow_the_job_catalog_in_order" {
  command = plan

  variables {
    bronze_job_enabled = true
  }

  assert {
    condition = (
      [for container in jsondecode(output.host_container_definitions["bronze"]) : container.name] == ["init", "fetch", "pin", "load", "check"] &&
      toset([for container in jsondecode(output.host_container_definitions["bronze"]) : container.name if container.name != "init"]) == toset(keys(jsondecode(file("${path.module}/../../config/orchestration/ecs_broker_jobs.json")).jobs.bronze.containers))
    )
    error_message = "The containers run in phase order after the volume ownership step and are the ones the job catalog names."
  }

  assert {
    condition = alltrue([for container in jsondecode(output.host_container_definitions["bronze"]) : (
      container.name == "init" || toset([for variable in container.environment : variable.name if variable.value == ""]) == toset(jsondecode(file("${path.module}/../../config/orchestration/ecs_broker_jobs.json")).jobs.bronze.containers[container.name])
    )])
    error_message = "The empty variables of every container equal the job catalog, so no table list, target or dry run flag is fixed in the definition."
  }

  assert {
    condition = alltrue([for index, container in jsondecode(output.host_container_definitions["bronze"]) : (
      index == 0 ? length(container.dependsOn) == 0 : (
        length(container.dependsOn) == 1 && container.dependsOn[0].containerName == jsondecode(output.host_container_definitions["bronze"])[index - 1].name && container.dependsOn[0].condition == "SUCCESS"
      )
    )])
    error_message = "Every container starts after the success of the one before, so a failed phase stops the task."
  }

  assert {
    condition     = [for container in jsondecode(output.host_container_definitions["bronze"]) : container.essential] == [false, false, false, false, true]
    error_message = "Only the last container is essential."
  }
}

run "each_phase_runs_the_bronze_driver_and_nothing_else" {
  command = plan

  variables {
    bronze_job_enabled = true
  }

  assert {
    condition = alltrue([for container in slice(jsondecode(output.host_container_definitions["bronze"]), 2, 4) :
      container.command == ["cd /workspace/code && python -m scripts.lakehouse.bronze_run ${container.name} --run \"$RUN_ID\""]
    ]) && jsondecode(output.host_container_definitions["bronze"])[4].command == ["(cd /workspace/code && python -m scripts.lakehouse.bronze_run check --run \"$RUN_ID\") && find /workspace /tmp -mindepth 1 -delete"]
    error_message = "Pin, load and check run the driver phase of their own name, with the run ID and no table, namespace or target argument. Only check, the last phase, follows it with the scratch cleanup."
  }

  assert {
    condition = (
      strcontains(jsondecode(output.host_container_definitions["bronze"])[1].command[0], "CODE_SHA256") && strcontains(jsondecode(output.host_container_definitions["bronze"])[1].command[0], "run_stage/") &&
      alltrue([for container in jsondecode(output.host_container_definitions["bronze"]) : container.name == "init" || container.image == var.images.spark])
    )
    error_message = "The fetch step downloads the staged archive and checks it against CODE_SHA256. Every phase runs in the Spark image."
  }

  assert {
    condition     = alltrue([for container in jsondecode(output.host_container_definitions["bronze"]) : !strcontains(jsonencode(container), "bronze_e2e") && !strcontains(jsonencode(container), "--namespace")])
    error_message = "No namespace is named in the definition: the driver derives it from the checked target."
  }
}

run "every_phase_runs_under_the_deadline_runner" {
  command = plan

  variables {
    bronze_job_enabled = true
  }

  assert {
    condition = alltrue([for container in slice(jsondecode(output.host_container_definitions["bronze"]), 1, 5) :
      container.entryPoint == ["/opt/hai/deadline_run", "sh", "-c"] && contains([for variable in container.environment : variable.name if variable.value == ""], "HAI_DEADLINE_UTC")
    ])
    error_message = "Fetch, pin, load and check start through the in-image deadline runner and take one absolute deadline from the launch, so no phase outlives the task deadline."
  }

  assert {
    condition     = jsondecode(output.host_container_definitions["bronze"])[0].entryPoint == ["sh", "-c"] && jsondecode(output.host_container_definitions["bronze"])[0].command == ["find /workspace /tmp -mindepth 1 -delete && chown 10001:10001 /workspace /tmp"]
    error_message = "The init step only wipes the folders and hands over the volumes, and stays outside the runner."
  }
}

run "the_init_step_hands_the_volumes_to_the_job_user" {
  command = plan

  variables {
    bronze_job_enabled = true
  }

  assert {
    condition = (
      jsondecode(output.host_container_definitions["bronze"])[0].user == "0:0" && jsondecode(output.host_container_definitions["bronze"])[0].command == ["find /workspace /tmp -mindepth 1 -delete && chown 10001:10001 /workspace /tmp"] &&
      jsondecode(output.host_container_definitions["bronze"])[0].secrets == [] && !jsondecode(output.host_container_definitions["bronze"])[0].privileged &&
      jsondecode(output.host_container_definitions["bronze"])[0].linuxParameters.capabilities.add == []
    )
    error_message = "The init step is the only root container, adds no capability and holds no secret."
  }

  assert {
    condition = alltrue([for container in slice(jsondecode(output.host_container_definitions["bronze"]), 1, 5) : (
      !container.privileged && container.readonlyRootFilesystem && container.user == "10001:10001" && container.linuxParameters.capabilities.drop == ["ALL"] &&
      container.logConfiguration.options["awslogs-group"] == "example_project_jobs_dev" && container.logConfiguration.options["awslogs-stream-prefix"] == "bronze" &&
      toset([for mount in container.mountPoints : mount.containerPath if !mount.readOnly]) == toset(["/workspace", "/tmp"])
    )])
    error_message = "Every phase container is unprivileged with a read-only root, no capability and the numeric user, and writes only the two volumes."
  }
}

run "only_the_load_phase_holds_the_bronze_pair" {
  command = plan

  variables {
    bronze_job_enabled = true
  }

  assert {
    condition = (
      [for container in jsondecode(output.host_container_definitions["bronze"]) : length(container.secrets)] == [0, 0, 0, 2, 0] &&
      toset([for secret in jsondecode(output.host_container_definitions["bronze"])[3].secrets : secret.valueFrom]) == toset([
        "arn:aws:ssm:us-west-2:111111111111:parameter/example_project/aws_polaris_bronze_client_id",
        "arn:aws:ssm:us-west-2:111111111111:parameter/example_project/aws_polaris_bronze_client_secret"
      ])
    )
    error_message = "Only the load phase receives a catalog pair and it is the bronze pair, never the reader or a test pair."
  }
}

run "the_flow_checks_find_no_violation" {
  command = plan

  variables {
    bronze_job_enabled = true
  }

  assert {
    condition     = length(output.host_secret_flow.violations) == 0 && length(output.host_secret_flow.mutation_mismatches) == 0
    error_message = "The shared secret-flow check finds no secret in a command line, file or log, and catches its planted mutations."
  }

  assert {
    condition     = toset(output.host_secret_flow.containers_checked) == toset(["host_bronze/load"])
    error_message = "The shared check follows the one container that receives a secret."
  }
}

run "another_layers_role_is_refused" {
  command = plan

  variables {
    bronze_job_enabled = true
    job_role_arns = {
      execution         = "arn:aws:iam::111111111111:role/example_project_job_execution_dev"
      write_bronze_task = "arn:aws:iam::111111111111:role/example_project_job_write_silver_task_dev"
    }
  }

  expect_failures = [var.job_role_arns]
}

run "a_missing_role_is_refused" {
  command = plan

  variables {
    bronze_job_enabled = true
    job_role_arns      = { execution = "arn:aws:iam::111111111111:role/example_project_job_execution_dev" }
  }

  expect_failures = [var.bronze_job_enabled]
}

run "a_missing_pair_is_refused" {
  command = plan

  variables {
    bronze_job_enabled     = true
    writer_parameter_names = {}
  }

  expect_failures = [var.bronze_job_enabled]
}

run "a_missing_image_is_refused" {
  command = plan

  variables {
    bronze_job_enabled = true
    images             = {}
  }

  expect_failures = [var.bronze_job_enabled]
}

run "another_layers_pair_is_refused" {
  command = plan

  variables {
    bronze_job_enabled = true
    writer_parameter_names = {
      write_bronze_task = ["/example_project/aws_polaris_gold_client_id", "/example_project/aws_polaris_gold_client_secret"]
    }
  }

  expect_failures = [var.writer_parameter_names]
}

run "no_bronze_container_fixes_an_engine_limit" {
  command = plan

  variables {
    bronze_job_enabled = true
  }

  assert {
    condition = alltrue([
      for container in jsondecode(output.host_container_definitions["bronze"]) : length([
        for item in container.environment : item.name if contains(["SPARK_JOB_MEMORY", "DUCKDB_MEMORY_LIMIT", "JOB_THREADS", "OMP_NUM_THREADS", "OPENBLAS_NUM_THREADS", "MKL_NUM_THREADS", "HAI_DUCKDB_MAX_TEMP_BYTES"], item.name)
      ]) == 0
    ])
    error_message = "The bronze task derives its Spark heap and threads from its own capacity when it starts, so no container fixes a heap, a thread count or a spill cap."
  }
}

# The IAM bronze writer contract states the read environment of the bronze writer role while the raw read grant is on. The host bronze job runs under that role.
run "the_raw_read_environment_reaches_every_bronze_phase" {
  command = plan

  variables {
    bronze_job_enabled      = true
    bronze_read_environment = { HAI_READ_RAW_ROOT = "true" }
  }

  assert {
    condition = alltrue([
      for container in slice(jsondecode(output.host_container_definitions["bronze"]), 1, 5) : contains(container.environment, { name = "HAI_READ_RAW_ROOT", value = "true" })
    ])
    error_message = "Every bronze phase receives the raw read environment of the bronze writer contract."
  }

  assert {
    condition     = !contains(jsondecode(output.host_container_definitions["bronze"])[0].environment, { name = "HAI_READ_RAW_ROOT", value = "true" })
    error_message = "The init step, which only hands over the folders, receives no read environment."
  }
}

run "without_the_raw_read_environment_no_container_carries_it" {
  command = plan

  variables {
    bronze_job_enabled = true
  }

  assert {
    condition     = alltrue([for container in jsondecode(output.host_container_definitions["bronze"]) : !strcontains(jsonencode(container.environment), "HAI_READ_RAW_ROOT")])
    error_message = "Without the setting the bronze containers stay as they were."
  }
}

run "another_read_environment_is_refused" {
  command = plan

  variables {
    bronze_job_enabled      = true
    bronze_read_environment = { HAI_READ_RAW_ROOT = "yes" }
  }

  expect_failures = [var.bronze_read_environment]
}

run "the_check_phase_gets_the_capture_root_and_the_alerts_topic_and_no_other_phase_does" {
  command = plan

  variables {
    bronze_job_enabled      = true
    bronze_read_environment = { HAI_READ_RAW_ROOT = "true" }
    alerts_topic_arn        = "arn:aws:sns:us-west-2:111111111111:example_project_alerts_dev"
  }

  assert {
    condition = (
      [for container in jsondecode(output.host_container_definitions["bronze"]) : contains([for item in container.environment : item.name], "ALERTS_TOPIC_ARN")] == [false, false, false, false, true] &&
      [for container in jsondecode(output.host_container_definitions["bronze"]) : contains([for item in container.environment : item.name], "HAI_CAPTURE_ROOT")] == [false, false, false, false, true] &&
      [for item in jsondecode(output.host_container_definitions["bronze"])[4].environment : item.value if item.name == "ALERTS_TOPIC_ARN"] == ["arn:aws:sns:us-west-2:111111111111:example_project_alerts_dev"] &&
      [for item in jsondecode(output.host_container_definitions["bronze"])[4].environment : item.value if item.name == "HAI_CAPTURE_ROOT"] == ["raw/"]
    )
    error_message = "Only the check phase receives the capture root (while the raw read is on) and the alerts topic (once it is given)."
  }
}

run "without_the_raw_read_or_the_topic_the_check_phase_gets_neither" {
  command = plan

  variables {
    bronze_job_enabled = true
  }

  assert {
    condition = alltrue([
      for container in jsondecode(output.host_container_definitions["bronze"]) :
      !contains([for item in container.environment : item.name], "ALERTS_TOPIC_ARN") && !contains([for item in container.environment : item.name], "HAI_CAPTURE_ROOT")
    ])
    error_message = "With neither the raw read nor the alerts topic, no bronze container names the capture root or the topic."
  }
}

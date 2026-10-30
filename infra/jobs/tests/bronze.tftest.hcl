mock_provider "aws" {}

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
  log_group_name = "example_project_jobs_dev"
}

run "default_plan_registers_no_bronze_task" {
  command = plan

  variables {
    images                 = {}
    job_role_arns          = {}
    writer_parameter_names = {}
    log_group_name         = null
  }

  assert {
    condition     = length(aws_ecs_task_definition.bronze) == 0 && length(output.bronze_run_task) == 0 && output.bronze_container_definitions == null
    error_message = "Without bronze_job_enabled the stack registers no bronze task and needs no image, role or pair for one."
  }
}

run "the_bronze_job_is_one_fargate_task_with_the_bronze_writer_role" {
  command = plan

  variables {
    bronze_job_enabled = true
  }

  assert {
    condition = (
      aws_ecs_task_definition.bronze[0].family == "example_project_dev_job_bronze" &&
      aws_ecs_task_definition.bronze[0].task_role_arn == "arn:aws:iam::111111111111:role/example_project_job_write_bronze_task_dev" &&
      aws_ecs_task_definition.bronze[0].execution_role_arn == "arn:aws:iam::111111111111:role/example_project_job_execution_dev" &&
      toset(aws_ecs_task_definition.bronze[0].requires_compatibilities) == toset(["FARGATE"]) && aws_ecs_task_definition.bronze[0].network_mode == "awsvpc" &&
      aws_ecs_task_definition.bronze[0].runtime_platform[0].cpu_architecture == "ARM64" && aws_ecs_task_definition.bronze[0].skip_destroy &&
      !aws_ecs_task_definition.bronze[0].enable_fault_injection
    )
    error_message = "The bronze job is one ARM64 Fargate definition with the execution role and only the bronze writer role."
  }
}

run "containers_follow_the_broker_table_in_order" {
  command = plan

  variables {
    bronze_job_enabled = true
  }

  assert {
    condition = (
      [for container in jsondecode(output.bronze_container_definitions) : container.name] == ["init", "fetch", "pin", "load", "check"] &&
      toset([for container in jsondecode(output.bronze_container_definitions) : container.name if container.name != "init"]) == toset(keys(jsondecode(file("${path.module}/../../config/orchestration/ecs_broker_jobs.json")).jobs.bronze.containers))
    )
    error_message = "The containers run in phase order after the volume ownership step and are the ones the broker table names."
  }

  assert {
    condition = alltrue([for container in jsondecode(output.bronze_container_definitions) : (
      container.name == "init" || toset([for variable in container.environment : variable.name if variable.value == ""]) == toset(jsondecode(file("${path.module}/../../config/orchestration/ecs_broker_jobs.json")).jobs.bronze.containers[container.name])
    )])
    error_message = "The empty variables of every container equal the broker table, so no table list, target or dry run flag is fixed in the definition."
  }

  assert {
    condition = alltrue([for index, container in jsondecode(output.bronze_container_definitions) : (
      index == 0 ? length(container.dependsOn) == 0 : (
        length(container.dependsOn) == 1 && container.dependsOn[0].containerName == jsondecode(output.bronze_container_definitions)[index - 1].name && container.dependsOn[0].condition == "SUCCESS"
      )
    )])
    error_message = "Every container starts after the success of the one before, so a failed phase stops the task."
  }

  assert {
    condition     = [for container in jsondecode(output.bronze_container_definitions) : container.essential] == [false, false, false, false, true]
    error_message = "Only the last container is essential."
  }
}

run "each_phase_runs_the_bronze_driver_and_nothing_else" {
  command = plan

  variables {
    bronze_job_enabled = true
  }

  assert {
    condition = alltrue([for container in slice(jsondecode(output.bronze_container_definitions), 2, 5) :
      container.command == ["cd /workspace/code && python -m scripts.lakehouse.bronze_run ${container.name} --run \"$RUN_ID\""]
    ])
    error_message = "Pin, load and check run the driver phase of their own name, with the run ID and no table, namespace or target argument."
  }

  assert {
    condition = (
      strcontains(jsondecode(output.bronze_container_definitions)[1].command[0], "CODE_SHA256") && strcontains(jsondecode(output.bronze_container_definitions)[1].command[0], "run_stage/") &&
      alltrue([for container in jsondecode(output.bronze_container_definitions) : container.name == "init" || container.image == var.images.spark])
    )
    error_message = "The fetch step downloads the staged archive and checks it against CODE_SHA256. Every phase runs in the Spark image."
  }

  assert {
    condition     = alltrue([for container in jsondecode(output.bronze_container_definitions) : !strcontains(jsonencode(container), "bronze_e2e") && !strcontains(jsonencode(container), "--namespace")])
    error_message = "No namespace is named in the definition: the driver derives it from the checked target."
  }
}

run "every_phase_runs_under_the_deadline_runner" {
  command = plan

  variables {
    bronze_job_enabled = true
  }

  assert {
    condition = alltrue([for container in slice(jsondecode(output.bronze_container_definitions), 1, 5) :
      container.entryPoint == ["/opt/hai/deadline_run", "sh", "-c"] && contains([for variable in container.environment : variable.name if variable.value == ""], "HAI_DEADLINE_UTC")
    ])
    error_message = "Fetch, pin, load and check start through the in-image deadline runner and take one absolute deadline from the broker, so no phase outlives the task deadline."
  }

  assert {
    condition     = jsondecode(output.bronze_container_definitions)[0].entryPoint == ["sh", "-c"] && jsondecode(output.bronze_container_definitions)[0].command == ["chown 10001:10001 /workspace /tmp"]
    error_message = "The init step only hands over the volumes and stays outside the runner."
  }
}

run "the_init_step_hands_the_volumes_to_the_job_user" {
  command = plan

  variables {
    bronze_job_enabled = true
  }

  assert {
    condition = (
      jsondecode(output.bronze_container_definitions)[0].user == "0:0" && jsondecode(output.bronze_container_definitions)[0].command == ["chown 10001:10001 /workspace /tmp"] &&
      jsondecode(output.bronze_container_definitions)[0].secrets == [] && !jsondecode(output.bronze_container_definitions)[0].privileged &&
      jsondecode(output.bronze_container_definitions)[0].linuxParameters.capabilities.add == []
    )
    error_message = "The init step is the only root container, adds no capability and holds no secret."
  }

  assert {
    condition = alltrue([for container in slice(jsondecode(output.bronze_container_definitions), 1, 5) : (
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
      [for container in jsondecode(output.bronze_container_definitions) : length(container.secrets)] == [0, 0, 0, 2, 0] &&
      toset([for secret in jsondecode(output.bronze_container_definitions)[3].secrets : secret.valueFrom]) == toset([
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
    condition     = length(output.bronze_secret_flow.violations) == 0 && length(output.bronze_secret_flow.mutation_mismatches) == 0
    error_message = "The shared secret-flow check finds no secret in a command line, file or log, and catches its planted mutations."
  }

  assert {
    condition     = toset(output.bronze_secret_flow.containers_checked) == toset(["bronze/load"])
    error_message = "The shared check follows the one container that receives a secret."
  }
}

run "the_run_task_output_names_the_dispatch_variables" {
  command = plan

  variables {
    bronze_job_enabled = true
  }

  assert {
    condition = (
      output.bronze_run_task.task_definition == "example_project_dev_job_bronze" && output.bronze_run_task.task_role_key == "write_bronze_task" &&
      output.bronze_run_task.assign_public_ip == "DISABLED" && output.bronze_run_task.propagate_tags == "TASK_DEFINITION" &&
      output.bronze_run_task.dry_run_variable == "BRONZE_DRY_RUN" && output.bronze_run_task.dry_run_value == "1" &&
      output.bronze_run_task.container_environment.load == jsondecode(file("${path.module}/../../config/orchestration/ecs_broker_jobs.json")).jobs.bronze.containers.load
    )
    error_message = "The run task output lists what the broker passes to each container and the one override that makes a dry run."
  }
}

run "a_valid_larger_shape_is_accepted" {
  command = plan

  variables {
    bronze_job_enabled = true
    bronze_resources   = { cpu = 8192, memory = 32768, ephemeral_storage_gib = 150 }
  }

  assert {
    condition     = aws_ecs_task_definition.bronze[0].cpu == "8192" && aws_ecs_task_definition.bronze[0].memory == "32768" && aws_ecs_task_definition.bronze[0].ephemeral_storage[0].size_in_gib == 150
    error_message = "A valid larger shape is accepted."
  }
}

run "an_invalid_shape_is_refused" {
  command = plan

  variables {
    bronze_job_enabled = true
    bronze_resources   = { cpu = 4096, memory = 4096, ephemeral_storage_gib = 50 }
  }

  expect_failures = [var.bronze_resources]
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

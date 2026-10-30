mock_provider "aws" {}

variables {
  aws_profile         = "example_project_dev"
  aws_region          = "us-west-2"
  expected_account_id = "111111111111"
  project_name        = "example_project"
  environment         = "dev"
  data_bucket_name    = "example-project-data-dev"
  drift_images = {
    training      = "111111111111.dkr.ecr.us-west-2.amazonaws.com/example_project_training_dev@sha256:dddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddd"
    feature_store = "111111111111.dkr.ecr.us-west-2.amazonaws.com/example_project_feature_store_dev@sha256:eeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee"
    drift         = "111111111111.dkr.ecr.us-west-2.amazonaws.com/example_project_drift_dev@sha256:ffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff"
  }
  drift_roles = {
    execution  = "arn:aws:iam::111111111111:role/example_project_job_execution_dev"
    drift_task = "arn:aws:iam::111111111111:role/example_project_job_drift_task_dev"
  }
  aws_polaris_reader_client_id_name     = "/example_project/aws_polaris_reader_client_id"
  aws_polaris_reader_client_secret_name = "/example_project/aws_polaris_reader_client_secret"
  drift_mlflow_password_name            = "/example_project/mlflow_admin_password"
  log_group_name                        = "example_project_jobs_dev"
}

run "default_plan_creates_no_drift_task" {
  command = plan

  variables {
    drift_images   = {}
    drift_roles    = {}
    log_group_name = null
  }

  assert {
    condition     = length(aws_ecs_task_definition.drift) == 0 && output.drift_run_task == null && output.drift_container_definitions == null
    error_message = "Without drift_enabled the stack registers no drift task and needs no drift image or role."
  }
}

run "drift_definition_shape" {
  command = plan

  variables {
    drift_enabled = true
  }

  assert {
    condition = (
      length(aws_ecs_task_definition.drift) == 1 &&
      aws_ecs_task_definition.drift[0].family == "example_project_dev_job_drift" &&
      aws_ecs_task_definition.drift[0].task_role_arn == "arn:aws:iam::111111111111:role/example_project_job_drift_task_dev" &&
      aws_ecs_task_definition.drift[0].execution_role_arn == "arn:aws:iam::111111111111:role/example_project_job_execution_dev" &&
      toset(aws_ecs_task_definition.drift[0].requires_compatibilities) == toset(["FARGATE"]) && aws_ecs_task_definition.drift[0].network_mode == "awsvpc" &&
      aws_ecs_task_definition.drift[0].runtime_platform[0].cpu_architecture == "ARM64" && aws_ecs_task_definition.drift[0].runtime_platform[0].operating_system_family == "LINUX" &&
      aws_ecs_task_definition.drift[0].cpu == "2048" && aws_ecs_task_definition.drift[0].memory == "8192" &&
      aws_ecs_task_definition.drift[0].ephemeral_storage[0].size_in_gib == 30 &&
      aws_ecs_task_definition.drift[0].skip_destroy && !aws_ecs_task_definition.drift[0].enable_fault_injection
    )
    error_message = "The drift task is one ARM64 Linux Fargate definition with its own role, the execution role and the sized scratch."
  }

  assert {
    condition = (
      [for container in jsondecode(output.drift_container_definitions) : container.name] == ["init", "fetch", "baseline", "current", "check", "publish"] &&
      [for container in jsondecode(output.drift_container_definitions) : container.image] == [
        var.drift_images.training, var.drift_images.training, var.drift_images.training, var.drift_images.feature_store, var.drift_images.drift, var.drift_images.training
      ] &&
      [for container in jsondecode(output.drift_container_definitions) : container.essential] == [false, false, false, false, false, true] &&
      alltrue([for index, container in jsondecode(output.drift_container_definitions) : (
        index == 0 ? length(container.dependsOn) == 0 : (
          length(container.dependsOn) == 1 && container.dependsOn[0].containerName == jsondecode(output.drift_container_definitions)[index - 1].name && container.dependsOn[0].condition == "SUCCESS"
        )
      )])
    )
    error_message = "Init, fetch, baseline, current, check and publish run in this order on their three images, each after the success of the one before, and only the last one is essential."
  }

  assert {
    condition = alltrue([for container in slice(jsondecode(output.drift_container_definitions), 1, 6) : (
      !container.privileged && container.readonlyRootFilesystem && container.user == "10001:10001" && container.linuxParameters.capabilities.drop == ["ALL"] &&
      container.logConfiguration.options["awslogs-group"] == "example_project_jobs_dev" && container.logConfiguration.options["awslogs-stream-prefix"] == "drift" &&
      container.entryPoint == ["sh", "-c"] && container.workingDirectory == "/workspace"
    )])
    error_message = "Every container after the init step is unprivileged with a read-only root, no capability, the numeric user and the existing jobs log group."
  }
}

run "drift_init_step_makes_the_volumes_writable_for_the_job_user" {
  command = plan

  variables {
    drift_enabled = true
  }

  assert {
    condition = (
      jsondecode(output.drift_container_definitions)[0].name == "init" &&
      jsondecode(output.drift_container_definitions)[0].user == "0:0" &&
      jsondecode(output.drift_container_definitions)[0].command == ["chown 10001:10001 /workspace /tmp"] &&
      !jsondecode(output.drift_container_definitions)[0].privileged &&
      jsondecode(output.drift_container_definitions)[0].readonlyRootFilesystem &&
      jsondecode(output.drift_container_definitions)[0].linuxParameters.capabilities.add == [] &&
      !contains(jsondecode(output.drift_container_definitions)[0].linuxParameters.capabilities.drop, "CHOWN") &&
      length(jsondecode(output.drift_container_definitions)[0].secrets) == 0
    )
    error_message = "The init step runs as root with only the capability to change owners, no secret and a read-only root, and hands both volumes to the job user."
  }

  assert {
    condition = (
      toset(["/workspace", "/tmp"]) == toset([for mount in jsondecode(output.drift_container_definitions)[0].mountPoints : mount.containerPath]) &&
      alltrue([for container in jsondecode(output.drift_container_definitions) : toset([for mount in container.mountPoints : mount.containerPath if !mount.readOnly]) == toset(["/workspace", "/tmp"])])
    )
    error_message = "Every writable mount of every container is one of the two volumes that the init step hands to the job user."
  }
}

run "drift_commands_and_environment" {
  command = plan

  variables {
    drift_enabled = true
  }

  assert {
    condition = (
      jsondecode(output.drift_container_definitions)[1].command == ["python -I /opt/hai/fetch_code.py"] &&
      jsondecode(output.drift_container_definitions)[2].command == ["cd /workspace/code && python -m monitoring.aws_run baseline --run \"$RUN_ID\""] &&
      jsondecode(output.drift_container_definitions)[3].command == ["cd /workspace/code && python -m monitoring.aws_run current --run \"$RUN_ID\""] &&
      jsondecode(output.drift_container_definitions)[4].command == ["cd /workspace/code && python -m monitoring.aws_run check --run \"$RUN_ID\""] &&
      jsondecode(output.drift_container_definitions)[5].command == ["cd /workspace/code && python -m monitoring.aws_run publish --run \"$RUN_ID\""]
    )
    error_message = "The fetch step runs the script baked into the image. The other four run the phases of the fetched code."
  }

  assert {
    condition = alltrue([
      for container in slice(jsondecode(output.drift_container_definitions), 1, 6) :
      contains(container.environment, { name = "DATA_BUCKET_NAME", value = "example-project-data-dev" }) &&
      contains(container.environment, { name = "PYTHONHASHSEED", value = "0" }) &&
      contains(container.environment, { name = "RUN_ID", value = "" }) &&
      contains(container.environment, { name = "AWS_EC2_METADATA_DISABLED", value = "true" })
    ])
    error_message = "Every container knows the bucket, a fixed hash seed and an empty run ID that the run-task call must fill."
  }

  assert {
    condition = (
      toset([for variable in jsondecode(output.drift_container_definitions)[1].environment : variable.name if variable.value == ""]) == toset(["RUN_ID", "CODE_SHA256"]) &&
      toset([for variable in jsondecode(output.drift_container_definitions)[2].environment : variable.name if variable.value == ""]) == toset(["RUN_ID", "MLFLOW_TRACKING_URI"]) &&
      toset([for variable in jsondecode(output.drift_container_definitions)[3].environment : variable.name if variable.value == ""]) == toset(["RUN_ID", "POLARIS_URI"]) &&
      toset([for variable in jsondecode(output.drift_container_definitions)[4].environment : variable.name if variable.value == ""]) == toset(["RUN_ID"]) &&
      toset([for variable in jsondecode(output.drift_container_definitions)[5].environment : variable.name if variable.value == ""]) == toset(["RUN_ID"])
    )
    error_message = "Each container lists exactly the empty variables the run-task override must fill for it."
  }
}

run "drift_secrets_reach_only_the_baseline_and_current_steps" {
  command = plan

  variables {
    drift_enabled = true
  }

  assert {
    condition = (
      jsondecode(output.drift_container_definitions)[0].secrets == [] && jsondecode(output.drift_container_definitions)[1].secrets == [] &&
      jsondecode(output.drift_container_definitions)[4].secrets == [] && jsondecode(output.drift_container_definitions)[5].secrets == [] &&
      jsondecode(output.drift_container_definitions)[2].secrets == [
        { name = "MLFLOW_TRACKING_PASSWORD", valueFrom = "arn:aws:ssm:us-west-2:111111111111:parameter/example_project/mlflow_admin_password" },
      ] &&
      jsondecode(output.drift_container_definitions)[3].secrets == [
        { name = "POLARIS_CLIENT_ID", valueFrom = "arn:aws:ssm:us-west-2:111111111111:parameter/example_project/aws_polaris_reader_client_id" },
        { name = "POLARIS_CLIENT_SECRET", valueFrom = "arn:aws:ssm:us-west-2:111111111111:parameter/example_project/aws_polaris_reader_client_secret" },
      ]
    )
    error_message = "Only the baseline step receives the MLflow login and only the current step the catalog reader pair. The check step, which runs Evidently, receives no secret."
  }

  assert {
    condition = (
      contains(jsondecode(output.drift_container_definitions)[2].environment, { name = "MLFLOW_TRACKING_USERNAME", value = "admin" }) &&
      !strcontains(jsondecode(output.drift_container_definitions)[2].command[0], "MLFLOW_TRACKING_PASSWORD")
    )
    error_message = "The baseline step logs in to MLflow with the administrator account. The password never appears in the command."
  }

  assert {
    condition     = length(output.drift_secret_flow.violations) == 0 && length(output.drift_secret_flow.mutation_mismatches) == 0
    error_message = "The shared secret-flow check finds no secret in a command line, file or log, and catches its planted mutations."
  }

  assert {
    condition     = toset(output.drift_secret_flow.containers_checked) == toset(["drift/baseline", "drift/current"])
    error_message = "The shared check follows the two containers that receive a secret."
  }
}

run "drift_run_task_contract" {
  command = plan

  variables {
    drift_enabled = true
  }

  assert {
    condition = (
      output.drift_run_task.task_definition == "example_project_dev_job_drift" &&
      output.drift_run_task.assign_public_ip == "DISABLED" && output.drift_run_task.capacity_providers == ["FARGATE"] &&
      output.drift_run_task.propagate_tags == "TASK_DEFINITION" &&
      output.drift_run_task.network_source == "training_network output of the IAM stack" &&
      output.drift_run_task.container_environment == {
        fetch    = ["RUN_ID", "CODE_SHA256"]
        baseline = ["RUN_ID", "MLFLOW_TRACKING_URI"]
        current  = ["RUN_ID", "POLARIS_URI"]
        check    = ["RUN_ID"]
        publish  = ["RUN_ID"]
      }
    )
    error_message = "The output names the on-demand private placement and the variables each container needs, for the owner's run-task call or the broker's."
  }
}

run "drift_resources_can_be_resized_within_a_fargate_shape" {
  command = plan

  variables {
    drift_enabled   = true
    drift_resources = { cpu = 4096, memory = 16384, ephemeral_storage_gib = 60 }
  }

  assert {
    condition     = aws_ecs_task_definition.drift[0].cpu == "4096" && aws_ecs_task_definition.drift[0].memory == "16384" && aws_ecs_task_definition.drift[0].ephemeral_storage[0].size_in_gib == 60
    error_message = "A valid larger shape is accepted."
  }
}

run "invalid_shape_is_refused" {
  command = plan

  variables {
    drift_enabled   = true
    drift_resources = { cpu = 2048, memory = 1024, ephemeral_storage_gib = 30 }
  }

  expect_failures = [var.drift_resources]
}

run "missing_image_is_refused" {
  command = plan

  variables {
    drift_enabled = true
    drift_images = {
      training      = "111111111111.dkr.ecr.us-west-2.amazonaws.com/example_project_training_dev@sha256:dddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddd"
      feature_store = "111111111111.dkr.ecr.us-west-2.amazonaws.com/example_project_feature_store_dev@sha256:eeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee"
    }
  }

  expect_failures = [var.drift_images]
}

run "image_by_tag_is_refused" {
  command = plan

  variables {
    drift_enabled = true
    drift_images = {
      training      = "111111111111.dkr.ecr.us-west-2.amazonaws.com/example_project_training_dev@sha256:dddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddd"
      feature_store = "111111111111.dkr.ecr.us-west-2.amazonaws.com/example_project_feature_store_dev@sha256:eeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee"
      drift         = "111111111111.dkr.ecr.us-west-2.amazonaws.com/example_project_drift_dev:latest"
    }
  }

  expect_failures = [var.drift_images]
}

run "other_layer_role_is_refused" {
  command = plan

  variables {
    drift_enabled = true
    drift_roles = {
      execution  = "arn:aws:iam::111111111111:role/example_project_job_execution_dev"
      drift_task = "arn:aws:iam::111111111111:role/example_project_job_write_gold_task_dev"
    }
  }

  expect_failures = [var.drift_roles]
}

run "other_mlflow_password_name_is_refused" {
  command = plan

  variables {
    drift_enabled              = true
    drift_mlflow_password_name = "/example_project/aws_polaris_reader_client_secret"
  }

  expect_failures = [var.drift_mlflow_password_name]
}

run "wrong_log_group_is_refused" {
  command = plan

  variables {
    drift_enabled  = true
    log_group_name = "another_group"
  }

  expect_failures = [var.log_group_name]
}

run "missing_log_group_is_refused" {
  command = plan

  variables {
    drift_enabled  = true
    log_group_name = null
  }

  expect_failures = [var.drift_enabled]
}

run "drift_containers_follow_the_broker_table" {
  command = plan

  variables {
    drift_enabled = true
  }

  assert {
    condition = (
      toset([for container in jsondecode(output.drift_container_definitions) : container.name if container.name != "init"]) == toset(keys(jsondecode(file("${path.module}/../../config/orchestration/ecs_broker_jobs.json")).jobs.drift.containers)) &&
      alltrue([for container in jsondecode(output.drift_container_definitions) : (
        container.name == "init" || toset([for variable in container.environment : variable.name if variable.value == ""]) == toset(jsondecode(file("${path.module}/../../config/orchestration/ecs_broker_jobs.json")).jobs.drift.containers[container.name])
      )]) &&
      contains([for container in jsondecode(output.drift_container_definitions) : container.name], jsondecode(file("${path.module}/../../config/orchestration/ecs_broker_jobs.json")).jobs.drift.log_container)
    )
    error_message = "The container names, their empty variables and the log container equal the broker job table, so every override the broker sends names a container of the task and fills what it needs."
  }
}

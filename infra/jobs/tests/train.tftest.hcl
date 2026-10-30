mock_provider "aws" {}

variables {
  aws_profile         = "example_project_dev"
  aws_region          = "us-west-2"
  expected_account_id = "111111111111"
  project_name        = "example_project"
  environment         = "dev"
  data_bucket_name    = "example-project-data-dev"
  train_images = {
    training      = "111111111111.dkr.ecr.us-west-2.amazonaws.com/example_project_training_dev@sha256:dddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddd"
    feature_store = "111111111111.dkr.ecr.us-west-2.amazonaws.com/example_project_feature_store_dev@sha256:eeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee"
  }
  train_roles = {
    execution  = "arn:aws:iam::111111111111:role/example_project_job_execution_dev"
    train_task = "arn:aws:iam::111111111111:role/example_project_job_train_task_dev"
  }
  aws_polaris_reader_client_id_name     = "/example_project/aws_polaris_reader_client_id"
  aws_polaris_reader_client_secret_name = "/example_project/aws_polaris_reader_client_secret"
  mlflow_admin_password_name            = "/example_project/mlflow_admin_password"
  log_group_name                        = "example_project_jobs_dev"
}

run "default_plan_creates_no_training_task" {
  command = plan

  variables {
    train_images   = {}
    train_roles    = {}
    log_group_name = null
  }

  assert {
    condition     = length(aws_ecs_task_definition.train) == 0 && output.train_run_task == null && output.train_container_definitions == null
    error_message = "Without train_enabled the stack registers no training task and needs no training image or role."
  }
}

run "train_definition_shape" {
  command = plan

  variables {
    train_enabled = true
  }

  assert {
    condition = (
      length(aws_ecs_task_definition.train) == 1 &&
      aws_ecs_task_definition.train[0].family == "example_project_dev_job_train" &&
      aws_ecs_task_definition.train[0].task_role_arn == "arn:aws:iam::111111111111:role/example_project_job_train_task_dev" &&
      aws_ecs_task_definition.train[0].execution_role_arn == "arn:aws:iam::111111111111:role/example_project_job_execution_dev" &&
      toset(aws_ecs_task_definition.train[0].requires_compatibilities) == toset(["FARGATE"]) && aws_ecs_task_definition.train[0].network_mode == "awsvpc" &&
      aws_ecs_task_definition.train[0].runtime_platform[0].cpu_architecture == "ARM64" && aws_ecs_task_definition.train[0].runtime_platform[0].operating_system_family == "LINUX" &&
      aws_ecs_task_definition.train[0].cpu == "4096" && aws_ecs_task_definition.train[0].memory == "16384" &&
      aws_ecs_task_definition.train[0].ephemeral_storage[0].size_in_gib == 50 &&
      aws_ecs_task_definition.train[0].skip_destroy && !aws_ecs_task_definition.train[0].enable_fault_injection
    )
    error_message = "The training task is one ARM64 Linux Fargate definition with its own role, the execution role and the sized scratch."
  }

  assert {
    condition = (
      [for container in jsondecode(output.train_container_definitions) : container.name] == ["init", "fetch", "prepare", "extract", "train"] &&
      [for container in jsondecode(output.train_container_definitions) : container.image] == [var.train_images.training, var.train_images.training, var.train_images.training, var.train_images.feature_store, var.train_images.training] &&
      [for container in jsondecode(output.train_container_definitions) : container.essential] == [false, false, false, false, true] &&
      alltrue([for index, container in jsondecode(output.train_container_definitions) : (
        index == 0 ? length(container.dependsOn) == 0 : (
          length(container.dependsOn) == 1 && container.dependsOn[0].containerName == jsondecode(output.train_container_definitions)[index - 1].name && container.dependsOn[0].condition == "SUCCESS"
        )
      )])
    )
    error_message = "Init, fetch, prepare, extract and train run in this order, each after the success of the one before, and only the last one is essential."
  }

  assert {
    condition = alltrue([for container in slice(jsondecode(output.train_container_definitions), 1, 5) : (
      !container.privileged && container.readonlyRootFilesystem && container.user == "10001:10001" && container.linuxParameters.capabilities.drop == ["ALL"] &&
      container.logConfiguration.options["awslogs-group"] == "example_project_jobs_dev" && container.logConfiguration.options["awslogs-stream-prefix"] == "train" &&
      container.entryPoint == ["sh", "-c"] && container.workingDirectory == "/workspace"
    )])
    error_message = "Every container after the init step is unprivileged with a read-only root, no capability, the numeric user and the existing jobs log group."
  }
}

run "train_init_step_makes_the_volumes_writable_for_the_job_user" {
  command = plan

  variables {
    train_enabled = true
  }

  assert {
    condition = (
      jsondecode(output.train_container_definitions)[0].name == "init" &&
      jsondecode(output.train_container_definitions)[0].user == "0:0" &&
      jsondecode(output.train_container_definitions)[0].command == ["chown 10001:10001 /workspace /tmp"] &&
      !jsondecode(output.train_container_definitions)[0].privileged &&
      jsondecode(output.train_container_definitions)[0].readonlyRootFilesystem &&
      jsondecode(output.train_container_definitions)[0].linuxParameters.capabilities.add == [] &&
      !contains(jsondecode(output.train_container_definitions)[0].linuxParameters.capabilities.drop, "CHOWN") &&
      toset(jsondecode(output.train_container_definitions)[0].linuxParameters.capabilities.drop) == toset(["DAC_OVERRIDE", "FSETID", "FOWNER", "MKNOD", "NET_RAW", "SETGID", "SETUID", "SETFCAP", "SETPCAP", "NET_BIND_SERVICE", "SYS_CHROOT", "KILL", "AUDIT_WRITE"]) &&
      length(jsondecode(output.train_container_definitions)[0].secrets) == 0
    )
    error_message = "The init step runs as root with only the capability to change owners (Fargate allows dropping, not adding), no secret and a read-only root, and hands both volumes to the job user."
  }

  assert {
    condition = (
      toset(["/workspace", "/tmp"]) == toset([for mount in jsondecode(output.train_container_definitions)[0].mountPoints : mount.containerPath]) &&
      alltrue([for container in jsondecode(output.train_container_definitions) : toset([for mount in container.mountPoints : mount.containerPath if !mount.readOnly]) == toset(["/workspace", "/tmp"])])
    )
    error_message = "Every writable mount of every container is one of the two volumes that the init step hands to the job user. No container writes anywhere else."
  }
}

run "train_commands_and_environment" {
  command = plan

  variables {
    train_enabled = true
  }

  assert {
    condition = (
      jsondecode(output.train_container_definitions)[0].command == ["chown 10001:10001 /workspace /tmp"] &&
      jsondecode(output.train_container_definitions)[1].command == ["python -I /opt/hai/fetch_code.py"] &&
      jsondecode(output.train_container_definitions)[2].command == ["cd /workspace/code && python -m scripts.training.aws_run prepare --run \"$RUN_ID\""] &&
      jsondecode(output.train_container_definitions)[3].command == ["cd /workspace/code && python -m scripts.training.aws_run extract --run \"$RUN_ID\""] &&
      jsondecode(output.train_container_definitions)[4].command == ["cd /workspace/code && python -m scripts.training.aws_run train --run \"$RUN_ID\""]
    )
    error_message = "The fetch step runs the script baked into the image. The other three run the phases of the fetched code."
  }

  assert {
    condition = alltrue([
      for container in slice(jsondecode(output.train_container_definitions), 1, 5) :
      contains(container.environment, { name = "DATA_BUCKET_NAME", value = "example-project-data-dev" }) &&
      contains(container.environment, { name = "PYTHONHASHSEED", value = "0" }) &&
      contains(container.environment, { name = "RUN_ID", value = "" }) &&
      contains(container.environment, { name = "AWS_EC2_METADATA_DISABLED", value = "true" })
    ])
    error_message = "Every container knows the bucket, a fixed hash seed and an empty run ID that the run-task call must fill."
  }

  assert {
    condition = (
      toset([for variable in jsondecode(output.train_container_definitions)[1].environment : variable.name if variable.value == ""]) == toset(["RUN_ID", "CODE_SHA256"]) &&
      toset([for variable in jsondecode(output.train_container_definitions)[2].environment : variable.name if variable.value == ""]) == toset(["RUN_ID", "CODE_SHA256", "TRAINING_PARAMETERS"]) &&
      toset([for variable in jsondecode(output.train_container_definitions)[3].environment : variable.name if variable.value == ""]) == toset(["RUN_ID", "POLARIS_URI"]) &&
      toset([for variable in jsondecode(output.train_container_definitions)[4].environment : variable.name if variable.value == ""]) == toset(["RUN_ID", "MLFLOW_TRACKING_URI"])
    )
    error_message = "Each container lists exactly the empty variables the run-task override must fill for it."
  }
}

run "train_container_records_its_image_digest" {
  command = plan

  variables {
    train_enabled = true
  }

  assert {
    condition = (
      contains(jsondecode(output.train_container_definitions)[4].environment, { name = "TRAINING_IMAGE_DIGEST", value = "sha256:dddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddd" }) &&
      alltrue([for container in slice(jsondecode(output.train_container_definitions), 0, 4) : !contains([for variable in container.environment : variable.name], "TRAINING_IMAGE_DIGEST")])
    )
    error_message = "The train container carries the digest of the training image, which the run key records. The other containers do not."
  }
}

run "train_secrets_reach_only_the_extract_and_train_steps" {
  command = plan

  variables {
    train_enabled = true
  }

  assert {
    condition = (
      jsondecode(output.train_container_definitions)[0].secrets == [] && jsondecode(output.train_container_definitions)[1].secrets == [] && jsondecode(output.train_container_definitions)[2].secrets == [] &&
      jsondecode(output.train_container_definitions)[3].secrets == [
        { name = "POLARIS_CLIENT_ID", valueFrom = "arn:aws:ssm:us-west-2:111111111111:parameter/example_project/aws_polaris_reader_client_id" },
        { name = "POLARIS_CLIENT_SECRET", valueFrom = "arn:aws:ssm:us-west-2:111111111111:parameter/example_project/aws_polaris_reader_client_secret" },
      ]
    )
    error_message = "Only the extract step receives the catalog reader pair and only the train step receives the MLflow login. No step receives a writer or administrator credential of the lakehouse."
  }

  assert {
    condition = (
      jsondecode(output.train_container_definitions)[4].secrets == [
        { name = "MLFLOW_TRACKING_PASSWORD", valueFrom = "arn:aws:ssm:us-west-2:111111111111:parameter/example_project/mlflow_admin_password" },
      ] &&
      contains(jsondecode(output.train_container_definitions)[4].environment, { name = "MLFLOW_TRACKING_USERNAME", value = "admin" }) &&
      !strcontains(jsondecode(output.train_container_definitions)[4].command[0], "MLFLOW_TRACKING_PASSWORD")
    )
    error_message = "The train step logs in to MLflow with the administrator account. The password comes from the secret store and never appears in the command."
  }

  assert {
    condition     = length(output.train_secret_flow.violations) == 0
    error_message = "The shared secret-flow check finds no secret in a command line, file or log of the four containers."
  }

  assert {
    condition     = toset(output.train_secret_flow.containers_checked) == toset(["train/extract", "train/train"])
    error_message = "The shared check follows the two containers that receive a secret."
  }

  assert {
    condition     = length(output.train_secret_flow.mutation_mismatches) == 0
    error_message = "The planted secret-flow mutations are all caught."
  }
}

run "train_run_task_contract" {
  command = plan

  variables {
    train_enabled = true
  }

  assert {
    condition = (
      output.train_run_task.task_definition == "example_project_dev_job_train" &&
      output.train_run_task.assign_public_ip == "DISABLED" && output.train_run_task.capacity_providers == ["FARGATE"] &&
      output.train_run_task.propagate_tags == "TASK_DEFINITION" &&
      output.train_run_task.network_source == "training_network output of the IAM stack" &&
      output.train_run_task.container_environment == {
        fetch   = ["RUN_ID", "CODE_SHA256"]
        prepare = ["RUN_ID", "CODE_SHA256", "TRAINING_PARAMETERS"]
        extract = ["RUN_ID", "POLARIS_URI"]
        train   = ["RUN_ID", "MLFLOW_TRACKING_URI"]
      }
    )
    error_message = "The output names the on-demand private placement and the variables each container needs, for the owner's run-task call."
  }
}

run "train_resources_can_be_resized_within_a_fargate_shape" {
  command = plan

  variables {
    train_enabled   = true
    train_resources = { cpu = 8192, memory = 32768, ephemeral_storage_gib = 100 }
  }

  assert {
    condition     = aws_ecs_task_definition.train[0].cpu == "8192" && aws_ecs_task_definition.train[0].memory == "32768" && aws_ecs_task_definition.train[0].ephemeral_storage[0].size_in_gib == 100
    error_message = "A valid larger shape is accepted."
  }
}

run "invalid_shape_is_refused" {
  command = plan

  variables {
    train_enabled   = true
    train_resources = { cpu = 4096, memory = 4096, ephemeral_storage_gib = 50 }
  }

  expect_failures = [var.train_resources]
}

run "missing_image_is_refused" {
  command = plan

  variables {
    train_enabled = true
    train_images  = { training = "111111111111.dkr.ecr.us-west-2.amazonaws.com/example_project_training_dev@sha256:dddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddd" }
  }

  expect_failures = [var.train_images]
}

run "image_by_tag_is_refused" {
  command = plan

  variables {
    train_enabled = true
    train_images = {
      training      = "111111111111.dkr.ecr.us-west-2.amazonaws.com/example_project_training_dev:latest"
      feature_store = "111111111111.dkr.ecr.us-west-2.amazonaws.com/example_project_feature_store_dev@sha256:eeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee"
    }
  }

  expect_failures = [var.train_images]
}

run "other_layer_role_is_refused" {
  command = plan

  variables {
    train_enabled = true
    train_roles = {
      execution  = "arn:aws:iam::111111111111:role/example_project_job_execution_dev"
      train_task = "arn:aws:iam::111111111111:role/example_project_job_write_gold_task_dev"
    }
  }

  expect_failures = [var.train_roles]
}

run "other_mlflow_password_name_is_refused" {
  command = plan

  variables {
    train_enabled              = true
    mlflow_admin_password_name = "/example_project/aws_polaris_reader_client_secret"
  }

  expect_failures = [var.mlflow_admin_password_name]
}

run "wrong_log_group_is_refused" {
  command = plan

  variables {
    train_enabled  = true
    log_group_name = "another_group"
  }

  expect_failures = [var.log_group_name]
}

run "missing_log_group_is_refused" {
  command = plan

  variables {
    train_enabled  = true
    log_group_name = null
  }

  expect_failures = [var.train_enabled]
}

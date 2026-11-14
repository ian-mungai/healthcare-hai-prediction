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
  host_job_memory_mib                   = 23552
  # TEST FIXTURE ONLY, not measured values: the services output host_capacity_binding for a 30720 MiB usable host.
  host_capacity_binding = "job_memory_mib=23552 usable_memory_mib=30720 system_reserve_mib=2048 service_reservations_mib=5120"
  host_jobs_enabled     = true
  host_jobs             = ["train"]
}

run "train_definition_shape" {
  command = plan

  variables {
    train_enabled = true
  }

  assert {
    condition = (
      aws_ecs_task_definition.host["train"].family == "example_project_dev_job_train_host" &&
      aws_ecs_task_definition.host["train"].task_role_arn == "arn:aws:iam::111111111111:role/example_project_job_train_task_dev" &&
      aws_ecs_task_definition.host["train"].execution_role_arn == "arn:aws:iam::111111111111:role/example_project_job_execution_dev" &&
      toset(aws_ecs_task_definition.host["train"].requires_compatibilities) == toset(["EC2"]) && aws_ecs_task_definition.host["train"].network_mode == "bridge" &&
      aws_ecs_task_definition.host["train"].runtime_platform[0].cpu_architecture == "ARM64" && aws_ecs_task_definition.host["train"].runtime_platform[0].operating_system_family == "LINUX" &&
      aws_ecs_task_definition.host["train"].cpu == "3072" && aws_ecs_task_definition.host["train"].memory == "23552" &&
      aws_ecs_task_definition.host["train"].skip_destroy && !aws_ecs_task_definition.host["train"].enable_fault_injection
    )
    error_message = "The training task is one ARM64 Linux host definition with its own role, the execution role and the host job cap."
  }

  assert {
    condition = (
      [for container in jsondecode(output.host_container_definitions["train"]) : container.name] == ["init", "fetch", "prepare", "extract", "train"] &&
      [for container in jsondecode(output.host_container_definitions["train"]) : container.image] == [var.train_images.training, var.train_images.training, var.train_images.training, var.train_images.feature_store, var.train_images.training] &&
      [for container in jsondecode(output.host_container_definitions["train"]) : container.essential] == [false, false, false, false, true] &&
      alltrue([for index, container in jsondecode(output.host_container_definitions["train"]) : (
        index == 0 ? length(container.dependsOn) == 0 : (
          length(container.dependsOn) == 1 && container.dependsOn[0].containerName == jsondecode(output.host_container_definitions["train"])[index - 1].name && container.dependsOn[0].condition == "SUCCESS"
        )
      )])
    )
    error_message = "Init, fetch, prepare, extract and train run in this order, each after the success of the one before, and only the last one is essential."
  }

  assert {
    condition = alltrue([for container in slice(jsondecode(output.host_container_definitions["train"]), 1, 5) : (
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
      jsondecode(output.host_container_definitions["train"])[0].name == "init" &&
      jsondecode(output.host_container_definitions["train"])[0].user == "0:0" &&
      jsondecode(output.host_container_definitions["train"])[0].command == ["find /workspace /tmp -mindepth 1 -delete && chown 10001:10001 /workspace /tmp"] &&
      !jsondecode(output.host_container_definitions["train"])[0].privileged &&
      jsondecode(output.host_container_definitions["train"])[0].readonlyRootFilesystem &&
      jsondecode(output.host_container_definitions["train"])[0].linuxParameters.capabilities.add == [] &&
      !contains(jsondecode(output.host_container_definitions["train"])[0].linuxParameters.capabilities.drop, "CHOWN") &&
      toset(jsondecode(output.host_container_definitions["train"])[0].linuxParameters.capabilities.drop) == toset(["FSETID", "MKNOD", "NET_RAW", "SETGID", "SETUID", "SETFCAP", "SETPCAP", "NET_BIND_SERVICE", "SYS_CHROOT", "KILL", "AUDIT_WRITE"]) &&
      length(jsondecode(output.host_container_definitions["train"])[0].secrets) == 0
    )
    error_message = "The init step runs as root with only the capability to change owners ((a container drops capabilities from the default list and adds none)), no secret and a read-only root, and hands both volumes to the job user."
  }

  assert {
    condition = (
      toset(["/workspace", "/tmp"]) == toset([for mount in jsondecode(output.host_container_definitions["train"])[0].mountPoints : mount.containerPath]) &&
      alltrue([for container in jsondecode(output.host_container_definitions["train"]) : toset([for mount in container.mountPoints : mount.containerPath if !mount.readOnly]) == toset(["/workspace", "/tmp"])])
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
      jsondecode(output.host_container_definitions["train"])[0].command == ["find /workspace /tmp -mindepth 1 -delete && chown 10001:10001 /workspace /tmp"] &&
      jsondecode(output.host_container_definitions["train"])[1].command == ["python -I /opt/hai/fetch_code.py"] &&
      jsondecode(output.host_container_definitions["train"])[2].command == ["cd /workspace/code && python -m scripts.training.aws_run prepare --run \"$RUN_ID\""] &&
      jsondecode(output.host_container_definitions["train"])[3].command == ["cd /workspace/code && python -m scripts.training.aws_run extract --run \"$RUN_ID\""] &&
      jsondecode(output.host_container_definitions["train"])[4].command == ["(cd /workspace/code && python -m scripts.training.aws_run train --run \"$RUN_ID\") && find /workspace /tmp -mindepth 1 -delete"]
    )
    error_message = "The fetch step runs the script baked into the image. The other three run the phases of the fetched code."
  }

  assert {
    condition = alltrue([
      for container in slice(jsondecode(output.host_container_definitions["train"]), 1, 5) :
      contains(container.environment, { name = "DATA_BUCKET_NAME", value = "example-project-data-dev" }) &&
      contains(container.environment, { name = "PYTHONHASHSEED", value = "0" }) &&
      contains(container.environment, { name = "RUN_ID", value = "" }) &&
      contains(container.environment, { name = "AWS_EC2_METADATA_DISABLED", value = "true" })
    ])
    error_message = "Every container knows the bucket, a fixed hash seed and an empty run ID that the run-task call must fill."
  }

  assert {
    condition = (
      toset([for variable in jsondecode(output.host_container_definitions["train"])[1].environment : variable.name if variable.value == ""]) == toset(["RUN_ID", "CODE_RUN_ID", "CODE_SHA256"]) &&
      toset([for variable in jsondecode(output.host_container_definitions["train"])[2].environment : variable.name if variable.value == ""]) == toset(["RUN_ID", "CODE_SHA256", "TRAINING_PARAMETERS", "POLARIS_URI", "MLFLOW_TRACKING_URI"]) &&
      toset([for variable in jsondecode(output.host_container_definitions["train"])[3].environment : variable.name if variable.value == ""]) == toset(["RUN_ID", "POLARIS_URI"]) &&
      toset([for variable in jsondecode(output.host_container_definitions["train"])[4].environment : variable.name if variable.value == ""]) == toset(["RUN_ID", "MLFLOW_TRACKING_URI"])
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
      contains(jsondecode(output.host_container_definitions["train"])[4].environment, { name = "TRAINING_IMAGE_DIGEST", value = "sha256:dddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddd" }) &&
      alltrue([for container in slice(jsondecode(output.host_container_definitions["train"]), 0, 4) : !contains([for variable in container.environment : variable.name], "TRAINING_IMAGE_DIGEST")])
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
      jsondecode(output.host_container_definitions["train"])[0].secrets == [] && jsondecode(output.host_container_definitions["train"])[1].secrets == [] && jsondecode(output.host_container_definitions["train"])[2].secrets == [] &&
      jsondecode(output.host_container_definitions["train"])[3].secrets == [
        { name = "POLARIS_CLIENT_ID", valueFrom = "arn:aws:ssm:us-west-2:111111111111:parameter/example_project/aws_polaris_reader_client_id" },
        { name = "POLARIS_CLIENT_SECRET", valueFrom = "arn:aws:ssm:us-west-2:111111111111:parameter/example_project/aws_polaris_reader_client_secret" },
      ]
    )
    error_message = "Only the extract step receives the catalog reader pair and only the train step receives the MLflow login. No step receives a writer or administrator credential of the lakehouse."
  }

  assert {
    condition = (
      jsondecode(output.host_container_definitions["train"])[4].secrets == [
        { name = "MLFLOW_TRACKING_PASSWORD", valueFrom = "arn:aws:ssm:us-west-2:111111111111:parameter/example_project/mlflow_admin_password" },
      ] &&
      contains(jsondecode(output.host_container_definitions["train"])[4].environment, { name = "MLFLOW_TRACKING_USERNAME", value = "admin" }) &&
      !strcontains(jsondecode(output.host_container_definitions["train"])[4].command[0], "MLFLOW_TRACKING_PASSWORD")
    )
    error_message = "The train step logs in to MLflow with the administrator account. The password comes from the secret store and never appears in the command."
  }

  assert {
    condition     = length(output.host_secret_flow.violations) == 0
    error_message = "The shared secret-flow check finds no secret in a command line, file or log of the four containers."
  }

  assert {
    condition     = toset(output.host_secret_flow.containers_checked) == toset(["host_train/extract", "host_train/train"])
    error_message = "The shared check follows the two containers that receive a secret."
  }

  assert {
    condition     = length(output.host_secret_flow.mutation_mismatches) == 0
    error_message = "The planted secret-flow mutations are all caught."
  }
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

run "train_containers_follow_the_job_catalog" {
  command = plan

  variables {
    train_enabled = true
  }

  assert {
    condition = (
      toset([for container in jsondecode(output.host_container_definitions["train"]) : container.name if container.name != "init"]) == toset(keys(jsondecode(file("${path.module}/../../config/orchestration/ecs_broker_jobs.json")).jobs.train.containers)) &&
      alltrue([for container in jsondecode(output.host_container_definitions["train"]) : (
        container.name == "init" || toset([for variable in container.environment : variable.name if variable.value == ""]) == toset(jsondecode(file("${path.module}/../../config/orchestration/ecs_broker_jobs.json")).jobs.train.containers[container.name])
      )]) &&
      contains([for container in jsondecode(output.host_container_definitions["train"]) : container.name], jsondecode(file("${path.module}/../../config/orchestration/ecs_broker_jobs.json")).jobs.train.log_container)
    )
    error_message = "The container names, their empty variables and the log container equal the job catalog, so every override the launch sends names a container of the task and fills what it needs."
  }
}

run "no_train_container_fixes_an_engine_limit" {
  command = plan

  variables {
    train_enabled = true
  }

  assert {
    condition = alltrue([
      for container in jsondecode(output.host_container_definitions["train"]) : length([
        for item in container.environment : item.name if contains(["SPARK_JOB_MEMORY", "DUCKDB_MEMORY_LIMIT", "JOB_THREADS", "OMP_NUM_THREADS", "OPENBLAS_NUM_THREADS", "MKL_NUM_THREADS", "HAI_DUCKDB_MAX_TEMP_BYTES"], item.name)
      ]) == 0
    ])
    error_message = "The training task derives its threads and DuckDB memory from its own capacity when it starts, so no container fixes a thread count, a memory limit or a spill cap."
  }
}

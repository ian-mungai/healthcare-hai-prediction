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
  host_job_memory_mib                   = 23552
  # TEST FIXTURE ONLY, not measured values: the services output host_capacity_binding for a 30720 MiB usable host.
  host_capacity_binding = "job_memory_mib=23552 usable_memory_mib=30720 system_reserve_mib=2048 service_reservations_mib=5120"
  host_jobs_enabled     = true
  host_jobs             = ["drift"]
}

run "drift_definition_shape" {
  command = plan

  variables {
    drift_enabled = true
  }

  assert {
    condition = (
      aws_ecs_task_definition.host["drift"].family == "example_project_dev_job_drift_host" &&
      aws_ecs_task_definition.host["drift"].task_role_arn == "arn:aws:iam::111111111111:role/example_project_job_drift_task_dev" &&
      aws_ecs_task_definition.host["drift"].execution_role_arn == "arn:aws:iam::111111111111:role/example_project_job_execution_dev" &&
      toset(aws_ecs_task_definition.host["drift"].requires_compatibilities) == toset(["EC2"]) && aws_ecs_task_definition.host["drift"].network_mode == "bridge" &&
      aws_ecs_task_definition.host["drift"].runtime_platform[0].cpu_architecture == "ARM64" && aws_ecs_task_definition.host["drift"].runtime_platform[0].operating_system_family == "LINUX" &&
      aws_ecs_task_definition.host["drift"].cpu == "2048" && aws_ecs_task_definition.host["drift"].memory == "8192" &&
      aws_ecs_task_definition.host["drift"].skip_destroy && !aws_ecs_task_definition.host["drift"].enable_fault_injection
    )
    error_message = "The drift task is one ARM64 Linux host definition with its own role, the execution role and its reservation of the host."
  }

  assert {
    condition = (
      [for container in jsondecode(output.host_container_definitions["drift"]) : container.name] == ["init", "fetch", "baseline", "current", "check", "publish"] &&
      [for container in jsondecode(output.host_container_definitions["drift"]) : container.image] == [
        var.drift_images.training, var.drift_images.training, var.drift_images.training, var.drift_images.feature_store, var.drift_images.drift, var.drift_images.training
      ] &&
      [for container in jsondecode(output.host_container_definitions["drift"]) : container.essential] == [false, false, false, false, false, true] &&
      alltrue([for index, container in jsondecode(output.host_container_definitions["drift"]) : (
        index == 0 ? length(container.dependsOn) == 0 : (
          length(container.dependsOn) == 1 && container.dependsOn[0].containerName == jsondecode(output.host_container_definitions["drift"])[index - 1].name && container.dependsOn[0].condition == "SUCCESS"
        )
      )])
    )
    error_message = "Init, fetch, baseline, current, check and publish run in this order on their three images, each after the success of the one before, and only the last one is essential."
  }

  assert {
    condition = alltrue([for container in slice(jsondecode(output.host_container_definitions["drift"]), 1, 6) : (
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
      jsondecode(output.host_container_definitions["drift"])[0].name == "init" &&
      jsondecode(output.host_container_definitions["drift"])[0].user == "0:0" &&
      jsondecode(output.host_container_definitions["drift"])[0].command == ["find /workspace /tmp -mindepth 1 -delete && chown 10001:10001 /workspace /tmp /evidently"] &&
      !jsondecode(output.host_container_definitions["drift"])[0].privileged &&
      jsondecode(output.host_container_definitions["drift"])[0].readonlyRootFilesystem &&
      jsondecode(output.host_container_definitions["drift"])[0].linuxParameters.capabilities.add == [] &&
      !contains(jsondecode(output.host_container_definitions["drift"])[0].linuxParameters.capabilities.drop, "CHOWN") &&
      length(jsondecode(output.host_container_definitions["drift"])[0].secrets) == 0
    )
    error_message = "The init step runs as root with only the capability to change owners, no secret and a read-only root, and hands the two job volumes and the Evidently workspace to the job user. It deletes only the two job volumes."
  }

  assert {
    condition = (
      toset(["/workspace", "/tmp", "/evidently"]) == toset([for mount in jsondecode(output.host_container_definitions["drift"])[0].mountPoints : mount.containerPath]) &&
      alltrue([for container in jsondecode(output.host_container_definitions["drift"]) : toset([for mount in container.mountPoints : mount.containerPath if !mount.readOnly]) == (
        contains(["init", "check"], container.name) ? toset(["/workspace", "/tmp", "/evidently"]) : toset(["/workspace", "/tmp"])
      )])
    )
    error_message = "Every writable mount of every container is one of the volumes that the init step hands to the job user, and only the init and check steps mount the Evidently workspace."
  }

  assert {
    condition = (
      aws_ecs_task_definition.host["drift"].volume != null &&
      toset([for volume in aws_ecs_task_definition.host["drift"].volume : volume.name]) == toset(["workspace", "tmp", "evidently_workspace"]) &&
      one([for volume in aws_ecs_task_definition.host["drift"].volume : volume.host_path if volume.name == "evidently_workspace"]) == "${var.host_scratch_root}/drift/evidently_workspace" &&
      alltrue([for container in jsondecode(output.host_container_definitions["drift"]) : alltrue([for mount in container.mountPoints : mount.sourceVolume == "evidently_workspace" ? mount.containerPath == "/evidently" : true])])
    )
    error_message = "The Evidently workspace is one host folder under the drift family's scratch root, mounted at /evidently, where the UI service of the services stack reads it."
  }

  assert {
    condition     = !strcontains(jsondecode(output.host_container_definitions["drift"])[0].command[0], "-delete /evidently") && !strcontains(jsondecode(output.host_container_definitions["drift"])[5].command[0], "/evidently")
    error_message = "No wipe reaches the Evidently workspace: the snapshots of earlier runs stay for the UI."
  }
}

run "drift_commands_and_environment" {
  command = plan

  variables {
    drift_enabled = true
  }

  assert {
    condition = (
      jsondecode(output.host_container_definitions["drift"])[1].command == ["python -I /opt/hai/fetch_code.py"] &&
      jsondecode(output.host_container_definitions["drift"])[2].command == ["cd /workspace/code && python -m monitoring.aws_run baseline --run \"$RUN_ID\" --model \"$FEATURE_SET\" --live-type \"$LIVE_HAI_TYPE\""] &&
      jsondecode(output.host_container_definitions["drift"])[3].command == ["cd /workspace/code && python -m monitoring.aws_run current --run \"$RUN_ID\" --features-run \"$INPUT_RUN_ID\" --live-type \"$LIVE_HAI_TYPE\""] &&
      jsondecode(output.host_container_definitions["drift"])[4].command == ["cd /workspace/code && python -m monitoring.aws_run check --run \"$RUN_ID\" --live-type \"$LIVE_HAI_TYPE\""] &&
      jsondecode(output.host_container_definitions["drift"])[5].command == ["(cd /workspace/code && python -m monitoring.aws_run publish --run \"$RUN_ID\" --live-type \"$LIVE_HAI_TYPE\") && find /workspace /tmp -mindepth 1 -delete"]
    )
    error_message = "The fetch step runs the script baked into the image. The other four run the phases of the fetched code, the baseline phase for the model of the run, the current phase for its model run, and each phase for the type of a live check, which an ordinary run leaves empty."
  }

  assert {
    condition = alltrue([
      for container in slice(jsondecode(output.host_container_definitions["drift"]), 1, 6) :
      contains(container.environment, { name = "DATA_BUCKET_NAME", value = "example-project-data-dev" }) &&
      contains(container.environment, { name = "PYTHONHASHSEED", value = "0" }) &&
      contains(container.environment, { name = "RUN_ID", value = "" }) &&
      contains(container.environment, { name = "AWS_EC2_METADATA_DISABLED", value = "true" })
    ])
    error_message = "Every container knows the bucket, a fixed hash seed and an empty run ID that the run-task call must fill."
  }

  assert {
    condition = (
      toset([for variable in jsondecode(output.host_container_definitions["drift"])[1].environment : variable.name if variable.value == ""]) == toset(["RUN_ID", "CODE_RUN_ID", "CODE_SHA256"]) &&
      toset([for variable in jsondecode(output.host_container_definitions["drift"])[2].environment : variable.name if variable.value == ""]) == toset(["RUN_ID", "MLFLOW_TRACKING_URI", "POLARIS_URI", "FEATURE_SET", "LIVE_HAI_TYPE"]) &&
      toset([for variable in jsondecode(output.host_container_definitions["drift"])[3].environment : variable.name if variable.value == ""]) == toset(["RUN_ID", "POLARIS_URI", "INPUT_RUN_ID", "LIVE_HAI_TYPE"]) &&
      toset([for variable in jsondecode(output.host_container_definitions["drift"])[4].environment : variable.name if variable.value == ""]) == toset(["RUN_ID", "LIVE_HAI_TYPE"]) &&
      toset([for variable in jsondecode(output.host_container_definitions["drift"])[5].environment : variable.name if variable.value == ""]) == toset(["RUN_ID", "LIVE_HAI_TYPE"])
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
      jsondecode(output.host_container_definitions["drift"])[0].secrets == [] && jsondecode(output.host_container_definitions["drift"])[1].secrets == [] &&
      jsondecode(output.host_container_definitions["drift"])[4].secrets == [] && jsondecode(output.host_container_definitions["drift"])[5].secrets == [] &&
      jsondecode(output.host_container_definitions["drift"])[2].secrets == [
        { name = "MLFLOW_TRACKING_PASSWORD", valueFrom = "arn:aws:ssm:us-west-2:111111111111:parameter/example_project/mlflow_admin_password" },
      ] &&
      jsondecode(output.host_container_definitions["drift"])[3].secrets == [
        { name = "POLARIS_CLIENT_ID", valueFrom = "arn:aws:ssm:us-west-2:111111111111:parameter/example_project/aws_polaris_reader_client_id" },
        { name = "POLARIS_CLIENT_SECRET", valueFrom = "arn:aws:ssm:us-west-2:111111111111:parameter/example_project/aws_polaris_reader_client_secret" },
      ]
    )
    error_message = "Only the baseline step receives the MLflow login and only the current step the catalog reader pair. The check step, which runs Evidently, receives no secret."
  }

  assert {
    condition = (
      contains(jsondecode(output.host_container_definitions["drift"])[2].environment, { name = "MLFLOW_TRACKING_USERNAME", value = "admin" }) &&
      !strcontains(jsondecode(output.host_container_definitions["drift"])[2].command[0], "MLFLOW_TRACKING_PASSWORD")
    )
    error_message = "The baseline step logs in to MLflow with the administrator account. The password never appears in the command."
  }

  assert {
    condition     = length(output.host_secret_flow.violations) == 0 && length(output.host_secret_flow.mutation_mismatches) == 0
    error_message = "The shared secret-flow check finds no secret in a command line, file or log, and catches its planted mutations."
  }

  assert {
    condition     = toset(output.host_secret_flow.containers_checked) == toset(["host_drift/baseline", "host_drift/current"])
    error_message = "The shared check follows the two containers that receive a secret."
  }
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

run "drift_containers_follow_the_job_catalog" {
  command = plan

  variables {
    drift_enabled = true
  }

  assert {
    condition = (
      toset([for container in jsondecode(output.host_container_definitions["drift"]) : container.name if container.name != "init"]) == toset(keys(jsondecode(file("${path.module}/../../config/orchestration/ecs_broker_jobs.json")).jobs.drift.containers)) &&
      alltrue([for container in jsondecode(output.host_container_definitions["drift"]) : (
        container.name == "init" || toset([for variable in container.environment : variable.name if variable.value == ""]) == toset(jsondecode(file("${path.module}/../../config/orchestration/ecs_broker_jobs.json")).jobs.drift.containers[container.name])
      )]) &&
      contains([for container in jsondecode(output.host_container_definitions["drift"]) : container.name], jsondecode(file("${path.module}/../../config/orchestration/ecs_broker_jobs.json")).jobs.drift.log_container)
    )
    error_message = "The container names, their empty variables and the log container equal the job catalog, so every override the launch sends names a container of the task and fills what it needs."
  }
}

run "no_drift_container_fixes_an_engine_limit" {
  command = plan

  variables {
    drift_enabled = true
  }

  assert {
    condition = alltrue([
      for container in jsondecode(output.host_container_definitions["drift"]) : length([
        for item in container.environment : item.name if contains(["SPARK_JOB_MEMORY", "DUCKDB_MEMORY_LIMIT", "JOB_THREADS", "OMP_NUM_THREADS", "OPENBLAS_NUM_THREADS", "MKL_NUM_THREADS", "HAI_DUCKDB_MAX_TEMP_BYTES"], item.name)
      ]) == 0
    ])
    error_message = "The drift task derives its threads and DuckDB memory from its own capacity when it starts, so no container fixes a thread count, a memory limit or a spill cap."
  }
}

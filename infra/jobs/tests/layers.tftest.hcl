mock_provider "aws" {}

variables {
  aws_profile         = "example_project_dev"
  aws_region          = "us-west-2"
  expected_account_id = "111111111111"
  project_name        = "example_project"
  environment         = "dev"
  data_bucket_name    = "example-project-data-dev"
  images = {
    analytics = "111111111111.dkr.ecr.us-west-2.amazonaws.com/example_project_analytics_dev@sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
    quality   = "111111111111.dkr.ecr.us-west-2.amazonaws.com/example_project_quality_dev@sha256:bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"
    spark     = "111111111111.dkr.ecr.us-west-2.amazonaws.com/example_project_spark_dev@sha256:cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc"
  }
  train_images = {
    training      = "111111111111.dkr.ecr.us-west-2.amazonaws.com/example_project_training_dev@sha256:dddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddd"
    feature_store = "111111111111.dkr.ecr.us-west-2.amazonaws.com/example_project_feature_store_dev@sha256:eeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee"
  }
  job_role_arns = {
    execution           = "arn:aws:iam::111111111111:role/example_project_job_execution_dev"
    write_silver_task   = "arn:aws:iam::111111111111:role/example_project_job_write_silver_task_dev"
    write_gold_task     = "arn:aws:iam::111111111111:role/example_project_job_write_gold_task_dev"
    write_features_task = "arn:aws:iam::111111111111:role/example_project_job_write_features_task_dev"
  }
  writer_parameter_names = {
    write_silver_task   = ["/example_project/aws_polaris_silver_client_id", "/example_project/aws_polaris_silver_client_secret"]
    write_gold_task     = ["/example_project/aws_polaris_gold_client_id", "/example_project/aws_polaris_gold_client_secret"]
    write_features_task = ["/example_project/aws_polaris_features_client_id", "/example_project/aws_polaris_features_client_secret"]
  }
  aws_polaris_reader_client_id_name     = "/example_project/aws_polaris_reader_client_id"
  aws_polaris_reader_client_secret_name = "/example_project/aws_polaris_reader_client_secret"
  log_group_name                        = "example_project_jobs_dev"
  layer_feast_redis_host                = "10.0.1.20"
}

run "default_plan_creates_no_layer_task" {
  command = plan

  variables {
    images                 = {}
    train_images           = {}
    job_role_arns          = {}
    writer_parameter_names = {}
    log_group_name         = null
    layer_feast_redis_host = ""
  }

  assert {
    condition     = length(aws_ecs_task_definition.layer) == 0 && length(output.layer_run_tasks) == 0 && length(output.layer_container_definitions) == 0
    error_message = "Without layer_jobs_enabled the stack registers no layer task and needs no image, role or pair for one."
  }
}

run "each_layer_is_one_fargate_task_with_its_own_writer_role" {
  command = plan

  variables {
    layer_jobs_enabled = true
  }

  assert {
    condition = (
      toset(keys(aws_ecs_task_definition.layer)) == toset(["silver", "gold", "features"]) &&
      aws_ecs_task_definition.layer["silver"].family == "example_project_dev_job_silver" &&
      aws_ecs_task_definition.layer["gold"].family == "example_project_dev_job_gold" &&
      aws_ecs_task_definition.layer["features"].family == "example_project_dev_job_features" &&
      aws_ecs_task_definition.layer["silver"].task_role_arn == "arn:aws:iam::111111111111:role/example_project_job_write_silver_task_dev" &&
      aws_ecs_task_definition.layer["gold"].task_role_arn == "arn:aws:iam::111111111111:role/example_project_job_write_gold_task_dev" &&
      aws_ecs_task_definition.layer["features"].task_role_arn == "arn:aws:iam::111111111111:role/example_project_job_write_features_task_dev" &&
      alltrue([for task in values(aws_ecs_task_definition.layer) : (
        task.execution_role_arn == "arn:aws:iam::111111111111:role/example_project_job_execution_dev" &&
        toset(task.requires_compatibilities) == toset(["FARGATE"]) && task.network_mode == "awsvpc" && task.runtime_platform[0].cpu_architecture == "ARM64" &&
        task.cpu == "4096" && task.memory == "16384" && task.ephemeral_storage[0].size_in_gib == 100 && task.skip_destroy && !task.enable_fault_injection
      )])
    )
    error_message = "Each layer is one ARM64 Fargate definition with the execution role and only its own layer's writer role."
  }
}

run "containers_follow_the_broker_table_in_order" {
  command = plan

  variables {
    layer_jobs_enabled = true
  }

  assert {
    condition = (
      [for container in jsondecode(output.layer_container_definitions["silver"]) : container.name] == ["init", "fetch", "pin", "build", "validate", "verify", "export", "publish", "upload"] &&
      [for container in jsondecode(output.layer_container_definitions["gold"]) : container.name] == ["init", "fetch", "pin", "build", "validate", "export", "publish", "upload"] &&
      [for container in jsondecode(output.layer_container_definitions["features"]) : container.name] == ["init", "fetch", "pin", "build", "validate", "export", "publish", "feast"]
    )
    error_message = "Each layer runs the table's containers in pipeline order after the volume ownership step."
  }

  assert {
    condition = alltrue([for layer in ["silver", "gold", "features"] : (
      toset([for container in jsondecode(output.layer_container_definitions[layer]) : container.name if container.name != "init"]) == toset(keys(jsondecode(file("${path.module}/../../config/orchestration/ecs_broker_jobs.json")).jobs[layer].containers)) &&
      alltrue([for container in jsondecode(output.layer_container_definitions[layer]) : (
        container.name == "init" || toset([for variable in container.environment : variable.name if variable.value == ""]) == toset(jsondecode(file("${path.module}/../../config/orchestration/ecs_broker_jobs.json")).jobs[layer].containers[container.name])
      )])
    )])
    error_message = "The container names and the empty variables of every container equal the broker job table, so the broker's overrides fit."
  }

  assert {
    condition = alltrue([for layer in ["silver", "gold", "features"] : (
      alltrue([for index, container in jsondecode(output.layer_container_definitions[layer]) : (
        index == 0 ? length(container.dependsOn) == 0 : (
          length(container.dependsOn) == 1 && container.dependsOn[0].containerName == jsondecode(output.layer_container_definitions[layer])[index - 1].name && container.dependsOn[0].condition == "SUCCESS"
        )
      )]) &&
      [for container in jsondecode(output.layer_container_definitions[layer]) : container.essential] == concat(
        [for container in slice(jsondecode(output.layer_container_definitions[layer]), 0, length(jsondecode(output.layer_container_definitions[layer])) - 1) : false], [true]
      )
    )])
    error_message = "Every container starts after the success of the one before and only the last one is essential, so a failed phase stops the task."
  }
}

run "init_step_hands_the_volumes_to_the_job_user" {
  command = plan

  variables {
    layer_jobs_enabled = true
  }

  assert {
    condition = alltrue([for layer in ["silver", "gold", "features"] : (
      jsondecode(output.layer_container_definitions[layer])[0].name == "init" && jsondecode(output.layer_container_definitions[layer])[0].user == "0:0" &&
      jsondecode(output.layer_container_definitions[layer])[0].command == ["date +%s > /workspace/task_started && chown 10001:10001 /workspace /tmp /workspace/task_started"] &&
      jsondecode(output.layer_container_definitions[layer])[0].linuxParameters.capabilities.add == [] &&
      !contains(jsondecode(output.layer_container_definitions[layer])[0].linuxParameters.capabilities.drop, "CHOWN") &&
      jsondecode(output.layer_container_definitions[layer])[0].secrets == [] && !jsondecode(output.layer_container_definitions[layer])[0].privileged
    )])
    error_message = "The init step is the only root container, holds only the capability to change owners and no secret."
  }

  assert {
    condition = alltrue([for layer in ["silver", "gold", "features"] : alltrue([for container in slice(jsondecode(output.layer_container_definitions[layer]), 1, length(jsondecode(output.layer_container_definitions[layer]))) : (
      !container.privileged && container.readonlyRootFilesystem && container.user == "10001:10001" && container.linuxParameters.capabilities.drop == ["ALL"] &&
      container.logConfiguration.options["awslogs-group"] == "example_project_jobs_dev" && container.logConfiguration.options["awslogs-stream-prefix"] == layer &&
      toset([for mount in container.mountPoints : mount.containerPath if !mount.readOnly]) == toset(["/workspace", "/tmp"])
    )])])
    error_message = "Every phase container is unprivileged with a read-only root, no capability and the numeric user, and writes only the two volumes."
  }
}

run "init_writes_the_task_start_and_fetch_is_bounded" {
  command = plan

  variables {
    layer_jobs_enabled = true
  }

  assert {
    condition = alltrue([for layer in ["silver", "gold", "features"] : (
      startswith(one([for container in jsondecode(output.layer_container_definitions[layer]) : container.command[0] if container.name == "fetch"]), "timeout 1800 python -I -c '")
    )])
    error_message = "The fetch step is not run by the driver, so it carries its own deadline. The driver counts the whole task from the start time that the init step writes."
  }
}

run "each_phase_runs_the_driver_for_its_own_layer" {
  command = plan

  variables {
    layer_jobs_enabled = true
  }

  assert {
    condition = alltrue([for layer in ["silver", "gold", "features"] : alltrue([
      for container in slice(jsondecode(output.layer_container_definitions[layer]), 2, length(jsondecode(output.layer_container_definitions[layer]))) :
      container.name == "feast" || container.command == ["cd /workspace/code && python -m scripts.lakehouse.layer_run ${container.name} --layer ${layer} --run \"$RUN_ID\""]
    ])])
    error_message = "Every phase container runs the driver phase of its own name for its own layer, and no other layer."
  }

  assert {
    condition = (
      jsondecode(output.layer_container_definitions["features"])[7].command == ["cd /workspace/code && python -m scripts.lakehouse.layer_run feast --layer features --run \"$RUN_ID\""] &&
      jsondecode(output.layer_container_definitions["features"])[7].image == var.train_images.feature_store &&
      contains(jsondecode(output.layer_container_definitions["features"])[7].environment, { name = "FEAST_PROFILE", value = "aws" }) &&
      contains(jsondecode(output.layer_container_definitions["features"])[7].environment, { name = "FEAST_REDIS_HOST", value = "10.0.1.20" }) &&
      contains(jsondecode(output.layer_container_definitions["features"])[7].environment, { name = "FEAST_REGISTRY_BUCKET", value = "example-project-data-dev" })
    )
    error_message = "The features job applies Feast from the feature store image on the AWS profile."
  }

  assert {
    condition = alltrue([for layer in ["silver", "gold", "features"] : (
      strcontains(jsondecode(output.layer_container_definitions[layer])[1].command[0], "CODE_SHA256") && strcontains(jsondecode(output.layer_container_definitions[layer])[1].command[0], "CODE_RUN_ID") && !strcontains(jsondecode(output.layer_container_definitions[layer])[1].command[0], "environ[\"RUN_ID\"]") && strcontains(jsondecode(output.layer_container_definitions[layer])[1].command[0], "run_stage/") &&
      jsondecode(output.layer_container_definitions[layer])[1].image == var.images.spark
    )])
    error_message = "The fetch step downloads the staged archive of the run, checks it against CODE_SHA256 and unpacks it."
  }

  assert {
    condition = alltrue([for layer in ["silver", "gold", "features"] : (
      [for container in jsondecode(output.layer_container_definitions[layer]) : container.image if container.name == "build"] == [var.images.analytics] &&
      [for container in jsondecode(output.layer_container_definitions[layer]) : container.image if contains(["validate", "export"], container.name)] == [var.images.quality, var.images.quality] &&
      [for container in jsondecode(output.layer_container_definitions[layer]) : container.image if contains(["pin", "publish"], container.name)] == [var.images.spark, var.images.spark]
    )])
    error_message = "Build runs in the analytics image, validate and export in the quality image and the catalog phases in the Spark image."
  }
}

run "uploads_exist_only_for_silver_and_gold" {
  command = plan

  variables {
    layer_jobs_enabled = true
  }

  assert {
    condition = (
      contains([for container in jsondecode(output.layer_container_definitions["silver"]) : container.name], "upload") &&
      contains([for container in jsondecode(output.layer_container_definitions["gold"]) : container.name], "upload") &&
      !contains([for container in jsondecode(output.layer_container_definitions["features"]) : container.name], "upload") &&
      !contains([for container in jsondecode(output.layer_container_definitions["silver"]) : container.name], "feast") &&
      !contains([for container in jsondecode(output.layer_container_definitions["gold"]) : container.name], "verify")
    )
    error_message = "Silver and gold upload their run folder. Features applies Feast instead. Only silver verifies bronze again."
  }
}

run "secrets_reach_only_the_phases_that_need_them" {
  command = plan

  variables {
    layer_jobs_enabled = true
  }

  assert {
    condition = alltrue([for layer in ["silver", "gold", "features"] : alltrue([
      for container in jsondecode(output.layer_container_definitions[layer]) : jsonencode(container.secrets) == jsonencode(
        contains(["pin", "verify", "feast"], container.name) ? [
          { name = "POLARIS_CLIENT_ID", valueFrom = "arn:aws:ssm:us-west-2:111111111111:parameter/example_project/aws_polaris_reader_client_id" },
          { name = "POLARIS_CLIENT_SECRET", valueFrom = "arn:aws:ssm:us-west-2:111111111111:parameter/example_project/aws_polaris_reader_client_secret" },
          ] : (layer == "silver" && container.name == "build" ? [
            { name = "DBT_ENV_SECRET_POLARIS_CLIENT_ID", valueFrom = "arn:aws:ssm:us-west-2:111111111111:parameter/example_project/aws_polaris_reader_client_id" },
            { name = "DBT_ENV_SECRET_POLARIS_CLIENT_SECRET", valueFrom = "arn:aws:ssm:us-west-2:111111111111:parameter/example_project/aws_polaris_reader_client_secret" },
            ] : container.name == "publish" ? [
            { name = "POLARIS_CLIENT_ID", valueFrom = "arn:aws:ssm:us-west-2:111111111111:parameter/example_project/aws_polaris_${layer}_client_id" },
            { name = "POLARIS_CLIENT_SECRET", valueFrom = "arn:aws:ssm:us-west-2:111111111111:parameter/example_project/aws_polaris_${layer}_client_secret" },
        ] : [])
      )
    ])])
    error_message = "The reader pair reaches the pin, verify and feast phases and, under the names dbt scrubs from its logs, the silver build. The layer's own writer pair reaches only its publish phase and no other phase holds a credential."
  }

  assert {
    condition     = length(output.layer_secret_flow.violations) == 0 && length(output.layer_secret_flow.mutation_mismatches) == 0
    error_message = "The shared secret-flow check finds no secret in a command line, file or log, and catches its planted mutations."
  }

  assert {
    condition     = toset(output.layer_secret_flow.containers_checked) == toset(["layer_silver/pin", "layer_silver/build", "layer_silver/verify", "layer_silver/publish", "layer_gold/pin", "layer_gold/publish", "layer_features/pin", "layer_features/publish", "layer_features/feast"])
    error_message = "The shared check follows every container that receives a secret."
  }
}

run "every_layer_container_names_its_duckdb_spill_folder_and_cap" {
  command = plan

  variables {
    layer_jobs_enabled = true
  }

  assert {
    condition = alltrue([for layer in ["silver", "gold", "features"] : alltrue([
      for container in jsondecode(output.layer_container_definitions[layer]) : (
        [for item in container.environment : item.value if item.name == "HAI_DUCKDB_TEMP_DIRECTORY"] == ["/workspace/duckdb_spill"] &&
        [for item in container.environment : item.value if item.name == "HAI_DUCKDB_MAX_TEMP_BYTES"] == ["64424509440"]
      )
    ])])
    error_message = "The task refuses a DuckDB build without an absolute spill folder and a positive byte cap, so every layer container names both, and the cap stays below the scratch volume."
  }

  assert {
    condition     = tonumber(one([for item in jsondecode(output.layer_container_definitions["silver"])[2].environment : item.value if item.name == "HAI_DUCKDB_MAX_TEMP_BYTES"])) < 100 * 1073741824
    error_message = "The spill cap must be smaller than the 100 GiB of scratch storage the default layer shape holds."
  }
}

run "a_single_layer_can_be_enabled_first" {
  command = plan

  variables {
    layer_jobs_enabled = true
    layer_jobs         = ["silver"]
    job_role_arns = {
      execution         = "arn:aws:iam::111111111111:role/example_project_job_execution_dev"
      write_silver_task = "arn:aws:iam::111111111111:role/example_project_job_write_silver_task_dev"
    }
    writer_parameter_names = {
      write_silver_task = ["/example_project/aws_polaris_silver_client_id", "/example_project/aws_polaris_silver_client_secret"]
    }
  }

  assert {
    condition     = toset(keys(aws_ecs_task_definition.layer)) == toset(["silver"]) && length(output.layer_run_tasks) == 1
    error_message = "Gold runs only after the silver publish passes, so the owner can register silver alone."
  }
}

run "run_task_contract" {
  command = plan

  variables {
    layer_jobs_enabled = true
  }

  assert {
    condition = (
      output.layer_run_tasks["gold"].task_definition == "example_project_dev_job_gold" && output.layer_run_tasks["gold"].task_role_key == "write_gold_task" &&
      output.layer_run_tasks["gold"].assign_public_ip == "DISABLED" && output.layer_run_tasks["gold"].capacity_providers == ["FARGATE"] &&
      output.layer_run_tasks["gold"].network == "lakehouse" && output.layer_run_tasks["gold"].propagate_tags == "TASK_DEFINITION" &&
      output.layer_run_tasks["gold"].container_environment["pin"] == ["RUN_ID", "CODE_RUN_ID", "CODE_SHA256", "POLARIS_URI", "INPUT_RUN_ID"] &&
      output.layer_run_tasks["silver"].container_environment["verify"] == ["RUN_ID", "CODE_RUN_ID", "POLARIS_URI"]
    )
    error_message = "The output names the on-demand private placement, the layer's role key and the variables each container needs."
  }
}

run "dry_run_override_is_accepted_on_every_container" {
  command = plan

  variables {
    layer_jobs_enabled = true
  }

  assert {
    condition = alltrue([for layer in ["silver", "gold", "features"] : (
      output.layer_run_tasks[layer].dry_run_variable == "LAYER_DRY_RUN" && output.layer_run_tasks[layer].dry_run_value == "1"
    )])
    error_message = "The output names the one override that makes a run a dry run, for the owner's run-task call."
  }
}

run "resources_can_be_resized_within_a_fargate_shape" {
  command = plan

  variables {
    layer_jobs_enabled = true
    layer_resources    = { cpu = 8192, memory = 32768, ephemeral_storage_gib = 150 }
  }

  assert {
    condition     = alltrue([for task in values(aws_ecs_task_definition.layer) : task.cpu == "8192" && task.memory == "32768" && task.ephemeral_storage[0].size_in_gib == 150])
    error_message = "A valid larger shape is accepted."
  }
}

run "invalid_shape_is_refused" {
  command = plan

  variables {
    layer_jobs_enabled = true
    layer_resources    = { cpu = 4096, memory = 4096, ephemeral_storage_gib = 100 }
  }

  expect_failures = [var.layer_resources]
}

run "another_layers_role_is_refused" {
  command = plan

  variables {
    layer_jobs_enabled = true
    job_role_arns = {
      execution           = "arn:aws:iam::111111111111:role/example_project_job_execution_dev"
      write_silver_task   = "arn:aws:iam::111111111111:role/example_project_job_write_gold_task_dev"
      write_gold_task     = "arn:aws:iam::111111111111:role/example_project_job_write_gold_task_dev"
      write_features_task = "arn:aws:iam::111111111111:role/example_project_job_write_features_task_dev"
    }
  }

  expect_failures = [var.job_role_arns]
}

run "missing_role_for_an_enabled_layer_is_refused" {
  command = plan

  variables {
    layer_jobs_enabled = true
    layer_jobs         = ["gold"]
    job_role_arns = {
      execution         = "arn:aws:iam::111111111111:role/example_project_job_execution_dev"
      write_silver_task = "arn:aws:iam::111111111111:role/example_project_job_write_silver_task_dev"
    }
  }

  expect_failures = [var.layer_jobs_enabled]
}

run "missing_pair_for_an_enabled_layer_is_refused" {
  command = plan

  variables {
    layer_jobs_enabled = true
    layer_jobs         = ["gold"]
    writer_parameter_names = {
      write_silver_task = ["/example_project/aws_polaris_silver_client_id", "/example_project/aws_polaris_silver_client_secret"]
    }
  }

  expect_failures = [var.layer_jobs_enabled]
}

run "another_layers_pair_is_refused" {
  command = plan

  variables {
    layer_jobs_enabled = true
    writer_parameter_names = {
      write_silver_task   = ["/example_project/aws_polaris_gold_client_id", "/example_project/aws_polaris_gold_client_secret"]
      write_gold_task     = ["/example_project/aws_polaris_gold_client_id", "/example_project/aws_polaris_gold_client_secret"]
      write_features_task = ["/example_project/aws_polaris_features_client_id", "/example_project/aws_polaris_features_client_secret"]
    }
  }

  expect_failures = [var.writer_parameter_names]
}

run "missing_image_for_an_enabled_layer_is_refused" {
  command = plan

  variables {
    layer_jobs_enabled = true
    images = {
      analytics = "111111111111.dkr.ecr.us-west-2.amazonaws.com/example_project_analytics_dev@sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
      quality   = "111111111111.dkr.ecr.us-west-2.amazonaws.com/example_project_quality_dev@sha256:bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"
    }
  }

  expect_failures = [var.layer_jobs_enabled]
}

run "features_without_the_feature_store_image_or_redis_host_is_refused" {
  command = plan

  variables {
    layer_jobs_enabled = true
    train_images       = {}
  }

  expect_failures = [var.layer_jobs_enabled]
}

run "features_without_a_redis_host_is_refused" {
  command = plan

  variables {
    layer_jobs_enabled     = true
    layer_feast_redis_host = ""
  }

  expect_failures = [var.layer_feast_redis_host]
}

run "a_mutable_image_tag_is_refused" {
  command = plan

  variables {
    layer_jobs_enabled = true
    images = {
      analytics = "111111111111.dkr.ecr.us-west-2.amazonaws.com/example_project_analytics_dev:latest"
      quality   = "111111111111.dkr.ecr.us-west-2.amazonaws.com/example_project_quality_dev@sha256:bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"
      spark     = "111111111111.dkr.ecr.us-west-2.amazonaws.com/example_project_spark_dev@sha256:cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc"
    }
  }

  expect_failures = [var.images]
}

run "layer_jobs_need_the_merged_jobs_log_group" {
  command = plan

  variables {
    layer_jobs_enabled = true
    log_group_name     = "another_group"
  }

  expect_failures = [var.log_group_name]
}

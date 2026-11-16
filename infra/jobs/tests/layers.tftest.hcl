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
  host_job_memory_mib                   = 23552
  # TEST FIXTURE ONLY, not measured values: the services output host_capacity_binding for a 30720 MiB usable host.
  host_capacity_binding = "job_memory_mib=23552 usable_memory_mib=30720 system_reserve_mib=2048 service_reservations_mib=5120"
  host_jobs_enabled     = true
  host_jobs             = ["silver", "gold", "features"]
}

run "each_layer_is_one_host_task_with_its_own_writer_role" {
  command = plan

  variables {
    layer_jobs_enabled = true
  }

  assert {
    condition = (
      toset(keys(aws_ecs_task_definition.host)) == toset(["silver", "gold", "features"]) &&
      aws_ecs_task_definition.host["silver"].family == "example_project_dev_job_silver_host" &&
      aws_ecs_task_definition.host["gold"].family == "example_project_dev_job_gold_host" &&
      aws_ecs_task_definition.host["features"].family == "example_project_dev_job_features_host" &&
      aws_ecs_task_definition.host["silver"].task_role_arn == "arn:aws:iam::111111111111:role/example_project_job_write_silver_task_dev" &&
      aws_ecs_task_definition.host["gold"].task_role_arn == "arn:aws:iam::111111111111:role/example_project_job_write_gold_task_dev" &&
      aws_ecs_task_definition.host["features"].task_role_arn == "arn:aws:iam::111111111111:role/example_project_job_write_features_task_dev" &&
      alltrue([for task in values(aws_ecs_task_definition.host) : (
        task.execution_role_arn == "arn:aws:iam::111111111111:role/example_project_job_execution_dev" &&
        toset(task.requires_compatibilities) == toset(["EC2"]) && task.network_mode == "bridge" && task.runtime_platform[0].cpu_architecture == "ARM64" &&
        task.cpu == "3072" && task.memory == "23552" && task.skip_destroy && !task.enable_fault_injection
      )])
    )
    error_message = "Each layer is one ARM64 host definition with the execution role, only its own layer's writer role and the host job cap."
  }
}

run "containers_follow_the_job_catalog_in_order" {
  command = plan

  variables {
    layer_jobs_enabled = true
  }

  assert {
    condition = (
      [for container in [for entry in jsondecode(output.host_container_definitions["silver"]) : entry if entry.name != "guard"] : container.name] == ["init", "fetch", "pin", "build", "validate", "verify", "export", "publish", "upload"] &&
      [for container in [for entry in jsondecode(output.host_container_definitions["gold"]) : entry if entry.name != "guard"] : container.name] == ["init", "fetch", "pin", "build", "validate", "export", "publish", "upload", "maintain"] &&
      [for container in [for entry in jsondecode(output.host_container_definitions["features"]) : entry if entry.name != "guard"] : container.name] == ["init", "fetch", "pin", "build", "validate", "export", "publish", "upload", "feast"]
    )
    error_message = "Each layer runs the table's containers in pipeline order after the volume ownership step."
  }

  assert {
    condition = alltrue([for layer in ["silver", "gold", "features"] : (
      toset([for container in [for entry in jsondecode(output.host_container_definitions[layer]) : entry if entry.name != "guard"] : container.name if container.name != "init"]) == toset(keys(jsondecode(file("${path.module}/../../config/orchestration/ecs_broker_jobs.json")).jobs[layer].containers)) &&
      alltrue([for container in [for entry in jsondecode(output.host_container_definitions[layer]) : entry if entry.name != "guard"] : (
        container.name == "init" || toset([for variable in container.environment : variable.name if variable.value == ""]) == toset(jsondecode(file("${path.module}/../../config/orchestration/ecs_broker_jobs.json")).jobs[layer].containers[container.name])
      )])
    )])
    error_message = "The container names and the empty variables of every container equal the job catalog, so the launch's overrides fit."
  }

  assert {
    condition = alltrue([for layer in ["silver", "gold", "features"] : (
      alltrue([for index, container in [for entry in jsondecode(output.host_container_definitions[layer]) : entry if entry.name != "guard"] : (
        index == 0 ? length(container.dependsOn) == 0 : (
          length(container.dependsOn) == 1 && container.dependsOn[0].containerName == [for entry in jsondecode(output.host_container_definitions[layer]) : entry if entry.name != "guard"][index - 1].name && container.dependsOn[0].condition == "SUCCESS"
        )
      )]) &&
      [for container in [for entry in jsondecode(output.host_container_definitions[layer]) : entry if entry.name != "guard"] : container.essential] == concat(
        [for container in slice([for entry in jsondecode(output.host_container_definitions[layer]) : entry if entry.name != "guard"], 0, length([for entry in jsondecode(output.host_container_definitions[layer]) : entry if entry.name != "guard"]) - 1) : false], [true]
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
      [for entry in jsondecode(output.host_container_definitions[layer]) : entry if entry.name != "guard"][0].name == "init" && [for entry in jsondecode(output.host_container_definitions[layer]) : entry if entry.name != "guard"][0].user == "0:0" &&
      [for entry in jsondecode(output.host_container_definitions[layer]) : entry if entry.name != "guard"][0].command == ["find /workspace /tmp -mindepth 1 -delete && date +%s > /workspace/task_started && chown 10001:10001 /workspace /tmp /workspace/task_started"] &&
      [for entry in jsondecode(output.host_container_definitions[layer]) : entry if entry.name != "guard"][0].linuxParameters.capabilities.add == [] &&
      !contains([for entry in jsondecode(output.host_container_definitions[layer]) : entry if entry.name != "guard"][0].linuxParameters.capabilities.drop, "CHOWN") &&
      [for entry in jsondecode(output.host_container_definitions[layer]) : entry if entry.name != "guard"][0].secrets == [] && ![for entry in jsondecode(output.host_container_definitions[layer]) : entry if entry.name != "guard"][0].privileged
    )])
    error_message = "The init step is the only root container, holds only the capability to change owners and no secret."
  }

  assert {
    condition = alltrue([for layer in ["silver", "gold", "features"] : alltrue([for container in slice([for entry in jsondecode(output.host_container_definitions[layer]) : entry if entry.name != "guard"], 1, length([for entry in jsondecode(output.host_container_definitions[layer]) : entry if entry.name != "guard"])) : (
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
      startswith(one([for container in [for entry in jsondecode(output.host_container_definitions[layer]) : entry if entry.name != "guard"] : container.command[0] if container.name == "fetch"]), "timeout 1800 python -I -c '")
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
      for container in slice([for entry in jsondecode(output.host_container_definitions[layer]) : entry if entry.name != "guard"], 2, length([for entry in jsondecode(output.host_container_definitions[layer]) : entry if entry.name != "guard"])) :
      contains(["feast", "maintain"], container.name) || (layer == "silver" && container.name == "upload") || container.command == ["cd /workspace/code && python -m scripts.lakehouse.layer_run ${container.name} --layer ${layer} --run \"$RUN_ID\""]
    ])])
    error_message = "Every phase container runs the driver phase of its own name for its own layer, and no other layer."
  }

  assert {
    condition     = [for entry in jsondecode(output.host_container_definitions["silver"]) : entry if entry.name == "upload"][0].command == ["(cd /workspace/code && python -m scripts.lakehouse.layer_run upload --layer silver --run \"$RUN_ID\") && find /workspace /tmp -mindepth 1 -delete"]
    error_message = "The silver upload, its last phase, runs the driver and then the scratch cleanup."
  }

  assert {
    condition = (
      [for entry in jsondecode(output.host_container_definitions["features"]) : entry if entry.name != "guard"][8].name == "feast" &&
      [for entry in jsondecode(output.host_container_definitions["features"]) : entry if entry.name != "guard"][8].command == ["(cd /workspace/code && python -m scripts.lakehouse.layer_run feast --layer features --run \"$RUN_ID\") && find /workspace /tmp -mindepth 1 -delete"] &&
      [for entry in jsondecode(output.host_container_definitions["features"]) : entry if entry.name != "guard"][8].dependsOn == [{ containerName = "upload", condition = "SUCCESS" }] &&
      [for entry in jsondecode(output.host_container_definitions["features"]) : entry if entry.name != "guard"][8].essential &&
      [for entry in jsondecode(output.host_container_definitions["features"]) : entry if entry.name != "guard"][8].image == var.train_images.feature_store &&
      contains([for entry in jsondecode(output.host_container_definitions["features"]) : entry if entry.name != "guard"][8].environment, { name = "FEAST_PROFILE", value = "aws" }) &&
      !anytrue([for entry in [for entry in jsondecode(output.host_container_definitions["features"]) : entry if entry.name != "guard"][8].environment : entry.name == "FEAST_REDIS_HOST"]) &&
      contains([for entry in jsondecode(output.host_container_definitions["features"]) : entry if entry.name != "guard"][8].environment, { name = "FEAST_REGISTRY_BUCKET", value = "example-project-data-dev" }) &&
      toset([for item in [for entry in jsondecode(output.host_container_definitions["features"]) : entry if entry.name != "guard"][8].environment : item.name if item.value == ""]) == toset(["RUN_ID", "CODE_RUN_ID", "POLARIS_URI", "FEATURE_SET", "FEATURES_RUN", "RELEASE_MODEL", "LAYER_PHASE"])
    )
    error_message = "The features job ends with its feast container: the last, essential container on the feature store image and the AWS profile. A build launch applies Feast in it. A release or reload launch fills LAYER_PHASE, RELEASE_MODEL and FEATURES_RUN, and the same container then releases or reloads the online load of a model set."
  }

  assert {
    condition = (
      length([for entry in jsondecode(output.host_container_definitions["features"]) : entry if entry.name != "guard"]) == 9 &&
      length(jsondecode(output.host_container_definitions["features"])) == 10 &&
      !contains([for entry in jsondecode(output.host_container_definitions["features"]) : entry.name], "release") &&
      !contains([for entry in jsondecode(output.host_container_definitions["silver"]) : entry.name], "release") &&
      !contains([for entry in jsondecode(output.host_container_definitions["gold"]) : entry.name], "release")
    )
    error_message = "The release and the reload are phases of the features job that its feast container runs, and no container of their own: ECS allows at most 10 containers in a task definition and the features task holds exactly 10 (init, eight phases and the guard)."
  }

  assert {
    condition = alltrue([for layer in ["silver", "gold", "features"] : (
      strcontains([for entry in jsondecode(output.host_container_definitions[layer]) : entry if entry.name != "guard"][1].command[0], "CODE_SHA256") && strcontains([for entry in jsondecode(output.host_container_definitions[layer]) : entry if entry.name != "guard"][1].command[0], "CODE_RUN_ID") && !strcontains([for entry in jsondecode(output.host_container_definitions[layer]) : entry if entry.name != "guard"][1].command[0], "environ[\"RUN_ID\"]") && strcontains([for entry in jsondecode(output.host_container_definitions[layer]) : entry if entry.name != "guard"][1].command[0], "run_stage/") &&
      [for entry in jsondecode(output.host_container_definitions[layer]) : entry if entry.name != "guard"][1].image == var.images.spark
    )])
    error_message = "The fetch step downloads the staged archive of the run, checks it against CODE_SHA256 and unpacks it."
  }

  assert {
    condition = alltrue([for layer in ["silver", "gold", "features"] : (
      [for container in [for entry in jsondecode(output.host_container_definitions[layer]) : entry if entry.name != "guard"] : container.image if container.name == "build"] == [var.images.analytics] &&
      [for container in [for entry in jsondecode(output.host_container_definitions[layer]) : entry if entry.name != "guard"] : container.image if contains(["validate", "export"], container.name)] == [var.images.quality, var.images.quality] &&
      [for container in [for entry in jsondecode(output.host_container_definitions[layer]) : entry if entry.name != "guard"] : container.image if contains(["pin", "publish"], container.name)] == [var.images.spark, var.images.spark]
    )])
    error_message = "Build runs in the analytics image, validate and export in the quality image and the catalog phases in the Spark image."
  }
}

run "gold_maintenance_is_the_last_gold_container" {
  command = plan

  variables {
    layer_jobs_enabled = true
  }

  assert {
    condition = (
      [for entry in jsondecode(output.host_container_definitions["gold"]) : entry if entry.name != "guard"][8].name == "maintain" && [for entry in jsondecode(output.host_container_definitions["gold"]) : entry if entry.name != "guard"][8].essential &&
      [for entry in jsondecode(output.host_container_definitions["gold"]) : entry if entry.name != "guard"][8].dependsOn == [{ containerName = "upload", condition = "SUCCESS" }] &&
      [for entry in jsondecode(output.host_container_definitions["gold"]) : entry if entry.name != "guard"][8].command == ["(cd /workspace/code && python -m scripts.lakehouse.layer_run maintain --layer gold --run \"$RUN_ID\") && find /workspace /tmp -mindepth 1 -delete"] &&
      [for entry in jsondecode(output.host_container_definitions["gold"]) : entry if entry.name != "guard"][8].image == var.images.spark &&
      toset([for item in [for entry in jsondecode(output.host_container_definitions["gold"]) : entry if entry.name != "guard"][8].environment : item.name if item.value == ""]) == toset(["RUN_ID", "CODE_RUN_ID", "POLARIS_URI"])
    )
    error_message = "Gold maintenance runs last on every gold run, after the upload, in the Spark image, with the catalog address and no input run."
  }

  assert {
    condition = (
      !contains([for container in [for entry in jsondecode(output.host_container_definitions["silver"]) : entry if entry.name != "guard"] : container.name], "maintain") &&
      !contains([for container in [for entry in jsondecode(output.host_container_definitions["features"]) : entry if entry.name != "guard"] : container.name], "maintain")
    )
    error_message = "Only the gold job maintains gold."
  }
}

run "every_layer_uploads_and_only_features_applies_feast" {
  command = plan

  variables {
    layer_jobs_enabled = true
  }

  assert {
    condition = (
      contains([for container in [for entry in jsondecode(output.host_container_definitions["silver"]) : entry if entry.name != "guard"] : container.name], "upload") &&
      contains([for container in [for entry in jsondecode(output.host_container_definitions["gold"]) : entry if entry.name != "guard"] : container.name], "upload") &&
      contains([for container in [for entry in jsondecode(output.host_container_definitions["features"]) : entry if entry.name != "guard"] : container.name], "upload") &&
      !contains([for container in [for entry in jsondecode(output.host_container_definitions["silver"]) : entry if entry.name != "guard"] : container.name], "feast") &&
      !contains([for container in [for entry in jsondecode(output.host_container_definitions["gold"]) : entry if entry.name != "guard"] : container.name], "verify")
    )
    error_message = "Every layer uploads its run folder. Only features applies Feast. Only silver verifies bronze again."
  }
}

run "secrets_reach_only_the_phases_that_need_them" {
  command = plan

  variables {
    layer_jobs_enabled = true
  }

  assert {
    condition = alltrue([for layer in ["silver", "gold", "features"] : alltrue([
      for container in [for entry in jsondecode(output.host_container_definitions[layer]) : entry if entry.name != "guard"] : jsonencode(container.secrets) == jsonencode(
        contains(["pin", "verify", "feast"], container.name) ? [
          { name = "POLARIS_CLIENT_ID", valueFrom = "arn:aws:ssm:us-west-2:111111111111:parameter/example_project/aws_polaris_reader_client_id" },
          { name = "POLARIS_CLIENT_SECRET", valueFrom = "arn:aws:ssm:us-west-2:111111111111:parameter/example_project/aws_polaris_reader_client_secret" },
          ] : (layer == "silver" && container.name == "build" ? [
            { name = "DBT_ENV_SECRET_POLARIS_CLIENT_ID", valueFrom = "arn:aws:ssm:us-west-2:111111111111:parameter/example_project/aws_polaris_reader_client_id" },
            { name = "DBT_ENV_SECRET_POLARIS_CLIENT_SECRET", valueFrom = "arn:aws:ssm:us-west-2:111111111111:parameter/example_project/aws_polaris_reader_client_secret" },
            ] : container.name == "publish" || (layer == "gold" && container.name == "maintain") ? [
            { name = "POLARIS_CLIENT_ID", valueFrom = "arn:aws:ssm:us-west-2:111111111111:parameter/example_project/aws_polaris_${layer}_client_id" },
            { name = "POLARIS_CLIENT_SECRET", valueFrom = "arn:aws:ssm:us-west-2:111111111111:parameter/example_project/aws_polaris_${layer}_client_secret" },
        ] : [])
      )
    ])])
    error_message = "The reader pair reaches the pin, verify and feast phases and, under the names dbt scrubs from its logs, the silver build. The layer's own writer pair reaches only its publish phase and gold maintenance, and no other phase holds a credential."
  }

  assert {
    condition     = length(output.host_secret_flow.violations) == 0 && length(output.host_secret_flow.mutation_mismatches) == 0
    error_message = "The shared secret-flow check finds no secret in a command line, file or log, and catches its planted mutations."
  }

  assert {
    condition     = toset(output.host_secret_flow.containers_checked) == toset(["host_silver/pin", "host_silver/build", "host_silver/verify", "host_silver/publish", "host_gold/pin", "host_gold/publish", "host_gold/maintain", "host_features/pin", "host_features/publish", "host_features/feast"])
    error_message = "The shared check follows every container that receives a secret."
  }
}

run "every_layer_container_names_its_duckdb_spill_folder_and_no_fixed_cap" {
  command = plan

  variables {
    layer_jobs_enabled = true
  }

  assert {
    condition = alltrue([for layer in ["silver", "gold", "features"] : alltrue([
      for container in [for entry in jsondecode(output.host_container_definitions[layer]) : entry if entry.name != "guard"] : (
        [for item in container.environment : item.value if item.name == "HAI_DUCKDB_TEMP_DIRECTORY"] == ["/workspace/duckdb_spill"] &&
        [for item in container.environment : item.value if item.name == "HAI_SPARK_LOCAL_DIRECTORY"] == ["/workspace/spill"]
      )
    ])])
    error_message = "Every layer container still names the spill folders of DuckDB and Spark, which are paths on the workspace volume."
  }
}

run "no_layer_container_fixes_an_engine_limit" {
  command = plan

  variables {
    layer_jobs_enabled = true
  }

  assert {
    condition = alltrue([for layer in ["silver", "gold", "features"] : alltrue([
      for container in jsondecode(output.host_container_definitions[layer]) : length([
        for item in container.environment : item.name if contains(["SPARK_JOB_MEMORY", "DUCKDB_MEMORY_LIMIT", "JOB_THREADS", "HAI_DUCKDB_MAX_TEMP_BYTES"], item.name)
      ]) == 0
    ])])
    error_message = "The task derives the Spark heap, the DuckDB memory, the threads and the spill cap from its own capacity when a phase starts, so no container of a layer task fixes one."
  }
}

run "a_single_layer_can_be_enabled_first" {
  command = plan

  variables {
    layer_jobs_enabled = true
    layer_jobs         = ["silver"]
    host_jobs          = ["silver"]
    job_role_arns = {
      execution         = "arn:aws:iam::111111111111:role/example_project_job_execution_dev"
      write_silver_task = "arn:aws:iam::111111111111:role/example_project_job_write_silver_task_dev"
    }
    writer_parameter_names = {
      write_silver_task = ["/example_project/aws_polaris_silver_client_id", "/example_project/aws_polaris_silver_client_secret"]
    }
  }

  assert {
    condition     = toset(keys(aws_ecs_task_definition.host)) == toset(["silver"]) && length(output.host_run_tasks) == 1
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
      output.host_run_tasks["gold"].task_definition == "example_project_dev_job_gold_host" && output.host_run_tasks["gold"].task_role_key == "write_gold_task" &&
      output.host_run_tasks["gold"].capacity_provider == "example_project_host_dev" && output.host_run_tasks["gold"].network_mode == "bridge" &&
      output.host_run_tasks["gold"].network == "lakehouse" && output.host_run_tasks["gold"].propagate_tags == "TASK_DEFINITION" &&
      output.host_run_tasks["gold"].container_environment["pin"] == ["RUN_ID", "CODE_RUN_ID", "CODE_SHA256", "POLARIS_URI", "INPUT_RUN_ID"] &&
      output.host_run_tasks["silver"].container_environment["verify"] == ["RUN_ID", "CODE_RUN_ID", "POLARIS_URI"] &&
      output.host_run_tasks["features"].container_environment["pin"] == ["RUN_ID", "CODE_RUN_ID", "CODE_SHA256", "POLARIS_URI", "INPUT_RUN_ID", "FEATURE_SET", "LAYER_PHASE"] &&
      output.host_run_tasks["features"].container_environment["feast"] == ["RUN_ID", "CODE_RUN_ID", "POLARIS_URI", "FEATURE_SET", "FEATURES_RUN", "RELEASE_MODEL", "LAYER_PHASE"] &&
      !contains(keys(output.host_run_tasks["features"].container_environment), "release")
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
      output.host_run_tasks[layer].dry_run_variable == "LAYER_DRY_RUN" && output.host_run_tasks[layer].dry_run_value == "1"
    )])
    error_message = "The output names the one override that makes a run a dry run, for the owner's run-task call."
  }
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

run "features_without_the_feature_store_image_is_refused" {
  command = plan

  variables {
    layer_jobs_enabled = true
    train_images       = {}
  }

  expect_failures = [var.layer_jobs_enabled]
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

run "host_jobs_need_the_merged_jobs_log_group" {
  command = plan

  variables {
    layer_jobs_enabled = true
    log_group_name     = "another_group"
  }

  expect_failures = [var.log_group_name]
}

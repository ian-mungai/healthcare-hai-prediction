mock_provider "aws" {}

# Every family has its source switched on, so each run can compare the host definitions with the Fargate definitions the same inputs register.
variables {
  aws_profile         = "example_project_dev"
  aws_region          = "us-west-2"
  expected_account_id = "111111111111"
  project_name        = "example_project"
  environment         = "dev"
  data_bucket_name    = "example-project-data-dev"
  log_group_name      = "example_project_jobs_dev"
  host_job_memory_mib = 23552
  images = {
    analytics = "111111111111.dkr.ecr.us-west-2.amazonaws.com/example_project_analytics_dev@sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
    quality   = "111111111111.dkr.ecr.us-west-2.amazonaws.com/example_project_quality_dev@sha256:bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"
    spark     = "111111111111.dkr.ecr.us-west-2.amazonaws.com/example_project_spark_dev@sha256:cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc"
  }
  train_images = {
    training      = "111111111111.dkr.ecr.us-west-2.amazonaws.com/example_project_training_dev@sha256:dddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddd"
    feature_store = "111111111111.dkr.ecr.us-west-2.amazonaws.com/example_project_feature_store_dev@sha256:eeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee"
  }
  drift_images = {
    training      = "111111111111.dkr.ecr.us-west-2.amazonaws.com/example_project_training_dev@sha256:dddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddd"
    feature_store = "111111111111.dkr.ecr.us-west-2.amazonaws.com/example_project_feature_store_dev@sha256:eeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee"
    drift         = "111111111111.dkr.ecr.us-west-2.amazonaws.com/example_project_drift_dev@sha256:ffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff"
  }
  openmetadata_images = {
    server    = "111111111111.dkr.ecr.us-west-2.amazonaws.com/example_project_openmetadata_server_dev@sha256:1111111111111111111111111111111111111111111111111111111111111111"
    ingestion = "111111111111.dkr.ecr.us-west-2.amazonaws.com/example_project_openmetadata_ingestion_dev@sha256:2222222222222222222222222222222222222222222222222222222222222222"
    search    = "111111111111.dkr.ecr.us-west-2.amazonaws.com/example_project_openmetadata_search_dev@sha256:3333333333333333333333333333333333333333333333333333333333333333"
    database  = "111111111111.dkr.ecr.us-west-2.amazonaws.com/example_project_openmetadata_database_dev@sha256:4444444444444444444444444444444444444444444444444444444444444444"
  }
  job_role_arns = {
    execution           = "arn:aws:iam::111111111111:role/example_project_job_execution_dev"
    write_silver_task   = "arn:aws:iam::111111111111:role/example_project_job_write_silver_task_dev"
    write_gold_task     = "arn:aws:iam::111111111111:role/example_project_job_write_gold_task_dev"
    write_features_task = "arn:aws:iam::111111111111:role/example_project_job_write_features_task_dev"
    write_bronze_task   = "arn:aws:iam::111111111111:role/example_project_job_write_bronze_task_dev"
  }
  train_roles = {
    execution  = "arn:aws:iam::111111111111:role/example_project_job_execution_dev"
    train_task = "arn:aws:iam::111111111111:role/example_project_job_train_task_dev"
  }
  drift_roles = {
    execution  = "arn:aws:iam::111111111111:role/example_project_job_execution_dev"
    drift_task = "arn:aws:iam::111111111111:role/example_project_job_drift_task_dev"
  }
  serve_roles = {
    serve_task = "arn:aws:iam::111111111111:role/example_project_job_serve_task_dev"
  }
  openmetadata_roles = {
    execution         = "arn:aws:iam::111111111111:role/example_project_job_execution_dev"
    openmetadata_task = "arn:aws:iam::111111111111:role/example_project_job_openmetadata_task_dev"
  }
  writer_parameter_names = {
    write_silver_task   = ["/example_project/aws_polaris_silver_client_id", "/example_project/aws_polaris_silver_client_secret"]
    write_gold_task     = ["/example_project/aws_polaris_gold_client_id", "/example_project/aws_polaris_gold_client_secret"]
    write_features_task = ["/example_project/aws_polaris_features_client_id", "/example_project/aws_polaris_features_client_secret"]
    write_bronze_task   = ["/example_project/aws_polaris_bronze_client_id", "/example_project/aws_polaris_bronze_client_secret"]
  }
  aws_polaris_reader_client_id_name     = "/example_project/aws_polaris_reader_client_id"
  aws_polaris_reader_client_secret_name = "/example_project/aws_polaris_reader_client_secret"
  mlflow_admin_password_name            = "/example_project/mlflow_admin_password"
  drift_mlflow_password_name            = "/example_project/mlflow_admin_password"
  openmetadata_admin_password_name      = "/example_project/openmetadata_admin_password"
  serve_manifest_prefix                 = "serving/deployments"
  openmetadata_cpu_architecture         = "ARM64"
  openmetadata_ingestion_token_name     = "/example_project/openmetadata_bot_ingestion"
  host_openmetadata_api                 = "http://10.0.1.20:8585/api"
  host_repo_path                        = "/data/repo"
  layer_jobs_enabled                    = true
  bronze_job_enabled                    = true
  train_enabled                         = true
  drift_enabled                         = true
  promote_enabled                       = true
  serve_enabled                         = true
  openmetadata_enabled                  = true
}

run "flag_off" {
  command = plan

  assert {
    condition     = !var.host_jobs_enabled && length(aws_ecs_task_definition.host) == 0
    error_message = "The host definitions must default to off and register nothing."
  }

  assert {
    condition = (
      output.host_task_definition_arns == null && output.host_container_definitions == null && output.host_run_tasks == null &&
      output.host_dispatch_contract == null && output.host_secret_flow == null
    )
    error_message = "Without host_jobs_enabled every host output is null, so the plan shows no output change."
  }

  assert {
    condition = (
      length(aws_ecs_task_definition.layer) == 3 && length(aws_ecs_task_definition.bronze) == 1 && length(aws_ecs_task_definition.train) == 1 &&
      length(aws_ecs_task_definition.drift) == 1 && length(aws_ecs_task_definition.promote) == 1 && length(aws_ecs_task_definition.serve) == 1 &&
      length(aws_ecs_task_definition.openmetadata) == 1 && !contains(keys(output.task_definition_settings), "silver")
    )
    error_message = "The nine Fargate definitions stay registered while the host flag is off."
  }
}

run "flag_on_plans_the_nine_host_definitions" {
  command = plan

  variables {
    host_jobs_enabled = true
  }

  assert {
    condition     = toset(keys(aws_ecs_task_definition.host)) == toset(["silver", "gold", "features", "bronze", "train", "drift", "promote", "serve", "metadata"])
    error_message = "The flag plans exactly one host definition for each of the nine families."
  }

  assert {
    condition = alltrue([for family, task in aws_ecs_task_definition.host : (
      task.family == "example_project_dev_job_${family}_host" && task.requires_compatibilities == toset(["EC2"]) && task.network_mode == "bridge" &&
      task.skip_destroy && length(task.ephemeral_storage) == 0 && one(task.runtime_platform).cpu_architecture == "ARM64" &&
      one(task.runtime_platform).operating_system_family == "LINUX"
    )])
    error_message = "A host definition is an EC2 bridge task named <project>_<environment>_job_<family>_host on ARM64, with no Fargate scratch block."
  }

  assert {
    condition = alltrue([for family, size in {
      silver   = [3072, 23552], gold = [3072, 23552], features = [3072, 23552], bronze = [3072, 23552], train = [3072, 23552],
      metadata = [3072, 23552], drift = [3072, 23552], promote = [3072, 23552], serve = [3072, 23552]
      } : (
      aws_ecs_task_definition.host[family].cpu == tostring(size[0]) && aws_ecs_task_definition.host[family].memory == tostring(size[1])
    )])
    error_message = "Every host job gets the 3072 CPU units and the memory the services stack leaves for one job (host_job_memory_mib), whatever its Fargate size."
  }

  assert {
    condition = alltrue([for family, roles in {
      silver  = ["execution", "write_silver_task"], gold = ["execution", "write_gold_task"], features = ["execution", "write_features_task"],
      bronze  = ["execution", "write_bronze_task"], train = ["execution", "train_task"], drift = ["execution", "drift_task"],
      promote = ["execution", "train_task"], serve = ["execution", "serve_task"], metadata = ["execution", "openmetadata_task"]
      } : (
      aws_ecs_task_definition.host[family].execution_role_arn == "arn:aws:iam::111111111111:role/example_project_job_execution_dev" &&
      aws_ecs_task_definition.host[family].task_role_arn == "arn:aws:iam::111111111111:role/example_project_job_${roles[1]}_dev"
    )])
    error_message = "Each host definition runs under the execution role and the same task role as its Fargate twin."
  }
}

run "flag_on_leaves_every_existing_definition_unchanged" {
  command = plan

  variables {
    host_jobs_enabled = true
  }

  assert {
    condition = (
      output.layer_container_definitions == run.flag_off.layer_container_definitions && output.bronze_container_definitions == run.flag_off.bronze_container_definitions &&
      output.train_container_definitions == run.flag_off.train_container_definitions && output.drift_container_definitions == run.flag_off.drift_container_definitions &&
      output.promote_container_definitions == run.flag_off.promote_container_definitions && output.serve_container_definitions == run.flag_off.serve_container_definitions &&
      output.openmetadata_container_definitions == run.flag_off.openmetadata_container_definitions && output.layer_run_tasks == run.flag_off.layer_run_tasks &&
      output.bronze_run_task == run.flag_off.bronze_run_task && output.train_run_task == run.flag_off.train_run_task && output.drift_run_task == run.flag_off.drift_run_task &&
      output.promote_run_task == run.flag_off.promote_run_task && output.serve_run_task == run.flag_off.serve_run_task &&
      output.openmetadata_run_task == run.flag_off.openmetadata_run_task && output.task_definition_settings == run.flag_off.task_definition_settings &&
      output.dispatch_contract == run.flag_off.dispatch_contract && output.writer_dispatch_contracts == run.flag_off.writer_dispatch_contracts &&
      output.layer_secret_flow == run.flag_off.layer_secret_flow && output.execution_digests == run.flag_off.execution_digests
    )
    error_message = "Turning the host flag on must not change any Fargate definition, run-task output, dispatch contract or pin."
  }

  assert {
    condition = alltrue([for family, task in aws_ecs_task_definition.layer : (
      task.network_mode == "awsvpc" && task.requires_compatibilities == toset(["FARGATE"]) && task.family == "example_project_dev_job_${family}"
    )]) && aws_ecs_task_definition.train[0].network_mode == "awsvpc" && aws_ecs_task_definition.bronze[0].family == "example_project_dev_job_bronze"
    error_message = "The Fargate definitions keep their names and their awsvpc mode."
  }
}

run "host_containers_run_the_same_phases_as_the_fargate_twin" {
  command = plan

  variables {
    host_jobs_enabled = true
  }

  assert {
    condition = alltrue([for family, fargate in {
      silver   = jsondecode(output.layer_container_definitions["silver"]), gold = jsondecode(output.layer_container_definitions["gold"]),
      features = jsondecode(output.layer_container_definitions["features"]), bronze = jsondecode(output.bronze_container_definitions),
      train    = jsondecode(output.train_container_definitions), drift = jsondecode(output.drift_container_definitions),
      promote  = jsondecode(output.promote_container_definitions), serve = jsondecode(output.serve_container_definitions)
      } : (
      [for container in jsondecode(output.host_container_definitions[family]) : { for key, value in container : key => value if key != "environment" && !((container.name == "init" && contains(["command", "linuxParameters"], key)) || (key == "command" && container.essential && container.name != "guard")) }] ==
      [for container in fargate : { for key, value in container : key => value if key != "environment" && !((container.name == "init" && contains(["command", "linuxParameters"], key)) || (key == "command" && container.essential && container.name != "guard")) }] &&
      [for container in jsondecode(output.host_container_definitions[family]) : [for item in container.environment : item.name]] ==
      [for container in fargate : [for item in container.environment : item.name]] &&
      [for container in jsondecode(output.host_container_definitions[family]) : [for item in container.environment : item.value]] ==
      [for container in fargate : [for item in container.environment : item.value]]
    )])
    error_message = "A host container of the eight job families equals its Fargate twin in command, image, user, secrets, dependencies, mounts and every variable. The init step differs by its wipe and capabilities and the last phase by its cleanup, which the scratch cases pin."
  }

  assert {
    condition = alltrue(flatten([for family, json in output.host_container_definitions : [for container in jsondecode(json) : (
      !contains(keys(container), "memory") && !contains(keys(container), "memoryReservation") && !contains(keys(container), "cpu") && !container.privileged &&
      container.logConfiguration.options["awslogs-region"] == "us-west-2"
    )]]))
    error_message = "Containers carry no memory or CPU of their own: the task level value counts once at placement."
  }
}

run "scratch_is_a_host_path_per_family" {
  command = plan

  variables {
    host_jobs_enabled = true
  }

  assert {
    condition = (
      toset([for volume in aws_ecs_task_definition.host["silver"].volume : "${volume.name}=${volume.host_path}"]) ==
      toset(["workspace=/data/job_scratch/silver/work", "tmp=/data/job_scratch/silver/tmp"]) &&
      toset([for volume in aws_ecs_task_definition.host["metadata"].volume : "${volume.name}=${volume.host_path}"]) == toset([
        "workspace=/data/job_scratch/metadata/work", "tmp=/data/job_scratch/metadata/tmp", "repo=/data/repo"
      ])
    )
    error_message = "Workspace and tmp are host folders under the scratch root, one folder per family, and the metadata job also mounts the repository checkout."
  }

  assert {
    condition = alltrue([for family, task in aws_ecs_task_definition.host : alltrue([for volume in task.volume : (
      volume.name == "repo" || startswith(volume.host_path, "/data/job_scratch/${family}/") && !strcontains(volume.host_path, "..") && !volume.configure_at_launch
    )])])
    error_message = "No volume leaves the family's own scratch folder except the read-only repository checkout of the metadata job."
  }

  assert {
    condition = alltrue(flatten([for family, json in output.host_container_definitions : [for container in jsondecode(json) : alltrue([
      for mount in container.mountPoints : contains([for volume in aws_ecs_task_definition.host[family].volume : volume.name], mount.sourceVolume)
    ])]]))
    error_message = "Every mount point names a volume of its own definition."
  }
}

run "scratch_root_can_move_within_the_scratch_mount" {
  command = plan

  variables {
    host_jobs_enabled = true
    host_jobs         = ["gold"]
    host_scratch_root = "/data/job_scratch/spill"
  }

  assert {
    condition     = toset([for volume in aws_ecs_task_definition.host["gold"].volume : volume.host_path]) == toset(["/data/job_scratch/spill/gold/work", "/data/job_scratch/spill/gold/tmp"])
    error_message = "The scratch root can name a folder inside the scratch mount."
  }
}

# Boot mounts the separate scratch volume only at /data/job_scratch, so a job bind source anywhere else is a folder of the
# snapshotted data volume. Every writable bind source of every family lies under that mount, for the default and for a folder in it.
run "every_writable_job_bind_source_lies_under_the_scratch_mount" {
  command = plan

  variables {
    host_jobs_enabled = true
  }

  assert {
    condition = alltrue([for family, task in aws_ecs_task_definition.host : alltrue([
      for volume in task.volume : volume.name == "repo" || startswith(volume.host_path, "/data/job_scratch/")
    ])])
    error_message = "Every volume except the read-only repository checkout binds a folder under the separate scratch mount /data/job_scratch."
  }

  assert {
    condition = alltrue(flatten([for family, json in output.host_container_definitions : [for container in jsondecode(json) : [
      for mount in container.mountPoints : mount.readOnly || startswith(one([for volume in aws_ecs_task_definition.host[family].volume : volume.host_path if volume.name == mount.sourceVolume]), "/data/job_scratch/")
    ]]]))
    error_message = "Every writable mount point binds a source under /data/job_scratch."
  }
}

run "the_scratch_root_must_lie_on_the_scratch_mount" {
  command = plan

  variables {
    host_jobs_enabled = true
    host_scratch_root = "/data/scratch_a"
  }

  expect_failures = [var.host_scratch_root]
}

run "the_scratch_root_may_not_be_the_data_volume_itself" {
  command = plan

  variables {
    host_jobs_enabled = true
    host_scratch_root = "/data"
  }

  expect_failures = [var.host_scratch_root]
}

run "the_scratch_root_may_not_share_only_the_mount_name_prefix" {
  command = plan

  variables {
    host_jobs_enabled = true
    host_scratch_root = "/data/job_scratch_other"
  }

  expect_failures = [var.host_scratch_root]
}

run "the_scratch_root_may_not_use_another_mount_point" {
  command = plan

  variables {
    host_jobs_enabled = true
    host_scratch_root = "/mnt/job_scratch"
  }

  expect_failures = [var.host_scratch_root]
}

run "one_family_can_move_first" {
  command = plan

  variables {
    host_jobs_enabled = true
    host_jobs         = ["serve"]
  }

  assert {
    condition     = toset(keys(aws_ecs_task_definition.host)) == toset(["serve"]) && toset(keys(output.host_run_tasks)) == toset(["serve"]) && toset(output.host_dispatch_contract.families) == toset(["serve"])
    error_message = "The owner can register one family at a time, as the per-family switch moves them one at a time."
  }
}

run "every_host_job_gets_the_computed_cap_whatever_its_fargate_shape" {
  command = plan

  variables {
    host_jobs_enabled   = true
    host_job_memory_mib = 20480
    layer_resources = {
      silver   = { cpu = 4096, memory = 30720, ephemeral_storage_gib = 100 }
      gold     = { cpu = 4096, memory = 16384, ephemeral_storage_gib = 100 }
      features = { cpu = 16384, memory = 65536, ephemeral_storage_gib = 100 }
    }
    drift_resources = { cpu = 1024, memory = 4096, ephemeral_storage_gib = 30 }
  }

  assert {
    condition = alltrue([for family in ["silver", "gold", "features", "drift", "metadata"] : (
      aws_ecs_task_definition.host[family].cpu == "3072" && aws_ecs_task_definition.host[family].memory == "20480"
    )])
    error_message = "A host job's size comes from the host room, not from its Fargate twin: one job runs at a time and its engines derive their limits from the task."
  }
}

run "host_jobs_need_the_room_the_services_leave" {
  command = plan

  variables {
    host_jobs_enabled   = true
    host_job_memory_mib = null
  }

  expect_failures = [var.host_job_memory_mib]
}

run "the_job_room_must_fit_the_host" {
  command = plan

  variables {
    host_jobs_enabled   = true
    host_job_memory_mib = 32768
  }

  expect_failures = [var.host_job_memory_mib]
}

run "the_job_room_may_not_be_too_small_for_a_layer_job" {
  command = plan

  variables {
    host_jobs_enabled   = true
    host_job_memory_mib = 2048
  }

  expect_failures = [var.host_job_memory_mib]
}

run "run_task_and_dispatch_contract" {
  command = plan

  variables {
    host_jobs_enabled = true
  }

  assert {
    condition = alltrue([for family, task in output.host_run_tasks : (
      task.task_definition == "example_project_dev_job_${family}_host" && task.capacity_provider == "example_project_host_dev" && task.network_mode == "bridge" &&
      task.propagate_tags == "TASK_DEFINITION" && task.network == jsondecode(file("${path.module}/../../config/orchestration/ecs_broker_jobs.json")).jobs[family].network
    )])
    error_message = "Every host run-task entry names its _host definition, the host capacity provider, bridge mode and the network class of the job table."
  }

  assert {
    condition = (
      toset(keys(output.host_run_tasks)) == toset(keys(jsondecode(file("${path.module}/../../config/orchestration/ecs_broker_jobs.json")).jobs)) &&
      output.host_run_tasks["silver"].container_environment == output.layer_run_tasks["silver"].container_environment &&
      output.host_run_tasks["features"].container_environment == output.layer_run_tasks["features"].container_environment &&
      output.host_run_tasks["bronze"].container_environment == output.bronze_run_task.container_environment &&
      output.host_run_tasks["train"].container_environment == output.train_run_task.container_environment &&
      output.host_run_tasks["drift"].container_environment == output.drift_run_task.container_environment &&
      output.host_run_tasks["promote"].container_environment == output.promote_run_task.container_environment &&
      output.host_run_tasks["serve"].container_environment == output.serve_run_task.container_environment &&
      jsonencode(output.host_run_tasks["metadata"].container_environment) == jsonencode({ ingest = ["RUN_ID", "POLARIS_URI"] })
    )
    error_message = "The host entries cover exactly the families of the job table and name the same container variables as the Fargate outputs."
  }

  assert {
    condition = (
      output.host_run_tasks["gold"].task_role_key == "write_gold_task" && output.host_run_tasks["bronze"].task_role_key == "write_bronze_task" &&
      output.host_run_tasks["promote"].task_role_key == "train_task" && output.host_run_tasks["serve"].task_role_key == "serve_task" &&
      output.host_run_tasks["silver"].dry_run_variable == "LAYER_DRY_RUN" && output.host_run_tasks["bronze"].dry_run_variable == "BRONZE_DRY_RUN" &&
      output.host_run_tasks["train"].dry_run_variable == null
    )
    error_message = "Each entry names the role key of its Fargate twin and the dry run override where the family has one."
  }

  assert {
    condition = (
      output.host_dispatch_contract.capacity_provider == "example_project_host_dev" && output.host_dispatch_contract.network_mode == "bridge" &&
      output.host_dispatch_contract.jobs_at_a_time == 1 && output.host_dispatch_contract.cpu_units_cap == 3072 && output.host_dispatch_contract.memory_mib_cap == 23552 &&
      output.host_dispatch_contract.propagate_tags == "TASK_DEFINITION" && toset(output.host_dispatch_contract.families) == toset(keys(aws_ecs_task_definition.host)) &&
      toset(keys(output.host_dispatch_contract.writers)) == toset(["silver", "gold", "features", "bronze"]) &&
      jsonencode(output.host_dispatch_contract.writers["gold"].parameter_names) == jsonencode(["/example_project/aws_polaris_gold_client_id", "/example_project/aws_polaris_gold_client_secret"]) &&
      output.host_dispatch_contract.family_networks["silver"] == "lakehouse" && output.host_dispatch_contract.family_networks["train"] == "training"
    )
    error_message = "The contract names the provider, the bridge mode, one job at a time, the caps, each family's network class and the writer pairs."
  }
}

run "host_definitions_pass_the_secret_flow_check" {
  command = plan

  variables {
    host_jobs_enabled = true
  }

  assert {
    condition = (
      output.host_secret_flow.violations == [] && output.host_secret_flow.alias_chains_converged && length(output.host_secret_flow.mutation_mismatches) == 0 &&
      output.host_secret_flow.mutations_checked > 0
    )
    error_message = "The host definitions pass the same secret-flow and pin mutation checks as the Fargate definitions."
  }

  assert {
    condition = toset(output.host_secret_flow.containers_checked) == toset([
      "host_silver/pin", "host_silver/build", "host_silver/verify", "host_silver/publish", "host_gold/pin", "host_gold/publish", "host_gold/maintain", "host_features/pin",
      "host_features/publish", "host_features/feast", "host_bronze/load", "host_train/extract", "host_train/train", "host_drift/baseline", "host_drift/current",
      "host_promote/promote", "host_serve/serve", "host_metadata/ingest"
    ])
    error_message = "The containers that receive a secret are the ones the Fargate twins have, and the flow check follows each of them."
  }
}

run "an_unready_family_is_refused" {
  command = plan

  variables {
    host_jobs_enabled = true
    drift_enabled     = false
  }

  expect_failures = [var.host_jobs]
}

run "an_unknown_family_is_refused" {
  command = plan

  variables {
    host_jobs = ["silver", "write_gold"]
  }

  expect_failures = [var.host_jobs]
}

run "the_catalog_task_needs_the_host_architecture" {
  command = plan

  variables {
    host_jobs_enabled             = true
    host_jobs                     = ["metadata"]
    openmetadata_cpu_architecture = "X86_64"
  }

  expect_failures = [var.host_jobs]
}

run "the_scratch_root_must_be_an_absolute_clean_path" {
  command = plan

  variables {
    host_jobs_enabled = true
    host_scratch_root = "data/job_scratch"
  }

  expect_failures = [var.host_scratch_root]
}

run "the_scratch_root_may_not_climb" {
  command = plan

  variables {
    host_jobs_enabled = true
    host_scratch_root = "/data/../etc"
  }

  expect_failures = [var.host_scratch_root]
}

run "the_scratch_root_has_no_trailing_slash" {
  command = plan

  variables {
    host_jobs_enabled = true
    host_scratch_root = "/data/job_scratch/"
  }

  expect_failures = [var.host_scratch_root]
}

run "flag_off_registers_nothing_even_with_a_family_list" {
  command = plan

  variables {
    host_jobs = ["silver"]
  }

  assert {
    condition     = length(aws_ecs_task_definition.host) == 0 && output.host_run_tasks == null
    error_message = "A family list alone registers nothing: only host_jobs_enabled does."
  }
}

run "the_metadata_job_is_one_ingestion_client_of_the_persistent_catalog" {
  command = plan

  variables {
    host_jobs_enabled = true
    host_jobs         = ["metadata"]
  }

  assert {
    condition = (
      length(jsondecode(output.host_container_definitions["metadata"])) == 2 &&
      jsondecode(output.host_container_definitions["metadata"])[0].name == "init" && !jsondecode(output.host_container_definitions["metadata"])[0].essential &&
      jsondecode(output.host_container_definitions["metadata"])[1].name == "ingest" && jsondecode(output.host_container_definitions["metadata"])[1].essential &&
      alltrue([for container in jsondecode(output.host_container_definitions["metadata"]) : container.image == var.openmetadata_images["ingestion"]])
    )
    error_message = "The metadata host task holds an init step and one ingestion container, both in the ingestion image, and not the database, search and server of the catalog."
  }

  assert {
    condition = (
      !strcontains(output.host_container_definitions["metadata"], "127.0.0.1") && !strcontains(output.host_container_definitions["metadata"], "localhost") &&
      !strcontains(output.host_container_definitions["metadata"], "portMappings") && !strcontains(output.host_container_definitions["metadata"], "links")
    )
    error_message = "No loopback address, port mapping or link: peers in bridge mode are not reachable that way."
  }

  assert {
    condition = alltrue([for item in jsondecode(output.host_container_definitions["metadata"])[1].environment : (
      !startswith(item.name, "DB_") && !startswith(item.name, "ELASTICSEARCH_") && !startswith(item.name, "POSTGRES_") && !startswith(item.name, "OPENSEARCH_") &&
      !contains(["SEARCH_TYPE", "PGDATA", "OM_DATABASE", "discovery.type", "OM_LIFETIME_SECONDS", "OM_INGESTION_TOKEN"], item.name)
    )])
    error_message = "The ingestion container carries no database or search setting, and the token is a secret, never a plain variable."
  }

  assert {
    condition = (
      one([for item in jsondecode(output.host_container_definitions["metadata"])[1].environment : item.value if item.name == "OM_API"]) == "http://10.0.1.20:8585/api" &&
      toset([for item in jsondecode(output.host_container_definitions["metadata"])[1].secrets : item.name]) == toset(["OM_INGESTION_TOKEN", "POLARIS_CLIENT_ID", "POLARIS_CLIENT_SECRET"]) &&
      one([for item in jsondecode(output.host_container_definitions["metadata"])[1].secrets : item.valueFrom if item.name == "OM_INGESTION_TOKEN"]) ==
      "arn:aws:ssm:us-west-2:111111111111:parameter/example_project/openmetadata_bot_ingestion"
    )
    error_message = "The container is told the catalog API address and receives the ingestion bot token from its Parameter Store parameter."
  }

  assert {
    condition = (
      jsondecode(output.host_container_definitions["metadata"])[1].entryPoint == ["sh", "-c"] &&
      startswith(jsondecode(output.host_container_definitions["metadata"])[1].command[0], "(python -m scripts.infrastructure.openmetadata_host_ingest --config-dir /opt/hai/repo/config/metadata/ingestion --work-dir /workspace polaris dbt)") &&
      jsondecode(output.host_container_definitions["metadata"])[1].workingDirectory == "/opt/hai/repo"
    )
    error_message = "The container runs the host ingestion script from the mounted checkout for the Polaris and dbt sources."
  }

  assert {
    condition = (
      toset([for mount in jsondecode(output.host_container_definitions["metadata"])[1].mountPoints : "${mount.sourceVolume}:${mount.containerPath}:${mount.readOnly}"]) ==
      toset(["workspace:/workspace:false", "tmp:/tmp:false", "repo:/opt/hai/repo:true"]) &&
      !contains(keys(jsondecode(output.host_container_definitions["metadata"])[1]), "memory") && jsondecode(output.host_container_definitions["metadata"])[1].readonlyRootFilesystem &&
      jsondecode(output.host_container_definitions["metadata"])[1].user == "10001:10001"
    )
    error_message = "The checkout is mounted read-only, the root filesystem stays read-only and the job user is not root."
  }

  assert {
    condition = (
      aws_ecs_task_definition.host["metadata"].memory == tostring(var.host_job_memory_mib) && aws_ecs_task_definition.host["metadata"].cpu == "3072"
    )
    error_message = "The metadata job gets the same room as every host job: 3072 CPU units and the memory the services leave (host_job_memory_mib); one job runs at a time."
  }
}

run "the_metadata_job_needs_only_the_ingestion_image_and_the_catalog_roles" {
  command = plan

  variables {
    host_jobs_enabled    = true
    host_jobs            = ["metadata"]
    openmetadata_enabled = false
    openmetadata_images  = { ingestion = "111111111111.dkr.ecr.us-west-2.amazonaws.com/example_project_openmetadata_ingestion_dev@sha256:2222222222222222222222222222222222222222222222222222222222222222" }
  }

  assert {
    condition     = toset(keys(aws_ecs_task_definition.host)) == toset(["metadata"]) && length(aws_ecs_task_definition.openmetadata) == 0
    error_message = "The host metadata job does not depend on the Fargate catalog task or on the server, search and database images."
  }
}

run "the_metadata_job_needs_a_catalog_address" {
  command = plan

  variables {
    host_jobs_enabled     = true
    host_jobs             = ["metadata"]
    host_openmetadata_api = null
  }

  expect_failures = [var.host_openmetadata_api]
}

run "the_catalog_address_may_not_be_loopback" {
  command = plan

  variables {
    host_jobs_enabled     = true
    host_jobs             = ["metadata"]
    host_openmetadata_api = "http://127.0.0.1:8585/api"
  }

  expect_failures = [var.host_openmetadata_api]
}

run "the_catalog_address_is_a_private_address_with_the_api_path" {
  command = plan

  variables {
    host_jobs_enabled     = true
    host_jobs             = ["metadata"]
    host_openmetadata_api = "http://10.0.1.20:8585"
  }

  expect_failures = [var.host_openmetadata_api]
}

run "the_metadata_job_needs_the_repository_path" {
  command = plan

  variables {
    host_jobs_enabled = true
    host_jobs         = ["metadata"]
    host_repo_path    = null
  }

  expect_failures = [var.host_repo_path]
}

run "the_repository_path_is_absolute_and_clean" {
  command = plan

  variables {
    host_jobs_enabled = true
    host_jobs         = ["metadata"]
    host_repo_path    = "/data/../repo"
  }

  expect_failures = [var.host_repo_path]
}

run "the_ingestion_token_is_the_project_bot_parameter" {
  command = plan

  variables {
    host_jobs_enabled                 = true
    host_jobs                         = ["metadata"]
    openmetadata_ingestion_token_name = "/example_project/openmetadata_bot_lineage"
  }

  expect_failures = [var.openmetadata_ingestion_token_name]
}

run "the_metadata_job_without_its_image_is_refused" {
  command = plan

  variables {
    host_jobs_enabled    = true
    host_jobs            = ["metadata"]
    openmetadata_enabled = false
    openmetadata_images  = {}
  }

  expect_failures = [var.host_jobs]
}

run "other_families_need_no_catalog_address" {
  command = plan

  variables {
    host_jobs_enabled     = true
    host_jobs             = ["silver"]
    host_openmetadata_api = null
    host_repo_path        = null
  }

  assert {
    condition     = toset(keys(aws_ecs_task_definition.host)) == toset(["silver"])
    error_message = "The catalog address and the checkout path matter only to the metadata job."
  }
}

run "the_catalog_address_refuses_public" {
  command = plan

  variables {
    host_jobs_enabled     = true
    host_jobs             = ["metadata"]
    host_openmetadata_api = "http://203.0.113.20:8585/api"
  }

  expect_failures = [var.host_openmetadata_api]
}

run "the_catalog_address_refuses_link_local" {
  command = plan

  variables {
    host_jobs_enabled     = true
    host_jobs             = ["metadata"]
    host_openmetadata_api = "http://169.254.169.254:8585/api"
  }

  expect_failures = [var.host_openmetadata_api]
}

run "the_catalog_address_refuses_octet_over_255" {
  command = plan

  variables {
    host_jobs_enabled     = true
    host_jobs             = ["metadata"]
    host_openmetadata_api = "http://10.0.1.256:8585/api"
  }

  expect_failures = [var.host_openmetadata_api]
}

run "the_catalog_address_refuses_first_octet_over_255" {
  command = plan

  variables {
    host_jobs_enabled     = true
    host_jobs             = ["metadata"]
    host_openmetadata_api = "http://999.0.1.20:8585/api"
  }

  expect_failures = [var.host_openmetadata_api]
}

run "the_catalog_address_refuses_leading_zero" {
  command = plan

  variables {
    host_jobs_enabled     = true
    host_jobs             = ["metadata"]
    host_openmetadata_api = "http://10.0.01.20:8585/api"
  }

  expect_failures = [var.host_openmetadata_api]
}

run "the_catalog_address_refuses_another_port" {
  command = plan

  variables {
    host_jobs_enabled     = true
    host_jobs             = ["metadata"]
    host_openmetadata_api = "http://10.0.1.20:8586/api"
  }

  expect_failures = [var.host_openmetadata_api]
}

run "the_catalog_address_refuses_port_over_65535" {
  command = plan

  variables {
    host_jobs_enabled     = true
    host_jobs             = ["metadata"]
    host_openmetadata_api = "http://10.0.1.20:99999/api"
  }

  expect_failures = [var.host_openmetadata_api]
}

run "the_catalog_address_refuses_a_name" {
  command = plan

  variables {
    host_jobs_enabled     = true
    host_jobs             = ["metadata"]
    host_openmetadata_api = "http://catalog.example.invalid:8585/api"
  }

  expect_failures = [var.host_openmetadata_api]
}

run "the_catalog_address_refuses_tls" {
  command = plan

  variables {
    host_jobs_enabled     = true
    host_jobs             = ["metadata"]
    host_openmetadata_api = "https://10.0.1.20:8585/api"
  }

  expect_failures = [var.host_openmetadata_api]
}

run "the_catalog_address_refuses_below_172_16" {
  command = plan

  variables {
    host_jobs_enabled     = true
    host_jobs             = ["metadata"]
    host_openmetadata_api = "http://172.15.255.255:8585/api"
  }

  expect_failures = [var.host_openmetadata_api]
}

run "the_catalog_address_refuses_above_172_31" {
  command = plan

  variables {
    host_jobs_enabled     = true
    host_jobs             = ["metadata"]
    host_openmetadata_api = "http://172.32.0.1:8585/api"
  }

  expect_failures = [var.host_openmetadata_api]
}

run "the_catalog_address_refuses_above_10" {
  command = plan

  variables {
    host_jobs_enabled     = true
    host_jobs             = ["metadata"]
    host_openmetadata_api = "http://11.0.0.1:8585/api"
  }

  expect_failures = [var.host_openmetadata_api]
}

run "the_catalog_address_refuses_above_192_168" {
  command = plan

  variables {
    host_jobs_enabled     = true
    host_jobs             = ["metadata"]
    host_openmetadata_api = "http://192.169.0.1:8585/api"
  }

  expect_failures = [var.host_openmetadata_api]
}

run "the_catalog_address_refuses_another_path" {
  command = plan

  variables {
    host_jobs_enabled     = true
    host_jobs             = ["metadata"]
    host_openmetadata_api = "http://10.0.1.20:8585/apix"
  }

  expect_failures = [var.host_openmetadata_api]
}

run "the_catalog_address_refuses_user_information" {
  command = plan

  variables {
    host_jobs_enabled     = true
    host_jobs             = ["metadata"]
    host_openmetadata_api = "http://10.0.1.20:8585@203.0.113.20:8585/api"
  }

  expect_failures = [var.host_openmetadata_api]
}

run "the_catalog_address_accepts_ten" {
  command = plan

  variables {
    host_jobs_enabled     = true
    host_jobs             = ["metadata"]
    host_openmetadata_api = "http://10.0.1.20:8585/api"
  }

  assert {
    condition     = one([for item in jsondecode(output.host_container_definitions["metadata"])[1].environment : item.value if item.name == "OM_API"]) == var.host_openmetadata_api
    error_message = "A private address on port 8585 with the /api path reaches the container."
  }
}

run "the_catalog_address_accepts_ten_edges" {
  command = plan

  variables {
    host_jobs_enabled     = true
    host_jobs             = ["metadata"]
    host_openmetadata_api = "http://10.255.255.255:8585/api"
  }

  assert {
    condition     = one([for item in jsondecode(output.host_container_definitions["metadata"])[1].environment : item.value if item.name == "OM_API"]) == var.host_openmetadata_api
    error_message = "A private address on port 8585 with the /api path reaches the container."
  }
}

run "the_catalog_address_accepts_low_172" {
  command = plan

  variables {
    host_jobs_enabled     = true
    host_jobs             = ["metadata"]
    host_openmetadata_api = "http://172.16.0.0:8585/api"
  }

  assert {
    condition     = one([for item in jsondecode(output.host_container_definitions["metadata"])[1].environment : item.value if item.name == "OM_API"]) == var.host_openmetadata_api
    error_message = "A private address on port 8585 with the /api path reaches the container."
  }
}

run "the_catalog_address_accepts_high_172" {
  command = plan

  variables {
    host_jobs_enabled     = true
    host_jobs             = ["metadata"]
    host_openmetadata_api = "http://172.31.255.255:8585/api"
  }

  assert {
    condition     = one([for item in jsondecode(output.host_container_definitions["metadata"])[1].environment : item.value if item.name == "OM_API"]) == var.host_openmetadata_api
    error_message = "A private address on port 8585 with the /api path reaches the container."
  }
}

run "the_catalog_address_accepts_private_192" {
  command = plan

  variables {
    host_jobs_enabled     = true
    host_jobs             = ["metadata"]
    host_openmetadata_api = "http://192.168.0.1:8585/api"
  }

  assert {
    condition     = one([for item in jsondecode(output.host_container_definitions["metadata"])[1].environment : item.value if item.name == "OM_API"]) == var.host_openmetadata_api
    error_message = "A private address on port 8585 with the /api path reaches the container."
  }
}

run "no_host_container_fixes_an_engine_limit" {
  command = plan

  variables {
    host_jobs_enabled = true
  }

  assert {
    condition = alltrue([for family in ["silver", "gold", "features", "bronze", "train", "drift"] : alltrue([
      for container in jsondecode(output.host_container_definitions[family]) : length([
        for item in container.environment : item.name if contains(["SPARK_JOB_MEMORY", "DUCKDB_MEMORY_LIMIT", "JOB_THREADS", "OMP_NUM_THREADS", "OPENBLAS_NUM_THREADS", "MKL_NUM_THREADS", "HAI_DUCKDB_MAX_TEMP_BYTES"], item.name)
      ]) == 0
    ])])
    error_message = "A host job derives its limits from the CPU units and memory of its task, so no host container fixes an engine limit and no thread count is rewritten."
  }
}

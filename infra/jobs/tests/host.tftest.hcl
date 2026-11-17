mock_provider "aws" {}

# Every family has its source switched on, so each run plans the ten host definitions from the same inputs.
variables {
  # The committed defaults of the jobs stack are the live ones (every job family on, host_repo_path and serve_manifest_prefix set). Every case here that does
  # not set a switch tests it off, so the fixture sets each one explicitly.
  host_jobs_enabled      = false
  publisher_job_enabled  = false
  publisher_keys_enabled = false
  host_jobs              = ["bronze", "drift", "features", "features_m1", "features_m2", "features_m3", "features_m4", "features_m5", "features_m6", "gold", "metadata", "promote", "serve", "silver", "train"]
  aws_profile            = "example_project_dev"
  aws_region             = "us-west-2"
  expected_account_id    = "111111111111"
  project_name           = "example_project"
  environment            = "dev"
  data_bucket_name       = "example-project-data-dev"
  log_group_name         = "example_project_jobs_dev"
  host_job_memory_mib    = 23552
  # TEST FIXTURE ONLY, not measured values: the services output host_capacity_binding for a 30720 MiB usable host.
  host_capacity_binding = "job_memory_mib=23552 usable_memory_mib=30720 system_reserve_mib=2048 service_reservations_mib=5120"
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
    ingestion = "111111111111.dkr.ecr.us-west-2.amazonaws.com/example_project_openmetadata_ingestion_dev@sha256:2222222222222222222222222222222222222222222222222222222222222222"
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
  serve_manifest_prefix                 = "serving/deployments"
  openmetadata_ingestion_token_name     = "/example_project/openmetadata_bot_ingestion"
  host_private_ip                       = "10.0.1.20"
  host_repo_path                        = "/data/repo"
  layer_jobs_enabled                    = true
  bronze_job_enabled                    = true
  train_enabled                         = true
  drift_enabled                         = true
  promote_enabled                       = true
  serve_enabled                         = true
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
}

run "flag_on_plans_the_ten_host_definitions_and_the_six_model_sets" {
  command = plan

  variables {
    host_jobs_enabled = true
  }

  assert {
    condition = toset(keys(aws_ecs_task_definition.host)) == toset([
      "silver", "gold", "features", "bronze", "bronze_stage", "train", "drift", "promote", "serve", "metadata",
      "features_m1", "features_m2", "features_m3", "features_m4", "features_m5", "features_m6"
    ])
    error_message = "The flag plans exactly one host definition for each of the ten families (the bronze stage job is the tenth) and for each of the six model sets, which are families of their own."
  }

  assert {
    condition = alltrue([for family, task in aws_ecs_task_definition.host : (
      task.family == "example_project_dev_job_${family}_host" && task.requires_compatibilities == toset(["EC2"]) && task.network_mode == "bridge" &&
      task.skip_destroy && length(task.ephemeral_storage) == 0 && one(task.runtime_platform).cpu_architecture == "ARM64" &&
      one(task.runtime_platform).operating_system_family == "LINUX"
    )])
    error_message = "A host definition is an EC2 bridge task named <project>_<environment>_job_<family>_host on ARM64, with no scratch block of its own."
  }

  assert {
    condition = alltrue([for family, size in {
      silver   = [3072, 23552], gold = [3072, 23552], features = [3072, 23552], bronze = [3072, 23552], train = [3072, 23552],
      metadata = [1024, 2048], bronze_stage = [512, 2048], drift = [2048, 8192], promote = [1024, 4096], serve = [1024, 4096]
      } : (
      aws_ecs_task_definition.host[family].cpu == tostring(size[0]) && aws_ecs_task_definition.host[family].memory == tostring(size[1])
    )])
    error_message = "Every host job gets the 3072 CPU units and the memory the services stack leaves for one job (host_job_memory_mib), whatever the job."
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
    error_message = "Each host definition runs under the execution role and the task role of its family."
  }

  assert {
    condition = alltrue([for family, json in output.host_container_definitions : (
      jsondecode(json)[0].name == "init" && jsondecode(json)[0].dockerLabels == { "hai.host_capacity" = replace(var.host_capacity_binding, "/^job_memory_mib=[0-9]+/", "job_memory_mib=${aws_ecs_task_definition.host[family].memory}") } &&
      alltrue([for container in slice(jsondecode(json), 1, length(jsondecode(json))) : !contains(keys(container), "dockerLabels")])
    )])
    error_message = "The init container of every host revision carries the capacity binding it was sized from (label hai.host_capacity), which the launch preflight checks against the current usable memory (owner decision A, Nov 10 2026); no other container carries a label."
  }
}

run "every_host_container_is_unprivileged_and_carries_no_size_of_its_own" {
  command = plan

  variables {
    host_jobs_enabled = true
  }

  assert {
    condition = alltrue(flatten([for family, json in output.host_container_definitions : [for container in jsondecode(json) : (
      !contains(keys(container), "memory") && !contains(keys(container), "memoryReservation") && !contains(keys(container), "cpu") && !container.privileged &&
      container.logConfiguration.options["awslogs-region"] == "us-west-2"
    )]]))
    error_message = "No host container is privileged or carries a memory or CPU of its own: the task level value counts once at placement. Every container logs to the project region."
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

run "every_host_job_gets_the_computed_cap" {
  command = plan

  variables {
    host_jobs_enabled     = true
    host_job_memory_mib   = 20480
    host_capacity_binding = "job_memory_mib=20480 usable_memory_mib=27648 system_reserve_mib=2048 service_reservations_mib=5120"
  }

  assert {
    condition = alltrue([for family in ["silver", "gold", "features"] : (
      aws_ecs_task_definition.host[family].cpu == "3072" && aws_ecs_task_definition.host[family].memory == "20480"
    )])
    error_message = "A host job's size comes from the host room, not from a size of its own: one job runs at a time and its engines derive their limits from the task."
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

run "host_jobs_need_the_capacity_binding" {
  command = plan

  variables {
    host_jobs_enabled     = true
    host_capacity_binding = null
  }

  expect_failures = [var.host_capacity_binding]
}

run "the_capacity_binding_has_the_rendered_form" {
  command = plan

  variables {
    host_jobs_enabled     = true
    host_capacity_binding = "job_memory_mib=23552"
  }

  expect_failures = [var.host_capacity_binding]
}

run "the_capacity_binding_may_not_give_a_job_more_than_the_room" {
  command = plan

  # The room is the usable memory less the reserve and the reservations: 30720 - 2048 - 5120 = 23552 MiB, below the 25600 MiB the binding gives a job.
  variables {
    host_jobs_enabled     = true
    host_job_memory_mib   = 25600
    host_capacity_binding = "job_memory_mib=25600 usable_memory_mib=30720 system_reserve_mib=2048 service_reservations_mib=5120"
  }

  expect_failures = [var.host_capacity_binding]
}

run "a_job_memory_below_the_room_is_accepted" {
  command = plan

  # The services cap host_job_cap_mib may be lower than the room it must fit (Nov 11 2026): every host job gets the cap.
  variables {
    host_jobs_enabled     = true
    host_job_memory_mib   = 20480
    host_capacity_binding = "job_memory_mib=20480 usable_memory_mib=30720 system_reserve_mib=2048 service_reservations_mib=5120"
  }

  assert {
    condition     = aws_ecs_task_definition.host["silver"].memory == "20480"
    error_message = "A host job gets the cap even when the room is larger."
  }
}

run "the_capacity_binding_gives_the_job_room" {
  command = plan

  variables {
    host_jobs_enabled     = true
    host_capacity_binding = "job_memory_mib=25600 usable_memory_mib=32768 system_reserve_mib=2048 service_reservations_mib=5120"
  }

  expect_failures = [aws_ecs_task_definition.host]
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
      task.propagate_tags == "TASK_DEFINITION" && task.network == merge(jsondecode(file("${path.module}/../../config/orchestration/ecs_broker_jobs.json")).jobs, jsondecode(file("${path.module}/../../config/orchestration/direct_jobs.json")).jobs)[startswith(family, "features_m") ? "features" : family].network
    )])
    error_message = "Every host run-task entry names its _host definition, the host capacity provider, bridge mode and the network class of the job table."
  }

  assert {
    condition = (
      toset(keys(output.host_run_tasks)) == setunion(toset(keys(jsondecode(file("${path.module}/../../config/orchestration/ecs_broker_jobs.json")).jobs)), toset(["bronze_stage", "features_m1", "features_m2", "features_m3", "features_m4", "features_m5", "features_m6"])) &&
      alltrue([for family in ["silver", "gold", "features", "bronze", "train", "drift", "promote", "serve", "metadata"] :
        output.host_run_tasks[family].container_environment == jsondecode(file("${path.module}/../../config/orchestration/ecs_broker_jobs.json")).jobs[family].containers
      ]) &&
      jsonencode(output.host_run_tasks["metadata"].container_environment) == jsonencode({ ingest = ["RUN_ID", "POLARIS_URI", "FEATURES_RUN"] })
    )
    error_message = "The host entries cover exactly the families of the job table and name the container variables of that table."
  }

  assert {
    condition = (
      output.host_run_tasks["gold"].task_role_key == "write_gold_task" && output.host_run_tasks["bronze"].task_role_key == "write_bronze_task" &&
      output.host_run_tasks["promote"].task_role_key == "train_task" && output.host_run_tasks["serve"].task_role_key == "serve_task" &&
      output.host_run_tasks["silver"].dry_run_variable == "LAYER_DRY_RUN" && output.host_run_tasks["bronze"].dry_run_variable == "BRONZE_DRY_RUN" &&
      output.host_run_tasks["train"].dry_run_variable == null
    )
    error_message = "Each entry names the role key of its family and the dry run override where the family has one."
  }

  assert {
    condition = (
      output.host_dispatch_contract.capacity_provider == "example_project_host_dev" && output.host_dispatch_contract.network_mode == "bridge" &&
      output.host_dispatch_contract.jobs_at_a_time == 1 && output.host_dispatch_contract.cpu_units_cap == 3072 && output.host_dispatch_contract.memory_mib_cap == 23552 &&
      output.host_dispatch_contract.propagate_tags == "TASK_DEFINITION" && toset(output.host_dispatch_contract.families) == toset(keys(aws_ecs_task_definition.host)) &&
      toset(keys(output.host_dispatch_contract.writers)) == toset(["silver", "gold", "features", "bronze", "bronze_stage", "features_m1", "features_m2", "features_m3", "features_m4", "features_m5", "features_m6"]) &&
      output.host_dispatch_contract.writers["features_m4"].parameter_names == output.host_dispatch_contract.writers["features"].parameter_names &&
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
    error_message = "The host definitions pass the secret-flow and pin mutation checks."
  }

  assert {
    condition = toset(output.host_secret_flow.containers_checked) == toset(concat([
      "host_silver/pin", "host_silver/build", "host_silver/verify", "host_silver/publish", "host_gold/pin", "host_gold/publish", "host_gold/maintain", "host_features/pin",
      "host_features/publish", "host_features/feast", "host_bronze/load", "host_train/extract", "host_train/train", "host_drift/baseline", "host_drift/current",
      "host_promote/promote", "host_serve/serve", "host_metadata/ingest"
    ], flatten([for set in ["m1", "m2", "m3", "m4", "m5", "m6"] : ["host_features_${set}/pin", "host_features_${set}/publish", "host_features_${set}/feast"]])))
    error_message = "The containers that receive a secret are the ones that receive a secret, and the flow check follows each of them."
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
      toset([for item in jsondecode(output.host_container_definitions["metadata"])[1].secrets : item.name]) == toset(["OM_INGESTION_TOKEN"]) &&
      one([for item in jsondecode(output.host_container_definitions["metadata"])[1].secrets : item.valueFrom if item.name == "OM_INGESTION_TOKEN"]) ==
      "arn:aws:ssm:us-west-2:111111111111:parameter/example_project/openmetadata_bot_ingestion"
    )
    error_message = "The container is told the catalog API address and receives the ingestion bot token from its Parameter Store parameter."
  }

  assert {
    condition = (
      jsondecode(output.host_container_definitions["metadata"])[1].entryPoint == ["sh", "-c"] &&
      startswith(jsondecode(output.host_container_definitions["metadata"])[1].command[0], "(python -m scripts.infrastructure.openmetadata_host_ingest --config-dir /opt/hai/repo/config/metadata/ingestion --work-dir /workspace glue dbt raw_files)") &&
      jsondecode(output.host_container_definitions["metadata"])[1].workingDirectory == "/opt/hai/repo"
    )
    error_message = "The container runs the host ingestion script from the mounted checkout for the Glue mirror and dbt sources."
  }

  assert {
    condition = (
      toset([for mount in jsondecode(output.host_container_definitions["metadata"])[1].mountPoints : "${mount.sourceVolume}:${mount.containerPath}:${mount.readOnly}"]) ==
      toset(["workspace:/workspace:false", "tmp:/tmp:false", "repo:/opt/hai/repo:true"]) &&
      !contains(keys(jsondecode(output.host_container_definitions["metadata"])[1]), "memory") && jsondecode(output.host_container_definitions["metadata"])[1].readonlyRootFilesystem &&
      jsondecode(output.host_container_definitions["metadata"])[1].user == "10001:0"
    )
    error_message = "The checkout is mounted read-only, the root filesystem stays read-only and the ingest container runs as user 10001 in group 0, the group that reads the ingestion image's Python environment (boto3 and the metadata command live there)."
  }

  # The ingestion image keeps Python, boto3 and the metadata command in a virtual environment under the airflow user's home folder, which is mode 770 owned by
  # airflow:root. A user outside group 0 cannot enter it and python resolves to the bare system install with no boto3. Group 0 reads the environment and holds
  # no root power: the user is not root, every capability stays dropped and the root filesystem stays read-only.
  assert {
    condition = (
      !jsondecode(output.host_container_definitions["metadata"])[1].privileged &&
      !startswith(jsondecode(output.host_container_definitions["metadata"])[1].user, "0:") &&
      jsondecode(output.host_container_definitions["metadata"])[1].linuxParameters.capabilities.drop == ["ALL"] &&
      jsondecode(output.host_container_definitions["metadata"])[1].readonlyRootFilesystem
    )
    error_message = "Group 0 gives no capability: the ingest container is not privileged or root, drops all capabilities and keeps a read-only root filesystem."
  }

  assert {
    condition = (
      aws_ecs_task_definition.host["metadata"].memory == "2048" && aws_ecs_task_definition.host["metadata"].cpu == "1024"
    )
    error_message = "The metadata job keeps its own size, 1024 CPU units and 2048 MiB (its row in config/orchestration/host_jobs.json), so it shares the host with other jobs."
  }
}

run "the_metadata_job_needs_only_the_ingestion_image_and_the_catalog_roles" {
  command = plan

  variables {
    host_jobs_enabled   = true
    host_jobs           = ["metadata"]
    openmetadata_images = { ingestion = "111111111111.dkr.ecr.us-west-2.amazonaws.com/example_project_openmetadata_ingestion_dev@sha256:2222222222222222222222222222222222222222222222222222222222222222" }
  }

  assert {
    condition     = toset(keys(aws_ecs_task_definition.host)) == toset(["metadata"])
    error_message = "The host metadata job does not depend on the server, search and database images."
  }
}

run "the_metadata_job_needs_the_host_address" {
  command = plan

  variables {
    host_jobs_enabled = true
    host_jobs         = ["metadata"]
    host_private_ip   = null
  }

  expect_failures = [var.host_private_ip]
}

run "the_host_address_may_not_be_loopback" {
  command = plan

  variables {
    host_jobs_enabled = true
    host_jobs         = ["metadata"]
    host_private_ip   = "127.0.0.1"
  }

  expect_failures = [var.host_private_ip]
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
    host_jobs_enabled   = true
    host_jobs           = ["metadata"]
    openmetadata_images = {}
  }

  expect_failures = [var.host_jobs]
}

run "other_families_need_no_host_address" {
  command = plan

  variables {
    host_jobs_enabled = true
    host_jobs         = ["silver"]
    host_private_ip   = null
    host_repo_path    = null
  }

  assert {
    condition     = toset(keys(aws_ecs_task_definition.host)) == toset(["silver"])
    error_message = "The catalog address and the checkout path matter only to the metadata job."
  }
}

run "the_host_address_refuses_public" {
  command = plan

  variables {
    host_jobs_enabled = true
    host_jobs         = ["metadata"]
    host_private_ip   = "203.0.113.20"
  }

  expect_failures = [var.host_private_ip]
}

run "the_host_address_refuses_link_local" {
  command = plan

  variables {
    host_jobs_enabled = true
    host_jobs         = ["metadata"]
    host_private_ip   = "169.254.169.254"
  }

  expect_failures = [var.host_private_ip]
}

run "the_host_address_refuses_octet_over_255" {
  command = plan

  variables {
    host_jobs_enabled = true
    host_jobs         = ["metadata"]
    host_private_ip   = "10.0.1.256"
  }

  expect_failures = [var.host_private_ip]
}

run "the_host_address_refuses_first_octet_over_255" {
  command = plan

  variables {
    host_jobs_enabled = true
    host_jobs         = ["metadata"]
    host_private_ip   = "999.0.1.20"
  }

  expect_failures = [var.host_private_ip]
}

run "the_host_address_refuses_leading_zero" {
  command = plan

  variables {
    host_jobs_enabled = true
    host_jobs         = ["metadata"]
    host_private_ip   = "10.0.01.20"
  }

  expect_failures = [var.host_private_ip]
}

run "the_host_address_refuses_a_name" {
  command = plan

  variables {
    host_jobs_enabled = true
    host_jobs         = ["metadata"]
    host_private_ip   = "catalog.example.invalid"
  }

  expect_failures = [var.host_private_ip]
}

run "the_host_address_refuses_a_port" {
  command = plan

  variables {
    host_jobs_enabled = true
    host_jobs         = ["metadata"]
    host_private_ip   = "10.0.1.20:8585"
  }

  expect_failures = [var.host_private_ip]
}

run "the_host_address_refuses_a_url" {
  command = plan

  variables {
    host_jobs_enabled = true
    host_jobs         = ["metadata"]
    host_private_ip   = "http://10.0.1.20"
  }

  expect_failures = [var.host_private_ip]
}

run "the_host_address_refuses_below_172_16" {
  command = plan

  variables {
    host_jobs_enabled = true
    host_jobs         = ["metadata"]
    host_private_ip   = "172.15.255.255"
  }

  expect_failures = [var.host_private_ip]
}

run "the_host_address_refuses_above_172_31" {
  command = plan

  variables {
    host_jobs_enabled = true
    host_jobs         = ["metadata"]
    host_private_ip   = "172.32.0.1"
  }

  expect_failures = [var.host_private_ip]
}

run "the_host_address_refuses_above_10" {
  command = plan

  variables {
    host_jobs_enabled = true
    host_jobs         = ["metadata"]
    host_private_ip   = "11.0.0.1"
  }

  expect_failures = [var.host_private_ip]
}

run "the_host_address_refuses_above_192_168" {
  command = plan

  variables {
    host_jobs_enabled = true
    host_jobs         = ["metadata"]
    host_private_ip   = "192.169.0.1"
  }

  expect_failures = [var.host_private_ip]
}

run "the_host_address_accepts_ten" {
  command = plan

  variables {
    host_jobs_enabled = true
    host_jobs         = ["metadata"]
    host_private_ip   = "10.0.1.20"
  }

  assert {
    condition     = one([for item in jsondecode(output.host_container_definitions["metadata"])[1].environment : item.value if item.name == "OM_API"]) == "http://${var.host_private_ip}:8585/api"
    error_message = "A private host address reaches the container on port 8585 with the /api path."
  }
}

run "the_host_address_accepts_ten_edges" {
  command = plan

  variables {
    host_jobs_enabled = true
    host_jobs         = ["metadata"]
    host_private_ip   = "10.255.255.255"
  }

  assert {
    condition     = one([for item in jsondecode(output.host_container_definitions["metadata"])[1].environment : item.value if item.name == "OM_API"]) == "http://${var.host_private_ip}:8585/api"
    error_message = "A private host address reaches the container on port 8585 with the /api path."
  }
}

run "the_host_address_accepts_low_172" {
  command = plan

  variables {
    host_jobs_enabled = true
    host_jobs         = ["metadata"]
    host_private_ip   = "172.16.0.0"
  }

  assert {
    condition     = one([for item in jsondecode(output.host_container_definitions["metadata"])[1].environment : item.value if item.name == "OM_API"]) == "http://${var.host_private_ip}:8585/api"
    error_message = "A private host address reaches the container on port 8585 with the /api path."
  }
}

run "the_host_address_accepts_high_172" {
  command = plan

  variables {
    host_jobs_enabled = true
    host_jobs         = ["metadata"]
    host_private_ip   = "172.31.255.255"
  }

  assert {
    condition     = one([for item in jsondecode(output.host_container_definitions["metadata"])[1].environment : item.value if item.name == "OM_API"]) == "http://${var.host_private_ip}:8585/api"
    error_message = "A private host address reaches the container on port 8585 with the /api path."
  }
}

run "the_host_address_accepts_private_192" {
  command = plan

  variables {
    host_jobs_enabled = true
    host_jobs         = ["metadata"]
    host_private_ip   = "192.168.0.1"
  }

  assert {
    condition     = one([for item in jsondecode(output.host_container_definitions["metadata"])[1].environment : item.value if item.name == "OM_API"]) == "http://${var.host_private_ip}:8585/api"
    error_message = "A private host address reaches the container on port 8585 with the /api path."
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

run "model_sets_reserve_their_own_size_and_every_other_family_the_job_room" {
  command = plan

  variables {
    host_jobs_enabled = true
  }

  assert {
    condition = alltrue([for set in ["m1", "m2", "m3", "m4", "m5", "m6"] : (
      aws_ecs_task_definition.host["features_${set}"].cpu == tostring(jsondecode(file("${path.module}/../../config/orchestration/host_jobs.json")).jobs["features_${set}"].cpu_units) &&
      aws_ecs_task_definition.host["features_${set}"].memory == tostring(jsondecode(file("${path.module}/../../config/orchestration/host_jobs.json")).jobs["features_${set}"].memory_mib)
    )])
    error_message = "Each model set reserves exactly the cpu_units and memory_mib of its row in config/orchestration/host_jobs.json, the file the launcher's fit check reads."
  }

  assert {
    condition = alltrue([for set, size in {
      m1 = [512, 4096], m3 = [512, 4096], m5 = [512, 4096], m6 = [512, 4096], m2 = [1024, 13312], m4 = [1024, 13312]
      } : (
      aws_ecs_task_definition.host["features_${set}"].cpu == tostring(size[0]) && aws_ecs_task_definition.host["features_${set}"].memory == tostring(size[1])
    )])
    error_message = "A model set reserves the size its measured peak asks for: 512 units and 4096 MiB (m1, m3, m5, m6) or 1024 units and 13312 MiB (m2, m4), each peak at most 70% of the reservation."
  }

  assert {
    condition = alltrue([for family in ["silver", "gold", "features", "bronze", "train"] : (
      aws_ecs_task_definition.host[family].cpu == "3072" && aws_ecs_task_definition.host[family].memory == tostring(var.host_job_memory_mib)
    )])
    error_message = "Silver, gold, the features base build, bronze and train still reserve the whole job room."
  }

  assert {
    condition     = sum([for set in ["m1", "m2", "m3", "m5"] : tonumber(aws_ecs_task_definition.host["features_${set}"].cpu)]) <= 3072 && sum([for set in ["m1", "m2", "m3", "m5"] : tonumber(aws_ecs_task_definition.host["features_${set}"].memory)]) <= 26816
    error_message = "A first wave of one large set and three small ones must fit the 3072 CPU units and the 26816 MiB job cap of the services default."
  }

  assert {
    condition = alltrue([for set in ["m1", "m2", "m3", "m4", "m5", "m6"] : (
      jsondecode(output.host_container_definitions["features_${set}"])[0].dockerLabels == { "hai.host_capacity" = "job_memory_mib=${aws_ecs_task_definition.host["features_${set}"].memory} usable_memory_mib=30720 system_reserve_mib=2048 service_reservations_mib=5120" } &&
      [for container in jsondecode(output.host_container_definitions["features_${set}"]) : container.name] == [for container in jsondecode(output.host_container_definitions["features"]) : container.name]
    )])
    error_message = "A set's init container carries the capacity binding with the set's own job memory, and a set runs the containers of the features job."
  }

  assert {
    condition = (
      length(distinct([for family, path in { for family in ["features", "features_m1", "features_m2", "features_m3", "features_m4", "features_m5", "features_m6"] : family => output.host_run_tasks[family].scratch["workspace"] } : path])) == 7
      && output.host_run_tasks["features_m3"].scratch["workspace"] == "/data/job_scratch/features_m3/work"
      && output.host_run_tasks["features_m3"].task_role_key == output.host_run_tasks["features"].task_role_key
      && output.host_run_tasks["features_m3"].container_environment == output.host_run_tasks["features"].container_environment
    )
    error_message = "Each model set has its own scratch folder (so six wipe nothing of each other) and the role and container variables of the features job."
  }

  assert {
    condition     = output.host_dispatch_contract.family_cpu_units["features_m2"] == 1024 && output.host_dispatch_contract.family_cpu_units["features_m1"] == 512 && output.host_dispatch_contract.family_cpu_units["silver"] == 3072 && output.host_dispatch_contract.family_memory_mib["features_m2"] == 13312
    error_message = "The dispatch contract names each family's own reservation."
  }
}

run "the_smaller_jobs_keep_their_own_size_and_binding" {
  command = plan

  variables {
    host_jobs_enabled = true
  }

  assert {
    condition = alltrue([for family in ["drift", "promote", "serve", "metadata"] : (
      aws_ecs_task_definition.host[family].cpu == tostring(jsondecode(file("${path.module}/../../config/orchestration/host_jobs.json")).jobs[family].cpu_units) &&
      aws_ecs_task_definition.host[family].memory == tostring(jsondecode(file("${path.module}/../../config/orchestration/host_jobs.json")).jobs[family].memory_mib) &&
      jsondecode(output.host_container_definitions[family])[0].dockerLabels == { "hai.host_capacity" = "job_memory_mib=${aws_ecs_task_definition.host[family].memory} usable_memory_mib=30720 system_reserve_mib=2048 service_reservations_mib=5120" }
    )])
    error_message = "Drift, promote, serve and metadata reserve their own row of config/orchestration/host_jobs.json and carry the binding with that job memory."
  }

  assert {
    condition     = tonumber(aws_ecs_task_definition.host["drift"].cpu) + tonumber(aws_ecs_task_definition.host["metadata"].cpu) <= 3072 && tonumber(aws_ecs_task_definition.host["drift"].memory) + tonumber(aws_ecs_task_definition.host["metadata"].memory) + tonumber(aws_ecs_task_definition.host["features_m2"].memory) <= 26816
    error_message = "Drift and metadata fit the 3072 CPU units together, and with a large model set they fit the 26816 MiB job cap, so different tests can share the host."
  }
}

run "a_model_set_may_not_reserve_more_than_the_job_room" {
  command = plan

  variables {
    host_jobs_enabled     = true
    host_job_memory_mib   = 8192
    host_capacity_binding = "job_memory_mib=8192 usable_memory_mib=30720 system_reserve_mib=2048 service_reservations_mib=5120"
  }

  expect_failures = [aws_ecs_task_definition.host["features_m2"], aws_ecs_task_definition.host["features_m4"]]
}

run "a_model_set_needs_the_features_family" {
  command = plan

  variables {
    host_jobs_enabled = true
    host_jobs         = ["features_m1"]
  }

  expect_failures = [var.host_jobs]
}

run "every_host_container_pushes_its_metrics_to_the_pushgateway_with_its_family_name" {
  command = plan

  variables {
    host_jobs_enabled = true
  }

  assert {
    condition = alltrue(flatten([
      for family, definition in aws_ecs_task_definition.host : [
        for container in jsondecode(definition.container_definitions) : (
          contains(container.environment, { name = "PUSHGATEWAY_URL", value = "http://10.0.1.20:9091" })
          && contains(container.environment, { name = "HAI_JOB", value = startswith(family, "features_m") ? "features" : family })
        )
      ]
    ]))
    error_message = "Every container of every host family gets the Pushgateway address on the host private IP (port 9091) and its job family name, which a model set family takes from the features family. The job code pushes its own metrics there."
  }

  # The six model sets share the features family, so each set pushes under its own grouping key HAI_JOB_SET (m1 to m6): a later set no longer replaces the
  # group of an earlier one in the Pushgateway, and a later success cannot hide an earlier set's failure (review round 2, finding 6). No other family has it.
  assert {
    condition = alltrue(flatten([
      for family, definition in aws_ecs_task_definition.host : [
        for container in jsondecode(definition.container_definitions) : (
          startswith(family, "features_m")
          ? [for entry in container.environment : entry.value if entry.name == "HAI_JOB_SET"] == [trimprefix(family, "features_")]
          : length([for entry in container.environment : entry if entry.name == "HAI_JOB_SET"]) == 0
        )
      ]
    ]))
    error_message = "A model set family names its set in HAI_JOB_SET in every container, and no other family sets it."
  }

  assert {
    condition = alltrue(flatten([
      for family, definition in aws_ecs_task_definition.host : [
        for container in jsondecode(definition.container_definitions) : length([for entry in container.environment : entry if entry.name == "HAI_JOB"]) == 1
      ]
    ]))
    error_message = "A container names its job family once."
  }

  assert {
    condition     = alltrue([for definition in aws_ecs_task_definition.host : length(jsondecode(definition.container_definitions)) <= 10])
    error_message = "The environment adds no container: no family passes the ECS limit of 10 containers per task definition."
  }
}

run "without_the_host_address_no_container_names_a_pushgateway" {
  command = plan

  variables {
    host_jobs_enabled = true
    host_jobs         = ["silver"]
    host_private_ip   = null
  }

  assert {
    condition = alltrue(flatten([
      for definition in aws_ecs_task_definition.host : [
        for container in jsondecode(definition.container_definitions) : length([for entry in container.environment : entry if entry.name == "PUSHGATEWAY_URL" || entry.name == "HAI_JOB"]) == 0
      ]
    ]))
    error_message = "With no host address the jobs push nothing and carry neither variable, so the definitions stay as they were."
  }
}

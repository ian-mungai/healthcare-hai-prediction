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
  layer_feast_redis_host                = "10.0.1.20"
  openmetadata_cpu_architecture         = "ARM64"
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
      length(output.host_task_definition_arns) == 0 && length(output.host_container_definitions) == 0 && length(output.host_run_tasks) == 0 &&
      length(output.host_dispatch_contract.families) == 0 && length(output.host_dispatch_contract.writers) == 0 && length(output.host_secret_flow.containers_checked) == 0
    )
    error_message = "Without host_jobs_enabled every host output is empty."
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
      silver   = [3072, 16384], gold = [3072, 16384], features = [3072, 16384], bronze = [3072, 16384], train = [3072, 16384],
      metadata = [3072, 16384], drift = [2048, 8192], promote = [1024, 4096], serve = [1024, 4096]
      } : (
      aws_ecs_task_definition.host[family].cpu == tostring(size[0]) && aws_ecs_task_definition.host[family].memory == tostring(size[1])
    )])
    error_message = "A job reserves at most 3072 CPU units and 16 GiB at task level; the smaller families keep their Fargate size."
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
      promote  = jsondecode(output.promote_container_definitions), serve = jsondecode(output.serve_container_definitions),
      metadata = jsondecode(output.openmetadata_container_definitions)
      } : (
      [for container in jsondecode(output.host_container_definitions[family]) : { for key, value in container : key => value if key != "environment" }] ==
      [for container in fargate : { for key, value in container : key => value if key != "environment" }] &&
      [for container in jsondecode(output.host_container_definitions[family]) : [for item in container.environment : item.name]] ==
      [for container in fargate : [for item in container.environment : item.name]] &&
      [for container in jsondecode(output.host_container_definitions[family]) : [for item in container.environment : item.value if !contains(["JOB_THREADS", "OMP_NUM_THREADS", "OPENBLAS_NUM_THREADS", "MKL_NUM_THREADS"], item.name)]] ==
      [for container in fargate : [for item in container.environment : item.value if !contains(["JOB_THREADS", "OMP_NUM_THREADS", "OPENBLAS_NUM_THREADS", "MKL_NUM_THREADS"], item.name)]]
    )])
    error_message = "A host container equals its Fargate twin in command, image, user, secrets, dependencies, mounts and every variable except the thread counts."
  }

  assert {
    condition = alltrue([for family, fargate in {
      silver   = jsondecode(output.layer_container_definitions["silver"]), gold = jsondecode(output.layer_container_definitions["gold"]),
      features = jsondecode(output.layer_container_definitions["features"]), bronze = jsondecode(output.bronze_container_definitions),
      train    = jsondecode(output.train_container_definitions), drift = jsondecode(output.drift_container_definitions),
      promote  = jsondecode(output.promote_container_definitions), serve = jsondecode(output.serve_container_definitions),
      metadata = jsondecode(output.openmetadata_container_definitions)
      } : (
      [for container in jsondecode(output.host_container_definitions[family]) : [for item in container.environment : item.value if contains(["JOB_THREADS", "OMP_NUM_THREADS", "OPENBLAS_NUM_THREADS", "MKL_NUM_THREADS"], item.name)]] ==
      [for container in fargate : [for item in container.environment : item.value == "4" ? "3" : item.value if contains(["JOB_THREADS", "OMP_NUM_THREADS", "OPENBLAS_NUM_THREADS", "MKL_NUM_THREADS"], item.name)]]
    )])
    error_message = "A job that ran four threads on Fargate runs three on the host; a job with fewer keeps its count."
  }

  assert {
    condition = (
      one([for item in jsondecode(output.host_container_definitions["silver"])[2].environment : item.value if item.name == "JOB_THREADS"]) == "3" &&
      one([for item in jsondecode(output.host_container_definitions["train"])[4].environment : item.value if item.name == "OMP_NUM_THREADS"]) == "3" &&
      one([for item in jsondecode(output.host_container_definitions["drift"])[2].environment : item.value if item.name == "JOB_THREADS"]) == "2"
    )
    error_message = "Silver and training use three threads and drift keeps two."
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
        "workspace=/data/job_scratch/metadata/work", "tmp=/data/job_scratch/metadata/tmp", "omrun=/data/job_scratch/metadata/omrun",
        "pgdata=/data/job_scratch/metadata/pgdata", "osdata=/data/job_scratch/metadata/osdata"
      ])
    )
    error_message = "Workspace and tmp are host folders under the scratch root, one folder per family, and the catalog task keeps its three more volumes there."
  }

  assert {
    condition = alltrue([for family, task in aws_ecs_task_definition.host : alltrue([for volume in task.volume : (
      startswith(volume.host_path, "/data/job_scratch/${family}/") && !strcontains(volume.host_path, "..") && !volume.configure_at_launch
    )])])
    error_message = "No volume leaves the family's own scratch folder."
  }

  assert {
    condition = alltrue(flatten([for family, json in output.host_container_definitions : [for container in jsondecode(json) : alltrue([
      for mount in container.mountPoints : contains([for volume in aws_ecs_task_definition.host[family].volume : volume.name], mount.sourceVolume)
    ])]]))
    error_message = "Every mount point names a volume of its own definition."
  }
}

run "scratch_root_can_move" {
  command = plan

  variables {
    host_jobs_enabled = true
    host_jobs         = ["gold"]
    host_scratch_root = "/mnt/job_scratch"
  }

  assert {
    condition     = toset([for volume in aws_ecs_task_definition.host["gold"].volume : volume.host_path]) == toset(["/mnt/job_scratch/gold/work", "/mnt/job_scratch/gold/tmp"])
    error_message = "The scratch root is one input, so the separate scratch volume can take another mount point."
  }
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

run "resources_follow_the_fargate_shape_up_to_the_host_cap" {
  command = plan

  variables {
    host_jobs_enabled = true
    layer_resources   = { cpu = 4096, memory = 30720, ephemeral_storage_gib = 100 }
    drift_resources   = { cpu = 1024, memory = 4096, ephemeral_storage_gib = 30 }
  }

  assert {
    condition = (
      aws_ecs_task_definition.host["silver"].cpu == "3072" && aws_ecs_task_definition.host["silver"].memory == "16384" &&
      aws_ecs_task_definition.host["drift"].cpu == "1024" && aws_ecs_task_definition.host["drift"].memory == "4096"
    )
    error_message = "A bigger Fargate shape is capped at 3072 CPU units and 16 GiB; a smaller one is kept."
  }
}

run "thread_count_can_be_lowered" {
  command = plan

  variables {
    host_jobs_enabled = true
    host_job_threads  = 2
  }

  assert {
    condition     = one([for item in jsondecode(output.host_container_definitions["silver"])[2].environment : item.value if item.name == "JOB_THREADS"]) == "2"
    error_message = "The thread count of the four-thread jobs is one input."
  }
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
      output.host_run_tasks["metadata"].container_environment == output.openmetadata_run_task.container_environment
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
      output.host_dispatch_contract.jobs_at_a_time == 1 && output.host_dispatch_contract.cpu_units_cap == 3072 && output.host_dispatch_contract.memory_mib_cap == 16384 &&
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
      "host_silver/pin", "host_silver/build", "host_silver/verify", "host_silver/publish", "host_gold/pin", "host_gold/publish", "host_features/pin",
      "host_features/publish", "host_features/feast", "host_bronze/load", "host_train/extract", "host_train/train", "host_drift/baseline", "host_drift/current",
      "host_promote/promote", "host_serve/serve", "host_metadata/bootstrap", "host_metadata/ingest_polaris"
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

run "a_job_may_not_ask_for_four_threads" {
  command = plan

  variables {
    host_jobs_enabled = true
    host_job_threads  = 4
  }

  expect_failures = [var.host_job_threads]
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
    condition     = length(aws_ecs_task_definition.host) == 0 && length(output.host_run_tasks) == 0
    error_message = "A family list alone registers nothing: only host_jobs_enabled does."
  }
}

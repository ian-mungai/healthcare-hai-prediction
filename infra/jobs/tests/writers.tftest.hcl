mock_provider "aws" {}

variables {
  aws_profile         = "example_project_dev"
  aws_region          = "us-west-2"
  expected_account_id = "111111111111"
  project_name        = "example_project"
  environment         = "dev"
  data_bucket_name    = "example-project-data-dev"
  job_role_arns = {
    execution           = "arn:aws:iam::111111111111:role/example_project_job_execution_dev"
    offline_task        = "arn:aws:iam::111111111111:role/example_project_job_offline_task_dev"
    seal_task           = "arn:aws:iam::111111111111:role/example_project_job_seal_task_dev"
    read_task           = "arn:aws:iam::111111111111:role/example_project_job_read_task_dev"
    write_e2e_task      = "arn:aws:iam::111111111111:role/example_project_job_write_e2e_task_dev"
    write_silver_task   = "arn:aws:iam::111111111111:role/example_project_job_write_silver_task_dev"
    write_gold_task     = "arn:aws:iam::111111111111:role/example_project_job_write_gold_task_dev"
    write_features_task = "arn:aws:iam::111111111111:role/example_project_job_write_features_task_dev"
    write_bronze_task   = "arn:aws:iam::111111111111:role/example_project_job_write_bronze_task_dev"
  }
  writer_parameter_names = {
    write_e2e_task      = ["/example_project/aws_polaris_e2e_client_id", "/example_project/aws_polaris_e2e_client_secret"]
    write_silver_task   = ["/example_project/aws_polaris_silver_client_id", "/example_project/aws_polaris_silver_client_secret"]
    write_gold_task     = ["/example_project/aws_polaris_gold_client_id", "/example_project/aws_polaris_gold_client_secret"]
    write_features_task = ["/example_project/aws_polaris_features_client_id", "/example_project/aws_polaris_features_client_secret"]
    write_bronze_task   = ["/example_project/aws_polaris_bronze_client_id", "/example_project/aws_polaris_bronze_client_secret"]
  }
  images = {
    spark     = "111111111111.dkr.ecr.us-west-2.amazonaws.com/example_project_spark_dev@sha256:cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc"
    analytics = "111111111111.dkr.ecr.us-west-2.amazonaws.com/example_project_analytics_dev@sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
    quality   = "111111111111.dkr.ecr.us-west-2.amazonaws.com/example_project_quality_dev@sha256:bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"
  }
  task_resources = {
    spark_write_e2e      = { cpu = 2048, memory = 4096, ephemeral_storage_gib = 21 }
    spark_write_silver   = { cpu = 2048, memory = 4096, ephemeral_storage_gib = 21 }
    spark_write_gold     = { cpu = 2048, memory = 4096, ephemeral_storage_gib = 21 }
    spark_write_features = { cpu = 2048, memory = 4096, ephemeral_storage_gib = 21 }
    spark_write_bronze   = { cpu = 2048, memory = 4096, ephemeral_storage_gib = 21 }
    analytics_offline    = { cpu = 1024, memory = 2048, ephemeral_storage_gib = 21 }
    quality_offline      = { cpu = 1024, memory = 2048, ephemeral_storage_gib = 21 }
    seal                 = { cpu = 1024, memory = 2048, ephemeral_storage_gib = 21 }
    analytics_read       = { cpu = 1024, memory = 2048, ephemeral_storage_gib = 21 }
    spark_read           = { cpu = 2048, memory = 4096, ephemeral_storage_gib = 21 }
  }
  aws_polaris_reader_client_id_name     = "/example_project/aws_polaris_reader_client_id"
  aws_polaris_reader_client_secret_name = "/example_project/aws_polaris_reader_client_secret"
  log_group_name                        = "example_project_jobs_dev"
  cloud_limits                          = { max_tasks = 3, max_vcpu = 16, max_memory_gib = 120 }
}

run "default_plan_has_no_writers" {
  command = plan
  variables {
    images                 = {}
    job_role_arns          = {}
    writer_parameter_names = {}
    task_resources         = {}
    log_group_name         = null
    cloud_limits           = null
  }
  assert {
    condition     = output.container_definitions == {} && output.task_definition_settings == {} && output.execution_digests == {} && output.writer_dispatch_contracts == {}
    error_message = "Disabled defaults must register no writer and export no writer dispatch contract."
  }
}

run "all_writers_have_separate_bindings" {
  command = plan
  variables {
    jobs_enabled        = true
    enabled_definitions = ["spark_write_e2e", "spark_write_silver", "spark_write_gold", "spark_write_features", "spark_write_bronze"]
  }
  assert {
    condition     = toset(keys(output.container_definitions)) == toset(["spark_write_e2e", "spark_write_silver", "spark_write_gold", "spark_write_features", "spark_write_bronze"])
    error_message = "The enabled writer inventory must contain exactly the test and four production definitions."
  }
  assert {
    condition = alltrue([for layer in ["e2e", "silver", "gold", "features", "bronze"] : (
      output.task_definition_settings["spark_write_${layer}"].task_role_arn == "arn:aws:iam::111111111111:role/example_project_job_write_${layer}_task_dev" &&
      jsondecode(output.container_definitions["spark_write_${layer}"])[0].secrets == [
        { name = "POLARIS_CLIENT_ID", valueFrom = "arn:aws:ssm:us-west-2:111111111111:parameter/example_project/aws_polaris_${layer}_client_id" },
        { name = "POLARIS_CLIENT_SECRET", valueFrom = "arn:aws:ssm:us-west-2:111111111111:parameter/example_project/aws_polaris_${layer}_client_secret" }
      ]
    )])
    error_message = "Every writer must bind exactly its own task role and pair, including the separate test writer."
  }
  assert {
    condition = alltrue([for name, settings in output.task_definition_settings : (
      settings.family == "example_project_dev_job_${name}" &&
      settings.execution_role_arn == "arn:aws:iam::111111111111:role/example_project_job_execution_dev" &&
      toset(settings.requires_compatibilities) == toset(["FARGATE"]) && settings.network_mode == "awsvpc" &&
      length(settings.runtime_platform) == 1 && settings.runtime_platform[0].cpu_architecture == "ARM64" && settings.runtime_platform[0].operating_system_family == "LINUX" &&
      settings.skip_destroy && !settings.enable_fault_injection &&
      settings.cpu == "2048" && settings.memory == "4096" && settings.ephemeral_storage[0].size_in_gib == 21 &&
      settings.tags == tomap({ Project = "example_project", Environment = "dev", ManagedBy = "Terraform" }) &&
      toset([for volume in settings.volume : volume.name]) == toset(["workspace", "tmp"]) &&
      alltrue([for volume in settings.volume : volume.host_path == null && length(volume.efs_volume_configuration) == 0 && length(volume.docker_volume_configuration) == 0 && !volume.configure_at_launch])
    )])
    error_message = "Writers must retain the private ARM64 Fargate settings, execution role, reviewed resources and task scratch."
  }
  assert {
    condition = alltrue([for name, content in output.container_definitions : (
      length(jsondecode(content)) == 1 && jsondecode(content)[0].name == name &&
      jsondecode(content)[0].image == "111111111111.dkr.ecr.us-west-2.amazonaws.com/example_project_spark_dev@sha256:cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc" &&
      jsondecode(content)[0].entryPoint == ["/opt/hai/deadline_run", "python", "-I", "/opt/hai/bootstrap/run.py"] &&
      jsondecode(content)[0].command == [] && jsondecode(content)[0].essential &&
      jsondecode(content)[0].readonlyRootFilesystem && !jsondecode(content)[0].privileged &&
      jsondecode(content)[0].user == "10001:10001" && jsondecode(content)[0].stopTimeout == 90 &&
      jsondecode(content)[0].linuxParameters == { capabilities = { add = [], drop = ["ALL"] }, initProcessEnabled = true } &&
      jsondecode(content)[0].mountPoints == [
        { sourceVolume = "workspace", containerPath = "/workspace", readOnly = false },
        { sourceVolume = "tmp", containerPath = "/tmp", readOnly = false }
      ] &&
      jsondecode(content)[0].logConfiguration.options["awslogs-group"] == "example_project_jobs_dev"
    )])
    error_message = "Writer containers must use the measured Spark image and the accepted unprivileged bootstrap contract."
  }
  assert {
    condition = jsonencode(output.writer_dispatch_contracts) == jsonencode({ for layer in ["e2e", "silver", "gold", "features", "bronze"] : "spark_write_${layer}" => {
      job_class          = "lakehouse_write"
      task_role_key      = "write_${layer}_task"
      parameter_names    = ["/example_project/aws_polaris_${layer}_client_id", "/example_project/aws_polaris_${layer}_client_secret"]
      capacity_providers = ["FARGATE"]
      network            = "lakehouse"
      assign_public_ip   = "DISABLED"
    } })
    error_message = "Every writer must use only on-demand capacity, its own pair and role, the lakehouse network and no public IP."
  }
  assert {
    condition = (
      output.dispatch_contract.class_networks == { offline = "offline", seal = "offline", lakehouse_read = "lakehouse", lakehouse_write = "lakehouse" } &&
      output.dispatch_contract.definition_classes == { for layer in ["e2e", "silver", "gold", "features", "bronze"] : "spark_write_${layer}" => "lakehouse_write" }
    )
    error_message = "The general dispatch contract must retain the writer class and lakehouse network for every selected writer."
  }
  assert {
    condition = (
      output.dispatch_contract.capacity_providers == ["FARGATE_SPOT", "FARGATE"] &&
      output.dispatch_contract.class_capacity_providers == {
        offline         = ["FARGATE_SPOT", "FARGATE"]
        seal            = ["FARGATE_SPOT", "FARGATE"]
        lakehouse_read  = ["FARGATE_SPOT", "FARGATE"]
        lakehouse_write = ["FARGATE"]
      } && output.dispatch_contract.assign_public_ip == "DISABLED"
    )
    error_message = "Dispatch must state explicit on-demand writer capacity and no public IP while preserving every reader strategy."
  }
  assert {
    condition     = length(output.execution_digests) == 5 && alltrue([for layer in ["e2e", "silver", "gold", "features", "bronze"] : can(regex("^[a-f0-9]{64}$", output.execution_digests["spark_write_${layer}/spark_write_${layer}"]))]) && output.secret_flow.violations == [] && output.secret_flow.alias_chains_converged && length(output.secret_flow.containers_checked) == 5 && output.secret_flow.mutation_mismatches == [] && output.secret_flow.mutations_checked > 0
    error_message = "The shared flow must inspect every writer and pin every complete execution definition."
  }
}

run "writer_pins_match_shared_flow" {
  command = plan
  module { source = "../services/tests/flow" }
  variables {
    definitions = run.all_writers_have_separate_bindings.container_definitions
    tasks       = run.all_writers_have_separate_bindings.task_definition_settings
  }
  assert {
    condition     = output.execution_digests == run.all_writers_have_separate_bindings.execution_digests
    error_message = "Every writer execution pin must come unchanged from the accepted shared flow module."
  }
}

run "e2e_writer_can_be_selected_alone" {
  command = plan
  variables {
    jobs_enabled        = true
    enabled_definitions = ["spark_write_e2e"]
  }
  assert {
    condition     = keys(output.container_definitions) == ["spark_write_e2e"] && keys(output.writer_dispatch_contracts) == ["spark_write_e2e"]
    error_message = "Selecting the e2e writer must register only that definition."
  }
}

run "silver_writer_can_be_selected_alone" {
  command = plan
  variables {
    jobs_enabled        = true
    enabled_definitions = ["spark_write_silver"]
  }
  assert {
    condition     = keys(output.container_definitions) == ["spark_write_silver"] && keys(output.writer_dispatch_contracts) == ["spark_write_silver"]
    error_message = "Selecting the silver writer must register only that definition."
  }
}

run "gold_writer_can_be_selected_alone" {
  command = plan
  variables {
    jobs_enabled        = true
    enabled_definitions = ["spark_write_gold"]
  }
  assert {
    condition     = keys(output.container_definitions) == ["spark_write_gold"] && keys(output.writer_dispatch_contracts) == ["spark_write_gold"]
    error_message = "Selecting the gold writer must register only that definition."
  }
}

run "features_writer_can_be_selected_alone" {
  command = plan
  variables {
    jobs_enabled        = true
    enabled_definitions = ["spark_write_features"]
  }
  assert {
    condition     = keys(output.container_definitions) == ["spark_write_features"] && keys(output.writer_dispatch_contracts) == ["spark_write_features"]
    error_message = "Selecting the features writer must register only that definition."
  }
}

run "bronze_writer_can_be_selected_alone" {
  command = plan
  variables {
    jobs_enabled        = true
    enabled_definitions = ["spark_write_bronze"]
  }
  assert {
    condition     = keys(output.container_definitions) == ["spark_write_bronze"] && keys(output.writer_dispatch_contracts) == ["spark_write_bronze"]
    error_message = "Selecting the bronze writer must register only that definition."
  }
}

run "e2e_writer_refuses_swapped_role" {
  command = plan
  variables {
    jobs_enabled        = true
    enabled_definitions = ["spark_write_e2e"]
    job_role_arns = {
      execution      = "arn:aws:iam::111111111111:role/example_project_job_execution_dev"
      write_e2e_task = "arn:aws:iam::111111111111:role/example_project_job_write_silver_task_dev"
    }
  }
  expect_failures = [var.job_role_arns]
}

run "e2e_writer_refuses_swapped_pair" {
  command = plan
  variables {
    jobs_enabled           = true
    enabled_definitions    = ["spark_write_e2e"]
    writer_parameter_names = { write_e2e_task = ["/example_project/aws_polaris_silver_client_id", "/example_project/aws_polaris_silver_client_secret"] }
  }
  expect_failures = [var.writer_parameter_names]
}

run "silver_writer_refuses_swapped_role" {
  command = plan
  variables {
    jobs_enabled        = true
    enabled_definitions = ["spark_write_silver"]
    job_role_arns = {
      execution         = "arn:aws:iam::111111111111:role/example_project_job_execution_dev"
      write_silver_task = "arn:aws:iam::111111111111:role/example_project_job_write_gold_task_dev"
    }
  }
  expect_failures = [var.job_role_arns]
}

run "silver_writer_refuses_swapped_pair" {
  command = plan
  variables {
    jobs_enabled           = true
    enabled_definitions    = ["spark_write_silver"]
    writer_parameter_names = { write_silver_task = ["/example_project/aws_polaris_gold_client_id", "/example_project/aws_polaris_gold_client_secret"] }
  }
  expect_failures = [var.writer_parameter_names]
}

run "gold_writer_refuses_swapped_role" {
  command = plan
  variables {
    jobs_enabled        = true
    enabled_definitions = ["spark_write_gold"]
    job_role_arns = {
      execution       = "arn:aws:iam::111111111111:role/example_project_job_execution_dev"
      write_gold_task = "arn:aws:iam::111111111111:role/example_project_job_write_features_task_dev"
    }
  }
  expect_failures = [var.job_role_arns]
}

run "gold_writer_refuses_swapped_pair" {
  command = plan
  variables {
    jobs_enabled           = true
    enabled_definitions    = ["spark_write_gold"]
    writer_parameter_names = { write_gold_task = ["/example_project/aws_polaris_features_client_id", "/example_project/aws_polaris_features_client_secret"] }
  }
  expect_failures = [var.writer_parameter_names]
}

run "features_writer_refuses_swapped_role" {
  command = plan
  variables {
    jobs_enabled        = true
    enabled_definitions = ["spark_write_features"]
    job_role_arns = {
      execution           = "arn:aws:iam::111111111111:role/example_project_job_execution_dev"
      write_features_task = "arn:aws:iam::111111111111:role/example_project_job_write_bronze_task_dev"
    }
  }
  expect_failures = [var.job_role_arns]
}

run "features_writer_refuses_swapped_pair" {
  command = plan
  variables {
    jobs_enabled           = true
    enabled_definitions    = ["spark_write_features"]
    writer_parameter_names = { write_features_task = ["/example_project/aws_polaris_bronze_client_id", "/example_project/aws_polaris_bronze_client_secret"] }
  }
  expect_failures = [var.writer_parameter_names]
}

run "bronze_writer_refuses_swapped_role" {
  command = plan
  variables {
    jobs_enabled        = true
    enabled_definitions = ["spark_write_bronze"]
    job_role_arns = {
      execution         = "arn:aws:iam::111111111111:role/example_project_job_execution_dev"
      write_bronze_task = "arn:aws:iam::111111111111:role/example_project_job_write_e2e_task_dev"
    }
  }
  expect_failures = [var.job_role_arns]
}

run "bronze_writer_refuses_swapped_pair" {
  command = plan
  variables {
    jobs_enabled           = true
    enabled_definitions    = ["spark_write_bronze"]
    writer_parameter_names = { write_bronze_task = ["/example_project/aws_polaris_e2e_client_id", "/example_project/aws_polaris_e2e_client_secret"] }
  }
  expect_failures = [var.writer_parameter_names]
}

run "writer_selection_requires_switch" {
  command = plan
  variables {
    enabled_definitions = ["spark_write_e2e"]
  }
  expect_failures = [var.enabled_definitions]
}

run "writer_requires_role" {
  command = plan
  variables {
    jobs_enabled        = true
    enabled_definitions = ["spark_write_e2e"]
    job_role_arns       = { execution = "arn:aws:iam::111111111111:role/example_project_job_execution_dev" }
  }
  expect_failures = [var.job_role_arns]
}

run "writer_requires_pair" {
  command = plan
  variables {
    jobs_enabled           = true
    enabled_definitions    = ["spark_write_e2e"]
    writer_parameter_names = {}
  }
  expect_failures = [var.writer_parameter_names]
}

run "writer_rejects_extra_parameter" {
  command = plan
  variables {
    jobs_enabled           = true
    enabled_definitions    = ["spark_write_e2e"]
    writer_parameter_names = { write_e2e_task = ["/example_project/aws_polaris_e2e_client_id", "/example_project/aws_polaris_e2e_client_secret", "/example_project/aws_polaris_reader_client_secret"] }
  }
  expect_failures = [var.writer_parameter_names]
}

run "writer_rejects_reader_pair" {
  command = plan
  variables {
    jobs_enabled           = true
    enabled_definitions    = ["spark_write_e2e"]
    writer_parameter_names = { write_e2e_task = ["/example_project/aws_polaris_reader_client_id", "/example_project/aws_polaris_reader_client_secret"] }
  }
  expect_failures = [var.writer_parameter_names]
}

run "writer_rejects_mixed_pair" {
  command = plan
  variables {
    jobs_enabled           = true
    enabled_definitions    = ["spark_write_gold"]
    writer_parameter_names = { write_gold_task = ["/example_project/aws_polaris_gold_client_id", "/example_project/aws_polaris_silver_client_secret"] }
  }
  expect_failures = [var.writer_parameter_names]
}

run "writer_requires_image" {
  command = plan
  variables {
    jobs_enabled        = true
    enabled_definitions = ["spark_write_e2e"]
    images              = {}
  }
  expect_failures = [var.images]
}

run "writer_rejects_tag_only_image" {
  command = plan
  variables {
    jobs_enabled        = true
    enabled_definitions = ["spark_write_e2e"]
    images              = { spark = "111111111111.dkr.ecr.us-west-2.amazonaws.com/example_project_spark_dev:latest" }
  }
  expect_failures = [var.images]
}

run "writer_requires_resources" {
  command = plan
  variables {
    jobs_enabled        = true
    enabled_definitions = ["spark_write_e2e"]
    task_resources      = {}
  }
  expect_failures = [var.task_resources]
}

run "writer_requires_cap" {
  command = plan
  variables {
    jobs_enabled        = true
    enabled_definitions = ["spark_write_e2e"]
    cloud_limits        = null
  }
  expect_failures = [var.cloud_limits]
}

run "readers_without_writers" {
  command = plan
  variables {
    jobs_enabled        = true
    enabled_definitions = ["analytics_offline", "quality_offline", "seal", "analytics_read", "spark_read"]
  }
  assert {
    condition     = output.writer_dispatch_contracts == {}
    error_message = "Read-only selection must create no writer contract."
  }
}

run "adding_writers_preserves_read_definitions" {
  command = plan
  variables {
    jobs_enabled        = true
    enabled_definitions = ["analytics_offline", "quality_offline", "seal", "analytics_read", "spark_read", "spark_write_e2e", "spark_write_silver", "spark_write_gold", "spark_write_features", "spark_write_bronze"]
  }
  assert {
    condition     = alltrue([for name in ["analytics_offline", "quality_offline", "seal", "analytics_read", "spark_read"] : output.container_definitions[name] == run.readers_without_writers.container_definitions[name] && output.task_definition_settings[name] == run.readers_without_writers.task_definition_settings[name] && output.execution_digests["${name}/${name}"] == run.readers_without_writers.execution_digests["${name}/${name}"]])
    error_message = "Adding writer definitions must preserve every read container, task setting and execution pin."
  }
}

mock_provider "aws" {}

variables {
  aws_profile         = "example_project_dev"
  aws_region          = "us-west-2"
  expected_account_id = "111111111111"
  project_name        = "example_project"
  environment         = "dev"
  data_bucket_name    = "example-project-data-dev"
  job_role_arns = {
    execution    = "arn:aws:iam::111111111111:role/example_project_job_execution_dev"
    offline_task = "arn:aws:iam::111111111111:role/example_project_job_offline_task_dev"
    seal_task    = "arn:aws:iam::111111111111:role/example_project_job_seal_task_dev"
    read_task    = "arn:aws:iam::111111111111:role/example_project_job_read_task_dev"
  }
  images = {
    analytics = "111111111111.dkr.ecr.us-west-2.amazonaws.com/example_project_analytics_dev@sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
    quality   = "111111111111.dkr.ecr.us-west-2.amazonaws.com/example_project_quality_dev@sha256:bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"
    spark     = "111111111111.dkr.ecr.us-west-2.amazonaws.com/example_project_spark_dev@sha256:cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc"
  }
  task_resources = {
    analytics_offline = { cpu = 1024, memory = 2048, ephemeral_storage_gib = 21 }
    quality_offline   = { cpu = 1024, memory = 2048, ephemeral_storage_gib = 21 }
    seal              = { cpu = 1024, memory = 2048, ephemeral_storage_gib = 21 }
    analytics_read    = { cpu = 1024, memory = 2048, ephemeral_storage_gib = 21 }
    spark_read        = { cpu = 2048, memory = 4096, ephemeral_storage_gib = 21 }
  }
  aws_polaris_reader_client_id_name     = "/example_project/aws_polaris_reader_client_id"
  aws_polaris_reader_client_secret_name = "/example_project/aws_polaris_reader_client_secret"
  log_group_name                        = "example_project_jobs_dev"
  cloud_limits                          = { max_tasks = 3, max_vcpu = 16, max_memory_gib = 120 }
}

run "default_plan_creates_nothing" {
  command = plan

  variables {
    images         = {}
    job_role_arns  = {}
    task_resources = {}
    log_group_name = null
    cloud_limits   = null
  }

  assert {
    condition     = length(aws_ecs_task_definition.read) == 0 && output.task_definition_arns == {} && output.execution_digests == {}
    error_message = "Disabled defaults must create no definitions and need no prepared images, roles or caps."
  }
}

run "analytics_offline_definition" {
  command = plan
  variables {
    jobs_enabled        = true
    enabled_definitions = ["analytics_offline"]
  }
  assert {
    condition = (
      keys(aws_ecs_task_definition.read) == ["analytics_offline"] &&
      aws_ecs_task_definition.read["analytics_offline"].task_role_arn == "arn:aws:iam::111111111111:role/example_project_job_offline_task_dev" &&
      jsondecode(output.container_definitions["analytics_offline"])[0].image == var.images.analytics &&
      jsondecode(output.container_definitions["analytics_offline"])[0].secrets == []
    )
    error_message = "Analytics offline must use its measured analytics image and offline role without catalog secrets."
  }
}

run "quality_offline_definition" {
  command = plan
  variables {
    jobs_enabled        = true
    enabled_definitions = ["quality_offline"]
  }
  assert {
    condition = (
      keys(aws_ecs_task_definition.read) == ["quality_offline"] &&
      aws_ecs_task_definition.read["quality_offline"].task_role_arn == "arn:aws:iam::111111111111:role/example_project_job_offline_task_dev" &&
      jsondecode(output.container_definitions["quality_offline"])[0].image == var.images.quality &&
      jsondecode(output.container_definitions["quality_offline"])[0].secrets == []
    )
    error_message = "Quality offline must use its measured quality image and offline role without catalog secrets."
  }
}

run "seal_definition" {
  command = plan
  variables {
    jobs_enabled        = true
    enabled_definitions = ["seal"]
  }
  assert {
    condition = (
      keys(aws_ecs_task_definition.read) == ["seal"] &&
      aws_ecs_task_definition.read["seal"].task_role_arn == "arn:aws:iam::111111111111:role/example_project_job_seal_task_dev" &&
      jsondecode(output.container_definitions["seal"])[0].image == var.images.analytics &&
      jsondecode(output.container_definitions["seal"])[0].secrets == []
    )
    error_message = "Seal must use the analytics image and the separate seal role without catalog secrets."
  }
}

run "analytics_read_definition" {
  command = plan
  variables {
    jobs_enabled        = true
    enabled_definitions = ["analytics_read"]
  }
  assert {
    condition = (
      keys(aws_ecs_task_definition.read) == ["analytics_read"] &&
      aws_ecs_task_definition.read["analytics_read"].task_role_arn == "arn:aws:iam::111111111111:role/example_project_job_read_task_dev" &&
      jsondecode(output.container_definitions["analytics_read"])[0].image == var.images.analytics &&
      jsondecode(output.container_definitions["analytics_read"])[0].secrets == [
        { name = "POLARIS_CLIENT_ID", valueFrom = "arn:aws:ssm:us-west-2:111111111111:parameter/example_project/aws_polaris_reader_client_id" },
        { name = "POLARIS_CLIENT_SECRET", valueFrom = "arn:aws:ssm:us-west-2:111111111111:parameter/example_project/aws_polaris_reader_client_secret" }
      ]
    )
    error_message = "Analytics read must bind only the read role, analytics image and exact reader pair."
  }
}

run "spark_read_definition" {
  command = plan
  variables {
    jobs_enabled        = true
    enabled_definitions = ["spark_read"]
  }
  assert {
    condition = (
      keys(aws_ecs_task_definition.read) == ["spark_read"] &&
      aws_ecs_task_definition.read["spark_read"].task_role_arn == "arn:aws:iam::111111111111:role/example_project_job_read_task_dev" &&
      jsondecode(output.container_definitions["spark_read"])[0].image == var.images.spark &&
      jsondecode(output.container_definitions["spark_read"])[0].secrets == [
        { name = "POLARIS_CLIENT_ID", valueFrom = "arn:aws:ssm:us-west-2:111111111111:parameter/example_project/aws_polaris_reader_client_id" },
        { name = "POLARIS_CLIENT_SECRET", valueFrom = "arn:aws:ssm:us-west-2:111111111111:parameter/example_project/aws_polaris_reader_client_secret" }
      ]
    )
    error_message = "Spark read must bind only the read role, Spark image and exact reader pair."
  }
}

run "all_read_definitions" {
  command = plan
  variables {
    jobs_enabled        = true
    enabled_definitions = ["analytics_offline", "quality_offline", "seal", "analytics_read", "spark_read"]
  }
  assert {
    condition     = toset(keys(aws_ecs_task_definition.read)) == toset(["analytics_offline", "quality_offline", "seal", "analytics_read", "spark_read"])
    error_message = "The read inventory must contain exactly five definitions."
  }
  assert {
    condition = alltrue([for name, task in aws_ecs_task_definition.read : (
      task.family == "example_project_dev_job_${name}" &&
      toset(task.requires_compatibilities) == toset(["FARGATE"]) && task.network_mode == "awsvpc" &&
      task.runtime_platform[0].cpu_architecture == "ARM64" && task.runtime_platform[0].operating_system_family == "LINUX" &&
      task.execution_role_arn == "arn:aws:iam::111111111111:role/example_project_job_execution_dev" && task.skip_destroy && !task.enable_fault_injection &&
      task.tags == tomap({ Project = "example_project", Environment = "dev", ManagedBy = "Terraform" }) &&
      tonumber(task.cpu) == var.task_resources[name].cpu && tonumber(task.memory) == var.task_resources[name].memory &&
      task.ephemeral_storage[0].size_in_gib == 21 &&
      toset([for volume in task.volume : volume.name]) == toset(["workspace", "tmp"]) &&
      alltrue([for volume in task.volume : volume.host_path == null && length(volume.efs_volume_configuration) == 0 && length(volume.docker_volume_configuration) == 0 && !volume.configure_at_launch])
    )])
    error_message = "Every application definition must match the private ARM64 Fargate, role, resource, tag and task-volume contract."
  }
  assert {
    condition = alltrue([for name, document in output.container_definitions : (
      length(jsondecode(document)) == 1 &&
      jsondecode(document)[0].name == name &&
      jsondecode(document)[0].entryPoint == ["/opt/hai/deadline_run", "python", "-I", "/opt/hai/bootstrap/run.py"] &&
      jsondecode(document)[0].command == [] && jsondecode(document)[0].essential &&
      jsondecode(document)[0].readonlyRootFilesystem && !jsondecode(document)[0].privileged &&
      jsondecode(document)[0].user == "10001:10001" && jsondecode(document)[0].stopTimeout == 90 &&
      jsondecode(document)[0].linuxParameters == { capabilities = { add = [], drop = ["ALL"] }, initProcessEnabled = true } &&
      jsondecode(document)[0].mountPoints == [
        { sourceVolume = "workspace", containerPath = "/workspace", readOnly = false },
        { sourceVolume = "tmp", containerPath = "/tmp", readOnly = false }
      ] &&
      jsondecode(document)[0].logConfiguration.options["awslogs-group"] == "example_project_jobs_dev"
    )])
    error_message = "Every application container must preserve the baked entry point, bounded stop, unprivileged user, dropped capabilities and writable scratch."
  }
  assert {
    condition = (
      output.dispatch_contract.assign_public_ip == "DISABLED" && output.dispatch_contract.propagate_tags == "TASK_DEFINITION" &&
      output.dispatch_contract.class_networks == { offline = "offline", seal = "offline", lakehouse_read = "lakehouse" } &&
      output.dispatch_contract.cloud_limits == { max_tasks = 3, max_vcpu = 16, max_memory_gib = 120 } &&
      output.dispatch_contract.deadlines == { hard_stop_required = true, pending_seconds = 600, stop_grace_seconds = 90, cleanup_seconds = 120, budget_alert_is_spend_stop = false }
    )
    error_message = "Dispatch must retain the fixed cap, private class networks, task tags and hard-stop requirements."
  }
  assert {
    condition = (
      length(output.execution_digests) == 5 &&
      alltrue([for pin in values(output.execution_digests) : can(regex("^[a-f0-9]{64}$", pin))]) &&
      output.secret_flow.violations == [] && output.secret_flow.alias_chains_converged &&
      toset(output.secret_flow.containers_checked) == toset(["analytics_read/analytics_read", "spark_read/spark_read"]) &&
      output.secret_flow.mutation_mismatches == [] && output.secret_flow.mutations_checked > 0
    )
    error_message = "The reused flow module must check both readers and bind all five execution definitions without a missed mutation."
  }
}

run "selection_requires_switch" {
  command = plan
  variables { enabled_definitions = ["analytics_read"] }
  expect_failures = [var.enabled_definitions]
}

run "mutable_image_is_refused" {
  command = plan
  variables {
    jobs_enabled        = true
    enabled_definitions = ["analytics_read"]
    images              = { analytics = "111111111111.dkr.ecr.us-west-2.amazonaws.com/example_project_analytics_dev:latest" }
  }
  expect_failures = [var.images]
}

run "missing_cap_is_refused" {
  command = plan
  variables {
    jobs_enabled        = true
    enabled_definitions = ["analytics_read"]
    cloud_limits        = null
  }
  expect_failures = [var.cloud_limits]
}

run "changed_cap_is_refused" {
  command = plan
  variables {
    jobs_enabled        = true
    enabled_definitions = ["analytics_read"]
    cloud_limits        = { max_tasks = 4, max_vcpu = 16, max_memory_gib = 120 }
  }
  expect_failures = [var.cloud_limits]
}

run "writer_role_is_refused" {
  command = plan
  variables {
    jobs_enabled        = true
    enabled_definitions = ["analytics_read"]
    job_role_arns = {
      execution = "arn:aws:iam::111111111111:role/example_project_job_execution_dev"
      read_task = "arn:aws:iam::111111111111:role/example_project_job_write_silver_task_dev"
    }
  }
  expect_failures = [var.job_role_arns]
}

run "missing_resources_are_refused" {
  command = plan
  variables {
    jobs_enabled        = true
    enabled_definitions = ["spark_read"]
    task_resources      = {}
  }
  expect_failures = [var.task_resources]
}

run "invalid_fargate_shape_is_refused" {
  command = plan
  variables {
    jobs_enabled        = true
    enabled_definitions = ["spark_read"]
    task_resources      = { spark_read = { cpu = 2048, memory = 2048, ephemeral_storage_gib = 21 } }
  }
  expect_failures = [var.task_resources]
}

run "unknown_definition_is_refused" {
  command = plan
  variables {
    jobs_enabled        = true
    enabled_definitions = ["silver_writer"]
  }
  expect_failures = [var.enabled_definitions]
}

run "writer_parameter_is_refused" {
  command = plan
  variables {
    jobs_enabled                      = true
    enabled_definitions               = ["analytics_read"]
    aws_polaris_reader_client_id_name = "/example_project/aws_polaris_silver_client_id"
  }
  expect_failures = [var.aws_polaris_reader_client_id_name]
}

run "exported_pins_match_shared_flow" {
  command = plan

  module {
    source = "../services/tests/flow"
  }

  variables {
    definitions = run.all_read_definitions.container_definitions
    tasks       = run.all_read_definitions.task_definition_settings
  }

  assert {
    condition     = output.execution_digests == run.all_read_definitions.execution_digests
    error_message = "Exported execution pins must come unchanged from the shared flow module."
  }
}

run "foreign_account_image_is_refused" {
  command = plan
  variables {
    jobs_enabled        = true
    enabled_definitions = ["analytics_read"]
    images              = { analytics = "222222222222.dkr.ecr.us-west-2.amazonaws.com/example_project_analytics_dev@sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa" }
  }
  expect_failures = [var.images]
}

run "wrong_repository_image_is_refused" {
  command = plan
  variables {
    jobs_enabled        = true
    enabled_definitions = ["analytics_read"]
    images              = { analytics = "111111111111.dkr.ecr.us-west-2.amazonaws.com/example_project_quality_dev@sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa" }
  }
  expect_failures = [var.images]
}

run "bare_image_is_refused" {
  command = plan
  variables {
    jobs_enabled        = true
    enabled_definitions = ["analytics_read"]
    images              = { analytics = "111111111111.dkr.ecr.us-west-2.amazonaws.com/example_project_analytics_dev" }
  }
  expect_failures = [var.images]
}

run "foreign_account_role_is_refused" {
  command = plan
  variables {
    jobs_enabled        = true
    enabled_definitions = ["analytics_read"]
    job_role_arns = {
      execution = "arn:aws:iam::111111111111:role/example_project_job_execution_dev"
      read_task = "arn:aws:iam::222222222222:role/example_project_job_read_task_dev"
    }
  }
  expect_failures = [var.job_role_arns]
}

run "missing_execution_role_is_refused" {
  command = plan
  variables {
    jobs_enabled        = true
    enabled_definitions = ["analytics_read"]
    job_role_arns       = { read_task = "arn:aws:iam::111111111111:role/example_project_job_read_task_dev" }
  }
  expect_failures = [var.job_role_arns]
}

run "wrong_log_group_is_refused" {
  command = plan
  variables {
    jobs_enabled        = true
    enabled_definitions = ["analytics_read"]
    log_group_name      = "other_project_jobs_dev"
  }
  expect_failures = [var.log_group_name]
}

run "writer_secret_parameter_is_refused" {
  command = plan
  variables {
    jobs_enabled                          = true
    enabled_definitions                   = ["analytics_read"]
    aws_polaris_reader_client_secret_name = "/example_project/aws_polaris_silver_client_secret"
  }
  expect_failures = [var.aws_polaris_reader_client_secret_name]
}

run "off_step_memory_is_refused" {
  command = plan
  variables {
    jobs_enabled        = true
    enabled_definitions = ["analytics_read"]
    task_resources      = { analytics_read = { cpu = 1024, memory = 2050, ephemeral_storage_gib = 21 } }
  }
  expect_failures = [var.task_resources]
}

run "oversized_memory_is_refused" {
  command = plan
  variables {
    jobs_enabled        = true
    enabled_definitions = ["analytics_read"]
    task_resources      = { analytics_read = { cpu = 1024, memory = 9216, ephemeral_storage_gib = 21 } }
  }
  expect_failures = [var.task_resources]
}

run "oversized_scratch_is_refused" {
  command = plan
  variables {
    jobs_enabled        = true
    enabled_definitions = ["analytics_read"]
    task_resources      = { analytics_read = { cpu = 1024, memory = 2048, ephemeral_storage_gib = 201 } }
  }
  expect_failures = [var.task_resources]
}

run "unused_writer_role_is_refused" {
  command = plan
  variables {
    jobs_enabled        = true
    enabled_definitions = ["analytics_read"]
    job_role_arns = {
      execution  = "arn:aws:iam::111111111111:role/example_project_job_execution_dev"
      read_task  = "arn:aws:iam::111111111111:role/example_project_job_read_task_dev"
      try_writer = "arn:aws:iam::111111111111:role/example_project_job_try_writer_dev"
    }
  }
  expect_failures = [var.job_role_arns]
}

mock_provider "aws" {}

variables {
  aws_profile         = "example_project_dev"
  aws_region          = "us-west-2"
  expected_account_id = "111111111111"
  project_name        = "example_project"
  environment         = "dev"
  seed_image          = "111111111111.dkr.ecr.us-west-2.amazonaws.com/example_project_spark_dev@sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
  execution_role_arn  = "arn:aws:iam::111111111111:role/example_project_job_execution_dev"
  task_role_arn       = "arn:aws:iam::111111111111:role/example_project_job_offline_task_dev"
  task_role_unique_id = "AROAAAAAAAAAAAAAAAAAA"
  log_group_name      = "example_project_jobs_dev"
  foundation_network = {
    subnet_ids         = ["subnet-11111111111111111"]
    security_group_ids = ["sg-22222222222222222"]
  }
}

run "default_probe_creates_nothing" {
  command = plan
  variables {
    seed_image          = null
    execution_role_arn  = null
    task_role_arn       = null
    task_role_unique_id = null
    log_group_name      = null
    foundation_network  = null
  }
  assert {
    condition     = length(aws_ecs_task_definition.probe) == 0 && output.task_definition_arn == null && output.dispatch_contract == null
    error_message = "The independent probe must be absent by default and need no application inputs."
  }
}

run "standalone_probe_uses_seed_image" {
  command = plan
  variables { probe_enabled = true }
  assert {
    condition = (
      length(aws_ecs_task_definition.probe) == 1 &&
      aws_ecs_task_definition.probe[0].family == "example_project_dev_job_identity_probe" &&
      toset(aws_ecs_task_definition.probe[0].requires_compatibilities) == toset(["FARGATE"]) &&
      aws_ecs_task_definition.probe[0].network_mode == "awsvpc" &&
      aws_ecs_task_definition.probe[0].runtime_platform[0].cpu_architecture == "ARM64" &&
      aws_ecs_task_definition.probe[0].task_role_arn == "arn:aws:iam::111111111111:role/example_project_job_offline_task_dev" &&
      aws_ecs_task_definition.probe[0].execution_role_arn == "arn:aws:iam::111111111111:role/example_project_job_execution_dev" &&
      aws_ecs_task_definition.probe[0].skip_destroy &&
      aws_ecs_task_definition.probe[0].cpu == "1024" && aws_ecs_task_definition.probe[0].memory == "2048" &&
      length(aws_ecs_task_definition.probe[0].volume) == 0 &&
      aws_ecs_task_definition.probe[0].tags == tomap({ Project = "example_project", Environment = "dev", ManagedBy = "Terraform" })
    )
    error_message = "The seed probe must be a separate minimal ARM64 Fargate definition with the merged offline and execution roles."
  }
  assert {
    condition = (
      jsondecode(output.container_definition)[0].image == var.seed_image &&
      jsondecode(output.container_definition)[0].entryPoint == ["timeout", "--signal=KILL", "60", "python", "-I", "-c"] &&
      jsondecode(output.container_definition)[0].command == [file("${path.module}/probe.py")] &&
      jsondecode(output.container_definition)[0].secrets == [] &&
      jsondecode(output.container_definition)[0].readonlyRootFilesystem && !jsondecode(output.container_definition)[0].privileged &&
      jsondecode(output.container_definition)[0].linuxParameters.capabilities == { add = [], drop = ["ALL"] } &&
      jsondecode(output.container_definition)[0].stopTimeout == 30 &&
      !strcontains(output.container_definition, "/opt/hai/bootstrap")
    )
    error_message = "The fixed seed command must be bounded and secret-free without the application worker bootstrap."
  }
  assert {
    condition = (
      output.dispatch_contract.assign_public_ip == "ENABLED" &&
      output.dispatch_contract.network == var.foundation_network &&
      output.dispatch_contract.propagate_tags == "TASK_DEFINITION" && output.dispatch_contract.capacity_provider == "FARGATE" &&
      output.dispatch_contract.hard_stop_seconds == 60 && output.dispatch_contract.outer_slot_seconds == 1800
    )
    error_message = "Only the separately approved probe may use the foundation public-IP network inside its bounded slot."
  }
}

run "mutable_seed_is_refused" {
  command = plan
  variables {
    probe_enabled = true
    seed_image    = "111111111111.dkr.ecr.us-west-2.amazonaws.com/example_project_spark_dev:latest"
  }
  expect_failures = [var.seed_image]
}

run "probe_writer_role_is_refused" {
  command = plan
  variables {
    probe_enabled = true
    task_role_arn = "arn:aws:iam::111111111111:role/example_project_job_write_silver_task_dev"
  }
  expect_failures = [var.task_role_arn]
}

run "probe_requires_own_network" {
  command = plan
  variables {
    probe_enabled      = true
    foundation_network = null
  }
  expect_failures = [var.foundation_network]
}

run "wrong_repository_seed_is_refused" {
  command = plan
  variables {
    probe_enabled = true
    seed_image    = "111111111111.dkr.ecr.us-west-2.amazonaws.com/example_project_analytics_dev@sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
  }
  expect_failures = [var.seed_image]
}

run "probe_execution_role_is_refused" {
  command = plan
  variables {
    probe_enabled      = true
    execution_role_arn = "arn:aws:iam::111111111111:role/example_project_task_execution_dev"
  }
  expect_failures = [var.execution_role_arn]
}

run "probe_role_unique_id_is_refused" {
  command = plan
  variables {
    probe_enabled       = true
    task_role_unique_id = "AROA_TOO_SHORT"
  }
  expect_failures = [var.task_role_unique_id]
}

run "probe_wrong_log_group_is_refused" {
  command = plan
  variables {
    probe_enabled  = true
    log_group_name = "other_project_jobs_dev"
  }
  expect_failures = [var.log_group_name]
}

run "probe_empty_groups_are_refused" {
  command = plan
  variables {
    probe_enabled      = true
    foundation_network = { subnet_ids = ["subnet-11111111111111111"], security_group_ids = [] }
  }
  expect_failures = [var.foundation_network]
}

run "foreign_account_seed_is_refused" {
  command = plan
  variables {
    probe_enabled = true
    seed_image    = "222222222222.dkr.ecr.us-west-2.amazonaws.com/example_project_spark_dev@sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
  }
  expect_failures = [var.seed_image]
}

run "bare_seed_is_refused" {
  command = plan
  variables {
    probe_enabled = true
    seed_image    = "111111111111.dkr.ecr.us-west-2.amazonaws.com/example_project_spark_dev"
  }
  expect_failures = [var.seed_image]
}

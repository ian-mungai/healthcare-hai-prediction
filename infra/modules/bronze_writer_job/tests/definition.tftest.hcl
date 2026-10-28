mock_provider "aws" {}

variables {
  project_name         = "example_project"
  environment          = "dev"
  aws_region           = "us-west-2"
  expected_account_id  = "111111111111"
  task_role_arn        = "arn:aws:iam::111111111111:role/example_project_job_write_bronze_task_dev"
  execution_role_arn   = "arn:aws:iam::111111111111:role/example_project_job_execution_dev"
  image                = "111111111111.dkr.ecr.us-west-2.amazonaws.com/example@sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
  cpu                  = 1024
  cpu_architecture     = "X86_64"
  memory               = 3072
  job_threads          = 1
  spark_memory         = "1g"
  polaris_uri          = "https://catalog.example.com/api/catalog"
  log_group_name       = "/example/jobs"
  bootstrap_entrypoint = ["python", "-B", "/app/bootstrap.py"]
  tags                 = { Project = "example_project" }
}

run "definition_is_unregistered_by_default" {
  command = plan

  assert {
    condition     = length(aws_ecs_task_definition.bronze_writer) == 0
    error_message = "The optional module must not register a task by default."
  }
}

run "definition_injects_only_the_bronze_pair" {
  command = plan

  variables {
    register_definition = true
  }

  assert {
    condition = (
      aws_ecs_task_definition.bronze_writer[0].requires_compatibilities == toset(["FARGATE"]) &&
      aws_ecs_task_definition.bronze_writer[0].network_mode == "awsvpc" &&
      output.dispatch_contract.capacity_provider == "FARGATE" &&
      output.dispatch_contract.retry_after_start == false &&
      jsondecode(aws_ecs_task_definition.bronze_writer[0].container_definitions)[0].secrets == [
        {
          name      = "POLARIS_CLIENT_ID"
          valueFrom = "arn:aws:ssm:us-west-2:111111111111:parameter/healthcare_hai_prediction/aws_polaris_bronze_client_id"
        },
        {
          name      = "POLARIS_CLIENT_SECRET"
          valueFrom = "arn:aws:ssm:us-west-2:111111111111:parameter/healthcare_hai_prediction/aws_polaris_bronze_client_secret"
        }
      ]
    )
    error_message = "The definition must use the bronze pair, on-demand Fargate and no retry after start."
  }
}


run "definition_tags_and_dispatch_override" {
  command = plan

  variables {
    register_definition = true
  }

  assert {
    condition     = try(aws_ecs_task_definition.bronze_writer[0].tags["Project"], "") == "example_project"
    error_message = "Task registration must explicitly carry the Project tag."
  }

  assert {
    condition     = try(output.dispatch_contract.propagate_tags, "") == "TASK_DEFINITION"
    error_message = "RunTask must propagate the task-definition tags."
  }

  assert {
    condition = try(
      output.dispatch_contract.command_override_required &&
      output.dispatch_contract.command_override.container_name == "bronze_writer" &&
      output.dispatch_contract.command_override.command_prefix == [
        "python", "-B", "-m", "scripts.lakehouse.aws_bronze", "--binding", "/run/job/bronze_binding.json"
      ] &&
      output.dispatch_contract.command_override.required_flags == ["--job", "--authority-epoch"] &&
      output.dispatch_contract.command_override.optional_flags == ["--tables", "--fixture-action"],
      false
    )
    error_message = "RunTask must replace the container command with the bound job, current epoch and declared job inputs."
  }
}

run "registration_requires_the_project_tag" {
  command = plan

  variables {
    register_definition = true
    tags                = {}
  }

  expect_failures = [var.tags]
}

run "family_matches_the_project_ecs_policy" {
  command = plan

  variables {
    register_definition = true
  }

  assert {
    condition = (
      aws_ecs_task_definition.bronze_writer[0].family == "example_project_dev_job_write_bronze" &&
      startswith(aws_ecs_task_definition.bronze_writer[0].family, "example_project_dev_")
    )
    error_message = "The family must start with <project>_<environment>_, the only task definitions the project user's ECS policy may register and run."
  }
}

mock_provider "aws" {}

variables {
  aws_profile         = "example_project_dev"
  aws_region          = "us-west-2"
  expected_account_id = "111111111111"
  project_name        = "example_project"
  environment         = "dev"
  data_bucket_name    = "example-project-data-dev"
  train_images = {
    training      = "111111111111.dkr.ecr.us-west-2.amazonaws.com/example_project_training_dev@sha256:dddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddd"
    feature_store = "111111111111.dkr.ecr.us-west-2.amazonaws.com/example_project_feature_store_dev@sha256:eeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee"
  }
  train_roles = {
    execution  = "arn:aws:iam::111111111111:role/example_project_job_execution_dev"
    train_task = "arn:aws:iam::111111111111:role/example_project_job_train_task_dev"
  }
  aws_polaris_reader_client_id_name     = "/example_project/aws_polaris_reader_client_id"
  aws_polaris_reader_client_secret_name = "/example_project/aws_polaris_reader_client_secret"
  mlflow_admin_password_name            = "/example_project/mlflow_admin_password"
  log_group_name                        = "example_project_jobs_dev"
}

run "default_plan_creates_no_promotion_task" {
  command = plan

  assert {
    condition     = length(aws_ecs_task_definition.promote) == 0 && output.promote_run_task == null && output.promote_container_definitions == null
    error_message = "Without promote_enabled the jobs root registers no promotion task."
  }
}

run "promote_definition_shape" {
  command = plan

  variables {
    train_enabled   = true
    promote_enabled = true
  }

  assert {
    condition = (
      length(aws_ecs_task_definition.promote) == 1 &&
      aws_ecs_task_definition.promote[0].family == "example_project_dev_job_promote" &&
      aws_ecs_task_definition.promote[0].task_role_arn == "arn:aws:iam::111111111111:role/example_project_job_train_task_dev" &&
      aws_ecs_task_definition.promote[0].execution_role_arn == "arn:aws:iam::111111111111:role/example_project_job_execution_dev" &&
      toset(aws_ecs_task_definition.promote[0].requires_compatibilities) == toset(["FARGATE"]) && aws_ecs_task_definition.promote[0].network_mode == "awsvpc" &&
      aws_ecs_task_definition.promote[0].runtime_platform[0].cpu_architecture == "ARM64" &&
      aws_ecs_task_definition.promote[0].cpu == "1024" && aws_ecs_task_definition.promote[0].memory == "4096" &&
      aws_ecs_task_definition.promote[0].skip_destroy && !aws_ecs_task_definition.promote[0].enable_fault_injection
    )
    error_message = "The promotion task is one small ARM64 Fargate definition with the training task role and the execution role."
  }
}

run "promote_steps_images_and_users" {
  command = plan

  variables {
    train_enabled   = true
    promote_enabled = true
  }

  assert {
    condition = (
      [for item in jsondecode(aws_ecs_task_definition.promote[0].container_definitions) : item.name] == ["init", "fetch", "promote"] &&
      alltrue([for item in jsondecode(aws_ecs_task_definition.promote[0].container_definitions) : item.image == var.train_images["training"]]) &&
      [for item in jsondecode(aws_ecs_task_definition.promote[0].container_definitions) : item.user] == ["0:0", "10001:10001", "10001:10001"] &&
      [for item in jsondecode(aws_ecs_task_definition.promote[0].container_definitions) : item.essential] == [false, false, true] &&
      jsondecode(aws_ecs_task_definition.promote[0].container_definitions)[2].dependsOn == [{ containerName = "fetch", condition = "SUCCESS" }] &&
      jsondecode(aws_ecs_task_definition.promote[0].container_definitions)[1].dependsOn == [{ containerName = "init", condition = "SUCCESS" }]
    )
    error_message = "The steps run in order from the training image. Only the first runs as root and only the last is essential."
  }

  assert {
    condition = (
      jsondecode(aws_ecs_task_definition.promote[0].container_definitions)[2].command[0] == "cd /workspace/code && python -m scripts.training.aws_run promote --run \"$RUN_ID\"" &&
      jsondecode(aws_ecs_task_definition.promote[0].container_definitions)[1].command[0] == "python -I /opt/hai/fetch_code.py"
    )
    error_message = "The last step runs the promote phase of the runner."
  }
}

run "promote_secret_and_variables" {
  command = plan

  variables {
    train_enabled   = true
    promote_enabled = true
  }

  assert {
    condition = (
      jsondecode(aws_ecs_task_definition.promote[0].container_definitions)[2].secrets == [
        { name = "MLFLOW_TRACKING_PASSWORD", valueFrom = "arn:aws:ssm:us-west-2:111111111111:parameter/example_project/mlflow_admin_password" }
      ] &&
      length(jsondecode(aws_ecs_task_definition.promote[0].container_definitions)[0].secrets) == 0 &&
      length(jsondecode(aws_ecs_task_definition.promote[0].container_definitions)[1].secrets) == 0
    )
    error_message = "Only the promote step holds the MLflow password, and no step holds a catalog credential."
  }

  assert {
    condition = (
      [for item in jsondecode(aws_ecs_task_definition.promote[0].container_definitions)[2].environment : item.name if item.value == ""] == ["RUN_ID", "CODE_SHA256", "TRAINING_PARAMETERS", "MLFLOW_TRACKING_URI"] &&
      [for item in jsondecode(aws_ecs_task_definition.promote[0].container_definitions)[1].environment : item.name if item.value == ""] == ["RUN_ID", "CODE_SHA256"] &&
      !anytrue([for item in jsondecode(aws_ecs_task_definition.promote[0].container_definitions) : anytrue([for entry in item.environment : strcontains(entry.name, "POLARIS")])])
    )
    error_message = "The broker fills exactly the variables the table names for each container, and the task needs no catalog address."
  }
}

run "promote_secret_flow_is_clean" {
  command = plan

  variables {
    train_enabled   = true
    promote_enabled = true
  }

  assert {
    condition     = output.promote_secret_flow.violations == [] && output.promote_secret_flow.mutation_mismatches == [] && contains(output.promote_secret_flow.containers_checked, "promote/promote")
    error_message = "The MLflow password reaches no file, argument, log line or environment dump."
  }
}

run "promote_needs_the_training_task" {
  command = plan

  variables {
    train_enabled   = false
    promote_enabled = true
  }

  expect_failures = [var.promote_enabled]
}

run "promote_needs_the_jobs_log_group" {
  command = plan

  variables {
    train_enabled   = true
    promote_enabled = true
    log_group_name  = "another_group"
  }

  expect_failures = [var.log_group_name]
}

run "promote_invalid_shape_is_refused" {
  command = plan

  variables {
    promote_resources = { cpu = 1024, memory = 1025, ephemeral_storage_gib = 21 }
  }

  expect_failures = [var.promote_resources]
}

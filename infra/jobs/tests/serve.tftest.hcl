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
  serve_roles = {
    serve_task = "arn:aws:iam::111111111111:role/example_project_job_serve_task_dev"
  }
  serve_manifest_prefix                 = "serving/deployments"
  aws_polaris_reader_client_id_name     = "/example_project/aws_polaris_reader_client_id"
  aws_polaris_reader_client_secret_name = "/example_project/aws_polaris_reader_client_secret"
  mlflow_admin_password_name            = "/example_project/mlflow_admin_password"
  log_group_name                        = "example_project_jobs_dev"
}

run "default_plan_creates_no_serve_task" {
  command = plan

  variables {
    serve_roles           = {}
    serve_manifest_prefix = ""
  }

  assert {
    condition     = length(aws_ecs_task_definition.serve) == 0 && output.serve_run_task == null && output.serve_container_definitions == null
    error_message = "Without serve_enabled the jobs root registers no serve task and needs no serve role or prefix."
  }
}

run "serve_definition_shape" {
  command = plan

  variables {
    train_enabled = true
    serve_enabled = true
  }

  assert {
    condition = (
      length(aws_ecs_task_definition.serve) == 1 &&
      aws_ecs_task_definition.serve[0].family == "example_project_dev_job_serve" &&
      aws_ecs_task_definition.serve[0].task_role_arn == "arn:aws:iam::111111111111:role/example_project_job_serve_task_dev" &&
      aws_ecs_task_definition.serve[0].execution_role_arn == "arn:aws:iam::111111111111:role/example_project_job_execution_dev" &&
      toset(aws_ecs_task_definition.serve[0].requires_compatibilities) == toset(["FARGATE"]) && aws_ecs_task_definition.serve[0].network_mode == "awsvpc" &&
      aws_ecs_task_definition.serve[0].runtime_platform[0].cpu_architecture == "ARM64" &&
      aws_ecs_task_definition.serve[0].cpu == "1024" && aws_ecs_task_definition.serve[0].memory == "4096" &&
      aws_ecs_task_definition.serve[0].skip_destroy && !aws_ecs_task_definition.serve[0].enable_fault_injection
    )
    error_message = "The serve task is one small ARM64 Fargate definition with its own task role and the shared execution role."
  }
}

run "serve_steps_images_and_users" {
  command = plan

  variables {
    train_enabled = true
    serve_enabled = true
  }

  assert {
    condition = (
      [for item in jsondecode(aws_ecs_task_definition.serve[0].container_definitions) : item.name] == ["init", "fetch", "serve"] &&
      alltrue([for item in jsondecode(aws_ecs_task_definition.serve[0].container_definitions) : item.image == var.train_images["training"]]) &&
      [for item in jsondecode(aws_ecs_task_definition.serve[0].container_definitions) : item.user] == ["0:0", "10001:10001", "10001:10001"] &&
      [for item in jsondecode(aws_ecs_task_definition.serve[0].container_definitions) : item.essential] == [false, false, true] &&
      jsondecode(aws_ecs_task_definition.serve[0].container_definitions)[2].dependsOn == [{ containerName = "fetch", condition = "SUCCESS" }] &&
      jsondecode(aws_ecs_task_definition.serve[0].container_definitions)[1].dependsOn == [{ containerName = "init", condition = "SUCCESS" }]
    )
    error_message = "The steps run in order from the training image. Only the first runs as root and only the last is essential."
  }

  assert {
    condition = (
      jsondecode(aws_ecs_task_definition.serve[0].container_definitions)[1].command[0] == "python -I /opt/hai/fetch_code.py" &&
      jsondecode(aws_ecs_task_definition.serve[0].container_definitions)[2].command[0] == "cd /workspace/code && timeout 600 python -m scripts.serving.deploy_job --run \"$RUN_ID\""
    )
    error_message = "The last step runs the deploy tool under a time limit that is shorter than the broker deadline. The tool reads the checked request from its environment."
  }
}

run "serve_variables_secret_and_reader_environment" {
  command = plan

  variables {
    train_enabled = true
    serve_enabled = true
  }

  assert {
    condition = (
      jsondecode(aws_ecs_task_definition.serve[0].container_definitions)[2].secrets == [
        { name = "MLFLOW_TRACKING_PASSWORD", valueFrom = "arn:aws:ssm:us-west-2:111111111111:parameter/example_project/mlflow_admin_password" }
      ] &&
      length(jsondecode(aws_ecs_task_definition.serve[0].container_definitions)[0].secrets) == 0 &&
      length(jsondecode(aws_ecs_task_definition.serve[0].container_definitions)[1].secrets) == 0
    )
    error_message = "Only the serve step holds the MLflow password, and no step holds a catalog credential."
  }

  assert {
    condition = (
      [for item in jsondecode(aws_ecs_task_definition.serve[0].container_definitions)[2].environment : item.name if item.value == ""] == ["RUN_ID", "SERVING_REQUEST", "MLFLOW_TRACKING_URI"] &&
      [for item in jsondecode(aws_ecs_task_definition.serve[0].container_definitions)[1].environment : item.name if item.value == ""] == ["RUN_ID", "CODE_RUN_ID", "CODE_SHA256"] &&
      !anytrue([for item in jsondecode(aws_ecs_task_definition.serve[0].container_definitions) : anytrue([for entry in item.environment : strcontains(entry.name, "POLARIS")])])
    )
    error_message = "The broker fills exactly the variables the table names for each container, and the task needs no catalog address."
  }

  assert {
    condition = (
      { for item in jsondecode(aws_ecs_task_definition.serve[0].container_definitions)[2].environment : item.name => item.value if item.value != "" }["MODEL_API_MANIFEST_URI"] == "s3://example-project-data-dev/serving/deployments" &&
      { for item in jsondecode(aws_ecs_task_definition.serve[0].container_definitions)[2].environment : item.name => item.value if item.value != "" }["MLFLOW_TRACKING_USERNAME"] == "admin" &&
      { for item in jsondecode(aws_ecs_task_definition.serve[0].container_definitions)[2].environment : item.name => item.value if item.value != "" }["MLFLOW_HTTP_REQUEST_TIMEOUT"] == "10" &&
      { for item in jsondecode(aws_ecs_task_definition.serve[0].container_definitions)[2].environment : item.name => item.value if item.value != "" }["MLFLOW_HTTP_REQUEST_MAX_RETRIES"] == "2"
    )
    error_message = "The deployment prefix is fixed in the definition, so no caller chooses where records go, and every MLflow request has a short limit."
  }
}

run "serve_secret_flow_is_clean" {
  command = plan

  variables {
    train_enabled = true
    serve_enabled = true
  }

  assert {
    condition     = output.serve_secret_flow.violations == [] && output.serve_secret_flow.mutation_mismatches == [] && contains(output.serve_secret_flow.containers_checked, "serve/serve")
    error_message = "The MLflow password reaches no file, argument, log line or environment dump."
  }
}

run "serve_needs_the_training_task" {
  command = plan

  variables {
    train_enabled = false
    serve_enabled = true
  }

  expect_failures = [var.serve_enabled]
}

run "serve_needs_the_jobs_log_group" {
  command = plan

  variables {
    train_enabled  = true
    serve_enabled  = true
    log_group_name = "another_group"
  }

  expect_failures = [var.log_group_name]
}

run "serve_needs_its_own_role_and_a_prefix" {
  command = plan

  variables {
    train_enabled = true
    serve_enabled = true
    serve_roles   = { serve_task = "arn:aws:iam::111111111111:role/example_project_job_train_task_dev" }
  }

  expect_failures = [var.serve_roles]
}

run "serve_prefix_is_refused_when_unsafe" {
  command = plan

  variables {
    train_enabled         = true
    serve_enabled         = true
    serve_manifest_prefix = "lakehouse/serving/deployments"
  }

  expect_failures = [var.serve_manifest_prefix]
}

run "serve_invalid_shape_is_refused" {
  command = plan

  variables {
    serve_resources = { cpu = 1024, memory = 1025, ephemeral_storage_gib = 21 }
  }

  expect_failures = [var.serve_resources]
}

run "serve_containers_follow_the_broker_table" {
  command = plan

  variables {
    train_enabled = true
    serve_enabled = true
  }

  assert {
    condition = (
      toset([for container in jsondecode(output.serve_container_definitions) : container.name if container.name != "init"]) == toset(keys(jsondecode(file("${path.module}/../../config/orchestration/ecs_broker_jobs.json")).jobs.serve.containers)) &&
      alltrue([for container in jsondecode(output.serve_container_definitions) : (
        container.name == "init" || toset([for variable in container.environment : variable.name if variable.value == ""]) == toset(jsondecode(file("${path.module}/../../config/orchestration/ecs_broker_jobs.json")).jobs.serve.containers[container.name])
      )]) &&
      contains([for container in jsondecode(output.serve_container_definitions) : container.name], jsondecode(file("${path.module}/../../config/orchestration/ecs_broker_jobs.json")).jobs.serve.log_container)
    )
    error_message = "The container names, their empty variables and the log container equal the broker job table, so every override the broker sends names a container of the task and fills what it needs."
  }
}

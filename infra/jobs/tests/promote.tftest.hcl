mock_provider "aws" {}

variables {
  # The committed defaults of the jobs stack are the live ones (every job family on, host_repo_path and serve_manifest_prefix set). Every case here that does
  # not set a switch tests it off, so the fixture sets each one explicitly.
  drift_enabled          = false
  promote_enabled        = false
  layer_jobs_enabled     = false
  serve_enabled          = false
  train_enabled          = false
  publisher_job_enabled  = false
  publisher_keys_enabled = false
  host_repo_path         = null
  serve_manifest_prefix  = ""
  aws_profile            = "example_project_dev"
  aws_region             = "us-west-2"
  expected_account_id    = "111111111111"
  project_name           = "example_project"
  environment            = "dev"
  data_bucket_name       = "example-project-data-dev"
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
  host_job_memory_mib                   = 23552
  # TEST FIXTURE ONLY, not measured values: the services output host_capacity_binding for a 30720 MiB usable host.
  host_capacity_binding = "job_memory_mib=23552 usable_memory_mib=30720 system_reserve_mib=2048 service_reservations_mib=5120"
  host_jobs_enabled     = true
  host_jobs             = ["promote"]
}

run "promote_definition_shape" {
  command = plan

  variables {
    train_enabled   = true
    promote_enabled = true
  }

  assert {
    condition = (
      aws_ecs_task_definition.host["promote"].family == "example_project_dev_job_promote_host" &&
      aws_ecs_task_definition.host["promote"].task_role_arn == "arn:aws:iam::111111111111:role/example_project_job_train_task_dev" &&
      aws_ecs_task_definition.host["promote"].execution_role_arn == "arn:aws:iam::111111111111:role/example_project_job_execution_dev" &&
      toset(aws_ecs_task_definition.host["promote"].requires_compatibilities) == toset(["EC2"]) && aws_ecs_task_definition.host["promote"].network_mode == "bridge" &&
      aws_ecs_task_definition.host["promote"].runtime_platform[0].cpu_architecture == "ARM64" &&
      aws_ecs_task_definition.host["promote"].cpu == "1024" && aws_ecs_task_definition.host["promote"].memory == "4096" &&
      aws_ecs_task_definition.host["promote"].skip_destroy && !aws_ecs_task_definition.host["promote"].enable_fault_injection
    )
    error_message = "The promotion task is one ARM64 host definition with the training task role, the execution role and its reservation of the host."
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
      [for item in jsondecode(aws_ecs_task_definition.host["promote"].container_definitions) : item.name] == ["init", "fetch", "promote"] &&
      alltrue([for item in jsondecode(aws_ecs_task_definition.host["promote"].container_definitions) : item.image == var.train_images["training"]]) &&
      [for item in jsondecode(aws_ecs_task_definition.host["promote"].container_definitions) : item.user] == ["0:0", "10001:10001", "10001:10001"] &&
      [for item in jsondecode(aws_ecs_task_definition.host["promote"].container_definitions) : item.essential] == [false, false, true] &&
      jsondecode(aws_ecs_task_definition.host["promote"].container_definitions)[2].dependsOn == [{ containerName = "fetch", condition = "SUCCESS" }] &&
      jsondecode(aws_ecs_task_definition.host["promote"].container_definitions)[1].dependsOn == [{ containerName = "init", condition = "SUCCESS" }]
    )
    error_message = "The steps run in order from the training image. Only the first runs as root and only the last is essential."
  }

  assert {
    condition = (
      jsondecode(aws_ecs_task_definition.host["promote"].container_definitions)[2].command[0] == "(cd /workspace/code && python -m scripts.training.aws_run \"$PHASE\" --run \"$RUN_ID\") && find /workspace /tmp -mindepth 1 -delete" &&
      jsondecode(aws_ecs_task_definition.host["promote"].container_definitions)[1].command[0] == "python -I /opt/hai/fetch_code.py"
    )
    error_message = "The last step runs the phase of the runner that the PHASE variable names: promote, research, canary or notify."
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
      jsondecode(aws_ecs_task_definition.host["promote"].container_definitions)[2].secrets == [
        { name = "MLFLOW_TRACKING_PASSWORD", valueFrom = "arn:aws:ssm:us-west-2:111111111111:parameter/example_project/mlflow_admin_password" }
      ] &&
      length(jsondecode(aws_ecs_task_definition.host["promote"].container_definitions)[0].secrets) == 0 &&
      length(jsondecode(aws_ecs_task_definition.host["promote"].container_definitions)[1].secrets) == 0
    )
    error_message = "Only the promote step holds the MLflow password, and no step holds a catalog credential."
  }

  assert {
    condition = (
      [for item in jsondecode(aws_ecs_task_definition.host["promote"].container_definitions)[2].environment : item.name if item.value == ""] == ["RUN_ID", "CODE_SHA256", "TRAINING_PARAMETERS", "MLFLOW_TRACKING_URI", "PHASE"] &&
      [for item in jsondecode(aws_ecs_task_definition.host["promote"].container_definitions)[1].environment : item.name if item.value == ""] == ["RUN_ID", "CODE_RUN_ID", "CODE_SHA256"] &&
      !anytrue([for item in jsondecode(aws_ecs_task_definition.host["promote"].container_definitions) : anytrue([for entry in item.environment : strcontains(entry.name, "POLARIS")])])
    )
    error_message = "The launch fills exactly the variables the table names for each container, and the task needs no catalog address."
  }
}

run "promote_secret_flow_is_clean" {
  command = plan

  variables {
    train_enabled   = true
    promote_enabled = true
  }

  assert {
    condition     = output.host_secret_flow.violations == [] && output.host_secret_flow.mutation_mismatches == [] && contains(output.host_secret_flow.containers_checked, "host_promote/promote")
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

run "promote_containers_follow_the_job_catalog" {
  command = plan

  variables {
    train_enabled   = true
    promote_enabled = true
  }

  assert {
    condition = (
      toset([for container in jsondecode(output.host_container_definitions["promote"]) : container.name if container.name != "init"]) == toset(keys(jsondecode(file("${path.module}/../../config/orchestration/ecs_broker_jobs.json")).jobs.promote.containers)) &&
      alltrue([for container in jsondecode(output.host_container_definitions["promote"]) : (
        container.name == "init" || toset([for variable in container.environment : variable.name if variable.value == ""]) == toset(jsondecode(file("${path.module}/../../config/orchestration/ecs_broker_jobs.json")).jobs.promote.containers[container.name])
      )]) &&
      contains([for container in jsondecode(output.host_container_definitions["promote"]) : container.name], jsondecode(file("${path.module}/../../config/orchestration/ecs_broker_jobs.json")).jobs.promote.log_container)
    )
    error_message = "The container names, their empty variables and the log container equal the job catalog, so every override the launch sends names a container of the task and fills what it needs."
  }
}

run "promote_phase_is_the_launch_variable_and_the_topic_is_fixed" {
  command = plan

  variables {
    train_enabled    = true
    promote_enabled  = true
    alerts_topic_arn = "arn:aws:sns:us-west-2:111111111111:example_project_alerts_dev"
  }

  assert {
    condition = (
      [for item in jsondecode(aws_ecs_task_definition.host["promote"].container_definitions)[2].environment : item.value if item.name == "ALERTS_TOPIC_ARN"] == ["arn:aws:sns:us-west-2:111111111111:example_project_alerts_dev"] &&
      length([for container in jsondecode(aws_ecs_task_definition.host["promote"].container_definitions) : container.name if anytrue([for item in container.environment : item.name == "ALERTS_TOPIC_ARN"])]) == 1 &&
      length([for container in jsondecode(aws_ecs_task_definition.host["promote"].container_definitions) : container.name if anytrue([for item in container.environment : item.name == "PHASE"])]) == 1
    )
    error_message = "Only the last step holds the alerts topic, and its value is fixed in the definition, not filled at launch. Only the last step is told a phase."
  }
}

run "promote_without_a_topic_has_no_topic_variable" {
  command = plan

  variables {
    train_enabled   = true
    promote_enabled = true
  }

  assert {
    condition     = !anytrue([for container in jsondecode(aws_ecs_task_definition.host["promote"].container_definitions) : anytrue([for item in container.environment : item.name == "ALERTS_TOPIC_ARN"])])
    error_message = "A definition planned without a topic names none, so the notify phase refuses by name instead of publishing to a default."
  }
}

run "alerts_topic_must_be_an_sns_topic_arn" {
  command = plan

  variables {
    train_enabled    = true
    promote_enabled  = true
    alerts_topic_arn = "not an arn"
  }

  expect_failures = [var.alerts_topic_arn]
}

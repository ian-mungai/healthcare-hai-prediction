mock_provider "aws" {
  override_during = plan

  mock_data "aws_ssm_parameter" {
    defaults = { insecure_value = "ami-0123456789abcdef0" }
  }

  mock_data "aws_ec2_instance_type_offerings" {
    defaults = { locations = ["eu-west-1a", "eu-west-1b"] }
  }

  mock_resource "aws_sns_topic" {
    defaults = { arn = "arn:aws:sns:eu-west-1:111111111111:example_project_budget_alerts_dev" }
  }

  mock_resource "aws_autoscaling_group" {
    defaults = { arn = "arn:aws:autoscaling:eu-west-1:111111111111:autoScalingGroup:00000000-0000-0000-0000-000000000000:autoScalingGroupName/example_project_host_dev" }
  }
}

override_resource {
  target          = aws_launch_template.host
  override_during = plan
  values          = { id = "lt-0123456789abcdef0", latest_version = 1 }
}

override_resource {
  target          = aws_iam_instance_profile.ecs_host
  override_during = plan
  values          = { arn = "arn:aws:iam::111111111111:instance-profile/example_project_ecs_host_dev" }
}

variables {
  aws_profile                          = "example_project_dev"
  environment                          = "dev"
  admin_profile                        = "example_admin"
  aws_region                           = "eu-west-1"
  expected_account_id                  = "111111111111"
  data_bucket_name                     = "example-project-ci-bucket"
  project_name                         = "example_project"
  owner_ip_cidrs                       = []
  openmetadata_admin_password_name     = "/example_project/openmetadata_admin_password"
  openmetadata_db_password_name        = "/example_project/openmetadata_db_password"
  mlflow_metadata_reader_password_name = "/example_project/mlflow_metadata_reader_password"
  openmetadata_bot_token_names = {
    lineage   = "/example_project/openmetadata_bot_lineage"
    ingestion = "/example_project/openmetadata_bot_ingestion"
  }
}

# The persistent catalog on the container host: the bootstrap task role, the execution grants for its parameters, one API port for the lakehouse job group and the
# Session Manager document for the UI. Every case reads the plan only. The default plan holds none of it.
run "default_prepares_nothing_persistent" {
  command = plan

  assert {
    condition = (
      !var.openmetadata_persistent_prepared
      && length(aws_iam_role.openmetadata_host_task) == 0
      && length(aws_iam_role_policy.task_execution_openmetadata) == 0
      && length(aws_ssm_document.openmetadata_ui_forward) == 0
    )
    error_message = "With the flag off the stack holds no persistent catalog role, grant or document."
  }

  assert {
    condition     = length(output.openmetadata_persistent_roles) == 0
    error_message = "With the flag off the output names no role."
  }
}

run "prepared_role_writes_exactly_three_token_parameters" {
  command = plan

  variables {
    openmetadata_persistent_prepared = true
  }

  assert {
    condition = (
      aws_iam_role.openmetadata_host_task[0].name == "example_project_openmetadata_host_task_dev"
      && can(regex("ecs-tasks.amazonaws.com", aws_iam_role.openmetadata_host_task[0].assume_role_policy))
      && contains(keys(output.openmetadata_persistent_roles), "openmetadata_host_task")
    )
    error_message = "The bootstrap role is named for the project and trusts ECS tasks only."
  }

  assert {
    condition = (
      toset(flatten([for statement in jsondecode(aws_iam_role_policy.openmetadata_host_task[0].policy).Statement : flatten([statement.Resource])])) == toset([
        "arn:aws:ssm:eu-west-1:111111111111:parameter/example_project/openmetadata_bot_lineage",
        "arn:aws:ssm:eu-west-1:111111111111:parameter/example_project/openmetadata_bot_ingestion",
      ])
      && toset(flatten([for statement in jsondecode(aws_iam_role_policy.openmetadata_host_task[0].policy).Statement : flatten([statement.Action])])) == toset(["ssm:GetParameter", "ssm:PutParameter"])
    )
    error_message = "The role may read and write exactly the three bot token parameters. It holds no other action and no wildcard resource."
  }
}

run "execution_role_injects_exactly_the_two_passwords" {
  command = plan

  variables {
    openmetadata_persistent_prepared = true
  }

  assert {
    condition = (
      toset(flatten([for statement in jsondecode(aws_iam_role_policy.task_execution_openmetadata[0].policy).Statement : flatten([statement.Resource])])) == toset([
        "arn:aws:ssm:eu-west-1:111111111111:parameter/example_project/openmetadata_admin_password",
        "arn:aws:ssm:eu-west-1:111111111111:parameter/example_project/openmetadata_db_password",
      ])
      && alltrue([for statement in jsondecode(aws_iam_role_policy.task_execution_openmetadata[0].policy).Statement : statement.Action == "ssm:GetParameters"])
    )
    error_message = "The host execution role injects exactly the administrator and database passwords, and no bot token."
  }
}

run "ui_document_forwards_port_8585_and_nothing_else" {
  command = plan

  variables {
    openmetadata_persistent_prepared = true
  }

  assert {
    condition = (
      jsondecode(aws_ssm_document.openmetadata_ui_forward[0].content).sessionType == "Port"
      && jsondecode(aws_ssm_document.openmetadata_ui_forward[0].content).properties.portNumber == "8585"
      && !contains(keys(jsondecode(aws_ssm_document.openmetadata_ui_forward[0].content).parameters), "portNumber")
      && aws_ssm_document.openmetadata_ui_forward[0].document_type == "Session"
      && aws_ssm_document.openmetadata_ui_forward[0].name == "example_project_openmetadata_forward_dev"
    )
    error_message = "The UI document is a Session Manager port forward whose destination port is fixed at 8585."
  }
}

run "persistent_flag_does_not_need_the_finite_catalog_flag" {
  command = plan

  variables {
    openmetadata_persistent_prepared = true
    openmetadata_prepared            = false
  }

  assert {
    condition     = length(aws_iam_role.openmetadata_task) == 0 && length(aws_iam_role.openmetadata_host_task) == 1
    error_message = "The persistent catalog stands alone: the finite catalog role stays unprepared."
  }
}

run "token_name_equal_to_the_admin_password_is_refused" {
  command = plan

  variables {
    openmetadata_persistent_prepared = true
    openmetadata_bot_token_names = {
      lineage   = "/example_project/openmetadata_admin_password"
      ingestion = "/example_project/openmetadata_bot_ingestion"
    }
  }

  expect_failures = [var.openmetadata_bot_token_names]
}

run "dashed_token_name_is_refused" {
  command = plan

  variables {
    openmetadata_persistent_prepared = true
    openmetadata_bot_token_names = {
      lineage   = "/example_project/openmetadata-bot-lineage"
      ingestion = "/example_project/openmetadata_bot_ingestion"
    }
  }

  expect_failures = [var.openmetadata_bot_token_names]
}

run "job_execution_role_injects_only_the_ingestion_token" {
  command = plan

  variables {
    openmetadata_persistent_prepared = true
  }

  assert {
    condition = (
      aws_iam_role_policy.job_execution_openmetadata_ingestion_token[0].name == "openmetadata_ingestion_token"
      && jsondecode(aws_iam_role_policy.job_execution_openmetadata_ingestion_token[0].policy).Statement[0].Action == "ssm:GetParameters"
      && jsondecode(aws_iam_role_policy.job_execution_openmetadata_ingestion_token[0].policy).Statement[0].Resource == ["arn:aws:ssm:eu-west-1:111111111111:parameter/example_project/openmetadata_bot_ingestion"]
    )
    error_message = "The job execution role may inject exactly the ingestion bot token for the ingestion steps, and neither the lineage token nor any other."
  }
}

# The metadata job's MLflow source reads the registry as the read-only metadata_reader. Its password parameter is injected through the same shared job execution role.
run "job_execution_role_injects_only_the_registry_reader_password" {
  command = plan

  variables {
    openmetadata_persistent_prepared = true
  }

  assert {
    condition = (
      aws_iam_role_policy.job_execution_mlflow_metadata_reader[0].name == "mlflow_metadata_reader_password"
      && jsondecode(aws_iam_role_policy.job_execution_mlflow_metadata_reader[0].policy).Statement[0].Action == "ssm:GetParameters"
      && jsondecode(aws_iam_role_policy.job_execution_mlflow_metadata_reader[0].policy).Statement[0].Resource == ["arn:aws:ssm:eu-west-1:111111111111:parameter/example_project/mlflow_metadata_reader_password"]
      && length(jsondecode(aws_iam_role_policy.job_execution_mlflow_metadata_reader[0].policy).Statement) == 1
    )
    error_message = "The job execution role may inject exactly the registry reader password, and neither the MLflow administrator password nor a bot token."
  }
}

run "registry_reader_grant_is_absent_by_default" {
  command = plan

  assert {
    condition     = length(aws_iam_role_policy.job_execution_mlflow_metadata_reader) == 0
    error_message = "With the flag off the job execution role gains no registry reader grant."
  }
}

run "registry_reader_password_name_is_its_own_parameter" {
  command = plan

  variables {
    openmetadata_persistent_prepared     = true
    mlflow_metadata_reader_password_name = "/example_project/openmetadata_admin_password"
  }

  expect_failures = [var.mlflow_metadata_reader_password_name]
}

run "job_execution_grant_is_absent_by_default" {
  command = plan

  assert {
    condition     = length(aws_iam_role_policy.job_execution_openmetadata_ingestion_token) == 0
    error_message = "With the flag off the job execution role gains no catalog token grant."
  }
}

# The Airflow scheduler injects the lineage bot token for the HTTP lineage transport through the shared task execution role.
run "airflow_lineage_flag_off_adds_no_grant_and_keeps_the_password_grant" {
  command = plan

  variables {
    openmetadata_persistent_prepared = true
  }

  assert {
    condition     = !var.airflow_lineage_prepared && length(aws_iam_role_policy.task_execution_airflow_lineage) == 0
    error_message = "With airflow_lineage_prepared false the execution role gains no lineage token grant."
  }

  assert {
    condition = (
      length(jsondecode(aws_iam_role_policy.task_execution_openmetadata[0].policy).Statement) == 1
      && !strcontains(aws_iam_role_policy.task_execution_openmetadata[0].policy, "openmetadata_bot")
    )
    error_message = "The password grant of the execution role stays as it was and holds no bot token."
  }
}

run "airflow_lineage_flag_on_injects_exactly_the_lineage_token" {
  command = plan

  variables {
    openmetadata_persistent_prepared = true
    airflow_lineage_prepared         = true
  }

  assert {
    condition = (
      aws_iam_role_policy.task_execution_airflow_lineage[0].name == "task_execution_airflow_lineage"
      && jsondecode(aws_iam_role_policy.task_execution_airflow_lineage[0].policy) == {
        Version = "2012-10-17"
        Statement = [{
          Sid      = "InjectLineageBotToken"
          Effect   = "Allow"
          Action   = "ssm:GetParameters"
          Resource = ["arn:aws:ssm:eu-west-1:111111111111:parameter/example_project/openmetadata_bot_lineage"]
        }]
      }
    )
    error_message = "The execution role may inject exactly the lineage bot token: one allow statement, ssm:GetParameters, one parameter ARN and no KMS or wildcard."
  }

  assert {
    condition = (
      toset(flatten([for statement in jsondecode(aws_iam_role_policy.task_execution_openmetadata[0].policy).Statement : flatten([statement.Resource])])) == toset([
        "arn:aws:ssm:eu-west-1:111111111111:parameter/example_project/openmetadata_admin_password",
        "arn:aws:ssm:eu-west-1:111111111111:parameter/example_project/openmetadata_db_password",
      ])
    )
    error_message = "The existing password grant stays byte equal when the lineage flag is on."
  }
}

run "airflow_lineage_flag_follows_a_renamed_token_parameter" {
  command = plan

  variables {
    openmetadata_persistent_prepared = true
    airflow_lineage_prepared         = true
    openmetadata_bot_token_names = {
      lineage   = "/example_project/renamed_lineage"
      ingestion = "/example_project/openmetadata_bot_ingestion"
    }
  }

  assert {
    condition     = jsondecode(aws_iam_role_policy.task_execution_airflow_lineage[0].policy).Statement[0].Resource == ["arn:aws:ssm:eu-west-1:111111111111:parameter/example_project/renamed_lineage"]
    error_message = "The grant names the configured lineage parameter."
  }
}

run "airflow_lineage_flag_without_the_persistent_catalog_is_refused" {
  command = plan

  variables {
    airflow_lineage_prepared = true
  }

  expect_failures = [var.airflow_lineage_prepared]
}

run "a_third_caller_is_refused" {
  command = plan

  variables {
    openmetadata_bot_token_names = {
      lineage    = "/example_project/openmetadata_bot_lineage"
      ingestion  = "/example_project/openmetadata_bot_ingestion"
      comparison = "/example_project/openmetadata_bot_comparison"
    }
  }

  expect_failures = [var.openmetadata_bot_token_names]
}

# The layer jobs (silver, gold, features and the model sets) inject the lineage bot token through the shared job execution role, for the lineage events they send
# from their pin and upload containers (orchestration/job_lineage.py).
run "job_lineage_flag_off_adds_no_grant_and_keeps_the_ingestion_grant" {
  command = plan

  variables {
    openmetadata_persistent_prepared = true
  }

  assert {
    condition     = !var.job_lineage_prepared && length(aws_iam_role_policy.job_execution_lineage_token) == 0
    error_message = "With job_lineage_prepared false the job execution role gains no lineage token grant."
  }

  assert {
    condition     = !strcontains(aws_iam_role_policy.job_execution_openmetadata_ingestion_token[0].policy, "openmetadata_bot_lineage")
    error_message = "The ingestion grant holds no lineage token."
  }
}

run "job_lineage_flag_on_injects_exactly_the_lineage_token" {
  command = plan

  variables {
    openmetadata_persistent_prepared = true
    job_lineage_prepared             = true
  }

  assert {
    condition = (
      aws_iam_role_policy.job_execution_lineage_token[0].name == "openmetadata_lineage_token"
      && jsondecode(aws_iam_role_policy.job_execution_lineage_token[0].policy) == {
        Version = "2012-10-17"
        Statement = [{
          Sid      = "InjectLineageBotToken"
          Effect   = "Allow"
          Action   = "ssm:GetParameters"
          Resource = ["arn:aws:ssm:eu-west-1:111111111111:parameter/example_project/openmetadata_bot_lineage"]
        }]
      }
    )
    error_message = "The job execution role may inject exactly the lineage bot token: one allow statement, ssm:GetParameters, one parameter ARN and no KMS or wildcard."
  }

  assert {
    condition     = jsondecode(aws_iam_role_policy.job_execution_openmetadata_ingestion_token[0].policy).Statement[0].Resource == ["arn:aws:ssm:eu-west-1:111111111111:parameter/example_project/openmetadata_bot_ingestion"]
    error_message = "The ingestion grant of the job execution role stays byte equal when the lineage flag is on."
  }

  assert {
    condition     = length(aws_iam_role_policy.task_execution_airflow_lineage) == 0
    error_message = "The scheduler's lineage grant is its own flag and stays off here."
  }
}

run "job_lineage_flag_follows_a_renamed_token_parameter" {
  command = plan

  variables {
    openmetadata_persistent_prepared = true
    job_lineage_prepared             = true
    openmetadata_bot_token_names = {
      lineage    = "/example_project/renamed_lineage"
      ingestion  = "/example_project/openmetadata_bot_ingestion"
      comparison = "/example_project/openmetadata_bot_comparison"
    }
  }

  assert {
    condition     = jsondecode(aws_iam_role_policy.job_execution_lineage_token[0].policy).Statement[0].Resource == ["arn:aws:ssm:eu-west-1:111111111111:parameter/example_project/renamed_lineage"]
    error_message = "The grant names the configured lineage parameter."
  }
}

run "job_lineage_flag_without_the_persistent_catalog_is_refused" {
  command = plan

  variables {
    job_lineage_prepared = true
  }

  expect_failures = [var.job_lineage_prepared]
}

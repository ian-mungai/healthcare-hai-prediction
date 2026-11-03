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
  aws_profile                      = "example_project_dev"
  environment                      = "dev"
  admin_profile                    = "example_admin"
  aws_region                       = "eu-west-1"
  expected_account_id              = "111111111111"
  data_bucket_name                 = "example-project-ci-bucket"
  project_name                     = "example_project"
  owner_ip_cidrs                   = []
  openmetadata_admin_password_name = "/example_project/openmetadata_admin_password"
  openmetadata_db_password_name    = "/example_project/openmetadata_db_password"
  openmetadata_bot_token_names = {
    lineage    = "/example_project/openmetadata_bot_lineage"
    ingestion  = "/example_project/openmetadata_bot_ingestion"
    comparison = "/example_project/openmetadata_bot_comparison"
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
      && length(aws_vpc_security_group_ingress_rule.host_from_openmetadata_api) == 0
      && length(aws_vpc_security_group_egress_rule.job_to_openmetadata_api) == 0
    )
    error_message = "With the flag off the stack holds no persistent catalog role, grant, document or port rule."
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
        "arn:aws:ssm:eu-west-1:111111111111:parameter/example_project/openmetadata_bot_comparison",
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

run "one_api_port_between_the_lakehouse_jobs_and_the_host" {
  command = plan

  variables {
    openmetadata_persistent_prepared = true
  }

  assert {
    condition = (
      aws_vpc_security_group_ingress_rule.host_from_openmetadata_api[0].from_port == 8585
      && aws_vpc_security_group_ingress_rule.host_from_openmetadata_api[0].to_port == 8585
      && aws_vpc_security_group_ingress_rule.host_from_openmetadata_api[0].ip_protocol == "tcp"
      && aws_vpc_security_group_egress_rule.job_to_openmetadata_api[0].from_port == 8585
      && aws_vpc_security_group_egress_rule.job_to_openmetadata_api[0].to_port == 8585
      && aws_vpc_security_group_ingress_rule.host_from_openmetadata_api[0].cidr_ipv4 == null
      && aws_vpc_security_group_egress_rule.job_to_openmetadata_api[0].cidr_ipv4 == null
    )
    error_message = "Only the lakehouse job group may reach host port 8585, by security group reference, and no CIDR rule opens it."
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
      lineage    = "/example_project/openmetadata_admin_password"
      ingestion  = "/example_project/openmetadata_bot_ingestion"
      comparison = "/example_project/openmetadata_bot_comparison"
    }
  }

  expect_failures = [var.openmetadata_bot_token_names]
}

run "dashed_token_name_is_refused" {
  command = plan

  variables {
    openmetadata_persistent_prepared = true
    openmetadata_bot_token_names = {
      lineage    = "/example_project/openmetadata-bot-lineage"
      ingestion  = "/example_project/openmetadata_bot_ingestion"
      comparison = "/example_project/openmetadata_bot_comparison"
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
    error_message = "The job execution role may inject exactly the ingestion bot token for the Fargate ingestion steps, and neither the lineage nor the comparison token."
  }
}

run "job_execution_grant_is_absent_by_default" {
  command = plan

  assert {
    condition     = length(aws_iam_role_policy.job_execution_openmetadata_ingestion_token) == 0
    error_message = "With the flag off the job execution role gains no catalog token grant."
  }
}

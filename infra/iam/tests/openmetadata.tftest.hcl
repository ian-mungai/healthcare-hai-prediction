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
}

run "default_prepares_no_catalog_role" {
  command = plan

  assert {
    condition     = !var.openmetadata_prepared && !var.openmetadata_marquez_reachable
    error_message = "Both catalog switches must default to false."
  }

  assert {
    condition     = length(aws_ssm_document.catalog_forward) == 0
    error_message = "The default plan must hold no catalog forward document, so the grant for it works for nothing."
  }

  assert {
    condition     = !contains(keys(output.job_roles), "openmetadata_task") && length(output.openmetadata_roles) == 0
    error_message = "The default plan must hold no catalog role."
  }

  assert {
    condition     = length(aws_vpc_security_group_ingress_rule.openmetadata_ui) == 0 && length(aws_vpc_security_group_egress_rule.openmetadata_marquez) == 0
    error_message = "The default plan must open no catalog port."
  }
}

run "prepared_role_reads_catalog_metadata_only" {
  command = plan

  variables {
    openmetadata_prepared = true
  }

  assert {
    condition = (
      aws_iam_role.openmetadata_task[0].name == "example_project_job_openmetadata_task_dev" &&
      can(regex("ecs-tasks.amazonaws.com", aws_iam_role.openmetadata_task[0].assume_role_policy)) &&
      contains(keys(output.openmetadata_roles), "openmetadata_task")
    )
    error_message = "The catalog role is named for the project and trusts ECS tasks only."
  }

  assert {
    condition = alltrue([
      for statement in jsondecode(aws_iam_role_policy.openmetadata_task["catalog_read"].policy).Statement :
      statement.Effect == "Deny" || alltrue([for action in flatten([statement.Action]) : contains(["s3:GetObject", "s3:GetObjectVersion", "s3:ListBucket"], action)])
    ])
    error_message = "The catalog role may only get and list objects. It holds no write, stop, run or parameter action."
  }

  assert {
    condition = alltrue([
      for statement in jsondecode(aws_iam_role_policy.openmetadata_task["catalog_read"].policy).Statement :
      statement.Effect == "Deny" || alltrue([for resource in flatten([statement.Resource]) :
        endswith(resource, "/metadata/*") || endswith(resource, ":::${var.data_bucket_name}") || endswith(resource, "/metadata/dbt/*")
      ])
    ])
    error_message = "The catalog role may read Iceberg metadata files and the dbt artifacts. It may not read a data file."
  }

  assert {
    condition = anytrue([
      for statement in jsondecode(aws_iam_role_policy.openmetadata_task["catalog_read"].policy).Statement :
      statement.Effect == "Deny" && contains(statement.Action, "s3:PutObject") && contains(statement.Action, "s3:DeleteObject")
    ])
    error_message = "The catalog role carries an explicit deny of writes and deletes."
  }

  assert {
    condition = (
      can(regex("openmetadata_server", aws_iam_role_policy.openmetadata_images[0].policy)) &&
      can(regex("openmetadata_ingestion", aws_iam_role_policy.openmetadata_images[0].policy)) &&
      can(regex("openmetadata_search", aws_iam_role_policy.openmetadata_images[0].policy)) &&
      can(regex("openmetadata_database", aws_iam_role_policy.openmetadata_images[0].policy)) &&
      aws_iam_role_policy.openmetadata_images[0].role == aws_iam_role.job["execution"].name
    )
    error_message = "The execution role may pull exactly the four catalog image repositories."
  }

  assert {
    condition = (
      can(regex("openmetadata_admin_password", aws_iam_role_policy.openmetadata_parameter[0].policy)) &&
      !can(regex("\\*", join("", [for s in jsondecode(aws_iam_role_policy.openmetadata_parameter[0].policy).Statement : s.Resource])))
    )
    error_message = "The execution role may inject exactly the one admin password parameter."
  }

  assert {
    condition = (
      aws_vpc_security_group_ingress_rule.openmetadata_ui[0].from_port == 8585 &&
      aws_vpc_security_group_ingress_rule.openmetadata_ui[0].to_port == 8585 &&
      aws_vpc_security_group_ingress_rule.openmetadata_ui[0].description == "Catalog UI from the container host only" &&
      length(aws_vpc_security_group_egress_rule.openmetadata_marquez) == 0
    )
    error_message = "Only the container host may reach the catalog UI port, and the Marquez port stays closed until asked for."
  }
}

run "prepared_forward_document_fixes_port_and_destination" {
  command = plan

  variables {
    openmetadata_prepared = true
  }

  assert {
    condition = (
      aws_ssm_document.catalog_forward[0].name == "example_project_catalog_forward_dev" &&
      aws_ssm_document.catalog_forward[0].document_type == "Session" &&
      jsondecode(aws_ssm_document.catalog_forward[0].content).sessionType == "Port" &&
      jsondecode(aws_ssm_document.catalog_forward[0].content).properties.portNumber == "8585" &&
      !contains(keys(jsondecode(aws_ssm_document.catalog_forward[0].content).parameters), "portNumber")
    )
    error_message = "The catalog forward document fixes the destination port at 8585 and takes no port from the caller."
  }

  assert {
    condition     = can(regex("^\\^[0-9]+\\\\.[0-9]+", jsondecode(aws_ssm_document.catalog_forward[0].content).parameters.host.allowedPattern))
    error_message = "The destination host is limited by a pattern that starts with the first two octets of the platform network."
  }
}

run "marquez_port_is_a_second_switch" {
  command = plan

  variables {
    openmetadata_prepared          = true
    openmetadata_marquez_reachable = true
  }

  assert {
    condition = (
      aws_vpc_security_group_egress_rule.openmetadata_marquez[0].from_port == 5000 &&
      aws_vpc_security_group_egress_rule.openmetadata_marquez[0].to_port == 5000 &&
      aws_vpc_security_group_egress_rule.openmetadata_marquez[0].description == "Lakehouse jobs to Marquez on the host" &&
      aws_vpc_security_group_ingress_rule.host_from_openmetadata_marquez[0].from_port == 5000
    )
    error_message = "The lakehouse jobs reach only port 5000 on the host, and the host admits only that."
  }
}

run "marquez_switch_needs_the_role" {
  command = plan

  variables {
    openmetadata_marquez_reachable = true
  }

  expect_failures = [var.openmetadata_marquez_reachable]
}

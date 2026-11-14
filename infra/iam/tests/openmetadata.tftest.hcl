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
      statement.Effect == "Deny" || alltrue([for action in flatten([statement.Action]) : contains(["s3:GetObject", "s3:ListBucket"], action)])
    ])
    error_message = "The catalog role may only get and list objects, with no version read. It holds no write, stop, run or parameter action."
  }

  assert {
    condition = alltrue([
      for statement in jsondecode(aws_iam_role_policy.openmetadata_task["catalog_read"].policy).Statement :
      statement.Effect == "Deny" || alltrue([for resource in flatten([statement.Resource]) :
        contains([
          "arn:aws:s3:::${var.data_bucket_name}",
          "arn:aws:s3:::${var.data_bucket_name}/metadata/dbt/*",
          "arn:aws:s3:::${var.data_bucket_name}/lakehouse/layer_runs/features/*/gold_pins.json",
          "arn:aws:s3:::${var.data_bucket_name}/lakehouse/layer_runs/gold/*/silver_pins.json",
          "arn:aws:s3:::${var.data_bucket_name}/lakehouse/bronze_runs/*/check.json",
          "arn:aws:s3:::${var.data_bucket_name}/${jsondecode(file("${path.module}/../../config/orchestration/ecs_launch.json")).record_prefix}*/silver/claim.json",
          "arn:aws:s3:::${var.data_bucket_name}/raw/ingestion_units/*",
        ], resource)
      ])
    ])
    error_message = "The catalog role reads the dbt artifacts and four named records and the unit completion records only: the two pin files that record a chain, the silver launch claim that names its bronze run, the bronze run record and the completion records of the deployed capture root that name the raw files and their receipts. The Glue connector reads the mirror registrations, so no Iceberg metadata file or data file is readable."
  }

  assert {
    condition = (
      toset(flatten([for statement in jsondecode(aws_iam_role_policy.openmetadata_task["catalog_read"].policy).Statement : statement.Resource if statement.Effect == "Allow" && contains(flatten([statement.Action]), "s3:GetObject")])) == toset([
        "arn:aws:s3:::${var.data_bucket_name}/metadata/dbt/*",
        "arn:aws:s3:::${var.data_bucket_name}/lakehouse/layer_runs/features/*/gold_pins.json",
        "arn:aws:s3:::${var.data_bucket_name}/lakehouse/layer_runs/gold/*/silver_pins.json",
        "arn:aws:s3:::${var.data_bucket_name}/lakehouse/bronze_runs/*/check.json",
        "arn:aws:s3:::${var.data_bucket_name}/${jsondecode(file("${path.module}/../../config/orchestration/ecs_launch.json")).record_prefix}*/silver/claim.json",
        "arn:aws:s3:::${var.data_bucket_name}/raw/ingestion_units/*",
      ]) &&
      toset(flatten([for statement in jsondecode(aws_iam_role_policy.openmetadata_task["catalog_read"].policy).Statement : statement.Condition.StringLike["s3:prefix"] if contains(flatten([statement.Action]), "s3:ListBucket")])) ==
      toset(["metadata/dbt/", "metadata/dbt/*", "raw/ingestion_units/", "raw/ingestion_units/*"])
    )
    error_message = "The catalog role lists only the dbt artifact prefix and the unit completion record folder of the deployed capture root (raw/), nothing at the bucket root and nothing under the lakehouse, and reads under the lakehouse only the four named records. The unit completion records of raw/ are the one read of the raw area, and no raw data object is readable."
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

run "marquez_switch_needs_the_role" {
  command = plan

  variables {
    openmetadata_marquez_reachable = true
  }

  expect_failures = [var.openmetadata_marquez_reachable]
}

run "marquez_switch_opens_no_port" {
  command = plan

  variables {
    openmetadata_prepared          = true
    openmetadata_marquez_reachable = true
  }

  expect_failures = [check.marquez_port_is_retired]
}

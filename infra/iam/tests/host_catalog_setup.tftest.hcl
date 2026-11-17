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
  aws_profile         = "example_project_dev"
  environment         = "dev"
  admin_profile       = "example_admin"
  aws_region          = "eu-west-1"
  expected_account_id = "111111111111"
  data_bucket_name    = "example-project-ci-bucket"
  project_name        = "example_project"
  owner_ip_cidrs      = []
}

# The catalog setup one-shot on the container host: one task role for the AWS Polaris pair parameters, its own S3 records, the services apply lock and the task listing.
# The evaluator cases with planted defects are scripts/infrastructure/catalog_setup_policy_cases.py; every case here reads the plan only.
run "default_prepares_no_catalog_setup_role" {
  command = plan

  assert {
    condition     = !var.host_catalog_setup_prepared && length(aws_iam_role.host_catalog_setup_task) == 0 && length(aws_iam_role_policy.host_catalog_setup_task) == 0
    error_message = "The catalog setup role exists only while host_catalog_setup_prepared is true."
  }
}

run "the_catalog_setup_role_is_assumed_only_by_ecs_tasks_of_the_project" {
  command = plan

  variables {
    host_catalog_setup_prepared = true
  }

  assert {
    condition = (
      length(aws_iam_role.host_catalog_setup_task) == 1
      && aws_iam_role.host_catalog_setup_task[0].name == "example_project_host_catalog_setup_task_dev"
      && jsondecode(aws_iam_role.host_catalog_setup_task[0].assume_role_policy).Statement[0].Principal.Service == "ecs-tasks.amazonaws.com"
      && jsondecode(aws_iam_role.host_catalog_setup_task[0].assume_role_policy).Statement[0].Condition.StringEquals["aws:SourceAccount"] == "111111111111"
      && output.host_catalog_setup_role.name == "example_project_host_catalog_setup_task_dev"
    )
    error_message = "The role is the catalog setup task role, assumed only by ECS tasks of this account, and the services root takes it from the host_catalog_setup_role output."
  }
}

run "the_catalog_setup_role_reaches_exactly_the_fourteen_pairs_and_nothing_of_the_root_secret" {
  command = plan

  variables {
    host_catalog_setup_prepared = true
  }

  assert {
    condition = (
      length(jsondecode(aws_iam_role_policy.host_catalog_setup_task[0].policy).Statement) == 10
      && length([for statement in jsondecode(aws_iam_role_policy.host_catalog_setup_task[0].policy).Statement : statement if statement.Sid == "SetUpAwsPolarisPairs"]) == 1
      && length(one([for statement in jsondecode(aws_iam_role_policy.host_catalog_setup_task[0].policy).Statement : statement.Resource if statement.Sid == "SetUpAwsPolarisPairs"])) == 14
      && one([for statement in jsondecode(aws_iam_role_policy.host_catalog_setup_task[0].policy).Statement : statement.Action if statement.Sid == "SetUpAwsPolarisPairs"]) == ["ssm:GetParameter", "ssm:PutParameter"]
    )
    error_message = "The pair grant is a read and a put of the fourteen pair parameters, with no delete, no listing and no batch read."
  }

  assert {
    condition = toset(one([for statement in jsondecode(aws_iam_role_policy.host_catalog_setup_task[0].policy).Statement : statement.Resource if statement.Sid == "SetUpAwsPolarisPairs"])) == toset([
      for name in [
        var.aws_polaris_reader_client_id_name, var.aws_polaris_reader_client_secret_name,
        var.aws_polaris_e2e_client_id_name, var.aws_polaris_e2e_client_secret_name,
        var.aws_polaris_bronze_client_id_name, var.aws_polaris_bronze_client_secret_name,
        var.aws_polaris_silver_client_id_name, var.aws_polaris_silver_client_secret_name,
        var.aws_polaris_gold_client_id_name, var.aws_polaris_gold_client_secret_name,
        var.aws_polaris_features_client_id_name, var.aws_polaris_features_client_secret_name,
        var.aws_polaris_serving_client_id_name, var.aws_polaris_serving_client_secret_name,
      ] : "arn:aws:ssm:eu-west-1:111111111111:parameter${name}"
    ])
    error_message = "Every identity's client ID and client secret parameter is granted, the features pair included."
  }

  assert {
    condition     = !strcontains(aws_iam_role_policy.host_catalog_setup_task[0].policy, "polaris_root") && !strcontains(aws_iam_role_policy.host_catalog_setup_task[0].policy, "secretsmanager") && !strcontains(aws_iam_role_policy.host_catalog_setup_task[0].policy, "kms:")
    error_message = "The role holds no grant on the Polaris root secret, on Secrets Manager or on a key: the execution role injects the root secret."
  }
}

run "the_catalog_setup_role_writes_only_its_own_records_and_the_apply_lock" {
  command = plan

  variables {
    host_catalog_setup_prepared = true
  }

  assert {
    condition = (
      one([for statement in jsondecode(aws_iam_role_policy.host_catalog_setup_task[0].policy).Statement : statement.Resource if statement.Sid == "ReadAndWriteOwnRecords"]) == "arn:aws:s3:::example-project-ci-bucket/lakehouse/catalog_setup/*"
      && one([for statement in jsondecode(aws_iam_role_policy.host_catalog_setup_task[0].policy).Statement : statement.Resource if statement.Sid == "HoldHostLaunches"]) == "arn:aws:ssm:eu-west-1:111111111111:parameter${var.services_apply_lock_parameter_name}"
      && one([for statement in jsondecode(aws_iam_role_policy.host_catalog_setup_task[0].policy).Statement : statement.Condition.StringLike["s3:prefix"] if statement.Sid == "TellAbsentRecordsFromDeniedOnes"]) == ["lakehouse/catalog_setup/*"]
    )
    error_message = "The S3 grant is the setup's own prefix, the hold grant is the services apply lock parameter, and the listing is limited to the setup's prefix."
  }

  assert {
    condition = (
      one([for statement in jsondecode(aws_iam_role_policy.host_catalog_setup_task[0].policy).Statement : statement.Condition.StringNotEquals["s3:if-none-match"] if statement.Sid == "DenyApprovalCreateWithoutCondition"]) == "*"
      && one([for statement in jsondecode(aws_iam_role_policy.host_catalog_setup_task[0].policy).Statement : statement.Condition.Null["s3:if-none-match"] if statement.Sid == "DenyLockWriteWithoutCondition"]) == "true"
      && one([for statement in jsondecode(aws_iam_role_policy.host_catalog_setup_task[0].policy).Statement : statement.Condition.Null["s3:if-match"] if statement.Sid == "DenyLockWriteWithoutCondition"]) == "true"
      && one([for statement in jsondecode(aws_iam_role_policy.host_catalog_setup_task[0].policy).Statement : statement.Effect if statement.Sid == "DenyApprovalAndLockDelete"]) == "Deny"
      && length(one([for statement in jsondecode(aws_iam_role_policy.host_catalog_setup_task[0].policy).Statement : statement.Resource if statement.Sid == "DenyApprovalAndLockDelete"])) == 2
      && one([for statement in jsondecode(aws_iam_role_policy.host_catalog_setup_task[0].policy).Statement : statement.Condition.StringEqualsIgnoreCase["ssm:Overwrite"] if statement.Sid == "DenyHoldOverwrite"]) == "true"
    )
    error_message = "An approval write without the create-only header, a lock write with no condition header, an approval or lock delete and an overwrite of the apply lock are denied."
  }

  assert {
    condition = (
      one([for statement in jsondecode(aws_iam_role_policy.host_catalog_setup_task[0].policy).Statement : statement.Condition.ArnEquals["ecs:cluster"] if statement.Sid == "ListProjectTasks"]) == "arn:aws:ecs:eu-west-1:111111111111:cluster/example_project_cluster_dev"
      && one([for statement in jsondecode(aws_iam_role_policy.host_catalog_setup_task[0].policy).Statement : statement.Resource if statement.Sid == "DescribeProjectTasks"]) == "arn:aws:ecs:eu-west-1:111111111111:task/example_project_cluster_dev/*"
    )
    error_message = "The task listing is limited to the project cluster, and the task description to tasks of that cluster."
  }
}
